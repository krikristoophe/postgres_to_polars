//! Stream PostgreSQL query results directly into Polars DataFrames.
//!
//! This crate provides a `#[derive(IntoDataFrame)]` macro and a `.to_dataframe()` extension trait
//! that work with [sqlx](https://docs.rs/sqlx) streams to build DataFrames efficiently via streaming.
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlx::PgPool;
//! use postgres_to_polars::{IntoDataFrame, StreamToDataFrame};
//!
//! #[derive(sqlx::FromRow, IntoDataFrame)]
//! struct User {
//!     id: i32,
//!     name: Option<String>,
//! }
//!
//! # async fn example() -> Result<(), postgres_to_polars::Error> {
//! let pool = PgPool::connect("postgres://localhost/db").await.unwrap();
//!
//! let df = sqlx::query_as!(User, "SELECT id, name FROM users")
//!     .fetch(&pool)
//!     .to_dataframe(10_000) // pre-allocate for ~10K rows
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub use postgres_to_polars_derive::IntoDataFrame;

use futures::TryStreamExt;
use polars::chunked_array::builder::{ListBuilderTrait, ListStringChunkedBuilder};
use polars::prelude::{Column, DataFrame, IntoSeries, NamedFrom, PlSmallStr, Series};

/// Default capacity for builder Vecs when using [`StreamToDataFrame::to_dataframe_default`].
const DEFAULT_CAPACITY: usize = 1024;

/// Converts a `Vec<T>` into a Polars [`Column`].
///
/// Implemented for all supported scalar, chrono, and list types.
/// Used by the generated `build()` method in the `IntoDataFrame` derive macro.
pub trait VecToColumn {
    fn to_column(name: &str, data: Self) -> Column;
}

macro_rules! impl_vec_to_column {
    ($t:ty) => {
        impl VecToColumn for Vec<$t> {
            fn to_column(name: &str, data: Self) -> Column {
                Series::new(PlSmallStr::from(name), &data).into()
            }
        }
    };
}

impl_vec_to_column!(i32);
impl_vec_to_column!(i64);
impl_vec_to_column!(f32);
impl_vec_to_column!(f64);
impl_vec_to_column!(bool);
impl_vec_to_column!(String);
impl_vec_to_column!(Option<i32>);
impl_vec_to_column!(Option<i64>);
impl_vec_to_column!(Option<f32>);
impl_vec_to_column!(Option<f64>);
impl_vec_to_column!(Option<bool>);
impl_vec_to_column!(Option<String>);
impl_vec_to_column!(chrono::NaiveDate);
impl_vec_to_column!(chrono::NaiveDateTime);
impl_vec_to_column!(chrono::NaiveTime);
impl_vec_to_column!(Option<chrono::NaiveDate>);
impl_vec_to_column!(Option<chrono::NaiveDateTime>);
impl_vec_to_column!(Option<chrono::NaiveTime>);

impl VecToColumn for Vec<Option<Vec<String>>> {
    fn to_column(name: &str, data: Self) -> Column {
        let name = PlSmallStr::from(name);
        let values_cap = data.iter().map(|o| o.as_ref().map_or(0, |v| v.len())).sum();
        let mut builder = ListStringChunkedBuilder::new(name, data.len(), values_cap);
        for opt in &data {
            match opt {
                Some(v) => builder.append_values_iter(v.iter().map(|s| s.as_str())),
                None => builder.append_null(),
            }
        }
        builder.finish().into_series().into()
    }
}

impl VecToColumn for Vec<Vec<String>> {
    fn to_column(name: &str, data: Self) -> Column {
        let name = PlSmallStr::from(name);
        let values_cap = data.iter().map(|v| v.len()).sum();
        let mut builder = ListStringChunkedBuilder::new(name, data.len(), values_cap);
        for v in &data {
            builder.append_values_iter(v.iter().map(|s| s.as_str()));
        }
        builder.finish().into_series().into()
    }
}

/// Error type wrapping both sqlx and Polars errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("polars error: {0}")]
    Polars(#[from] polars::prelude::PolarsError),
}

/// Trait implemented by structs deriving `IntoDataFrame`.
///
/// Provides a factory method to create a columnar builder with a given capacity.
pub trait HasDataFrameBuilder: Sized {
    type Builder: DataFrameBuilder<Self> + Send;
    fn dataframe_builder(capacity: usize) -> Self::Builder;
}

/// Columnar builder that accumulates rows and produces a DataFrame.
pub trait DataFrameBuilder<T>: Sized {
    fn push(&mut self, row: T);
    fn build(self) -> Result<DataFrame, polars::prelude::PolarsError>;
}

/// Extension trait on sqlx streams to convert query results into a Polars DataFrame.
pub trait StreamToDataFrame: Sized {
    /// Consume the stream and build a DataFrame, pre-allocating for `capacity` rows.
    ///
    /// Use this when you know (or can estimate) the number of rows to avoid reallocations.
    fn to_dataframe<T: HasDataFrameBuilder<Builder: Send>>(
        self,
        capacity: usize,
    ) -> impl std::future::Future<Output = Result<DataFrame, Error>> + Send
    where
        Self: futures::Stream<Item = Result<T, sqlx::Error>> + Unpin + Send;

    /// Consume the stream and build a DataFrame with a default capacity of 1024 rows.
    fn to_dataframe_default<T: HasDataFrameBuilder<Builder: Send>>(
        self,
    ) -> impl std::future::Future<Output = Result<DataFrame, Error>> + Send
    where
        Self: futures::Stream<Item = Result<T, sqlx::Error>> + Unpin + Send;
}

impl<S> StreamToDataFrame for S
where
    S: Sized,
{
    async fn to_dataframe<T: HasDataFrameBuilder<Builder: Send>>(
        mut self,
        capacity: usize,
    ) -> Result<DataFrame, Error>
    where
        Self: futures::Stream<Item = Result<T, sqlx::Error>> + Unpin + Send,
    {
        let mut builder = T::dataframe_builder(capacity);
        while let Some(row) = self.try_next().await? {
            builder.push(row);
        }
        Ok(builder.build()?)
    }

    async fn to_dataframe_default<T: HasDataFrameBuilder<Builder: Send>>(
        self,
    ) -> Result<DataFrame, Error>
    where
        Self: futures::Stream<Item = Result<T, sqlx::Error>> + Unpin + Send,
    {
        self.to_dataframe(DEFAULT_CAPACITY).await
    }
}
