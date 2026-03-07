//! Proc-macro crate providing `#[derive(IntoDataFrame)]`.
//!
//! This is an implementation detail of the `postgres_to_polars` crate.
//! Use `postgres_to_polars::IntoDataFrame` instead of depending on this crate directly.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derive macro that generates a columnar builder for converting rows into a Polars DataFrame.
///
/// The struct must have named fields. Each field type must implement
/// `postgres_to_polars::VecToColumn` (all common Rust/sqlx types are supported).
///
/// # Generated code
///
/// For a struct `User { id: i32, name: Option<String> }`, this generates:
/// - `UserDataFrameBuilder` with `Vec<i32>` and `Vec<Option<String>>` fields
/// - `User::dataframe_builder(capacity)` constructor
/// - `UserDataFrameBuilder::push(row)` and `build()` methods
/// - Trait impls for `HasDataFrameBuilder` and `DataFrameBuilder`
#[proc_macro_derive(IntoDataFrame)]
pub fn derive_into_dataframe(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let builder_name = syn::Ident::new(
        &format!("{}DataFrameBuilder", struct_name),
        struct_name.span(),
    );

    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return syn::Error::new_spanned(
                    struct_name,
                    "IntoDataFrame can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                struct_name,
                "IntoDataFrame can only be derived for structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();

    let builder_fields = field_names
        .iter()
        .zip(field_types.iter())
        .map(|(name, ty)| {
            quote! { #name: Vec<#ty> }
        });

    let builder_init = field_names.iter().map(|name| {
        quote! { #name: Vec::with_capacity(capacity) }
    });

    let push_fields = field_names.iter().map(|name| {
        quote! { self.#name.push(row.#name); }
    });

    let first_field = &field_names[0];

    let column_exprs = field_names.iter().map(|name| {
        let name_str = name.to_string();
        quote! {
            postgres_to_polars::VecToColumn::to_column(#name_str, self.#name)
        }
    });

    let expanded = quote! {
        pub struct #builder_name {
            #(#builder_fields,)*
        }

        impl #struct_name {
            pub fn dataframe_builder(capacity: usize) -> #builder_name {
                #builder_name {
                    #(#builder_init,)*
                }
            }
        }

        impl #builder_name {
            pub fn push(&mut self, row: #struct_name) {
                #(#push_fields)*
            }

            pub fn build(self) -> Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
                let len = self.#first_field.len();
                polars::prelude::DataFrame::new(len, vec![
                    #(#column_exprs,)*
                ])
            }
        }

        impl postgres_to_polars::HasDataFrameBuilder for #struct_name {
            type Builder = #builder_name;
            fn dataframe_builder(capacity: usize) -> Self::Builder {
                #struct_name::dataframe_builder(capacity)
            }
        }

        impl postgres_to_polars::DataFrameBuilder<#struct_name> for #builder_name {
            fn push(&mut self, row: #struct_name) {
                #builder_name::push(self, row)
            }
            fn build(self) -> Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
                #builder_name::build(self)
            }
        }
    };

    TokenStream::from(expanded)
}
