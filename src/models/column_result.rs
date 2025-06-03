use polars::{
    prelude::{DataType, ListChunked, NamedFrom, TimeUnit},
    series::{IntoSeries, Series},
};
use postgres_protocol::message::backend::Field;

use crate::utils::text_array::parse_text_array;

pub struct ColumnResult<T> {
    pub name: String,
    pub data: Vec<Option<T>>,
}

impl<T> ColumnResult<T> {
    pub fn new(name: String) -> Self {
        ColumnResult {
            name,
            data: Vec::with_capacity(1024),
        }
    }

    pub fn push(&mut self, value: T) {
        self.data.push(Some(value));
        self.extend_vec();
    }

    pub fn push_null(&mut self) {
        self.data.push(None);
        self.extend_vec();
    }

    fn extend_vec(&mut self) {
        if self.data.len() == self.data.capacity() {
            self.data.reserve(1024);
        }
    }
}

pub enum ColumnStorage {
    Ints(ColumnResult<i32>),
    Texts(ColumnResult<String>),
    Bools(ColumnResult<bool>),
    Dates(ColumnResult<i32>), // i32 = jours depuis 1970-01-01 (compatible Polars)
    TextArray(ColumnResult<Vec<Option<String>>>),
    Timestamps(ColumnResult<i64>), // microsecondes depuis 1970-01-01 UTC
    Doubles(ColumnResult<f64>),
    TimestampsWtz(ColumnResult<i64>), // microsecondes depuis 2000-01-01
    Times(ColumnResult<i64>),         // microsecondes depuis minuit
    Bytes(ColumnResult<Vec<u8>>),     // fallback
}

pub fn column_from_field(field: &Field) -> ColumnStorage {
    let name = String::from(field.name());
    let oid = field.type_oid();

    match oid {
        23 => ColumnStorage::Ints(ColumnResult::new(name)), // int4
        25 | 1043 => ColumnStorage::Texts(ColumnResult::new(name)), // text, varchar
        16 => ColumnStorage::Bools(ColumnResult::new(name)), // bool
        1082 => ColumnStorage::Dates(ColumnResult::new(name)), // date
        1009 => ColumnStorage::TextArray(ColumnResult::new(name)), // text[]
        1184 => ColumnStorage::Timestamps(ColumnResult::new(name)), // timestamptz
        701 => ColumnStorage::Doubles(ColumnResult::new(name)), // float8
        1114 => ColumnStorage::TimestampsWtz(ColumnResult::new(name)), // timestamp
        1083 => ColumnStorage::Times(ColumnResult::new(name)), // time
        _ => {
            println!(
                "⚠️ Unknown type column: name={}, type_oid={}, format={}",
                field.name(),
                field.type_oid(),
                field.format()
            );
            ColumnStorage::Bytes(ColumnResult::new(name))
        } // fallback: raw bytes
    }
}

pub fn push_column_value(column: &mut ColumnStorage, value: Option<&[u8]>) {
    match column {
        ColumnStorage::Ints(col) => match value {
            Some(bytes) if bytes.len() == 4 => {
                let val = i32::from_be_bytes(bytes.try_into().unwrap());
                col.push(val);
            }
            _ => col.push_null(),
        },
        ColumnStorage::Texts(col) => match value {
            Some(bytes) => {
                let val = std::str::from_utf8(bytes).unwrap().to_string();
                col.push(val);
            }
            _ => col.push_null(),
        },
        ColumnStorage::Bools(col) => match value {
            Some(&[b]) => col.push(b != 0),
            _ => col.push_null(),
        },
        ColumnStorage::Bytes(col) => match value {
            Some(bytes) => col.push(bytes.to_vec()),
            _ => col.push_null(),
        },
        ColumnStorage::Dates(col) => match value {
            Some(bytes) if bytes.len() == 4 => {
                let pg_days = i32::from_be_bytes(bytes.try_into().unwrap()); // jours depuis 2000-01-01
                let unix_days = pg_days + 10957; // => 2000-01-01 - 1970-01-01 = 10957 jours
                col.push(unix_days);
            }
            _ => col.push_null(),
        },
        ColumnStorage::TextArray(col) => match value {
            Some(bytes) => {
                let val = parse_text_array(bytes).unwrap();
                col.push(val);
            }
            _ => col.push_null(),
        },
        ColumnStorage::Timestamps(col) => match value {
            Some(bytes) if bytes.len() == 8 => {
                // PostgreSQL: microseconds since 2000-01-01
                let pg_microseconds = i64::from_be_bytes(bytes.try_into().unwrap());
                let unix_microseconds = pg_microseconds + 946684800_000_000; // seconds between 1970-01-01 and 2000-01-01
                col.push(unix_microseconds);
            }
            _ => col.push_null(),
        },
        ColumnStorage::Doubles(col) => match value {
            Some(bytes) if bytes.len() == 8 => {
                let val = f64::from_be_bytes(bytes.try_into().unwrap());
                col.push(val);
            }
            _ => col.push_null(),
        },
        ColumnStorage::TimestampsWtz(col) => match value {
            Some(bytes) if bytes.len() == 8 => {
                let micros_pg_epoch = i64::from_be_bytes(bytes.try_into().unwrap());
                let micros_unix_epoch = micros_pg_epoch + 946684800_000_000; // 2000-01-01 => 1970-01-01
                col.push(micros_unix_epoch);
            }
            _ => col.push_null(),
        },
        ColumnStorage::Times(col) => match value {
            Some(bytes) if bytes.len() == 8 => {
                let micros_since_midnight = i64::from_be_bytes(bytes.try_into().unwrap());
                col.push(micros_since_midnight);
            }
            _ => col.push_null(),
        },
    }
}

pub fn column_to_series(column: ColumnStorage) -> Series {
    match column {
        ColumnStorage::Ints(col) => Series::new(col.name.into(), &col.data),
        ColumnStorage::Texts(col) => Series::new(col.name.into(), &col.data),
        ColumnStorage::Bools(col) => Series::new(col.name.into(), &col.data),
        ColumnStorage::Bytes(col) => Series::new(col.name.into(), &col.data),
        ColumnStorage::Dates(col) => Series::new(col.name.into(), &col.data)
            .cast(&DataType::Date)
            .unwrap(),
        ColumnStorage::TextArray(col) => text_array_to_series(col.name.as_str(), col.data),
        ColumnStorage::Timestamps(col) => Series::new(col.name.into(), &col.data)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap(),
        ColumnStorage::Doubles(col) => Series::new(col.name.into(), &col.data),
        ColumnStorage::TimestampsWtz(col) => Series::new(col.name.into(), &col.data)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap(),
        ColumnStorage::Times(col) => Series::new(col.name.into(), &col.data)
            .cast(&DataType::Time)
            .unwrap(),
    }
}

pub fn text_array_to_series(name: &str, data: Vec<Option<Vec<Option<String>>>>) -> Series {
    let list_chunked: ListChunked = data
        .into_iter()
        .map(|maybe_vec| maybe_vec.map(|v| Series::new("".into(), v)))
        .collect();

    list_chunked.into_series().with_name(name.into())
}
