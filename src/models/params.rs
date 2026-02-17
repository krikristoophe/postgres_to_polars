use std::{borrow::Cow, fmt::Debug};

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryParam {
    Int4(i32),
    Text(String),
    Bool(bool),
    Int8(i64),
    Float8(f64),
    // ajoute d'autres types ici si besoin
}

impl std::hash::Hash for BinaryParam {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            BinaryParam::Int4(v) => v.hash(state),
            BinaryParam::Text(v) => v.hash(state),
            BinaryParam::Bool(v) => v.hash(state),
            BinaryParam::Int8(v) => v.hash(state),
            BinaryParam::Float8(v) => v.to_bits().hash(state),
        }
    }
}

#[cfg(feature = "execution")]
pub fn format_params<P>(params: P) -> (Vec<postgres_protocol::Oid>, Vec<Option<Vec<u8>>>)
where
    P: IntoIterator<Item = Option<BinaryParam>>,
{
    let mut param_types: Vec<postgres_protocol::Oid> = Vec::new();
    let mut param_values = Vec::new();

    for param in params {
        match param {
            Some(BinaryParam::Int4(val)) => {
                param_types.push(23); // OID for int4
                param_values.push(Some(val.to_be_bytes().to_vec()));
            }
            Some(BinaryParam::Text(s)) => {
                param_types.push(25); // OID for text
                param_values.push(Some(s.into_bytes()));
            }
            Some(BinaryParam::Bool(val)) => {
                param_types.push(16); // OID for bool
                param_values.push(Some(vec![val as u8]));
            }
            Some(BinaryParam::Int8(val)) => {
                param_types.push(20); // OID for int8
                param_values.push(Some(val.to_be_bytes().to_vec()));
            }
            Some(BinaryParam::Float8(val)) => {
                param_types.push(701); // OID for float8
                param_values.push(Some(val.to_be_bytes().to_vec()));
            }
            None => {
                tracing::warn!("Unknown parameter");
                param_types.push(0); // unknown
                param_values.push(None);
            }
        }
    }

    (param_types, param_values)
}

pub trait IntoBinaryParam {
    fn into_binary_param(self) -> BinaryParam;
}

impl<T> IntoBinaryParam for T
where
    T: Into<BinaryParam>,
{
    fn into_binary_param(self) -> BinaryParam {
        self.into()
    }
}

impl<T> From<(Option<T>, T)> for BinaryParam
where
    T: Into<BinaryParam>,
{
    fn from(value: (Option<T>, T)) -> Self {
        match value {
            (Some(val), _) => val.into(),
            (None, default) => default.into(),
        }
    }
}

impl From<i32> for BinaryParam {
    fn from(value: i32) -> Self {
        BinaryParam::Int4(value)
    }
}

impl From<i64> for BinaryParam {
    fn from(value: i64) -> Self {
        BinaryParam::Int8(value)
    }
}

impl From<f64> for BinaryParam {
    fn from(value: f64) -> Self {
        BinaryParam::Float8(value)
    }
}

impl From<bool> for BinaryParam {
    fn from(value: bool) -> Self {
        BinaryParam::Bool(value)
    }
}

impl From<String> for BinaryParam {
    fn from(value: String) -> Self {
        BinaryParam::Text(value)
    }
}

impl From<chrono::NaiveDate> for BinaryParam {
    fn from(value: chrono::NaiveDate) -> Self {
        BinaryParam::Text(value.format("%Y-%m-%d").to_string())
    }
}

impl From<&String> for BinaryParam {
    fn from(value: &String) -> Self {
        BinaryParam::Text(value.clone())
    }
}

impl From<&str> for BinaryParam {
    fn from(value: &str) -> Self {
        BinaryParam::Text(value.to_string())
    }
}

impl From<Cow<'_, str>> for BinaryParam {
    fn from(value: Cow<'_, str>) -> Self {
        BinaryParam::Text(value.into_owned())
    }
}

#[macro_export]
macro_rules! binary_param {
    ($opt:expr, $default:expr) => {
        $opt.clone().unwrap_or($default.clone()).into()
    };
    ($value:expr) => {
        $value.clone().into()
    };
}

#[macro_export]
macro_rules! binary_param_opt {
    ($opt:expr, $default:expr) => {
        Some($opt.clone().unwrap_or($default.clone()).into())
    };
    ($value:expr) => {
        Some($value.clone().into())
    };
}

/// Compte le nombre de placeholders $1, $2, ... dans une chaîne SQL
pub const fn count_placeholders(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut seen: usize = 0;
    let mut count = 0;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            // Vérifie qu'il y a au moins un chiffre après le $
            if i + 1 < bytes.len() && bytes[i + 1] >= b'0' && bytes[i + 1] <= b'9' {
                let mut value = bytes[i + 1] - b'0';

                // Saute tous les chiffres suivants
                i += 1;
                while i < bytes.len() && bytes[i] >= b'0' && bytes[i] <= b'9' {
                    value = value * 10 + bytes[i] - b'0';
                    i += 1;
                }
                if seen & (1 << value) == 0 {
                    seen = seen | (1 << value);
                    count += 1;
                }
                continue;
            }
        }
        i += 1;
    }
    count
}

#[macro_export]
macro_rules! prepared_query {
    ($sql:expr, $($para:expr),* $(,)?) => {{
        use $crate::count_placeholders;
        const SQL: &str = $sql;
        const EXPECTED: usize = $crate::count_placeholders(SQL);
        const PROVIDED: usize = $crate::count_args!($($para),*);
        const _: () = assert!(
            EXPECTED == PROVIDED,
            "Mismatch between SQL placeholders and provided parameters.",
        );
        (SQL.to_string(), vec![$($crate::binary_param_opt!($para)),*])
    }};
    ($sql:expr) => {{
        use $crate::count_placeholders;
        const SQL: &str = $sql;
        const EXPECTED: usize = $crate::count_placeholders(SQL);
        const _: () = assert!(
            EXPECTED == 0,
            "SQL contains placeholders but no parameters were provided"
        );
        (SQL.to_string(), vec![])
    }};
}

#[macro_export]
macro_rules! count_args {
    () => { 0usize };
    ($head:expr $(, $tail:expr)*) => { 1usize + $crate::count_args!($($tail),*) };
}
