use postgres_protocol::Oid;

#[derive(Debug, Clone)]
pub enum BinaryParam {
    Int4(i32),
    Text(String),
    Bool(bool),
    Int8(i64),
    Float8(f64),
    // ajoute d'autres types ici si besoin
}

pub fn format_params<P>(params: P) -> (Vec<Oid>, Vec<Option<Vec<u8>>>)
where
    P: IntoIterator<Item = Option<BinaryParam>>,
{
    let mut param_types: Vec<Oid> = Vec::new();
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
                param_types.push(0); // unknown
                param_values.push(None);
            }
        }
    }

    (param_types, param_values)
}
