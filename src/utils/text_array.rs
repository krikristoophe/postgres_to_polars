use byteorder::{BigEndian, ReadBytesExt};
use std::io;

use crate::{PgToPlError, utils::error::PgToPlResult};

pub fn parse_text_array(mut bytes: &[u8]) -> PgToPlResult<Vec<Option<String>>> {
    let ndim = bytes.read_i32::<BigEndian>()?;
    if ndim == 0 {
        return Ok(Vec::new()); // tableau vide
    } else if ndim != 1 {
        println!("ndim {}", ndim);
        return Err(PgToPlError::OnlyOneDimensionArraySupported);
    }

    let _has_null = bytes.read_i32::<BigEndian>()?;
    let _element_oid = bytes.read_i32::<BigEndian>()?;

    let dim_len = bytes.read_i32::<BigEndian>()? as usize;
    let _lower_bound = bytes.read_i32::<BigEndian>()?;

    let mut values = Vec::with_capacity(dim_len);

    for _ in 0..dim_len {
        let item_len = bytes.read_i32::<BigEndian>()?;
        if item_len == -1 {
            values.push(None);
        } else {
            let item_len = item_len as usize;
            if bytes.len() < item_len {
                return Err(PgToPlError::NotEnoughBytes);
            }
            let (str_bytes, rest) = bytes.split_at(item_len);
            bytes = rest;
            let s = std::str::from_utf8(str_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            values.push(Some(s.to_string()));
        }
    }

    Ok(values)
}
