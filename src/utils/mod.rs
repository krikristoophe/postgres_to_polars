use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::ErrorResponseBody;

pub mod error;
pub mod text_array;

pub fn md5_hash(user: &str, password: &str, salt: &[u8; 4]) -> String {
    // Étape 1 : md5(password + username)
    let inner = md5::compute(format!("{}{}", password, user));

    // inner est un [u8; 16], on le convertit en hex string
    let inner_hex = format!("{:x}", inner);

    // Étape 2 : md5(inner_hex + salt)
    let mut outer = md5::Context::new();
    outer.consume(inner_hex.as_bytes());
    outer.consume(salt);
    let final_hash = outer.finalize();

    // Préfixer par "md5"
    format!("md5{:x}", final_hash)
}

pub fn statement_name(query: &str) -> String {
    let digest = md5::compute(query.as_bytes());
    format!("stmt_{:x}", digest) // Toujours 32 caractères
}

pub fn print_error(err: &ErrorResponseBody) {
    println!("Received error: {:?}", error_to_string(err));
}

pub fn error_to_string(err: &ErrorResponseBody) -> String {
    let fields = err.fields().iterator();
    let mut error_strings = Vec::with_capacity(5);
    for field in fields {
        match field {
            Ok(f) => {
                let bytes = f.value_bytes();
                let value = String::from_utf8_lossy(&bytes);

                error_strings.push(value.to_string());
            }
            Err(err) => {
                error_strings.push(format!("Error parsing error field: {:?}", err));
                break;
            }
        }
    }
    error_strings.join("\n").to_string()
}
