use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::ErrorResponseBody;

use crate::PgToPlError;

#[derive(Debug, Clone)]
pub struct PostgresError {
    pub severity: Option<String>,
    pub code: Option<String>,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<String>,
    pub internal_position: Option<String>,
    pub internal_query: Option<String>,
    pub where_: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub datatype: Option<String>,
    pub constraint: Option<String>,
    pub file: Option<String>,
    pub line: Option<String>,
    pub routine: Option<String>,
}

impl PostgresError {
    pub fn from_error_response(err: &ErrorResponseBody) -> Self {
        let fields = err.fields().iterator();

        let mut severity = None;
        let mut code = None;
        let mut message = None;
        let mut detail = None;
        let mut hint = None;
        let mut position = None;
        let mut internal_position = None;
        let mut internal_query = None;
        let mut where_ = None;
        let mut schema = None;
        let mut table = None;
        let mut column = None;
        let mut datatype = None;
        let mut constraint = None;
        let mut file = None;
        let mut line = None;
        let mut routine = None;

        let mut aggregate = "".to_string();

        for field in fields {
            if let Ok(f) = field {
                let field_type = f.type_();
                let value = String::from_utf8_lossy(f.value_bytes()).to_string();

                aggregate.push_str("\n");
                aggregate.push_str(&value);

                match field_type {
                    b'S' => severity = Some(value),          // Severity
                    b'V' => severity = Some(value),          // Severity (non-localized)
                    b'C' => code = Some(value),              // SQLSTATE code
                    b'M' => message = Some(value),           // Message
                    b'D' => detail = Some(value),            // Detail
                    b'H' => hint = Some(value),              // Hint
                    b'P' => position = Some(value),          // Position
                    b'p' => internal_position = Some(value), // Internal position
                    b'q' => internal_query = Some(value),    // Internal query
                    b'W' => where_ = Some(value),            // Where
                    b's' => schema = Some(value),            // Schema name
                    b't' => table = Some(value),             // Table name
                    b'c' => column = Some(value),            // Column name
                    b'd' => datatype = Some(value),          // Data type name
                    b'n' => constraint = Some(value),        // Constraint name
                    b'F' => file = Some(value),              // File
                    b'L' => line = Some(value),              // Line
                    b'R' => routine = Some(value),           // Routine
                    _ => {}
                }
            }
        }

        PostgresError {
            severity,
            code,
            message: message.unwrap_or(aggregate),
            detail,
            hint,
            position,
            internal_position,
            internal_query,
            where_,
            schema,
            table,
            column,
            datatype,
            constraint,
            file,
            line,
            routine,
        }
    }

    /// Format compact pour les logs : [CODE] Message
    pub fn to_compact_string(&self) -> String {
        match (&self.code, &self.message) {
            (Some(code), msg) => format!("[{}] {}", code, msg),
            (None, msg) => format!("Unknown PostgreSQL error: {}", msg),
        }
    }

    /// Format détaillé avec tous les champs disponibles
    pub fn to_detailed_string(&self) -> String {
        let mut parts = Vec::new();

        if let Some(ref severity) = self.severity {
            parts.push(format!("Severity: {}", severity));
        }
        if let Some(ref code) = self.code {
            parts.push(format!("Code: {}", code));
        }
        parts.push(format!("Message: {}", self.message));
        if let Some(ref detail) = self.detail {
            parts.push(format!("Detail: {}", detail));
        }
        if let Some(ref hint) = self.hint {
            parts.push(format!("Hint: {}", hint));
        }
        if let Some(ref position) = self.position {
            parts.push(format!("Position: {}", position));
        }
        if let Some(ref where_) = self.where_ {
            parts.push(format!("Where: {}", where_));
        }
        if let Some(ref schema) = self.schema {
            parts.push(format!("Schema: {}", schema));
        }
        if let Some(ref table) = self.table {
            parts.push(format!("Table: {}", table));
        }
        if let Some(ref column) = self.column {
            parts.push(format!("Column: {}", column));
        }
        if let Some(ref constraint) = self.constraint {
            parts.push(format!("Constraint: {}", constraint));
        }

        parts.join("\n")
    }

    /// Vérifie si c'est une erreur spécifique par son code SQLSTATE
    pub fn is_code(&self, code: &str) -> bool {
        self.code.as_deref() == Some(code)
    }

    /// Vérifie si c'est une erreur de statement déjà existant
    pub fn is_duplicate_prepared_statement(&self) -> bool {
        self.is_code("42P05")
    }

    /// Vérifie si c'est une erreur de violation de contrainte
    pub fn is_integrity_constraint_violation(&self) -> bool {
        self.code.as_ref().map_or(false, |c| c.starts_with("23"))
    }

    /// Vérifie si c'est une erreur de syntaxe
    pub fn is_syntax_error(&self) -> bool {
        self.is_code("42601")
    }

    /// Vérifie si c'est une erreur fatale (nécessite reconnexion)
    pub fn is_fatal(&self) -> bool {
        matches!(self.severity.as_deref(), Some("FATAL") | Some("PANIC"))
    }
}

impl std::fmt::Display for PostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_compact_string())
    }
}

impl From<ErrorResponseBody> for PostgresError {
    fn from(err: ErrorResponseBody) -> Self {
        PostgresError::from_error_response(&err)
    }
}

impl From<PostgresError> for PgToPlError {
    fn from(err: PostgresError) -> Self {
        PgToPlError::QueryError(err)
    }
}

impl From<ErrorResponseBody> for PgToPlError {
    fn from(err: ErrorResponseBody) -> Self {
        PgToPlError::QueryError(err.into())
    }
}
