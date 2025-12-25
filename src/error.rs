//! Error types for the `SQLite` reader library

use thiserror::Error;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::string::String;

/// Result type alias for operations that can fail with our Error type
pub type Result<T> = core::result::Result<T, Error>;

/// Errors that can occur when reading `SQLite` databases
#[derive(Debug, Error)]
pub enum Error {
    /// Error related to database schema
    #[error("Schema error: {0}")]
    SchemaError(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid SQLite file format: {0}")]
    InvalidFormat(String),

    #[error("Unsupported SQLite feature: {0}")]
    UnsupportedFeature(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid page number: {0}")]
    InvalidPage(u32),

    #[error("Invalid record format")]
    InvalidRecord,

    #[error("UTF-8 decoding error")]
    Utf8Error(#[from] core::str::Utf8Error),

    #[error("Integer overflow")]
    IntegerOverflow,

    #[error("Invalid varint")]
    InvalidVarint,

    #[error("SQL query error: {0}")]
    QueryError(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_creation() {
        let io_error = Error::Io(io::Error::new(io::ErrorKind::NotFound, "File not found"));
        let format_error = Error::InvalidFormat("Invalid SQLite format".to_string());
        let page_error = Error::InvalidPage(42);
        let table_error = Error::TableNotFound("users".to_string());
        let record_error = Error::InvalidRecord;

        assert!(matches!(io_error, Error::Io(_)));
        assert!(matches!(format_error, Error::InvalidFormat(_)));
        assert!(matches!(page_error, Error::InvalidPage(42)));
        assert!(matches!(table_error, Error::TableNotFound(_)));
        assert!(matches!(record_error, Error::InvalidRecord));
    }

    #[test]
    fn test_error_display() {
        let format_error = Error::InvalidFormat("Test format error".to_string());
        let page_error = Error::InvalidPage(123);
        let table_error = Error::TableNotFound("test_table".to_string());
        let record_error = Error::InvalidRecord;

        assert_eq!(
            format_error.to_string(),
            "Invalid SQLite file format: Test format error"
        );
        assert_eq!(page_error.to_string(), "Invalid page number: 123");
        assert_eq!(table_error.to_string(), "Table not found: test_table");
        assert_eq!(record_error.to_string(), "Invalid record format");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
        let sqlite_error: Error = io_error.into();

        assert!(matches!(sqlite_error, Error::Io(_)));
    }

    #[test]
    fn test_error_debug() {
        let error = Error::InvalidFormat("Test error".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("InvalidFormat"));
        assert!(debug_str.contains("Test error"));
    }

    #[test]
    fn test_result_type() {
        // Test that Result<T> works correctly
        let success: Result<()> = Ok(());
        let failure: Result<()> = Err(Error::InvalidFormat("Test".to_string()));

        assert!(success.is_ok());
        assert!(failure.is_err());
    }
}
