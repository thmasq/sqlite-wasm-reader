#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

//! `SQLite` WASM Reader - A pure Rust `SQLite` reader for WASI environments
//!
//! This library provides a lightweight `SQLite` database reader that can be used
//! in WebAssembly environments, particularly WASI. It supports reading tables,
//! parsing records, and basic database operations without requiring native
//! `SQLite` bindings.
//!
//! # Example
//!
//! ```no_run
//! use sqlite_wasm_reader::{Database, Error, SelectQuery};
//!
//! fn main() -> Result<(), Error> {
//!     let mut db = Database::open("example.db")?;
//!     
//!     // List all tables
//!     let tables = db.tables()?;
//!     for table in tables {
//!         println!("Table: {}", table);
//!     }
//!     
//!     // Execute a query using indexes
//!     let query = SelectQuery::parse("SELECT * FROM users WHERE id = 1")?;
//!     let rows = db.execute_query(&query)?;
//!     for row in rows {
//!         println!("{:?}", row);
//!     }
//!     
//!     Ok(())
//! }
//! ```

// Only use no_std for non-WASI WebAssembly targets
#![cfg_attr(all(target_arch = "wasm32", not(target_os = "wasi")), no_std)]

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
extern crate alloc;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{format, string::String, vec::Vec};

pub mod btree;
pub mod database;
pub mod error;
pub mod format;
pub mod logging;
pub mod page;
pub mod query;
pub mod record;
pub mod value;

pub use database::Database;
pub use error::{Error, Result};
pub use logging::{
    LogLevel, Logger, init_default_logger, log_debug, log_error, log_info, log_trace, log_warn,
    set_log_level,
};
pub use query::{ComparisonOperator, OrderBy, SelectQuery};
pub use value::Value;

// Re-export commonly used types
pub use btree::{BTreeCursor, Cell};
pub use format::{FileHeader, PageType};
pub use page::Page;

// Re-export key types
pub use database::Row;
