//! Example demonstrating database operations and logging in sqlite_wasm_reader
//!
//! This example shows how to use the library to read SQLite databases
//! and handle different data types with comprehensive logging.

use sqlite_wasm_reader::{
    Database, Error, LogLevel, Value, init_default_logger, log_debug, log_info, log_warn,
    set_log_level,
};
use std::env;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <database.db> [log_level]", args[0]);
        eprintln!("Example: {} test.db debug", args[0]);
        eprintln!("Log levels: error, warn, info, debug, trace");
        std::process::exit(1);
    }

    let db_path = &args[1];

    // Initialize logging with specified level or default to Info
    if args.len() > 2 {
        if let Some(log_level) = LogLevel::from_str(&args[2]) {
            init_default_logger();
            set_log_level(log_level);
            log_info(&format!(
                "Starting database analysis with log level: {:?}",
                log_level
            ));
        } else {
            eprintln!("Invalid log level: {}. Using default (info)", args[2]);
            init_default_logger();
        }
    } else {
        init_default_logger();
        log_info("Starting database analysis with default log level (info)");
    }

    println!("Opening database: {}", db_path);

    // Open the database
    let mut db = Database::open(db_path)?;
    log_debug("Database opened successfully");

    // List tables
    println!("\nListing tables:");
    let tables = db.tables()?;
    log_info(&format!("Found {} tables in database", tables.len()));

    for table in &tables {
        println!("  - {}", table);
    }

    // Read and analyze each table
    for table in tables {
        println!("\n--- Analyzing table: {} ---", table);
        log_debug(&format!("Starting analysis of table: {}", table));

        // Count rows efficiently first
        match db.count_table_rows(&table) {
            Ok(count) => {
                println!("  Total rows: {}", count);
                log_debug(&format!("Table {} has {} rows", table, count));
            }
            Err(e) => {
                log_warn(&format!("Failed to count rows in table {}: {}", table, e));
                println!("  Error counting rows: {}", e);
                continue;
            }
        }

        // Try to execute a query to get sample data
        match db.execute_query(&sqlite_wasm_reader::SelectQuery::parse(&format!(
            "SELECT * FROM {} LIMIT 10",
            table
        ))?) {
            Ok(rows) => {
                if rows.is_empty() {
                    println!("  Empty table");
                    log_debug(&format!("Table {} is empty", table));
                    continue;
                }

                println!("  Found {} rows (showing first 10)", rows.len());
                log_info(&format!(
                    "Successfully queried {} rows from table {}",
                    rows.len(),
                    table
                ));

                // Analyze the first row to understand the schema
                if let Some(first_row) = rows.first() {
                    println!("  Columns and data types:");
                    log_debug(&format!("Analyzing schema for table {}", table));

                    for (column, value) in first_row {
                        let data_type = match value {
                            Value::Null => "NULL",
                            Value::Integer(_) => "INTEGER",
                            Value::Real(_) => "REAL",
                            Value::Text(_) => "TEXT",
                            Value::Blob(b) => &format!("BLOB({} bytes)", b.len()),
                        };
                        println!("    {}: {}", column, data_type);
                    }
                }

                // Show sample data
                for (i, row) in rows.iter().enumerate() {
                    println!("    Row {}: {:?}", i + 1, row);
                }

                // Analyze data distribution
                analyze_data_distribution(&rows);
            }
            Err(e) => {
                log_warn(&format!(
                    "Failed to query table {}: {} (table may not have suitable indexes)",
                    table, e
                ));
                eprintln!(
                    "  Error querying table: {} (table may not have suitable indexes)",
                    e
                );
            }
        }
    }

    log_info("Database analysis completed");
    Ok(())
}

/// Analyze the distribution of data types in a table
fn analyze_data_distribution(rows: &[sqlite_wasm_reader::Row]) {
    if rows.is_empty() {
        return;
    }

    let mut type_counts = std::collections::HashMap::new();

    // Count data types for each column
    for row in rows {
        for (column, value) in row {
            let data_type = match value {
                Value::Null => "NULL",
                Value::Integer(_) => "INTEGER",
                Value::Real(_) => "REAL",
                Value::Text(_) => "TEXT",
                Value::Blob(b) => &format!("BLOB({} bytes)", b.len()),
            };

            let key = format!("{}:{}", column, data_type);
            *type_counts.entry(key).or_insert(0) += 1;
        }
    }

    println!("  Data type distribution:");
    for (key, count) in type_counts {
        let percentage = (count as f64 / rows.len() as f64) * 100.0;
        println!("    {}: {} ({:.1}%)", key, count, percentage);
    }
}
