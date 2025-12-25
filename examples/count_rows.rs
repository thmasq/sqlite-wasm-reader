//! Example demonstrating efficient row counting in sqlite_wasm_reader
//!
//! This example shows how to use the count_table_rows method to efficiently
//! count rows in tables without loading all data into memory.

use sqlite_wasm_reader::{Database, Error, init_default_logger, log_debug, log_info};
use std::env;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <database.db>", args[0]);
        eprintln!("Example: {} large_database.db", args[0]);
        std::process::exit(1);
    }

    let db_path = &args[1];

    // Initialize logging
    init_default_logger();
    log_info(&format!("Starting row count analysis: {}", db_path));

    println!("Opening database: {}", db_path);
    let mut db = Database::open(db_path)?;

    // List all tables
    println!("\nTables in database:");
    let tables = db.tables()?;
    for table in &tables {
        println!("  - {}", table);
    }

    println!("\n=== Row Count Analysis ===");

    // Count rows in each table efficiently
    let mut total_rows = 0;
    let table_count = tables.len();
    for table in tables {
        println!("\nTable: {}", table);

        match db.count_table_rows(&table) {
            Ok(count) => {
                println!("  Rows: {}", count);
                log_debug(&format!("Table {} has {} rows", table, count));
                total_rows += count;

                // Provide some context about the table size
                if count == 0 {
                    println!("  Status: Empty table");
                } else if count < 100 {
                    println!("  Status: Small table");
                } else if count < 10000 {
                    println!("  Status: Medium table");
                } else if count < 1000000 {
                    println!("  Status: Large table");
                } else {
                    println!("  Status: Very large table");
                }
            }
            Err(e) => {
                println!("  Error: {}", e);
                log_info(&format!("Failed to count rows in table {}: {}", table, e));
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Total tables: {}", table_count);
    println!("Total rows across all tables: {}", total_rows);

    if total_rows > 0 {
        let avg_rows = total_rows as f64 / table_count as f64;
        println!("Average rows per table: {:.1}", avg_rows);
    }

    log_info(&format!(
        "Row count analysis completed. Total rows: {}",
        total_rows
    ));
    Ok(())
}
