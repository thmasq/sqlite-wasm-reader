//! WASI-compatible example for sqlite_wasm_reader
//!
//! This example can be compiled to WASI and run with wasmtime:
//! cargo build --example wasi_example --target wasm32-wasip1
//! wasmtime run --dir=. target/wasm32-wasip1/debug/examples/wasi_example.wasm -- test.db

use sqlite_wasm_reader::{Database, Error, init_default_logger, log_info};
use std::env;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <database.db>", args[0]);
        return Ok(());
    }

    let db_path = &args[1];
    println!("Opening database: {}", db_path);

    // Initialize logging for WASI environment
    init_default_logger();
    log_info(&format!("Starting WASI database analysis: {}", db_path));

    let mut db = Database::open(db_path)?;
    log_info("Database opened successfully");

    // List tables
    let tables = db.tables()?;
    println!("\nFound {} tables:", tables.len());
    for table in &tables {
        println!("  - {}", table);
    }

    // Read first table if any exist
    if let Some(first_table) = tables.first() {
        println!("\nReading table '{}':", first_table);
        log_info(&format!("Analyzing table: {}", first_table));

        // Count rows efficiently first
        match db.count_table_rows(first_table) {
            Ok(count) => {
                println!("  Total rows: {}", count);
                log_info(&format!("Table {} has {} rows", first_table, count));
            }
            Err(e) => {
                eprintln!("  Error counting rows: {}", e);
                log_info(&format!(
                    "Failed to count rows in table {}: {}",
                    first_table, e
                ));
            }
        }

        // Try to execute a query to get sample data
        match db.execute_query(&sqlite_wasm_reader::SelectQuery::parse(&format!(
            "SELECT * FROM {} LIMIT 5",
            first_table
        ))?) {
            Ok(rows) => {
                println!("  Found {} rows (showing first 5)", rows.len());
                log_info(&format!(
                    "Successfully queried {} rows from table {}",
                    rows.len(),
                    first_table
                ));

                for (i, row) in rows.iter().enumerate() {
                    println!("  Row {}: {:?}", i + 1, row);
                }
            }
            Err(e) => {
                eprintln!(
                    "  Error querying table: {} (table may not have suitable indexes)",
                    e
                );
                log_info(&format!("Failed to query table {}: {}", first_table, e));
            }
        }
    }

    log_info("WASI database analysis completed");
    Ok(())
}
