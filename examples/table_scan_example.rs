//! Example demonstrating table scan fallback functionality in sqlite_wasm_reader
//!
//! This example shows how the library automatically falls back to table scans
//! when no suitable index is available, ensuring all queries work.

use sqlite_wasm_reader::{
    Database, Error, LogLevel, SelectQuery, Value, init_default_logger, set_log_level,
};
use std::env;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <database.db>", args[0]);
        std::process::exit(1);
    }

    let db_path = &args[1];

    // Enable debug logging to see index vs table scan decisions
    init_default_logger();
    set_log_level(LogLevel::Debug);

    println!("Opening database: {}", db_path);
    let mut db = Database::open(db_path)?;

    // List available tables
    println!("\nAvailable tables:");
    let tables = db.tables()?;
    for table in &tables {
        println!("  - {}", table);
    }

    if tables.is_empty() {
        println!("No tables found in database");
        return Ok(());
    }

    // Use the first table for demonstration
    let table_name = &tables[0];
    println!("\nUsing table: {}", table_name);

    // Get table columns
    let columns = db.get_table_columns(table_name)?;
    println!("Columns: {:?}", columns);

    // Count total rows
    let total_rows = db.count_table_rows(table_name)?;
    println!("Total rows: {}", total_rows);

    if total_rows == 0 {
        println!("Table is empty");
        return Ok(());
    }

    // Example 1: Query without WHERE clause (always uses table scan)
    println!("\n=== Example 1: Query without WHERE clause ===");
    let query1 = SelectQuery::parse(&format!("SELECT * FROM {} LIMIT 3", table_name))?;
    let rows1 = db.execute_query(&query1)?;
    println!("Found {} rows (showing first 3):", rows1.len());
    for (i, row) in rows1.iter().enumerate() {
        println!("  Row {}: {:?}", i + 1, row);
    }

    // Example 2: Query with WHERE clause (may use index or table scan)
    if !columns.is_empty() {
        println!("\n=== Example 2: Query with WHERE clause ===");
        let first_column = &columns[0];

        // Try to get a sample value from the first row
        if let Some(first_row) = rows1.first() {
            if let Some(sample_value) = first_row.get(first_column) {
                let query2 = match sample_value {
                    Value::Text(s) => SelectQuery::parse(&format!(
                        "SELECT * FROM {} WHERE {} = '{}'",
                        table_name, first_column, s
                    ))?,
                    Value::Integer(i) => SelectQuery::parse(&format!(
                        "SELECT * FROM {} WHERE {} = {}",
                        table_name, first_column, i
                    ))?,
                    Value::Real(r) => SelectQuery::parse(&format!(
                        "SELECT * FROM {} WHERE {} = {}",
                        table_name, first_column, r
                    ))?,
                    _ => {
                        println!("Skipping WHERE example due to unsupported value type");
                        return Ok(());
                    }
                };

                println!("Executing query with WHERE clause...");
                let rows2 = db.execute_query(&query2)?;
                println!("Found {} matching rows:", rows2.len());
                for (i, row) in rows2.iter().take(3).enumerate() {
                    println!("  Row {}: {:?}", i + 1, row);
                }
            }
        }
    }

    // Example 3: Complex query (likely uses table scan)
    if columns.len() >= 2 {
        println!("\n=== Example 3: Complex query with OR condition ===");
        let col1 = &columns[0];
        let col2 = &columns[1];

        let query3 = SelectQuery::parse(&format!(
            "SELECT * FROM {} WHERE {} IS NOT NULL OR {} IS NOT NULL LIMIT 5",
            table_name, col1, col2
        ))?;

        println!("Executing complex query...");
        let rows3 = db.execute_query(&query3)?;
        println!("Found {} rows (showing first 5):", rows3.len());
        for (i, row) in rows3.iter().enumerate() {
            println!("  Row {}: {:?}", i + 1, row);
        }
    }

    println!("\n=== Summary ===");
    println!("The library automatically chose the best execution strategy:");
    println!("- Used indexes when available for exact equality matches");
    println!("- Fell back to table scans when no suitable index was found");
    println!("- All queries completed successfully regardless of index availability");

    Ok(())
}
