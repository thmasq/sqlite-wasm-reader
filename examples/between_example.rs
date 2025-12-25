//! Example demonstrating BETWEEN functionality in sqlite_wasm_reader

use sqlite_wasm_reader::query::Expr;
use sqlite_wasm_reader::{Database, Error, SelectQuery, Value};
use std::env;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <database.db>", args[0]);
        std::process::exit(1);
    }

    let db_path = &args[1];

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

    // Use the first table for examples
    let table_name = &tables[0];
    println!("\nDemonstrating BETWEEN queries on table: {}", table_name);

    // Example 1: SQL parsing with BETWEEN
    println!("\n=== Example 1: SQL BETWEEN parsing ===");
    let sql_query = format!("SELECT * FROM {} WHERE rowid BETWEEN 1 AND 5", table_name);
    println!("Query: {}", sql_query);

    match SelectQuery::parse(&sql_query).and_then(|q| db.execute_query(&q)) {
        Ok(rows) => {
            println!("Found {} rows with ROWID between 1 and 5:", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("  Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Example 2: Builder API with BETWEEN
    println!("\n=== Example 2: Builder API with BETWEEN ===");
    let builder_query = SelectQuery::new(table_name)
        .with_where(Expr::between("rowid", Value::Integer(3), Value::Integer(7)))
        .with_limit(10);

    println!("Builder query: {:?}", builder_query);

    match db.execute_query(&builder_query) {
        Ok(rows) => {
            println!("Found {} rows with ROWID between 3 and 7:", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("  Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Example 3: BETWEEN with different data types
    println!("\n=== Example 3: BETWEEN with different data types ===");

    // Try to find a numeric column for demonstration
    let sample_query = SelectQuery::new(table_name).with_limit(5);
    if let Ok(sample_rows) = db.execute_query(&sample_query) {
        if !sample_rows.is_empty() {
            for (column_name, value) in sample_rows[0].iter() {
                match value {
                    Value::Integer(int_val) => {
                        let query = SelectQuery::new(table_name)
                            .with_where(Expr::between(
                                column_name.clone(),
                                Value::Integer(int_val - 5),
                                Value::Integer(int_val + 5),
                            ))
                            .with_limit(3);

                        println!("Integer BETWEEN query on column '{}':", column_name);
                        match db.execute_query(&query) {
                            Ok(rows) => {
                                println!("  Found {} rows:", rows.len());
                                for row in rows.iter().take(2) {
                                    println!("    {:?}", row);
                                }
                            }
                            Err(e) => println!("  Error: {}", e),
                        }
                        break;
                    }
                    Value::Real(real_val) => {
                        let query = SelectQuery::new(table_name)
                            .with_where(Expr::between(
                                column_name.clone(),
                                Value::Real(real_val - 5.0),
                                Value::Real(real_val + 5.0),
                            ))
                            .with_limit(3);

                        println!("Real BETWEEN query on column '{}':", column_name);
                        match db.execute_query(&query) {
                            Ok(rows) => {
                                println!("  Found {} rows:", rows.len());
                                for row in rows.iter().take(2) {
                                    println!("    {:?}", row);
                                }
                            }
                            Err(e) => println!("  Error: {}", e),
                        }
                        break;
                    }
                    _ => continue,
                }
            }
        }
    }

    // Example 4: Complex query with BETWEEN and other operators
    println!("\n=== Example 4: Complex query with BETWEEN ===");
    let complex_query = SelectQuery::new(table_name)
        .with_where(
            Expr::between("rowid", Value::Integer(1), Value::Integer(10))
                .and(Expr::is_not_null("rowid")),
        )
        .with_limit(5);

    println!("Complex query: {:?}", complex_query);

    match db.execute_query(&complex_query) {
        Ok(rows) => {
            println!("Found {} rows matching complex conditions:", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("  Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("\nBETWEEN examples completed!");
    Ok(())
}
