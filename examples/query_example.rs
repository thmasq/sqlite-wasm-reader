//! Example demonstrating SELECT query functionality

use sqlite_wasm_reader::query::Expr;
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

    // Initialize logging
    init_default_logger();
    set_log_level(LogLevel::Info);

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
    println!("\nRunning queries on table: {}", table_name);

    // ------------------------------------------------------------------
    // Example 0: Using the builder-style helper API
    // ------------------------------------------------------------------
    println!("\n=== Example 0: Builder-style query helpers ===");
    // Fetch first column name for demonstration
    let first_row = db.execute_query(&SelectQuery::new(table_name).with_limit(1))?;
    if let Some(row) = first_row.first() {
        if let Some(first_col) = row.keys().next() {
            let builder_query = SelectQuery::new(table_name)
                .select_columns(vec![first_col.clone()])
                .with_where(Expr::is_not_null(first_col.clone()))
                .with_limit(5);

            println!("Builder query struct: {:?}", builder_query);
            match db.execute_query(&builder_query) {
                Ok(rows) => {
                    println!("Found {} rows using builder:", rows.len());
                    for (i, row) in rows.iter().enumerate() {
                        println!("Row {}: {:?}", i + 1, row);
                    }
                }
                Err(e) => println!("Error executing builder query: {}", e),
            }
        }
    }

    // Example 1: Simple SELECT *
    println!("\n=== Example 1: SELECT * FROM {} ===", table_name);
    match SelectQuery::parse(&format!("SELECT * FROM {}", table_name))
        .and_then(|q| db.execute_query(&q))
    {
        Ok(rows) => {
            println!("Found {} rows:", rows.len());
            for (i, row) in rows.iter().take(3).enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
            if rows.len() > 3 {
                println!("... and {} more rows", rows.len() - 3);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Example 2: SELECT with specific columns
    println!("\n=== Example 2: SELECT specific columns ===");
    // Try to get column names by querying the table
    let query = format!("SELECT * FROM {} LIMIT 1", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            let columns: Vec<String> = rows[0].keys().cloned().collect();
            if columns.len() >= 2 {
                let col1 = &columns[0];
                let col2 = &columns[1];
                let query = format!("SELECT {}, {} FROM {}", col1, col2, table_name);
                println!("Query: {}", query);

                match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                    Ok(rows) => {
                        println!("Found {} rows:", rows.len());
                        for (i, row) in rows.iter().take(3).enumerate() {
                            println!("Row {}: {:?}", i + 1, row);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
        }
    }

    // Example 3: SELECT with WHERE clause
    println!("\n=== Example 3: SELECT with WHERE clause ===");
    // Try to find a numeric column for the WHERE clause
    let query = format!("SELECT * FROM {} LIMIT 10", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            for (column, value) in rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!(
                        "SELECT * FROM {} WHERE {} > {}",
                        table_name,
                        column,
                        int_val - 1
                    );
                    println!("Query: {}", query);

                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching condition:", rows.len());
                            for (i, row) in rows.iter().take(2).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }

    // Example 4: SELECT with ORDER BY
    println!("\n=== Example 4: SELECT with ORDER BY ===");
    let query = format!("SELECT * FROM {} LIMIT 1", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            let columns: Vec<String> = rows[0].keys().cloned().collect();
            if !columns.is_empty() {
                let order_column = &columns[0];
                let query = format!(
                    "SELECT * FROM {} ORDER BY {} DESC",
                    table_name, order_column
                );
                println!("Query: {}", query);

                match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                    Ok(rows) => {
                        println!("Found {} rows (ordered):", rows.len());
                        for (i, row) in rows.iter().take(3).enumerate() {
                            println!("Row {}: {:?}", i + 1, row);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
        }
    }

    // Example 5: SELECT with LIMIT
    println!("\n=== Example 5: SELECT with LIMIT ===");
    let query = format!("SELECT * FROM {} LIMIT 5", table_name);
    println!("Query: {}", query);

    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        Ok(rows) => {
            println!("Found {} rows (limited to 5):", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Example 6: Complex query with WHERE, ORDER BY, and LIMIT
    println!("\n=== Example 6: Complex query ===");
    let query = format!("SELECT * FROM {} LIMIT 10", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            let columns: Vec<String> = rows[0].keys().cloned().collect();
            if columns.len() >= 2 {
                let col1 = &columns[0];
                let col2 = &columns[1];

                // Try to find a good value for WHERE clause
                for row in &rows {
                    if let Some(Value::Text(text_val)) = row.get(col2) {
                        if !text_val.is_empty() {
                            let query = format!(
                                "SELECT {} FROM {} WHERE {} = '{}' ORDER BY {} LIMIT 3",
                                col1, table_name, col2, text_val, col1
                            );
                            println!("Query: {}", query);

                            match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                                Ok(rows) => {
                                    println!("Found {} rows:", rows.len());
                                    for (i, row) in rows.iter().enumerate() {
                                        println!("Row {}: {:?}", i + 1, row);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    // Example 7: Demonstrate LIKE operator
    println!("\n=== Example 7: SELECT with LIKE ===");
    let query = format!("SELECT * FROM {} LIMIT 10", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            for (column, value) in rows[0].iter() {
                if let Value::Text(text_val) = value {
                    if text_val.len() > 2 {
                        let prefix = &text_val[..2];
                        let query = format!(
                            "SELECT * FROM {} WHERE {} LIKE '{}%'",
                            table_name, column, prefix
                        );
                        println!("Query: {}", query);

                        match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                            Ok(rows) => {
                                println!("Found {} rows matching pattern:", rows.len());
                                for (i, row) in rows.iter().take(2).enumerate() {
                                    println!("Row {}: {:?}", i + 1, row);
                                }
                            }
                            Err(e) => println!("Error: {}", e),
                        }
                        break;
                    }
                }
            }
        }
    }

    // Example 8: Demonstrate OR operator
    println!("\n=== Example 8: SELECT with OR ===");
    let query = format!("SELECT * FROM {} LIMIT 10", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            for (column, value) in rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!(
                        "SELECT * FROM {} WHERE {} = {} OR {} = {}",
                        table_name,
                        column,
                        int_val,
                        column,
                        int_val + 1
                    );
                    println!("Query: {}", query);

                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching OR condition:", rows.len());
                            for (i, row) in rows.iter().take(3).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }

    // Example 9: Demonstrate IN operator
    println!("\n=== Example 9: SELECT with IN ===");
    let query = format!("SELECT * FROM {} LIMIT 10", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            for (column, value) in rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!(
                        "SELECT * FROM {} WHERE {} IN ({}, {}, {})",
                        table_name,
                        column,
                        int_val,
                        int_val + 1,
                        int_val + 2
                    );
                    println!("Query: {}", query);

                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching IN condition:", rows.len());
                            for (i, row) in rows.iter().take(3).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }

    // Example 10: Demonstrate BETWEEN operator
    println!("\n=== Example 10: SELECT with BETWEEN ===");
    let query = format!("SELECT * FROM {} LIMIT 10", table_name);
    if let Ok(rows) = SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        if !rows.is_empty() {
            for (column, value) in rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!(
                        "SELECT * FROM {} WHERE {} BETWEEN {} AND {}",
                        table_name,
                        column,
                        int_val,
                        int_val + 5
                    );
                    println!("Query: {}", query);

                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows in range:", rows.len());
                            for (i, row) in rows.iter().take(3).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }

    // Example 11: Demonstrate IS NULL
    println!("\n=== Example 11: SELECT with IS NULL ===");
    let query = format!("SELECT * FROM {} WHERE id IS NULL LIMIT 5", table_name);
    println!("Query: {}", query);

    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        Ok(rows) => {
            println!("Found {} rows with NULL id:", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Example 12: Demonstrate IS NOT NULL
    println!("\n=== Example 12: SELECT with IS NOT NULL ===");
    let query = format!("SELECT * FROM {} WHERE id IS NOT NULL LIMIT 5", table_name);
    println!("Query: {}", query);

    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        Ok(rows) => {
            println!("Found {} rows with non-NULL id:", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("\nQuery examples completed!");
    Ok(())
}
