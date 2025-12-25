use sqlite_wasm_reader::{
    Database, Error, LogLevel, SelectQuery, Value, init_default_logger, set_log_level,
};
use std::path::Path;

fn main() -> Result<(), Error> {
    let db_path = "./ExcelDB.db";

    if !Path::new(db_path).exists() {
        eprintln!("Error: Database file '{}' not found.", db_path);
        std::process::exit(1);
    }

    init_default_logger();
    set_log_level(LogLevel::Debug);

    println!("=== ExcelDB.db Reader ===");
    println!("Opening database: {}", db_path);

    let mut db = Database::open(db_path)?;

    let mut tables = db.tables()?;
    tables.sort();

    println!("\nFound {} table(s):", tables.len());

    for (i, table) in tables.iter().enumerate() {
        println!("{}. {}", i + 1, table);
    }

    for table_name in tables {
        println!("\n------------------------------------------------");
        println!("Table: {}", table_name);

        match db.count_table_rows(&table_name) {
            Ok(count) => println!("Total Rows: {}", count),
            Err(e) => println!("Error counting rows: {}", e),
        }

        match db.get_table_columns(&table_name) {
            Ok(cols) => println!("Columns: {}", cols.join(", ")),
            Err(e) => println!("Error getting columns: {}", e),
        }

        println!("Sample Data (First 5 rows):");

        let query = SelectQuery::new(&table_name).with_limit(5);

        match db.execute_query(&query) {
            Ok(rows) => {
                if rows.is_empty() {
                    println!("  (Table is empty)");
                } else {
                    for (row_idx, row) in rows.iter().enumerate() {
                        println!("  Row {}:", row_idx + 1);
                        for (col_name, val) in row {
                            println!("    {}: {}", col_name, format_value(val));
                        }
                    }
                }
            }
            Err(e) => println!("  Error querying data: {}", e),
        }
    }

    println!("\nDone.");
    Ok(())
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => format!("\"{}\"", s),
        Value::Blob(b) => format!("<BLOB {} bytes>", b.len()),
    }
}
