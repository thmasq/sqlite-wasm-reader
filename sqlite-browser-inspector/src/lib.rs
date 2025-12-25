use sqlite_wasm_reader::{Database, SelectQuery};
use std::io::Cursor;
use wasm_bindgen::prelude::*;

// Expose this function to JavaScript
#[wasm_bindgen]
pub fn inspect_db(file_data: &[u8]) -> Result<String, String> {
    // 1. Wrap the raw byte array in a generic Cursor
    //    This provides the Read + Seek traits required by Database::new
    let cursor = Cursor::new(file_data);

    // 2. Initialize the Database
    let mut db = Database::new(cursor).map_err(|e| format!("Failed to open DB: {}", e))?;

    // 3. Perform operations (e.g., list tables)
    let tables = db
        .tables()
        .map_err(|e| format!("Failed to list tables: {}", e))?;

    let mut report = String::from("=== Database Inspection ===\n\n");

    if tables.is_empty() {
        report.push_str("No tables found.\n");
    }

    for table in tables {
        report.push_str(&format!("Table: {}\n", table));

        // Count rows
        match db.count_table_rows(&table) {
            Ok(count) => report.push_str(&format!("  Row Count: {}\n", count)),
            Err(e) => report.push_str(&format!("  Error counting rows: {}\n", e)),
        }

        // Preview first 3 rows
        let query = SelectQuery::new(&table).with_limit(3);
        match db.execute_query(&query) {
            Ok(rows) => {
                if !rows.is_empty() {
                    report.push_str("  Sample Data:\n");
                    for (i, row) in rows.iter().enumerate() {
                        report.push_str(&format!("    Row {}: {:?}\n", i + 1, row));
                    }
                }
            }
            Err(e) => report.push_str(&format!("  Error querying data: {}\n", e)),
        }
        report.push_str("\n");
    }

    Ok(report)
}
