use sqlite_wasm_reader::{Database, Row, SelectQuery, Value};
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Inspector {
    db: Database<Cursor<Vec<u8>>>,
    // The master copy of all data
    cached_rows: Vec<Row>,
    cached_columns: Vec<String>,
    // A list of pointers (indices) to rows that match the current filter
    filtered_indices: Vec<usize>,
}

#[wasm_bindgen]
impl Inspector {
    #[wasm_bindgen(constructor)]
    pub fn new(file_data: Vec<u8>) -> Result<Inspector, String> {
        let cursor = Cursor::new(file_data);
        let db = Database::new(cursor).map_err(|e| format!("Failed to open DB: {}", e))?;
        Ok(Inspector {
            db,
            cached_rows: Vec::new(),
            cached_columns: Vec::new(),
            filtered_indices: Vec::new(),
        })
    }

    pub fn get_tables(&mut self) -> Result<Box<[JsValue]>, String> {
        let mut tables = self
            .db
            .tables()
            .map_err(|e| format!("Failed to list tables: {}", e))?;
        tables.sort();
        let js_array: Vec<JsValue> = tables.into_iter().map(JsValue::from).collect();
        Ok(js_array.into_boxed_slice())
    }

    pub fn load_table(&mut self, table_name: &str) -> Result<usize, String> {
        self.cached_columns = self
            .db
            .get_table_columns(table_name)
            .map_err(|e| format!("Failed to get columns: {}", e))?;

        let query = SelectQuery::new(table_name);
        self.cached_rows = self
            .db
            .execute_query(&query)
            .map_err(|e| format!("Failed to query data: {}", e))?;

        // Reset filter to show all rows (0..n)
        self.filtered_indices = (0..self.cached_rows.len()).collect();

        Ok(self.filtered_indices.len())
    }

    // NEW: Search function that updates the view
    pub fn apply_filter(&mut self, query: &str) -> usize {
        if query.is_empty() {
            // Restore full view
            if self.filtered_indices.len() != self.cached_rows.len() {
                self.filtered_indices = (0..self.cached_rows.len()).collect();
            }
        } else {
            let q_lower = query.to_lowercase();
            // Filter cached_rows and store the indices of matches
            self.filtered_indices = self
                .cached_rows
                .iter()
                .enumerate()
                .filter(|(_, row)| {
                    // Check all columns for a match
                    for col in &self.cached_columns {
                        let val = row.get(col).unwrap_or(&Value::Null);
                        let text = match val {
                            Value::Text(s) => s.to_lowercase(),
                            Value::Integer(i) => i.to_string(),
                            Value::Real(f) => f.to_string(),
                            _ => String::new(),
                        };
                        if text.contains(&q_lower) {
                            return true;
                        }
                    }
                    false
                })
                .map(|(index, _)| index)
                .collect();
        }

        self.filtered_indices.len()
    }

    pub fn get_columns(&self) -> Result<String, String> {
        let mut json = String::from("[");
        for (i, col) in self.cached_columns.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            json.push_str(&format!("{:?}", col));
        }
        json.push(']');
        Ok(json)
    }

    pub fn get_rows_slice(&self, start: usize, count: usize) -> Result<String, String> {
        // Use filtered_indices to determine which rows to return
        let end = (start + count).min(self.filtered_indices.len());
        if start >= self.filtered_indices.len() {
            return Ok("[]".to_string());
        }

        let slice_indices = &self.filtered_indices[start..end];
        let mut json = String::from("[");

        for (i, &original_index) in slice_indices.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }

            // Start row array
            json.push('[');

            // FIRST ELEMENT IS ALWAYS THE ORIGINAL ROW INDEX (1-based for display)
            json.push_str(&(original_index + 1).to_string());
            json.push(',');

            let row = &self.cached_rows[original_index];

            for (j, col_name) in self.cached_columns.iter().enumerate() {
                if j > 0 {
                    json.push(',');
                }
                match row.get(col_name).unwrap_or(&Value::Null) {
                    Value::Null => json.push_str("null"),
                    Value::Integer(v) => json.push_str(&v.to_string()),
                    Value::Real(v) => json.push_str(&v.to_string()),
                    Value::Text(v) => json.push_str(&format!("{:?}", v)),
                    Value::Blob(_) => json.push_str("\"<blob>\""),
                }
            }
            json.push(']');
        }
        json.push(']');
        Ok(json)
    }
}
