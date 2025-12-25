//! Integration tests for sqlite_wasm_reader

use sqlite_wasm_reader::{Database, Error, SelectQuery, Value};
use std::fs;
use std::process::Command;

/// Create a simple test SQLite database using sqlite3 command
fn create_test_db(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Use sqlite3 command to create a test database
    let output = Command::new("sqlite3")
        .arg(path)
        .arg(
            "
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER,
                score REAL,
                data BLOB
            );
            
            INSERT INTO users (id, name, email, age, score, data) VALUES 
                (1, 'Alice', 'alice@example.com', 25, 95.5, X'01020304'),
                (2, 'Bob', 'bob@example.com', 30, 87.2, NULL),
                (3, 'Charlie', 'charlie@example.com', 35, 92.1, X'05060708'),
                (4, 'Diana', NULL, 28, 88.9, NULL),
                (5, 'Eve', 'eve@example.com', 22, 96.7, X'090A0B0C');
            
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL,
                category TEXT
            );
            
            INSERT INTO products (id, name, price, category) VALUES 
                (1, 'Laptop', 999.99, 'Electronics'),
                (2, 'Book', 19.99, 'Education'),
                (3, 'Coffee', 4.50, 'Food');
        ",
        )
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to create test database: {}", stderr).into());
    }

    Ok(())
}

/// Clean up test database
fn cleanup_test_db(path: &str) {
    let _ = fs::remove_file(path);
}

#[test]
fn test_open_valid_database() {
    let test_db = "test_valid.db";

    // Create a valid SQLite database
    if let Err(e) = create_test_db(test_db) {
        eprintln!(
            "Skipping test_open_valid_database: sqlite3 command not available: {}",
            e
        );
        return;
    }

    // Test opening the database
    let result = Database::open(test_db);

    // Clean up
    cleanup_test_db(test_db);

    match result {
        Ok(mut db) => {
            // Test listing tables
            let tables = db.tables().expect("Failed to list tables");
            assert!(tables.contains(&"users".to_string()));
            assert!(tables.contains(&"products".to_string()));
            assert_eq!(tables.len(), 2);
        }
        Err(e) => panic!("Expected to open valid database, got error: {:?}", e),
    }
}

#[test]
fn test_read_users_table() {
    let test_db = "test_users.db";

    // Create a valid SQLite database
    if let Err(e) = create_test_db(test_db) {
        eprintln!(
            "Skipping test_read_users_table: sqlite3 command not available: {}",
            e
        );
        return;
    }

    let mut db = Database::open(test_db).expect("Failed to open database");

    // Test reading the users table using execute_query with a WHERE clause
    // Since we need indexes, let's try to query by id which should have an index
    let query =
        SelectQuery::parse("SELECT * FROM users WHERE id >= 1").expect("Failed to parse query");
    let rows = db
        .execute_query(&query)
        .expect("Failed to read users table");

    // Clean up
    cleanup_test_db(test_db);

    // Verify we got the expected number of rows
    assert_eq!(rows.len(), 5);

    // Debug: Print the first row to see what we actually got
    println!("First row: {:?}", rows[0]);

    // Check first row (Alice)
    let alice = &rows[0];
    assert_eq!(alice.get("id"), Some(&Value::Integer(1)));
    assert_eq!(alice.get("name"), Some(&Value::Text("Alice".to_string())));
    assert_eq!(
        alice.get("email"),
        Some(&Value::Text("alice@example.com".to_string()))
    );
    assert_eq!(alice.get("age"), Some(&Value::Integer(25)));
    assert_eq!(alice.get("score"), Some(&Value::Real(95.5)));

    // Check blob data
    if let Some(Value::Blob(data)) = alice.get("data") {
        assert_eq!(data, &[1, 2, 3, 4]);
    } else {
        panic!("Expected blob data for Alice");
    }

    // Check row with NULL email (Diana)
    let diana = &rows[3];
    assert_eq!(diana.get("email"), Some(&Value::Null));
}

#[test]
fn test_read_products_table() {
    let test_db = "test_products.db";

    // Create a valid SQLite database
    if let Err(e) = create_test_db(test_db) {
        eprintln!(
            "Skipping test_read_products_table: sqlite3 command not available: {}",
            e
        );
        return;
    }

    let mut db = Database::open(test_db).expect("Failed to open database");

    // Test reading the products table using execute_query with a WHERE clause
    let query =
        SelectQuery::parse("SELECT * FROM products WHERE id >= 1").expect("Failed to parse query");
    let rows = db
        .execute_query(&query)
        .expect("Failed to read products table");

    // Clean up
    cleanup_test_db(test_db);

    // Verify we got the expected number of rows
    assert_eq!(rows.len(), 3);

    // Check first product (Laptop)
    let laptop = &rows[0];
    assert_eq!(laptop.get("id"), Some(&Value::Integer(1)));
    assert_eq!(laptop.get("name"), Some(&Value::Text("Laptop".to_string())));
    assert_eq!(laptop.get("price"), Some(&Value::Real(999.99)));
    assert_eq!(
        laptop.get("category"),
        Some(&Value::Text("Electronics".to_string()))
    );
}

#[test]
fn test_table_not_found() {
    let test_db = "test_not_found.db";

    // Create a valid SQLite database
    if let Err(e) = create_test_db(test_db) {
        eprintln!(
            "Skipping test_table_not_found: sqlite3 command not available: {}",
            e
        );
        return;
    }

    let mut db = Database::open(test_db).expect("Failed to open database");

    // Test reading a non-existent table
    let query = SelectQuery::parse("SELECT * FROM nonexistent_table");
    let result = query.and_then(|q| db.execute_query(&q));

    // Clean up
    cleanup_test_db(test_db);

    match result {
        Ok(_) => panic!("Expected error for non-existent table"),
        Err(Error::TableNotFound(table_name)) => {
            assert_eq!(table_name, "nonexistent_table");
        }
        Err(e) => panic!("Expected TableNotFound error, got: {:?}", e),
    }
}

#[test]
fn test_open_invalid_file() {
    let test_db = "test_invalid.db";

    // Create an invalid file (not a SQLite database) that's large enough to read header
    let invalid_data = vec![0u8; 100]; // 100 bytes of zeros
    fs::write(test_db, invalid_data).expect("Failed to write test file");

    let result = Database::open(test_db);

    // Clean up
    cleanup_test_db(test_db);

    match result {
        Ok(_) => panic!("Expected error for invalid file"),
        Err(Error::InvalidFormat(_)) => {
            // Expected error for non-SQLite file
        }
        Err(e) => panic!("Expected InvalidFormat error, got: {:?}", e),
    }
}

#[test]
fn test_value_conversions() {
    // Test integer value
    let int_val = Value::Integer(42);
    assert_eq!(int_val.as_integer(), Some(42));
    assert_eq!(int_val.as_real(), Some(42.0));
    assert_eq!(int_val.as_text(), None);
    assert!(!int_val.is_null());

    // Test real value
    let real_val = Value::Real(3.14);
    assert_eq!(real_val.as_real(), Some(3.14));
    assert_eq!(real_val.as_integer(), None);
    assert!(!real_val.is_null());

    // Test text value
    let text_val = Value::Text("Hello".to_string());
    assert_eq!(text_val.as_text(), Some("Hello"));
    assert_eq!(text_val.as_integer(), None);
    assert!(!text_val.is_null());

    // Test null value
    let null_val = Value::Null;
    assert!(null_val.is_null());
    assert_eq!(null_val.as_integer(), None);
    assert_eq!(null_val.as_text(), None);

    // Test blob value
    let blob_val = Value::Blob(vec![1, 2, 3, 4]);
    assert_eq!(blob_val.as_blob(), Some(&[1, 2, 3, 4][..]));
    assert!(!blob_val.is_null());
}

#[test]
fn test_value_display() {
    assert_eq!(Value::Null.to_string(), "NULL");
    assert_eq!(Value::Integer(123).to_string(), "123");
    assert_eq!(Value::Real(45.67).to_string(), "45.67");
    assert_eq!(Value::Text("test".to_string()).to_string(), "test");
    assert_eq!(Value::Blob(vec![1, 2, 3]).to_string(), "BLOB(3 bytes)");
}

#[test]
fn test_value_equality() {
    assert_eq!(Value::Null, Value::Null);
    assert_eq!(Value::Integer(42), Value::Integer(42));
    assert_eq!(Value::Real(3.14), Value::Real(3.14));
    assert_eq!(
        Value::Text("hello".to_string()),
        Value::Text("hello".to_string())
    );
    assert_eq!(Value::Blob(vec![1, 2, 3]), Value::Blob(vec![1, 2, 3]));

    assert_ne!(Value::Null, Value::Integer(0));
    assert_ne!(Value::Integer(42), Value::Integer(43));
    assert_ne!(Value::Real(3.14), Value::Real(3.15));
    assert_ne!(
        Value::Text("hello".to_string()),
        Value::Text("world".to_string())
    );
    assert_ne!(Value::Blob(vec![1, 2, 3]), Value::Blob(vec![1, 2, 4]));
}
