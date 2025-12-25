use sqlite_wasm_reader::{Database, SelectQuery, Value};
use std::collections::HashMap;
use std::sync::Once;

static SETUP: Once = Once::new();

fn setup() {
    SETUP.call_once(|| {
        create_test_db();
    });
}

fn create_test_db() {
    let _ = std::fs::remove_file("test_db.sqlite");
    let conn = rusqlite::Connection::open("test_db.sqlite").unwrap();
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)",
        [],
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_age_city ON users (age, city)", [])
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON users (name)", [])
        .unwrap();
    conn.execute("CREATE INDEX idx_city ON users (city)", [])
        .unwrap();
    conn.execute(
        "INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 30, 'New York')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO users (id, name, age, city) VALUES (2, 'Bob', 25, 'Los Angeles')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO users (id, name, age, city) VALUES (3, 'Charlie', 35, 'New York')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO users (id, name, age, city) VALUES (4, 'David', 30, 'Chicago')",
        [],
    )
    .unwrap();
}

#[test]
fn test_or_condition_with_index() {
    setup();
    let mut db = Database::open("test_db.sqlite").unwrap();
    let query = SelectQuery::parse(
        "SELECT name, age, city FROM users WHERE age = 30 OR city = 'Los Angeles'",
    )
    .unwrap();
    let mut result = db.execute_query(&query).unwrap();

    assert_eq!(result.len(), 3);

    result.sort_by(|a, b| a["name"].to_string().cmp(&b["name"].to_string()));

    let expected_rows = vec![
        HashMap::from([
            ("name".to_string(), Value::Text("Alice".to_string())),
            ("age".to_string(), Value::Integer(30)),
            ("city".to_string(), Value::Text("New York".to_string())),
        ]),
        HashMap::from([
            ("name".to_string(), Value::Text("Bob".to_string())),
            ("age".to_string(), Value::Integer(25)),
            ("city".to_string(), Value::Text("Los Angeles".to_string())),
        ]),
        HashMap::from([
            ("name".to_string(), Value::Text("David".to_string())),
            ("age".to_string(), Value::Integer(30)),
            ("city".to_string(), Value::Text("Chicago".to_string())),
        ]),
    ];

    assert_eq!(result, expected_rows);
}

#[test]
fn test_composite_index_prefix_query() {
    setup();
    let mut db = Database::open("test_db.sqlite").unwrap();
    let query = SelectQuery::parse("SELECT name, age, city FROM users WHERE age = 30").unwrap();
    let mut result = db.execute_query(&query).unwrap();

    assert_eq!(result.len(), 2);

    result.sort_by(|a, b| a["name"].to_string().cmp(&b["name"].to_string()));

    let expected_rows = vec![
        HashMap::from([
            ("name".to_string(), Value::Text("Alice".to_string())),
            ("age".to_string(), Value::Integer(30)),
            ("city".to_string(), Value::Text("New York".to_string())),
        ]),
        HashMap::from([
            ("name".to_string(), Value::Text("David".to_string())),
            ("age".to_string(), Value::Integer(30)),
            ("city".to_string(), Value::Text("Chicago".to_string())),
        ]),
    ];

    assert_eq!(result, expected_rows);
}

#[test]
fn test_composite_index_full_key_query() {
    setup();
    let mut db = Database::open("test_db.sqlite").unwrap();
    let query = SelectQuery::parse(
        "SELECT name, age, city FROM users WHERE age = 30 AND city = 'New York'",
    )
    .unwrap();
    let result = db.execute_query(&query).unwrap();

    assert_eq!(result.len(), 1);

    let expected_row = HashMap::from([
        ("name".to_string(), Value::Text("Alice".to_string())),
        ("age".to_string(), Value::Integer(30)),
        ("city".to_string(), Value::Text("New York".to_string())),
    ]);

    assert_eq!(result[0], expected_row);
}

#[test]
fn test_single_column_index_query() {
    setup();
    let mut db = Database::open("test_db.sqlite").unwrap();
    let query =
        SelectQuery::parse("SELECT name, age, city FROM users WHERE name = 'Charlie'").unwrap();
    let result = db.execute_query(&query).unwrap();

    assert_eq!(result.len(), 1);

    let expected_row = HashMap::from([
        ("name".to_string(), Value::Text("Charlie".to_string())),
        ("age".to_string(), Value::Integer(35)),
        ("city".to_string(), Value::Text("New York".to_string())),
    ]);

    assert_eq!(result[0], expected_row);
}
