//! SQL query parsing and execution for SELECT statements

use crate::{Error, Result, Row, Value};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SqlValue,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{boxed::Box, format, string::String, vec::Vec};

/// Represents a parsed SELECT query
#[derive(Debug, Clone)]
pub struct SelectQuery {
    /// Columns to select (None means SELECT *)
    pub columns: Option<Vec<String>>,
    /// Table name
    pub table: String,
    /// WHERE clause root expression
    pub where_expr: Option<Expr>,
    /// ORDER BY clause
    pub order_by: Option<OrderBy>,
    /// LIMIT clause
    pub limit: Option<usize>,
}

/// Expression for WHERE clause
#[derive(Debug, Clone)]
pub enum Expr {
    /// Comparison: column op value
    Comparison {
        column: String,
        operator: ComparisonOperator,
        value: Value,
    },
    /// Logical AND
    And(Box<Self>, Box<Self>),
    /// Logical OR
    Or(Box<Self>, Box<Self>),
    /// Logical NOT
    Not(Box<Self>),
    /// IS NULL
    IsNull(String),
    /// IS NOT NULL
    IsNotNull(String),
    /// IN (list of values)
    In { column: String, values: Vec<Value> },
    /// BETWEEN (range check)
    Between {
        column: String,
        low: Value,
        high: Value,
    },
}

/// Comparison operators for WHERE clauses
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
}

/// ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub ascending: bool,
}

// -----------------------------------------------------------------------------
// Helper constructors & combinators for `Expr`
// -----------------------------------------------------------------------------
impl Expr {
    /// Create `column = value` comparison expression
    pub fn eq(column: impl Into<String>, value: Value) -> Self {
        Self::Comparison {
            column: column.into(),
            operator: ComparisonOperator::Equal,
            value,
        }
    }

    /// Create `column != value` comparison expression
    pub fn ne(column: impl Into<String>, value: Value) -> Self {
        Self::Comparison {
            column: column.into(),
            operator: ComparisonOperator::NotEqual,
            value,
        }
    }

    /// Create `column < value` comparison expression
    pub fn lt(column: impl Into<String>, value: Value) -> Self {
        Self::Comparison {
            column: column.into(),
            operator: ComparisonOperator::LessThan,
            value,
        }
    }

    /// Create `column <= value` comparison expression
    pub fn le(column: impl Into<String>, value: Value) -> Self {
        Self::Comparison {
            column: column.into(),
            operator: ComparisonOperator::LessThanOrEqual,
            value,
        }
    }

    /// Create `column > value` comparison expression
    pub fn gt(column: impl Into<String>, value: Value) -> Self {
        Self::Comparison {
            column: column.into(),
            operator: ComparisonOperator::GreaterThan,
            value,
        }
    }

    /// Create `column >= value` comparison expression
    pub fn ge(column: impl Into<String>, value: Value) -> Self {
        Self::Comparison {
            column: column.into(),
            operator: ComparisonOperator::GreaterThanOrEqual,
            value,
        }
    }

    /// Create `column LIKE value` expression
    pub fn like(column: impl Into<String>, value: Value) -> Self {
        Self::Comparison {
            column: column.into(),
            operator: ComparisonOperator::Like,
            value,
        }
    }

    /// Create `column IS NULL` expression
    pub fn is_null(column: impl Into<String>) -> Self {
        Self::IsNull(column.into())
    }

    /// Create `column IS NOT NULL` expression
    pub fn is_not_null(column: impl Into<String>) -> Self {
        Self::IsNotNull(column.into())
    }

    /// Create `column IN (values...)` expression
    pub fn in_values(column: impl Into<String>, values: Vec<Value>) -> Self {
        Self::In {
            column: column.into(),
            values,
        }
    }

    /// Create `column BETWEEN low AND high` expression
    pub fn between(column: impl Into<String>, low: Value, high: Value) -> Self {
        Self::Between {
            column: column.into(),
            low,
            high,
        }
    }

    /// Logical AND: `self AND other`
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    /// Logical OR: `self OR other`
    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }

    /// Logical NOT: `NOT self`
    #[must_use]
    pub fn not(self) -> Self {
        Self::Not(Box::new(self))
    }
}

impl SelectQuery {
    /// Parse a SELECT SQL statement using sqlparser
    pub fn parse(sql: &str) -> Result<Self> {
        let dialect = SQLiteDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::QueryError(format!("SQL parse error: {e}")))?;

        if statements.len() != 1 {
            return Err(Error::QueryError(
                "Expected a single SELECT statement".to_string(),
            ));
        }

        if let Statement::Query(query) = &statements[0] {
            Self::from_sqlparser_query(query)
        } else {
            Err(Error::QueryError(
                "Only SELECT statements are supported".to_string(),
            ))
        }
    }

    fn from_sqlparser_query(query: &Query) -> Result<Self> {
        let SetExpr::Select(select) = &*query.body else {
            return Err(Error::QueryError("Unsupported query type".to_string()));
        };

        let table = Self::parse_table_name(select)?;
        let columns = Self::parse_columns(&select.projection)?;
        let where_expr = if let Some(expr) = &select.selection {
            Some(Self::parse_where_expr(expr)?)
        } else {
            None
        };

        let order_by = Self::parse_order_by(query.order_by.as_ref())?;
        let limit = Self::parse_limit(query.limit_clause.as_ref())?;

        Ok(Self {
            columns,
            table,
            where_expr,
            order_by,
            limit,
        })
    }

    fn parse_table_name(select: &Select) -> Result<String> {
        if select.from.len() != 1 {
            return Err(Error::QueryError(
                "Query must involve exactly one table".to_string(),
            ));
        }
        let table = &select.from[0];
        if !table.joins.is_empty() {
            return Err(Error::QueryError("JOINs are not supported".to_string()));
        }
        if let TableFactor::Table { name, .. } = &table.relation {
            Ok(name
                .0
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
                .join("."))
        } else {
            Err(Error::QueryError("Unsupported table factor".to_string()))
        }
    }

    fn parse_columns(projection: &[SelectItem]) -> Result<Option<Vec<String>>> {
        if projection.len() == 1
            && let SelectItem::Wildcard(_) = &projection[0]
        {
            return Ok(None);
        }

        let mut columns = Vec::new();
        for item in projection {
            if let SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) = item {
                columns.push(ident.value.clone());
            } else {
                return Err(Error::QueryError(
                    "Unsupported column expression".to_string(),
                ));
            }
        }
        Ok(Some(columns))
    }

    fn parse_where_expr(expr: &SqlExpr) -> Result<Expr> {
        match expr {
            SqlExpr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Ok(Expr::And(
                    Box::new(Self::parse_where_expr(left)?),
                    Box::new(Self::parse_where_expr(right)?),
                )),
                BinaryOperator::Or => Ok(Expr::Or(
                    Box::new(Self::parse_where_expr(left)?),
                    Box::new(Self::parse_where_expr(right)?),
                )),
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => Self::parse_comparison_expr(expr),
                _ => Err(Error::QueryError(format!("Unsupported operator: {op:?}"))),
            },
            SqlExpr::IsNull(expr) => {
                if let SqlExpr::Identifier(ident) = &**expr {
                    Ok(Expr::IsNull(ident.value.clone()))
                } else {
                    Err(Error::QueryError(
                        "Expected column name before IS NULL".to_string(),
                    ))
                }
            }
            SqlExpr::IsNotNull(expr) => {
                if let SqlExpr::Identifier(ident) = &**expr {
                    Ok(Expr::IsNotNull(ident.value.clone()))
                } else {
                    Err(Error::QueryError(
                        "Expected column name before IS NOT NULL".to_string(),
                    ))
                }
            }
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                if *negated {
                    return Err(Error::QueryError("NOT LIKE is not supported".to_string()));
                }
                if let (SqlExpr::Identifier(ident), value_expr) = (&**expr, &**pattern) {
                    let value = Self::parse_sql_value(value_expr)?;
                    Ok(Expr::Comparison {
                        column: ident.value.clone(),
                        operator: ComparisonOperator::Like,
                        value,
                    })
                } else {
                    Err(Error::QueryError(
                        "Expected column LIKE 'pattern'".to_string(),
                    ))
                }
            }
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => {
                if *negated {
                    return Err(Error::QueryError("NOT IN is not supported".to_string()));
                }
                if let SqlExpr::Identifier(ident) = &**expr {
                    let mut values = Vec::new();
                    for item in list {
                        values.push(Self::parse_sql_value(item)?);
                    }
                    Ok(Expr::In {
                        column: ident.value.clone(),
                        values,
                    })
                } else {
                    Err(Error::QueryError(
                        "Expected column name before IN".to_string(),
                    ))
                }
            }
            SqlExpr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if *negated {
                    return Err(Error::QueryError(
                        "NOT BETWEEN is not supported".to_string(),
                    ));
                }
                if let SqlExpr::Identifier(ident) = &**expr {
                    let low_value = Self::parse_sql_value(low)?;
                    let high_value = Self::parse_sql_value(high)?;
                    Ok(Expr::Between {
                        column: ident.value.clone(),
                        low: low_value,
                        high: high_value,
                    })
                } else {
                    Err(Error::QueryError(
                        "Expected column name before BETWEEN".to_string(),
                    ))
                }
            }
            SqlExpr::Nested(expr) => Self::parse_where_expr(expr),
            _ => Err(Error::QueryError(format!(
                "Unsupported expression: {expr:?}"
            ))),
        }
    }

    fn parse_comparison_expr(expr: &SqlExpr) -> Result<Expr> {
        if let SqlExpr::BinaryOp { left, op, right } = expr {
            let operator = match op {
                BinaryOperator::Eq => ComparisonOperator::Equal,
                BinaryOperator::NotEq => ComparisonOperator::NotEqual,
                BinaryOperator::Lt => ComparisonOperator::LessThan,
                BinaryOperator::LtEq => ComparisonOperator::LessThanOrEqual,
                BinaryOperator::Gt => ComparisonOperator::GreaterThan,
                BinaryOperator::GtEq => ComparisonOperator::GreaterThanOrEqual,
                _ => {
                    return Err(Error::QueryError(format!(
                        "Unsupported comparison operator: {op:?}"
                    )));
                }
            };

            // Handle both column = value and value = column
            match (&**left, &**right) {
                (SqlExpr::Identifier(ident), value) => {
                    let value = Self::parse_sql_value(value)?;
                    Ok(Expr::Comparison {
                        column: ident.value.clone(),
                        operator,
                        value,
                    })
                }
                (value, SqlExpr::Identifier(ident)) => {
                    // For non-equality operators, we need to reverse the operator
                    let operator = match operator {
                        ComparisonOperator::LessThan => ComparisonOperator::GreaterThan,
                        ComparisonOperator::LessThanOrEqual => {
                            ComparisonOperator::GreaterThanOrEqual
                        }
                        ComparisonOperator::GreaterThan => ComparisonOperator::LessThan,
                        ComparisonOperator::GreaterThanOrEqual => {
                            ComparisonOperator::LessThanOrEqual
                        }
                        _ => operator, // For =, != the order doesn't matter
                    };
                    let value = Self::parse_sql_value(value)?;
                    Ok(Expr::Comparison {
                        column: ident.value.clone(),
                        operator,
                        value,
                    })
                }
                _ => Err(Error::QueryError(
                    "Expected column = value comparison".to_string(),
                )),
            }
        } else {
            Err(Error::QueryError(
                "Expected comparison expression".to_string(),
            ))
        }
    }

    fn parse_sql_value(sql_value: &SqlExpr) -> Result<Value> {
        match sql_value {
            SqlExpr::Value(value_with_span) => match &value_with_span.value {
                SqlValue::Number(s, _) => {
                    if s.contains('.') {
                        s.parse::<f64>()
                            .map(Value::Real)
                            .map_err(|_| Error::QueryError("Invalid float value".to_string()))
                    } else {
                        s.parse::<i64>()
                            .map(Value::Integer)
                            .map_err(|_| Error::QueryError("Invalid integer value".to_string()))
                    }
                }
                SqlValue::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
                SqlValue::DoubleQuotedString(s) => Ok(Value::Text(s.clone())),
                SqlValue::Null => Ok(Value::Null),
                _ => Err(Error::QueryError("Unsupported value type".to_string())),
            },
            SqlExpr::Identifier(ident) => Ok(Value::Text(ident.value.clone())),
            _ => Err(Error::QueryError(format!(
                "Expected a literal value, found {sql_value:?}"
            ))),
        }
    }

    fn parse_order_by(order_by: Option<&sqlparser::ast::OrderBy>) -> Result<Option<OrderBy>> {
        if let Some(order_by) = order_by {
            // In sqlparser 0.57.0, OrderBy has a 'kind' field
            match &order_by.kind {
                sqlparser::ast::OrderByKind::Expressions(expressions) => {
                    // Take the first expression for now (we could extend this to support multiple later)
                    if let Some(order_expr) = expressions.first() {
                        // Extract column name from the expression
                        let column = match &order_expr.expr {
                            sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                            _ => {
                                return Err(Error::QueryError(
                                    "Unsupported ORDER BY expression".to_string(),
                                ));
                            }
                        };

                        // Extract sort direction
                        let ascending = order_expr.options.asc.unwrap_or(true);

                        Ok(Some(OrderBy { column, ascending }))
                    } else {
                        Ok(None)
                    }
                }
                sqlparser::ast::OrderByKind::All(_) => {
                    Err(Error::QueryError("Unsupported ORDER BY kind".to_string()))
                }
            }
        } else {
            Ok(None)
        }
    }

    fn parse_limit(limit_clause: Option<&sqlparser::ast::LimitClause>) -> Result<Option<usize>> {
        if let Some(limit_clause) = limit_clause {
            // In sqlparser 0.57.0, LimitClause can be different types
            match limit_clause {
                sqlparser::ast::LimitClause::LimitOffset { limit, .. } => {
                    if let Some(limit_expr) = limit {
                        // Extract the limit value
                        let limit_value = Self::parse_sql_value(limit_expr)?;
                        match limit_value {
                            Value::Integer(n) => Ok(Some(n as usize)),
                            _ => Err(Error::QueryError("LIMIT must be an integer".to_string())),
                        }
                    } else {
                        Ok(None)
                    }
                }
                sqlparser::ast::LimitClause::OffsetCommaLimit { .. } => Err(Error::QueryError(
                    "Unsupported LIMIT clause type".to_string(),
                )),
            }
        } else {
            Ok(None)
        }
    }
}

impl SelectQuery {
    /// Execute the query against the provided rows
    pub fn execute(&self, mut rows: Vec<Row>, all_columns: &[String]) -> Result<Vec<Row>> {
        // Apply WHERE conditions
        rows = self.apply_where_conditions(rows);

        // Apply ORDER BY
        if let Some(ref order_by) = self.order_by {
            rows = Self::apply_order_by(rows, order_by);
        }

        // Apply column selection
        rows = self.apply_column_selection(rows, all_columns)?;

        // Apply LIMIT
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }

        Ok(rows)
    }

    /// Apply WHERE conditions to filter rows
    fn apply_where_conditions(&self, rows: Vec<Row>) -> Vec<Row> {
        if self.where_expr.is_none() {
            return rows;
        }

        let total_rows = rows.len();
        let filtered_rows: Vec<Row> = rows
            .into_iter()
            .filter(|row| Self::evaluate_expr(row, self.where_expr.as_ref().unwrap()))
            .collect();

        // Add debug logging for WHERE clause filtering
        crate::logging::log_debug(&format!(
            "WHERE clause filtered {} rows from {} total rows",
            filtered_rows.len(),
            total_rows
        ));

        filtered_rows
    }

    /// Evaluate a WHERE expression against a row
    #[must_use]
    pub fn evaluate_expr(row: &Row, expr: &Expr) -> bool {
        match expr {
            Expr::Comparison {
                column,
                operator,
                value,
            } => {
                let Some(row_value) = row.get(column.as_str()) else {
                    return false;
                };

                match operator {
                    ComparisonOperator::Equal => Self::values_equal(row_value, value),
                    ComparisonOperator::NotEqual => !Self::values_equal(row_value, value),
                    ComparisonOperator::LessThan => Self::value_less_than(row_value, value),
                    ComparisonOperator::LessThanOrEqual => {
                        Self::value_less_than(row_value, value)
                            || Self::values_equal(row_value, value)
                    }
                    ComparisonOperator::GreaterThan => {
                        !Self::value_less_than(row_value, value)
                            && !Self::values_equal(row_value, value)
                    }
                    ComparisonOperator::GreaterThanOrEqual => {
                        !Self::value_less_than(row_value, value)
                    }
                    ComparisonOperator::Like => Self::value_like(row_value, value),
                }
            }
            Expr::And(left, right) => {
                Self::evaluate_expr(row, left) && Self::evaluate_expr(row, right)
            }
            Expr::Or(left, right) => {
                Self::evaluate_expr(row, left) || Self::evaluate_expr(row, right)
            }
            Expr::Not(expr) => !Self::evaluate_expr(row, expr),
            Expr::IsNull(column) => row
                .get(column.as_str())
                .is_some_and(super::value::Value::is_null),
            Expr::IsNotNull(column) => row.get(column.as_str()).is_some_and(|v| !v.is_null()),
            Expr::In { column, values } => {
                let row_value = row.get(column.as_str()).cloned().unwrap_or(Value::Null);
                values.iter().any(|v| Self::values_equal(&row_value, v))
            }
            Expr::Between { column, low, high } => {
                let Some(row_value) = row.get(column.as_str()) else {
                    return false;
                };

                // Check if row_value >= low AND row_value <= high
                (Self::values_equal(row_value, low) || !Self::value_less_than(row_value, low))
                    && (Self::values_equal(row_value, high)
                        || Self::value_less_than(row_value, high))
            }
        }
    }

    /// Compare two values for equality
    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => (a - b).abs() < f64::EPSILON,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            // Type coercion
            (Value::Integer(a), Value::Real(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (Value::Real(a), Value::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
            _ => false,
        }
    }

    /// Check if value a is less than value b
    fn value_less_than(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a < b,
            (Value::Real(a), Value::Real(b)) => a < b,
            (Value::Text(a), Value::Text(b)) => a < b,
            (Value::Integer(a), Value::Real(b)) => (*a as f64) < *b,
            (Value::Real(a), Value::Integer(b)) => *a < (*b as f64),
            _ => false,
        }
    }

    /// Check if value matches LIKE pattern (improved implementation)
    fn value_like(value: &Value, pattern: &Value) -> bool {
        match (value, pattern) {
            (Value::Text(text), Value::Text(pattern)) => {
                // Improved LIKE implementation with % wildcard
                if pattern.contains('%') {
                    let pattern_parts: Vec<&str> = pattern.split('%').collect();

                    // Handle simple cases like 'prefix%', '%suffix', '%middle%'
                    match pattern_parts.len() {
                        2 => {
                            let prefix = pattern_parts[0];
                            let suffix = pattern_parts[1];

                            // Handle 'prefix%' case (suffix is empty)
                            if suffix.is_empty() {
                                return text.starts_with(prefix);
                            }
                            // Handle '%suffix' case (prefix is empty)
                            if prefix.is_empty() {
                                return text.ends_with(suffix);
                            }
                            // Handle 'prefix%suffix' case
                            text.starts_with(prefix)
                                && text.ends_with(suffix)
                                && text.len() >= prefix.len() + suffix.len()
                        }
                        1 => {
                            // No % found, exact match
                            text == pattern
                        }
                        3 => {
                            // Handle '%middle%' case
                            let prefix = pattern_parts[0];
                            let middle = pattern_parts[1];
                            let suffix = pattern_parts[2];

                            if prefix.is_empty() && suffix.is_empty() {
                                // Pattern is '%middle%' - check if text contains middle
                                return text.contains(middle);
                            }
                            // More complex patterns - fall back to basic matching
                            text.starts_with(prefix)
                                && text.contains(middle)
                                && text.ends_with(suffix)
                        }
                        _ => {
                            // Multiple % wildcards - more complex pattern
                            // For now, do a simple contains check for each non-empty part
                            for part in pattern_parts {
                                if !part.is_empty() && !text.contains(part) {
                                    return false;
                                }
                            }
                            true
                        }
                    }
                } else {
                    // No wildcards, exact match
                    text == pattern
                }
            }
            _ => false,
        }
    }

    /// Apply ORDER BY to sort rows
    fn apply_order_by(mut rows: Vec<Row>, order_by: &OrderBy) -> Vec<Row> {
        rows.sort_by(|a, b| {
            let val_a = a.get(order_by.column.as_str());
            let val_b = b.get(order_by.column.as_str());

            let cmp = match (val_a, val_b) {
                (Some(a), Some(b)) => Self::compare_values(a, b),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            };

            if order_by.ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });

        rows
    }

    /// Compare two values for ordering
    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Integer(a), Value::Real(b)) => (*a as f64)
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal),
            (Value::Real(a), Value::Integer(b)) => a
                .partial_cmp(&(*b as f64))
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => std::cmp::Ordering::Equal,
        }
    }

    /// Apply column selection (SELECT specific columns or *)
    fn apply_column_selection(&self, rows: Vec<Row>, all_columns: &[String]) -> Result<Vec<Row>> {
        match &self.columns {
            None => Ok(rows), // SELECT * - return all columns
            Some(selected_columns) => {
                let mut result_rows = Vec::new();

                for row in rows {
                    let mut new_row = HashMap::new();

                    for column in selected_columns {
                        if !all_columns.contains(column) {
                            return Err(Error::ColumnNotFound(column.clone()));
                        }

                        let value = row.get(column.as_str()).cloned().unwrap_or(Value::Null);
                        new_row.insert(column.clone(), value);
                    }

                    result_rows.push(new_row);
                }

                Ok(result_rows)
            }
        }
    }
}

impl SelectQuery {
    /// Create a new `SelectQuery` for the given `table` with default values (SELECT *)
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            columns: None,
            table: table.into(),
            where_expr: None,
            order_by: None,
            limit: None,
        }
    }

    /// Specify the columns to select (equivalent to the projection in SQL).
    /// Passing an empty vector is the same as `SELECT *`.
    #[must_use]
    pub fn select_columns(mut self, columns: Vec<String>) -> Self {
        if columns.is_empty() {
            self.columns = None;
        } else {
            self.columns = Some(columns);
        }
        self
    }

    /// Attach a WHERE expression to the query.
    #[must_use]
    pub fn with_where(mut self, expr: Expr) -> Self {
        self.where_expr = Some(expr);
        self
    }

    /// Attach an ORDER BY clause to the query.
    #[must_use]
    pub fn with_order_by(mut self, column: impl Into<String>, ascending: bool) -> Self {
        self.order_by = Some(OrderBy {
            column: column.into(),
            ascending,
        });
        self
    }

    /// Attach a LIMIT clause to the query.
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let query = SelectQuery::parse("SELECT * FROM users").unwrap();
        assert_eq!(query.table, "users");
        assert!(query.columns.is_none());
        assert!(query.where_expr.is_none());
    }

    #[test]
    fn test_parse_select_with_columns() {
        let query = SelectQuery::parse("SELECT name, email FROM users").unwrap();
        assert_eq!(query.table, "users");
        assert_eq!(
            query.columns.as_ref().unwrap(),
            &vec!["name".to_string(), "email".to_string()]
        );
    }

    #[test]
    fn test_parse_select_with_where() {
        let query = SelectQuery::parse("SELECT * FROM users WHERE age > 18").unwrap();
        assert_eq!(query.table, "users");
        assert!(query.where_expr.is_some());
        let expr = query.where_expr.as_ref().unwrap();
        if let Expr::Comparison {
            column, operator, ..
        } = expr
        {
            assert_eq!(column, "age");
            assert_eq!(operator, &ComparisonOperator::GreaterThan);
        } else {
            panic!("Expected Comparison expr");
        }
    }

    #[test]
    fn test_parse_select_with_order_by() {
        let query = SelectQuery::parse("SELECT * FROM users ORDER BY name ASC").unwrap();
        assert_eq!(query.table, "users");
        assert!(query.order_by.is_some());
        let order_by = query.order_by.unwrap();
        assert_eq!(order_by.column, "name");
        assert!(order_by.ascending);
    }

    #[test]
    fn test_parse_select_with_limit() {
        let query = SelectQuery::parse("SELECT * FROM users LIMIT 10").unwrap();
        assert_eq!(query.table, "users");
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_like_pattern_matching() {
        // Test prefix pattern 'f%'
        assert!(SelectQuery::value_like(
            &Value::Text("foo".to_string()),
            &Value::Text("f%".to_string())
        ));
        assert!(SelectQuery::value_like(
            &Value::Text("ff736190-1479-4681-b9b2-78757cd55821".to_string()),
            &Value::Text("f%".to_string())
        ));
        assert!(SelectQuery::value_like(
            &Value::Text("fa18fc4d-11dc-466b-84cd-d6793ff93774".to_string()),
            &Value::Text("f%".to_string())
        ));
        assert!(!SelectQuery::value_like(
            &Value::Text("bar".to_string()),
            &Value::Text("f%".to_string())
        ));

        // Test suffix pattern '%bar'
        assert!(SelectQuery::value_like(
            &Value::Text("foobar".to_string()),
            &Value::Text("%bar".to_string())
        ));
        assert!(!SelectQuery::value_like(
            &Value::Text("foo".to_string()),
            &Value::Text("%bar".to_string())
        ));

        // Test contains pattern '%middle%'
        assert!(SelectQuery::value_like(
            &Value::Text("foo middle bar".to_string()),
            &Value::Text("%middle%".to_string())
        ));
        assert!(!SelectQuery::value_like(
            &Value::Text("foo bar".to_string()),
            &Value::Text("%middle%".to_string())
        ));

        // Test exact match (no wildcards)
        assert!(SelectQuery::value_like(
            &Value::Text("exact".to_string()),
            &Value::Text("exact".to_string())
        ));
        assert!(!SelectQuery::value_like(
            &Value::Text("different".to_string()),
            &Value::Text("exact".to_string())
        ));
    }

    #[test]
    fn test_parse_select_with_between() {
        let query = SelectQuery::parse("SELECT * FROM users WHERE age BETWEEN 18 AND 65").unwrap();
        assert_eq!(query.table, "users");
        assert!(query.where_expr.is_some());

        let expr = query.where_expr.as_ref().unwrap();
        if let Expr::Between { column, low, high } = expr {
            assert_eq!(column, "age");
            assert_eq!(low, &Value::Integer(18));
            assert_eq!(high, &Value::Integer(65));
        } else {
            panic!("Expected Between expr, got {:?}", expr);
        }
    }

    #[test]
    fn test_between_evaluation() {
        let mut row = std::collections::HashMap::new();

        // Test integer BETWEEN
        row.insert("age".to_string(), Value::Integer(25));
        let expr = Expr::Between {
            column: "age".to_string(),
            low: Value::Integer(18),
            high: Value::Integer(65),
        };
        assert!(SelectQuery::evaluate_expr(&row, &expr));

        // Test value outside range (too low)
        row.insert("age".to_string(), Value::Integer(15));
        assert!(!SelectQuery::evaluate_expr(&row, &expr));

        // Test value outside range (too high)
        row.insert("age".to_string(), Value::Integer(70));
        assert!(!SelectQuery::evaluate_expr(&row, &expr));

        // Test boundary values
        row.insert("age".to_string(), Value::Integer(18));
        assert!(SelectQuery::evaluate_expr(&row, &expr));

        row.insert("age".to_string(), Value::Integer(65));
        assert!(SelectQuery::evaluate_expr(&row, &expr));

        // Test float BETWEEN
        row.insert("score".to_string(), Value::Real(85.5));
        let expr = Expr::Between {
            column: "score".to_string(),
            low: Value::Real(80.0),
            high: Value::Real(90.0),
        };
        assert!(SelectQuery::evaluate_expr(&row, &expr));

        // Test mixed types (integer value, real bounds)
        row.insert("score".to_string(), Value::Integer(85));
        assert!(SelectQuery::evaluate_expr(&row, &expr));
    }

    #[test]
    fn test_between_builder_api() {
        let query = SelectQuery::new("users").with_where(Expr::between(
            "age",
            Value::Integer(18),
            Value::Integer(65),
        ));

        assert_eq!(query.table, "users");
        assert!(query.where_expr.is_some());

        if let Some(Expr::Between { column, low, high }) = &query.where_expr {
            assert_eq!(column, "age");
            assert_eq!(low, &Value::Integer(18));
            assert_eq!(high, &Value::Integer(65));
        } else {
            panic!("Expected Between expr");
        }
    }
}
