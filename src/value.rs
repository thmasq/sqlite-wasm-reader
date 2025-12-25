//! `SQLite` value types

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{string::String, vec::Vec};

/// Represents a value stored in `SQLite`
// f64 does not implement Eq or Ord, so we must implement them manually.
// We implement PartialOrd and Ord manually to handle NaN values robustly.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// NULL value
    Null,
    /// Integer value
    Integer(i64),
    /// Floating point value
    Real(f64),
    /// Text value
    Text(String),
    /// BLOB value
    Blob(Vec<u8>),
}

// Manual implementation of Eq, required for Ord.
// This is safe as long as we don't have NaN values for f64,
// which is a reasonable assumption for a database.
impl Eq for Value {}

// Manual implementation of PartialOrd to handle NaN values robustly
// Delegates to Ord to ensure consistency and fix the lint
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Value {
    /// Returns true if this value is NULL
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to get this value as an integer
    #[must_use]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get this value as a float
    #[must_use]
    pub const fn as_real(&self) -> Option<f64> {
        match self {
            Self::Real(f) => Some(*f),
            #[allow(clippy::cast_precision_loss)]
            Self::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get this value as text
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get this value as a blob
    #[must_use]
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get this value as a boolean
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Integer(i) => Some(*i != 0),
            _ => None,
        }
    }
}

impl core::fmt::Display for Value {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Real(r) => write!(f, "{r}"),
            Self::Text(s) => write!(f, "{s}"),
            Self::Blob(b) => write!(f, "BLOB({} bytes)", b.len()),
        }
    }
}

// Manual implementation of Ord, required for B-tree key comparisons.
impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            // --- Same Type Comparisons ---
            (Self::Real(a), Self::Real(b)) => {
                // Handle NaN values deterministically
                if a.is_nan() && b.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.is_nan() {
                    std::cmp::Ordering::Less
                } else if b.is_nan() {
                    std::cmp::Ordering::Greater
                } else {
                    // Safe to unwrap because we handled NaNs
                    a.partial_cmp(b).unwrap()
                }
            }
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Blob(a), Self::Blob(b)) => a.cmp(b),
            (Self::Null, Self::Null) => std::cmp::Ordering::Equal,

            // --- Mixed Numeric Comparisons (Integer vs Real) ---
            (Self::Integer(a), Self::Real(b)) => {
                if b.is_nan() {
                    std::cmp::Ordering::Greater
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    (*a as f64)
                        .partial_cmp(b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            }
            (Self::Real(a), Self::Integer(b)) => {
                if a.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    a.partial_cmp(&(*b as f64))
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            }

            // --- Cross-Type Comparisons ---
            // SQLite Order: Null < Numeric (Int/Real) < Text < Blob

            // Cases resulting in Less:
            // 1. Null is less than everything (except Null, handled above)
            // 2. Numeric is less than Text and Blob
            // 3. Text is less than Blob
            (Self::Null, _)
            | (Self::Integer(_) | Self::Real(_), Self::Text(_) | Self::Blob(_))
            | (Self::Text(_), Self::Blob(_)) => std::cmp::Ordering::Less,

            // Cases resulting in Greater:
            // 1. Anything (except Null) is greater than Null
            // 2. Text and Blob are greater than Numeric
            // 3. Blob is greater than Text
            (_, Self::Null)
            | (Self::Text(_) | Self::Blob(_), Self::Integer(_) | Self::Real(_))
            | (Self::Blob(_), Self::Text(_)) => std::cmp::Ordering::Greater,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_creation() {
        let null = Value::Null;
        let int = Value::Integer(42);
        let real = Value::Real(3.14);
        let text = Value::Text("hello".to_string());
        let blob = Value::Blob(vec![1, 2, 3, 4]);

        assert!(matches!(null, Value::Null));
        assert!(matches!(int, Value::Integer(42)));
        assert!(matches!(real, Value::Real(3.14)));
        assert!(matches!(text, Value::Text(ref s) if s == "hello"));
        assert!(matches!(blob, Value::Blob(ref b) if b == &[1, 2, 3, 4]));
    }

    #[test]
    fn test_as_integer() {
        assert_eq!(Value::Integer(42).as_integer(), Some(42));
        assert_eq!(Value::Real(3.14).as_integer(), None);
        assert_eq!(Value::Text("42".to_string()).as_integer(), None);
        assert_eq!(Value::Null.as_integer(), None);
        assert_eq!(Value::Blob(vec![1, 2, 3]).as_integer(), None);
    }

    #[test]
    fn test_as_real() {
        assert_eq!(Value::Real(3.14).as_real(), Some(3.14));
        assert_eq!(Value::Integer(42).as_real(), Some(42.0));
        assert_eq!(Value::Text("3.14".to_string()).as_real(), None);
        assert_eq!(Value::Null.as_real(), None);
        assert_eq!(Value::Blob(vec![1, 2, 3]).as_real(), None);
    }

    #[test]
    fn test_as_text() {
        assert_eq!(Value::Text("hello".to_string()).as_text(), Some("hello"));
        assert_eq!(Value::Integer(42).as_text(), None);
        assert_eq!(Value::Real(3.14).as_text(), None);
        assert_eq!(Value::Null.as_text(), None);
        assert_eq!(Value::Blob(vec![1, 2, 3]).as_text(), None);
    }

    #[test]
    fn test_as_blob() {
        let blob_data = vec![1, 2, 3, 4];
        assert_eq!(
            Value::Blob(blob_data.clone()).as_blob(),
            Some(&blob_data[..])
        );
        assert_eq!(Value::Integer(42).as_blob(), None);
        assert_eq!(Value::Real(3.14).as_blob(), None);
        assert_eq!(Value::Text("hello".to_string()).as_blob(), None);
        assert_eq!(Value::Null.as_blob(), None);
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

        assert_ne!(Value::Null, Value::Integer(42));
        assert_ne!(Value::Integer(42), Value::Integer(43));
        assert_ne!(Value::Real(3.14), Value::Real(3.15));
        assert_ne!(
            Value::Text("hello".to_string()),
            Value::Text("world".to_string())
        );
        assert_ne!(Value::Blob(vec![1, 2, 3]), Value::Blob(vec![1, 2, 4]));
    }

    #[test]
    fn test_value_debug() {
        assert_eq!(format!("{:?}", Value::Null), "Null");
        assert_eq!(format!("{:?}", Value::Integer(42)), "Integer(42)");
        assert_eq!(format!("{:?}", Value::Real(3.14)), "Real(3.14)");
        assert_eq!(
            format!("{:?}", Value::Text("hello".to_string())),
            "Text(\"hello\")"
        );
        assert_eq!(
            format!("{:?}", Value::Blob(vec![1, 2, 3])),
            "Blob([1, 2, 3])"
        );
    }

    #[test]
    fn test_value_clone() {
        let original = Value::Text("hello".to_string());
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_nan_handling() {
        use std::f64;

        let nan = Value::Real(f64::NAN);
        let pos_inf = Value::Real(f64::INFINITY);
        let neg_inf = Value::Real(f64::NEG_INFINITY);
        let normal = Value::Real(3.14);
        let integer = Value::Integer(42);

        // NaN should be less than all other values
        assert!(nan < pos_inf);
        assert!(nan < neg_inf);
        assert!(nan < normal);
        assert!(nan < integer);

        // NaN should equal NaN
        assert_eq!(nan.cmp(&nan), std::cmp::Ordering::Equal);

        // Other values should be greater than NaN
        assert!(pos_inf > nan);
        assert!(neg_inf > nan);
        assert!(normal > nan);
        assert!(integer > nan);

        // Normal comparisons should still work
        assert!(normal < pos_inf);
        assert!(normal > neg_inf);
        assert!(integer > normal); // 42 > 3.14 in SQLite's numeric comparison
    }
}
