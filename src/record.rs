//! `SQLite` record parsing

use crate::{Error, Result, Value, btree::read_varint};
use byteorder::{BigEndian, ByteOrder};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{string::String, vec::Vec};

/// Parse a record from payload data
/// # Errors
///
/// Returns an error if:
/// * The header size varint cannot be read or exceeds the payload length (`InvalidRecord`, `InvalidVarint`).
/// * The header size is too large (> 64KB) (`InvalidFormat`).
/// * The record contains too many columns (> 1000) (`InvalidFormat`).
pub fn parse_record(payload: &[u8]) -> Result<Vec<Value>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }

    // Read header size varint
    let (header_size, header_size_bytes) = read_varint(payload)?;
    if header_size as usize > payload.len() {
        return Err(Error::InvalidRecord);
    }

    // Safety check: limit header size to prevent memory issues
    if header_size > 65536 {
        return Err(Error::InvalidFormat(format!(
            "Header size too large: {header_size} bytes"
        )));
    }

    // Read serial types from header with pre-allocation
    let header_end = header_size as usize;
    let mut serial_types = Vec::new();
    let mut offset = header_size_bytes;

    while offset < header_end {
        let (serial_type, bytes_read) = read_varint(&payload[offset..])?;
        serial_types.push(serial_type);
        offset += bytes_read;

        // Safety check
        if serial_types.len() > 1000 {
            return Err(Error::InvalidFormat("Too many columns in record".into()));
        }
    }

    // Pre-allocate values vector
    let mut values = Vec::with_capacity(serial_types.len());
    let mut data_offset = header_end;

    // Parse values with minimal allocations
    for &serial_type in &serial_types {
        if data_offset >= payload.len() {
            values.push(Value::Null);
            continue;
        }

        let (value, bytes_consumed) = parse_value(serial_type, &payload[data_offset..]);
        values.push(value);
        data_offset += bytes_consumed;
    }

    Ok(values)
}

#[must_use]
pub fn parse_value(serial_type: i64, data: &[u8]) -> (Value, usize) {
    match serial_type {
        0 => (Value::Null, 0),
        1 => {
            if data.is_empty() {
                (Value::Integer(0), 0)
            } else {
                (Value::Integer(i64::from(data[0] as i8)), 1)
            }
        }
        2 => {
            if data.len() < 2 {
                (Value::Integer(0), 0)
            } else {
                let value = i64::from(BigEndian::read_i16(data));
                (Value::Integer(value), 2)
            }
        }
        3 => {
            if data.len() < 3 {
                (Value::Integer(0), 0)
            } else {
                let mut bytes = [0u8; 4];
                bytes[1..4].copy_from_slice(&data[0..3]);
                let value = BigEndian::read_i32(&bytes) >> 8; // Sign extend
                (Value::Integer(i64::from(value)), 3)
            }
        }
        4 => {
            if data.len() < 4 {
                (Value::Integer(0), 0)
            } else {
                let value = i64::from(BigEndian::read_i32(data));
                (Value::Integer(value), 4)
            }
        }
        5 => {
            if data.len() < 6 {
                (Value::Integer(0), 0)
            } else {
                let mut bytes = [0u8; 8];
                bytes[2..8].copy_from_slice(&data[0..6]);
                let value = BigEndian::read_i64(&bytes) >> 16; // Sign extend
                (Value::Integer(value), 6)
            }
        }
        6 => {
            if data.len() < 8 {
                (Value::Integer(0), 0)
            } else {
                let value = BigEndian::read_i64(data);
                (Value::Integer(value), 8)
            }
        }
        7 => {
            if data.len() < 8 {
                (Value::Real(0.0), 0)
            } else {
                let bits = BigEndian::read_u64(data);
                let value = f64::from_bits(bits);
                (Value::Real(value), 8)
            }
        }
        8 => (Value::Integer(0), 0),
        9 => (Value::Integer(1), 0),
        _ => {
            if serial_type >= 12 {
                if serial_type % 2 == 0 {
                    // BLOB
                    let length = ((serial_type - 12) / 2) as usize;
                    if data.len() < length {
                        (Value::Blob(Vec::new()), 0)
                    } else {
                        // Use from_slice to avoid unnecessary allocation
                        (Value::Blob(data[0..length].to_vec()), length)
                    }
                } else {
                    // TEXT
                    let length = ((serial_type - 13) / 2) as usize;
                    if data.len() < length {
                        (Value::Text(String::new()), 0)
                    } else {
                        // Use from_utf8_lossy to handle invalid UTF-8 gracefully
                        let text = String::from_utf8_lossy(&data[0..length]).into_owned();
                        (Value::Text(text), length)
                    }
                }
            } else {
                (Value::Null, 0)
            }
        }
    }
}
