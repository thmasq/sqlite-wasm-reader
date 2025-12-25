//! Page reading and parsing functionality

use crate::{
    Error, Result,
    format::{CELL_POINTER_SIZE, PAGE_HEADER_SIZE, PageType},
};
use byteorder::{BigEndian, ByteOrder};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{format, vec::Vec};

/// Represents a page in the `SQLite` database
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Page {
    /// Page number (1-indexed)
    pub page_number: u32,
    /// Raw page data
    pub data: Vec<u8>,
    /// Page type
    pub page_type: PageType,
    /// Number of cells on this page
    pub cell_count: u16,
    /// Offset to first cell
    pub first_cell_offset: u16,
    /// Number of fragmented free bytes
    pub fragmented_free_bytes: u8,
    /// Right-most pointer (for interior pages)
    pub right_pointer: Option<u32>,
}

impl Page {
    /// Parse a page from raw bytes
    pub fn parse(page_number: u32, data: &[u8], is_first_page: bool) -> Result<Self> {
        let header_offset = if is_first_page { 100 } else { 0 };

        if data.len() < header_offset + PAGE_HEADER_SIZE {
            return Err(Error::InvalidFormat("Page too small".into()));
        }

        let page_type_byte = data[header_offset];
        let page_type = PageType::from_byte(page_type_byte)
            .ok_or_else(|| Error::InvalidFormat(format!("Invalid page type: {page_type_byte}")))?;

        let first_cell_offset = BigEndian::read_u16(&data[header_offset + 1..]);
        let cell_count = BigEndian::read_u16(&data[header_offset + 3..]);
        let fragmented_free_bytes = data[header_offset + 7];

        let right_pointer = if page_type.is_leaf() {
            None
        } else {
            Some(BigEndian::read_u32(&data[header_offset + 8..]))
        };

        Ok(Self {
            page_number,
            data: data.to_vec(), // Clone only when creating the Page struct
            page_type,
            cell_count,
            first_cell_offset,
            fragmented_free_bytes,
            right_pointer,
        })
    }

    /// Get the cell pointer array
    pub fn cell_pointers(&self, is_first_page: bool) -> Result<Vec<u16>> {
        let header_offset = if is_first_page { 100 } else { 0 };
        let cell_pointer_offset = header_offset + if self.page_type.is_leaf() { 8 } else { 12 };

        let mut pointers = Vec::with_capacity(self.cell_count as usize);

        for i in 0..self.cell_count {
            let offset = cell_pointer_offset + (i as usize) * CELL_POINTER_SIZE;
            if offset + CELL_POINTER_SIZE > self.data.len() {
                return Err(Error::InvalidFormat("Cell pointer out of bounds".into()));
            }
            pointers.push(BigEndian::read_u16(&self.data[offset..]));
        }

        Ok(pointers)
    }

    /// Get cell content at the given offset
    pub fn cell_content(&self, offset: u16) -> Result<&[u8]> {
        let offset = offset as usize;
        if offset >= self.data.len() {
            return Err(Error::InvalidFormat(format!(
                "Cell offset {} out of bounds (data length: {})",
                offset,
                self.data.len()
            )));
        }

        // For safety, we should try to determine a reasonable end point
        // Since we don't know the exact cell size, we'll return the rest of the page
        // The calling code (parse_leaf_table_cell) will handle the actual bounds checking
        Ok(&self.data[offset..])
    }
}
