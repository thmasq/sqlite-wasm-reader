//! B-tree traversal functionality

use crate::{
    Error, Result,
    logging::{log_debug, log_warn},
    page::Page,
    record::parse_value,
    value::Value,
};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::vec::Vec;

/// Cell in a B-tree page
#[derive(Debug)]
#[allow(dead_code)]
pub struct Cell {
    /// Left child page number (for interior pages)
    pub left_child: Option<u32>,
    /// Key (rowid for table b-trees)
    pub key: i64,
    /// Payload data
    pub payload: Vec<u8>,
}

/// An entry in an index B-tree
#[derive(Debug)]
struct IndexCell {
    /// The indexed value(s)
    pub key: Vec<Value>,
    /// The rowid of the corresponding row
    pub rowid: i64,
}

/// An entry in an interior index page
#[derive(Debug)]
struct InteriorIndexCell {
    /// The page number of the left child.
    pub left_child: u32,
    /// The indexed value(s)
    pub key: Vec<Value>,
}

/// B-tree cursor for traversing pages
pub struct BTreeCursor {
    /// Stack of pages being traversed
    /// Each entry contains: (page, `current_cell_index`)
    page_stack: Vec<(Page, usize)>,
    /// Safety counter to prevent infinite loops during traversal
    iteration_count: usize,
}

impl BTreeCursor {
    /// Create a new cursor starting at the given page
    #[must_use]
    pub fn new(root_page: Page) -> Self {
        Self {
            page_stack: vec![(root_page, 0)],
            iteration_count: 0,
        }
    }

    /// Check if the given page number exists in the current stack.
    /// This provides O(D) cycle detection where D is the tree depth.
    fn stack_contains(&self, page_id: u32) -> bool {
        self.page_stack
            .iter()
            .any(|(p, _)| p.page_number == page_id)
    }

    /// Find a cell with the specified key (ROWID) in the B-tree
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The page reader fails to read a page (e.g., I/O error).
    /// - The page data is invalid or cannot be parsed.
    /// - An interior page is missing a required right pointer.
    /// - The tree depth exceeds the safety limit (50 levels).
    ///
    /// # Panics
    ///
    /// Panics if an interior table cell is parsed but does not contain a left child pointer,
    /// which violates the invariant that interior table cells must have a left child.
    pub fn find_cell<F>(&mut self, key: i64, mut read_page: F) -> Result<Option<Cell>>
    where
        F: FnMut(u32) -> Result<Page>,
    {
        if self.page_stack.is_empty() {
            return Ok(None);
        }

        let root_page_num = self.page_stack[0].0.page_number;
        let mut current_page = read_page(root_page_num)?;
        let mut depth = 0;

        loop {
            if depth > 50 {
                return Err(Error::InvalidFormat(
                    "B-tree depth exceeded safety limit during search".into(),
                ));
            }
            depth += 1;

            let cell_pointers = current_page.cell_pointers(current_page.page_number == 1)?;
            if current_page.page_type.is_leaf() {
                // Binary search for the key in this leaf page
                let mut low = 0;
                let mut high = cell_pointers.len();

                while low < high {
                    let mid = low + (high - low) / 2;
                    let cell_offset = cell_pointers[mid];
                    let cell_data = current_page.cell_content(cell_offset)?;
                    let cell = parse_leaf_table_cell(cell_data)?;

                    match cell.key.cmp(&key) {
                        std::cmp::Ordering::Equal => return Ok(Some(cell)),
                        std::cmp::Ordering::Less => low = mid + 1,
                        std::cmp::Ordering::Greater => high = mid,
                    }
                }

                // Key not found
                return Ok(None);
            }

            // This is an interior page, find the appropriate child page to descend to.
            let mut next_page_num = current_page.right_pointer.ok_or_else(|| {
                Error::InvalidFormat("Interior page missing right pointer".into())
            })?;

            for &cell_offset in &cell_pointers {
                let cell_data = current_page.cell_content(cell_offset)?;
                let cell = parse_interior_table_cell(cell_data)?;
                if key <= cell.key {
                    next_page_num = cell.left_child.unwrap();
                    break;
                }
            }

            // Descend to the child page.
            current_page = read_page(next_page_num)?;
        }
    }

    /// Move to the next cell in the B-tree using in-order traversal
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidFormat` if the traversal exceeds the safety iteration limit
    /// (currently 100,000), which protects against infinite loops caused by cycles in the B-tree.
    ///
    /// # Panics
    ///
    /// Panics if the page stack is empty when attempting to access the current page,
    /// though the loop condition checks for emptiness beforehand.
    pub fn next_cell<F>(&mut self, mut read_page: F) -> Result<Option<Cell>>
    where
        F: FnMut(u32) -> Result<Page>,
    {
        // Safety check: prevent infinite loops
        self.iteration_count += 1;
        if self.iteration_count > 100_000 {
            return Err(Error::InvalidFormat(
                "B-tree traversal exceeded safety limit".into(),
            ));
        }

        loop {
            if self.page_stack.is_empty() {
                return Ok(None);
            }

            // Check if leaf or interior without holding mutable borrow too long
            let is_leaf = self.page_stack.last().unwrap().0.page_type.is_leaf();

            if is_leaf {
                if let Some(cell) = self.process_leaf_page() {
                    return Ok(Some(cell));
                }
            } else {
                self.process_interior_page(&mut read_page);
            }
        }
    }

    /// Process the current leaf page on the stack.
    /// Returns `Ok(Some(cell))` if a cell was found.
    /// Returns `Ok(None)` if the loop should continue (e.g. page finished or error skipped).
    fn process_leaf_page(&mut self) -> Option<Cell> {
        let (page, cell_index) = self.page_stack.last_mut().unwrap();

        // If we've processed all cells in this leaf page
        if *cell_index >= page.cell_count as usize {
            // Pop this page and continue with parent
            self.page_stack.pop();
            return None;
        }

        let is_first_page = page.page_number == 1;
        let cell_pointers = match page.cell_pointers(is_first_page) {
            Ok(pointers) => pointers,
            Err(e) => {
                log_warn(&format!(
                    "Failed to get cell pointers for page {}: {}",
                    page.page_number, e
                ));
                // Skip this page and continue with parent
                self.page_stack.pop();
                return None;
            }
        };

        if *cell_index >= cell_pointers.len() {
            // Pop this page and continue with parent
            self.page_stack.pop();
            return None;
        }

        let cell_offset = cell_pointers[*cell_index];
        // Clone data to satisfy borrow checker when we increment index later
        let cell_data = match page.cell_content(cell_offset) {
            Ok(data) => data.to_vec(),
            Err(e) => {
                log_warn(&format!(
                    "Failed to get cell content at offset {} on page {}: {}",
                    cell_offset, page.page_number, e
                ));
                // Skip this cell and move to next
                *cell_index += 1;
                return None;
            }
        };

        // Move to next cell in current page
        *cell_index += 1;

        // Parse and return the leaf cell
        match parse_leaf_table_cell(&cell_data) {
            Ok(cell) => Some(cell),
            Err(e) => {
                log_debug(&format!(
                    "Failed to parse leaf cell on page {}: {}",
                    page.page_number, e
                ));
                // Skip this cell and continue to next iteration
                None
            }
        }
    }

    /// Process the current interior page on the stack.
    fn process_interior_page<F>(&mut self, mut read_page: F)
    where
        F: FnMut(u32) -> Result<Page>,
    {
        // Check if we are done with cells in this page
        let (is_done, right_ptr, page_num) = {
            let (page, cell_index) = self.page_stack.last_mut().unwrap();
            (
                *cell_index >= page.cell_count as usize,
                page.right_pointer,
                page.page_number,
            )
        };

        if is_done {
            // We've processed all cells in this interior page
            if let Some(right_ptr) = right_ptr {
                if self.stack_contains(right_ptr) {
                    log_warn(&format!(
                        "Cycle detected: page {page_num} points to ancestor {right_ptr} (right ptr). Breaking cycle."
                    ));
                    self.page_stack.pop();
                } else {
                    match read_page(right_ptr) {
                        Ok(right_page) => {
                            self.page_stack.push((right_page, 0));
                        }
                        Err(e) => {
                            log_warn(&format!("Failed to read right child {right_ptr}: {e}"));
                            self.page_stack.pop();
                        }
                    }
                }
            } else {
                // No right pointer, pop this page
                self.page_stack.pop();
            }
            return;
        }

        // Process the current cell
        let (cell_data, page_number) = {
            let (page, cell_index) = self.page_stack.last_mut().unwrap();
            let is_first_page = page.page_number == 1;

            let pointers = match page.cell_pointers(is_first_page) {
                Ok(p) => p,
                Err(e) => {
                    log_warn(&format!(
                        "Failed to get cell pointers for interior page {}: {}",
                        page.page_number, e
                    ));
                    self.page_stack.pop();
                    return;
                }
            };

            if *cell_index >= pointers.len() {
                self.page_stack.pop();
                return;
            }

            let offset = pointers[*cell_index];
            let data = match page.cell_content(offset) {
                Ok(d) => d.to_vec(),
                Err(e) => {
                    log_warn(&format!(
                        "Failed to get cell content at offset {} on interior page {}: {}",
                        offset, page.page_number, e
                    ));
                    *cell_index += 1;
                    return;
                }
            };

            *cell_index += 1;
            (data, page.page_number)
        };

        // Parse the interior cell and descend
        match parse_interior_table_cell(&cell_data) {
            Ok(cell) => {
                if let Some(left_child) = cell.left_child {
                    if self.stack_contains(left_child) {
                        log_warn(&format!(
                            "Cycle detected: page {page_number} points to ancestor {left_child} (left child). Skipping."
                        ));
                    } else {
                        match read_page(left_child) {
                            Ok(child_page) => {
                                self.page_stack.push((child_page, 0));
                            }
                            Err(e) => {
                                log_warn(&format!("Failed to read child page {left_child}: {e}"));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log_warn(&format!(
                    "Failed to parse interior cell on page {page_number}: {e}"
                ));
            }
        }
    }

    /// Find all rowids for a composite index key (exact match on all components).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The page reader fails to read a page.
    /// - The page format is invalid or corrupted.
    /// - An interior index page is missing its right pointer.
    /// - Cell content cannot be parsed.
    pub fn find_rowids_by_key<F>(&mut self, key: &[&Value], mut read_page: F) -> Result<Vec<i64>>
    where
        F: FnMut(u32) -> Result<Page>,
    {
        log_debug(&format!(
            "[BTreeCursor] Searching for composite key: {key:?}"
        ));
        if self.page_stack.is_empty() {
            return Ok(Vec::new());
        }

        let root_page_num = self.page_stack[0].0.page_number;
        let mut current_page = read_page(root_page_num)?;
        let mut depth = 0;

        // Descend until we hit a leaf page in the index B-tree
        loop {
            if depth > 50 {
                return Err(Error::InvalidFormat(
                    "B-tree depth exceeded safety limit during index search".into(),
                ));
            }
            depth += 1;

            if current_page.page_type.is_leaf() {
                break;
            }

            let cell_pointers = current_page.cell_pointers(current_page.page_number == 1)?;
            // By default, follow the right-most child ( > all keys )
            let mut next_page_num = current_page.right_pointer.ok_or_else(|| {
                Error::InvalidFormat("Interior index page missing right pointer".into())
            })?;

            // Iterate over cells to find the first key >= search key (lexicographically by component)
            for &cell_offset in &cell_pointers {
                let cell_data = current_page.cell_content(cell_offset)?;
                let cell = parse_interior_index_cell(cell_data)?;
                let cell_key_refs: Vec<&Value> = cell.key.iter().collect();

                let cmp_len = std::cmp::min(key.len(), cell_key_refs.len());
                let ord = key[..cmp_len].cmp(&cell_key_refs[..cmp_len]);
                if ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal {
                    next_page_num = cell.left_child;
                    break;
                }
            }

            current_page = read_page(next_page_num)?;
        }

        // We're now on a leaf page – gather all rowids whose key matches exactly
        let mut rowids = Vec::new();
        let cell_pointers = current_page.cell_pointers(current_page.page_number == 1)?;

        for &cell_offset in &cell_pointers {
            let cell_data = current_page.cell_content(cell_offset)?;
            let cell = parse_leaf_index_cell(cell_data)?;

            // Need at least as many components as the search key
            if cell.key.len() < key.len() {
                continue;
            }

            // Exact component-wise equality for the prefix length of key
            let matches = cell.key.iter().zip(key.iter()).all(|(a, b)| a == *b);

            log_debug(&format!(
                "[BTreeCursor] Checking cell: key={:?} vs search={:?}, matches={}",
                cell.key, key, matches
            ));

            if matches {
                log_debug(&format!(
                    "[BTreeCursor] MATCH FOUND! Adding rowid {}",
                    cell.rowid
                ));
                rowids.push(cell.rowid);
            } else {
                // Check if we've passed the search key alphabetically
                // Since the page is sorted, if the current cell key is greater than the search key,
                // no further cells will match
                if let (Some(search_first), Some(cell_first)) = (key.first(), cell.key.first()) {
                    match cell_first.cmp(search_first) {
                        std::cmp::Ordering::Greater => {
                            // We've passed the search key, stop searching
                            log_debug(&format!(
                                "[BTreeCursor] Cell key {:?} > search key {:?}, stopping early scan on page {}",
                                cell.key, key, current_page.page_number
                            ));
                            break;
                        }
                        std::cmp::Ordering::Less => {
                            // Cell key is less than search key, continue searching
                            log_debug(&format!(
                                "[BTreeCursor] Cell key {:?} < search key {:?}, continuing search",
                                cell.key, key
                            ));
                        }
                        std::cmp::Ordering::Equal => {
                            // First component matches but full key doesn't - could be composite key mismatch
                            log_debug(&format!(
                                "[BTreeCursor] Partial composite key mismatch – cell key {:?} vs search {:?}",
                                cell.key, key
                            ));
                        }
                    }
                }
            }
        }

        log_debug(&format!(
            "[BTreeCursor] Composite key search found {} rowids",
            rowids.len()
        ));
        Ok(rowids)
    }
}

/// Parse a leaf table cell
fn parse_leaf_table_cell(data: &[u8]) -> Result<Cell> {
    let (payload_size, offset) = read_varint(data)?;
    let (rowid, offset2) = read_varint(&data[offset..])?;
    let offset = offset + offset2;

    // Add bounds checking to prevent panic
    let payload_end = offset + payload_size as usize;
    if payload_end > data.len() {
        return Err(Error::InvalidFormat(format!(
            "Payload size {} exceeds available data (offset: {}, data_len: {})",
            payload_size,
            offset,
            data.len()
        )));
    }

    let payload = data[offset..payload_end].to_vec();

    Ok(Cell {
        left_child: None,
        key: rowid,
        payload,
    })
}

/// Parse an interior table cell
fn parse_interior_table_cell(data: &[u8]) -> Result<Cell> {
    // Check if we have enough data for the left child pointer
    if data.len() < 4 {
        return Err(Error::InvalidFormat(format!(
            "Interior cell data too short: {} bytes, need at least 4",
            data.len()
        )));
    }

    let left_child = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let (rowid, _) = read_varint(&data[4..])?;

    Ok(Cell {
        left_child: Some(left_child),
        key: rowid,
        payload: Vec::new(),
    })
}

/// Parse a leaf index cell
fn parse_leaf_index_cell(data: &[u8]) -> Result<IndexCell> {
    let (payload_size, offset) = read_varint(data)?;
    let payload = &data[offset..offset + payload_size as usize];
    let (header_size, mut header_offset) = read_varint(payload)?;
    let mut values = Vec::new();
    let mut content_offset = header_size as usize;

    while header_offset < header_size as usize {
        let (serial_type, bytes_read) = read_varint(&payload[header_offset..])?;
        header_offset += bytes_read;
        let (value, value_bytes) = parse_value(serial_type, &payload[content_offset..]);
        values.push(value);
        content_offset += value_bytes;
    }

    // In SQLite index leaf cells, the ROWID is the last value in the payload
    // Extract it from the values array
    if values.is_empty() {
        return Err(Error::InvalidFormat("Index cell has no values".into()));
    }

    let Value::Integer(rowid) = values.pop().unwrap() else {
        return Err(Error::InvalidFormat(
            "Index cell ROWID is not an integer".into(),
        ));
    };

    Ok(IndexCell { key: values, rowid })
}

/// Parse an interior index cell
fn parse_interior_index_cell(data: &[u8]) -> Result<InteriorIndexCell> {
    let left_child = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let (payload_size, offset) = read_varint(&data[4..])?;
    let payload = &data[4 + offset..4 + offset + payload_size as usize];
    let (header_size, mut header_offset) = read_varint(payload)?;
    let mut values = Vec::new();
    let mut content_offset = header_size as usize;

    while header_offset < header_size as usize {
        let (serial_type, bytes_read) = read_varint(&payload[header_offset..])?;
        header_offset += bytes_read;
        let (value, value_bytes) = parse_value(serial_type, &payload[content_offset..]);
        values.push(value);
        content_offset += value_bytes;
    }

    Ok(InteriorIndexCell {
        left_child,
        key: values,
    })
}

/// Read a variable-length integer
///
/// # Errors
///
/// Returns `Error::InvalidVarint` if:
/// - The data buffer is empty or exhausted before the varint terminates.
/// - The varint exceeds the maximum size of 9 bytes.
pub fn read_varint(data: &[u8]) -> Result<(i64, usize)> {
    let mut value = 0i64;
    let mut offset = 0;

    for i in 0..9 {
        if offset >= data.len() {
            return Err(Error::InvalidVarint);
        }

        let byte = data[offset];
        offset += 1;

        if i < 8 {
            value = (value << 7) | i64::from(byte & 0x7f);
            if byte < 0x80 {
                return Ok((value, offset));
            }
        } else {
            value = (value << 8) | i64::from(byte);
            return Ok((value, offset));
        }
    }

    Err(Error::InvalidVarint)
}
