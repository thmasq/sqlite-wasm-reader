//! B-tree traversal functionality

use crate::{
    Error, Result,
    format::{FileHeader, OVERFLOW_PAGE_HEADER_SIZE},
    logging::{log_debug, log_warn},
    page::Page,
    record::parse_value,
    value::Value,
};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::vec::Vec;

const ITERATION_LIMIT: usize = 100_000_000;

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
pub struct BTreeCursor<'a> {
    /// Stack of pages being traversed
    /// Each entry contains: (page, `current_cell_index`)
    page_stack: Vec<(Page, usize)>,
    /// Safety counter to prevent infinite loops during traversal
    iteration_count: usize,
    /// Reference to the database file header (needed for overflow calculations)
    file_header: &'a FileHeader,
}

impl<'a> BTreeCursor<'a> {
    /// Create a new cursor starting at the given page
    #[must_use]
    pub fn new(root_page: Page, file_header: &'a FileHeader) -> Self {
        Self {
            page_stack: vec![(root_page, 0)],
            iteration_count: 0,
            file_header,
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

                    // Note: We pass read_page here for overflow handling
                    let cell = parse_leaf_table_cell(cell_data, self.file_header, &mut read_page)?;

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
                // Interior table cells don't have payloads (only keys), so no overflow logic needed
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
    /// (currently 100,000,000), which protects against infinite loops caused by cycles in the B-tree.
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
        if self.iteration_count > ITERATION_LIMIT {
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
                // Pass read_page for overflow handling
                if let Some(cell) = self.process_leaf_page(&mut read_page)? {
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
    fn process_leaf_page<F>(&mut self, read_page: &mut F) -> Result<Option<Cell>>
    where
        F: FnMut(u32) -> Result<Page>,
    {
        let (page, cell_index) = self.page_stack.last_mut().unwrap();

        // If we've processed all cells in this leaf page
        if *cell_index >= page.cell_count as usize {
            // Pop this page and continue with parent
            self.page_stack.pop();
            return Ok(None);
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
                return Ok(None);
            }
        };

        if *cell_index >= cell_pointers.len() {
            // Pop this page and continue with parent
            self.page_stack.pop();
            return Ok(None);
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
                return Ok(None);
            }
        };

        // Move to next cell in current page
        *cell_index += 1;

        // Parse and return the leaf cell (with overflow support)
        match parse_leaf_table_cell(&cell_data, self.file_header, read_page) {
            Ok(cell) => Ok(Some(cell)),
            Err(e) => {
                log_debug(&format!(
                    "Failed to parse leaf cell on page {}: {}",
                    page.page_number, e
                ));
                return Err(e);
            }
        }
    }

    /// Process the current interior page on the stack.
    fn process_interior_page<F>(&mut self, mut read_page: F)
    where
        F: FnMut(u32) -> Result<Page>,
    {
        let (page, cell_index) = self.page_stack.last_mut().unwrap();
        let cell_count = page.cell_count as usize;
        let page_num = page.page_number;
        let right_ptr = page.right_pointer;
        let is_first_page = page_num == 1;

        // State 1: Visit Left Children (Cells 0 to N-1)
        if *cell_index < cell_count {
            let pointers = match page.cell_pointers(is_first_page) {
                Ok(p) => p,
                Err(e) => {
                    log_warn(&format!("Interior page {} error: {}", page_num, e));
                    self.page_stack.pop();
                    return;
                }
            };

            // Should not happen if index < cell_count, but safety first
            if *cell_index >= pointers.len() {
                // Skip to right child processing
                *cell_index = cell_count;
                return;
            }

            let offset = pointers[*cell_index];
            let cell_data = match page.cell_content(offset) {
                Ok(d) => d.to_vec(),
                Err(e) => {
                    log_warn(&format!("Cell read error on page {}: {}", page_num, e));
                    *cell_index += 1; // Skip bad cell
                    return;
                }
            };

            // Increment index NOW so when we return, we move to next cell
            *cell_index += 1;

            if let Ok(cell) = parse_interior_table_cell(&cell_data) {
                if let Some(left_child) = cell.left_child {
                    if self.stack_contains(left_child) {
                        log_warn(&format!("Cycle detected: {} -> {}", page_num, left_child));
                    } else {
                        match read_page(left_child) {
                            Ok(child_page) => self.page_stack.push((child_page, 0)),
                            Err(e) => log_warn(&format!("Read child {} error: {}", left_child, e)),
                        }
                    }
                }
            }
            return;
        }

        // State 2: Visit Right Child
        if *cell_index == cell_count {
            // Mark right child as processed by incrementing index
            *cell_index += 1;

            if let Some(right_ptr) = right_ptr {
                if self.stack_contains(right_ptr) {
                    log_warn(&format!("Cycle detected: {} -> {}", page_num, right_ptr));
                } else {
                    match read_page(right_ptr) {
                        Ok(child_page) => self.page_stack.push((child_page, 0)),
                        Err(e) => log_warn(&format!("Read right child {} error: {}", right_ptr, e)),
                    }
                }
            } else {
                // No right pointer? (Shouldn't happen for valid interior pages)
                self.page_stack.pop();
            }
            return;
        }

        // State 3: Done
        // We have visited all cells and the right pointer. Pop self.
        self.page_stack.pop();
    }

    /// Find all rowids for a composite index key (exact match on all components).
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
                // Pass read_page for overflow support in index cells
                let cell = parse_interior_index_cell(cell_data, self.file_header, &mut read_page)?;
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
            // Pass read_page for overflow support in index cells
            let cell = parse_leaf_index_cell(cell_data, self.file_header, &mut read_page)?;

            // Need at least as many components as the search key
            if cell.key.len() < key.len() {
                continue;
            }

            // Exact component-wise equality for the prefix length of key
            let matches = cell.key.iter().zip(key.iter()).all(|(a, b)| a == *b);

            if matches {
                rowids.push(cell.rowid);
            } else {
                // Check if we've passed the search key alphabetically
                if let (Some(search_first), Some(cell_first)) = (key.first(), cell.key.first()) {
                    if let std::cmp::Ordering::Greater = cell_first.cmp(search_first) {
                        break;
                    }
                }
            }
        }

        Ok(rowids)
    }
}

/// Helper function to read payload that might be split across overflow pages.
///
/// # Arguments
/// * `initial_data` - The raw cell data from the current page.
/// * `offset` - The offset in `initial_data` where the payload (or local part of it) begins.
/// * `payload_size` - The total size of the payload in bytes (read from varint).
/// * `max_local` - The maximum number of bytes stored locally on the B-tree page.
/// * `min_local` - The minimum number of bytes stored locally on the B-tree page.
/// * `usable_space` - The usable space on a page (Page Size - Reserved).
/// * `file_header` - The database file header (for page size).
/// * `read_page` - Callback to fetch overflow pages.
fn read_payload_with_overflow<F>(
    initial_data: &[u8],
    mut offset: usize,
    payload_size: u64,
    max_local: u32,
    min_local: u32,
    usable_space: u32,
    read_page: &mut F,
) -> Result<Vec<u8>>
where
    F: FnMut(u32) -> Result<Page>,
{
    let payload_size_usize = payload_size as usize;
    let max_local = max_local as usize;
    let min_local = min_local as usize;
    let usable_space = usable_space as usize;

    // Calculate how much payload is stored locally
    let local_size = if payload_size_usize <= max_local {
        payload_size_usize
    } else {
        // Calculate surplus bytes
        let surplus = min_local + (payload_size_usize - min_local) % (usable_space - 4);
        if surplus <= max_local {
            surplus
        } else {
            min_local
        }
    };

    if payload_size > max_local as u64 {
        crate::logging::log_debug(&format!(
            "Overflow detected! Payload size: {}, Max local: {}, Local size: {}",
            payload_size, max_local, local_size
        ));
    }

    // Bounds check for local read
    if offset + local_size > initial_data.len() {
        return Err(Error::InvalidFormat(format!(
            "Local payload size {} exceeds available data {} (offset: {})",
            local_size,
            initial_data.len(),
            offset
        )));
    }

    // Read local portion
    let mut payload = Vec::with_capacity(payload_size_usize);
    payload.extend_from_slice(&initial_data[offset..offset + local_size]);
    offset += local_size;

    // If there is overflow, follow the linked list of overflow pages
    if payload_size_usize > local_size {
        // The next 4 bytes in the local cell are the page number of the first overflow page
        if offset + 4 > initial_data.len() {
            return Err(Error::InvalidFormat(
                "Missing overflow page pointer in local cell".into(),
            ));
        }

        let mut next_page_num = u32::from_be_bytes([
            initial_data[offset],
            initial_data[offset + 1],
            initial_data[offset + 2],
            initial_data[offset + 3],
        ]);

        crate::logging::log_debug(&format!("First overflow page pointer: {}", next_page_num));

        let mut bytes_read = local_size;
        let overflow_payload_capacity = usable_space - OVERFLOW_PAGE_HEADER_SIZE; // usually 4096 - 4 = 4092

        while bytes_read < payload_size_usize {
            if next_page_num == 0 {
                return Err(Error::InvalidFormat(
                    "Overflow chain terminated early".into(),
                ));
            }

            crate::logging::log_debug(&format!("Fetching overflow page: {}", next_page_num));

            let page = read_page(next_page_num)?;

            // Overflow page format:
            // - 4 bytes: Next overflow page number (0 if last)
            // - Remainder: Payload data
            if page.data.len() < OVERFLOW_PAGE_HEADER_SIZE {
                return Err(Error::InvalidFormat("Overflow page too small".into()));
            }

            // Read next pointer
            let next_ptr_bytes = &page.data[0..4];
            next_page_num = u32::from_be_bytes([
                next_ptr_bytes[0],
                next_ptr_bytes[1],
                next_ptr_bytes[2],
                next_ptr_bytes[3],
            ]);

            // Determine how many bytes to read from this page
            let remaining = payload_size_usize - bytes_read;
            let bytes_to_copy = std::cmp::min(remaining, overflow_payload_capacity);

            // Bounds check
            if OVERFLOW_PAGE_HEADER_SIZE + bytes_to_copy > page.data.len() {
                return Err(Error::InvalidFormat("Overflow page data too short".into()));
            }

            payload.extend_from_slice(
                &page.data[OVERFLOW_PAGE_HEADER_SIZE..OVERFLOW_PAGE_HEADER_SIZE + bytes_to_copy],
            );
            bytes_read += bytes_to_copy;
        }
    }

    Ok(payload)
}

/// Parse a leaf table cell (supporting overflow pages)
fn parse_leaf_table_cell<F>(
    data: &[u8],
    file_header: &FileHeader,
    read_page: &mut F,
) -> Result<Cell>
where
    F: FnMut(u32) -> Result<Page>,
{
    let (payload_size, offset) = read_varint(data)?;
    let (rowid, offset2) = read_varint(&data[offset..])?;
    let content_offset = offset + offset2;

    // Retrieve threshold constants from file header
    let max_local = file_header.leaf_table_max_local();
    let min_local = file_header.leaf_table_min_local();
    let usable_space = file_header.usable_space();

    let payload = read_payload_with_overflow(
        data,
        content_offset,
        payload_size as u64,
        max_local,
        min_local,
        usable_space,
        read_page,
    )?;

    Ok(Cell {
        left_child: None,
        key: rowid,
        payload,
    })
}

/// Parse an interior table cell
fn parse_interior_table_cell(data: &[u8]) -> Result<Cell> {
    // Interior table cells have no payload, only a Child Ptr and a Key.
    // No overflow logic required.
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

/// Parse a leaf index cell (supporting overflow pages)
fn parse_leaf_index_cell<F>(
    data: &[u8],
    file_header: &FileHeader,
    read_page: &mut F,
) -> Result<IndexCell>
where
    F: FnMut(u32) -> Result<Page>,
{
    let (payload_size, offset) = read_varint(data)?;

    // Calculate thresholds for Index B-Trees (different from Table B-Trees)
    // Leaf Index:
    // X = ((U-12)*64/255)-23
    // M = ((U-12)*32/255)-23
    let u = file_header.usable_space();
    let u_minus_12 = u.saturating_sub(12);
    let max_local = (u_minus_12 * 64 / 255).saturating_sub(23);
    let min_local = (u_minus_12 * 32 / 255).saturating_sub(23);

    let payload = read_payload_with_overflow(
        data,
        offset,
        payload_size as u64,
        max_local,
        min_local,
        u,
        read_page,
    )?;

    // Parse values from the reconstructed payload
    let (header_size, mut header_offset) = read_varint(&payload)?;
    let mut values = Vec::new();
    let mut content_offset = header_size as usize;

    while header_offset < header_size as usize {
        let (serial_type, bytes_read) = read_varint(&payload[header_offset..])?;
        header_offset += bytes_read;
        let (value, value_bytes) = parse_value(serial_type, &payload[content_offset..]);
        values.push(value);
        content_offset += value_bytes;
    }

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

/// Parse an interior index cell (supporting overflow pages)
fn parse_interior_index_cell<F>(
    data: &[u8],
    file_header: &FileHeader,
    read_page: &mut F,
) -> Result<InteriorIndexCell>
where
    F: FnMut(u32) -> Result<Page>,
{
    if data.len() < 4 {
        return Err(Error::InvalidFormat("Interior index cell too small".into()));
    }

    let left_child = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let (payload_size, offset) = read_varint(&data[4..])?;

    // Interior Index: Same thresholds as Leaf Index
    let u = file_header.usable_space();
    let u_minus_12 = u.saturating_sub(12);
    let max_local = (u_minus_12 * 64 / 255).saturating_sub(23);
    let min_local = (u_minus_12 * 32 / 255).saturating_sub(23);

    let payload = read_payload_with_overflow(
        data,
        4 + offset, // Offset includes the 4-byte left_child ptr
        payload_size as u64,
        max_local,
        min_local,
        u,
        read_page,
    )?;

    let (header_size, mut header_offset) = read_varint(&payload)?;
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
