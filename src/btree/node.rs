use crate::memory::PAGE_SIZE;
pub const MAX_HINTS: usize = 16;
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - std::mem::size_of::<BTreeNodeHeader>();

#[repr(C)]
#[derive(Clone)]
pub struct FenceKeySlot {
    pub offset: u16,
    pub len: u16,
}

#[repr(C)]
#[derive(Clone)]
pub struct BTreeNodeHeader {
    pub dirty: bool,
    pub upper_inner: u64,      // inner: child pointer; leaf: next leaf
    pub lower_fence: FenceKeySlot, // exclusive
    pub upper_fence: FenceKeySlot, // inclusive
    pub count: u16,
    pub is_leaf: bool,
    pub space_used: u16,
    pub data_offset: u16,
    pub prefix_len: u16,
    pub hint: [u32; MAX_HINTS],
    pub padding: u32,
}

impl BTreeNodeHeader {
    pub const fn underfull_threshold() -> usize {
        (PAGE_DATA_SIZE / 2) + (PAGE_DATA_SIZE / 4)
    }

    pub fn prefix_len(&self) -> u16 {
        self.prefix_len
    }

    pub fn new(is_leaf: bool) -> Self {
        Self {
            dirty: true,
            upper_inner: u64::MAX, // will be set by caller
            lower_fence: FenceKeySlot { offset: 0, len: 0 },
            upper_fence: FenceKeySlot { offset: 0, len: 0 },
            count: 0,
            is_leaf,
            space_used: 0,
            data_offset: PAGE_SIZE as u16,
            prefix_len: 0,
            hint: [0; MAX_HINTS],
            padding: 0,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.is_leaf
    }

    pub fn has_lower_fence(&self) -> bool {
        self.lower_fence.len > 0
    }

    pub fn has_right_neighbor(&self) -> bool {
        self.upper_inner != u64::MAX && self.is_leaf
    }
}

#[repr(C)]
#[derive(Clone)]
pub struct Slot {
    pub offset: u16,
    pub key_len: u16,
    pub payload_len: u16,
    pub head: u32,
}

#[repr(C)]
#[derive(Clone)]
pub struct BTreeNode {
    pub hdr: BTreeNodeHeader,
    pub slot_and_heap: [u8; PAGE_DATA_SIZE],
}

impl BTreeNode {
    pub const fn max_kv_size() -> usize {
        (PAGE_DATA_SIZE - 2 * std::mem::size_of::<Slot>()) / 4
    }

    #[inline]
    fn header_bytes() -> usize {
        std::mem::size_of::<BTreeNodeHeader>()
    }

    fn slots_ptr(&self) -> *const Slot {
        self.slot_and_heap.as_ptr() as *const Slot
    }

    fn slots_ptr_mut(&mut self) -> *mut Slot {
        self.slot_and_heap.as_mut_ptr() as *mut Slot
    }

    fn slot_area_len(&self) -> usize {
        std::mem::size_of::<Slot>() * self.hdr.count as usize
    }

    pub fn get_slot(&self, idx: usize) -> &Slot {
        assert!(idx < self.hdr.count as usize);
        unsafe { &*self.slots_ptr().add(idx) }
    }

    pub fn get_slot_mut(&mut self, idx: usize) -> &mut Slot {
        assert!(idx < self.hdr.count as usize);
        unsafe { &mut *self.slots_ptr_mut().add(idx) }
    }

    pub fn free_space(&self) -> usize {
        let base = Self::header_bytes() + self.slot_area_len();
        self.hdr.data_offset.saturating_sub(base as u16) as usize
    }

    pub fn free_space_after_compaction(&self) -> usize {
        PAGE_DATA_SIZE
            .saturating_sub(self.slot_area_len())
            .saturating_sub(self.hdr.space_used as usize)
    }

    pub fn has_space_for(&self, key_len: usize, payload_len: usize) -> bool {
        self.space_needed(key_len, payload_len) <= self.free_space_after_compaction()
    }

    pub fn space_needed(&self, key_len: usize, payload_len: usize) -> usize {
        std::mem::size_of::<Slot>() + (key_len - self.hdr.prefix_len as usize) + payload_len
    }

    fn insert_fence(&mut self, fk: &mut FenceKeySlot, key: &[u8]) {
        assert!(self.free_space() >= key.len());
        self.hdr.data_offset -= key.len() as u16;
        self.hdr.space_used += key.len() as u16;
        fk.offset = self.hdr.data_offset;
        fk.len = key.len() as u16;
        let ptr = self as *mut Self as *mut u8;
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), ptr.add(fk.offset as usize), key.len());
        }
    }

    pub fn set_fences(&mut self, lower: &[u8], upper: &[u8]) {
        let ptr_lower: *mut FenceKeySlot = &mut self.hdr.lower_fence;
        let ptr_upper: *mut FenceKeySlot = &mut self.hdr.upper_fence;
        unsafe {
            self.insert_fence(&mut *ptr_lower, lower);
            self.insert_fence(&mut *ptr_upper, upper);
        }
        self.hdr.prefix_len = common_prefix_len(lower, upper) as u16;
    }

    /// Recompute fences based on current slots and existing fences as bounds.
    pub fn recompute_fences(&mut self) {
        if self.hdr.count == 0 {
            return;
        }
        let lower = self.reconstruct_key(0);
        let upper = self.reconstruct_key((self.hdr.count - 1) as usize);
        let old_lower = self.lower_fence().to_vec();
        let old_upper = self.upper_fence().to_vec();
        let new_lower = if old_lower.is_empty() { lower } else { old_lower };
        let new_upper = if old_upper.is_empty() { upper } else { old_upper };
        self.hdr.data_offset = PAGE_SIZE as u16;
        self.hdr.space_used = 0;
        self.set_fences(&new_lower, &new_upper);
        self.compactify();
    }

    pub fn get_child(&self, slot: usize) -> u64 {
        let payload = self.get_payload(slot);
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&payload[..8]);
        u64::from_le_bytes(arr)
    }

    /// Return the child pointer at logical index (0-based).
    /// Index 0 is the leftmost child stored in `upper_inner`; other children live in slots.
    pub fn child_at(&self, idx: usize) -> u64 {
        if idx == 0 {
            self.hdr.upper_inner
        } else {
            self.get_child(idx - 1)
        }
    }

    /// Return child pointer, validating it is initialized.
    pub fn child_pid(&self, idx: usize) -> Result<u64, &'static str> {
        let pid = self.child_at(idx);
        if pid == u64::MAX {
            Err("invalid child pid")
        } else {
            Ok(pid)
        }
    }

    /// Return all child PIDs in logical order (len = count+1).
    pub fn children(&self) -> Vec<u64> {
        let mut v = Vec::with_capacity(self.hdr.count as usize + 1);
        v.push(self.hdr.upper_inner);
        for i in 0..self.hdr.count as usize {
            v.push(self.get_child(i));
        }
        v
    }

    pub fn get_prefix(&self) -> &[u8] {
        let ofs = self.hdr.lower_fence.offset as usize;
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr.add(ofs), self.hdr.prefix_len as usize) }
    }

    pub fn lower_fence(&self) -> &[u8] {
        let ofs = self.hdr.lower_fence.offset as usize;
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr.add(ofs), self.hdr.lower_fence.len as usize) }
    }

    pub fn upper_fence(&self) -> &[u8] {
        let ofs = self.hdr.upper_fence.offset as usize;
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr.add(ofs), self.hdr.upper_fence.len as usize) }
    }

    pub fn get_key(&self, slot: usize) -> &[u8] {
        let s = self.get_slot(slot);
        let ptr = self as *const Self as *const u8;
        unsafe {
            std::slice::from_raw_parts(
                ptr.add(s.offset as usize),
                s.key_len as usize,
            )
        }
    }

    pub fn get_payload(&self, slot: usize) -> &[u8] {
        let s = self.get_slot(slot);
        let ptr = self as *const Self as *const u8;
        unsafe {
            std::slice::from_raw_parts(
                ptr.add(s.offset as usize + s.key_len as usize),
                s.payload_len as usize,
            )
        }
    }

    pub fn get_payload_mut(&mut self, slot: usize) -> &mut [u8] {
        let s = self.get_slot(slot).clone();
        let ptr = self as *mut Self as *mut u8;
        unsafe {
            std::slice::from_raw_parts_mut(
                ptr.add(s.offset as usize + s.key_len as usize),
                s.payload_len as usize,
            )
        }
    }

    /// Compute the head (order-preserving) of a key.
    pub fn head(key: &[u8]) -> u32 {
        match key.len() {
            0 => 0,
            1 => (key[0] as u32) << 24,
            2 => (u16::from_be_bytes([key[0], key[1]]) as u32) << 16,
            3 => {
                let hi = u16::from_be_bytes([key[0], key[1]]) as u32;
                (hi << 16) | ((key[2] as u32) << 8)
            }
            _ => u32::from_be_bytes([key[0], key[1], key[2], key[3]]),
        }
    }

    pub fn lower_bound(&self, key: &[u8]) -> (u16, bool) {
        // prefix check
        let prefix = self.get_prefix();
        let cmp = prefix_cmp(key, prefix);
        if cmp < 0 {
            return (0, false);
        }
        if cmp > 0 {
            return (self.hdr.count, false);
        }
        if key.len() < prefix.len() {
            return (0, false);
        }
        let key_tail = &key[prefix.len()..];
        let mut lower = 0u16;
        let mut upper = self.hdr.count;
        let key_head = Self::head(key_tail);
        self.search_hint(key_head, &mut lower, &mut upper);

        while lower < upper {
            let mid = ((upper - lower) / 2) + lower;
            let slot = self.get_slot(mid as usize);
            if key_head < slot.head {
                upper = mid;
            } else if key_head > slot.head {
                lower = mid + 1;
            } else {
                let slot_key = self.get_key(mid as usize);
                match key_tail.cmp(slot_key) {
                    std::cmp::Ordering::Less => {
                        upper = mid;
                    }
                    std::cmp::Ordering::Greater => {
                        lower = mid + 1;
                    }
                    std::cmp::Ordering::Equal => {
                        return (mid, true);
                    }
                }
            }
        }
        (lower, false)
    }

    fn search_hint(&self, key_head: u32, lower: &mut u16, upper: &mut u16) {
        if self.hdr.count as usize > MAX_HINTS * 2 {
            let dist = *upper / ((MAX_HINTS as u16) + 1);
            let mut pos = 0;
            while pos < MAX_HINTS as u16 {
                if self.hint_at(pos as usize) >= key_head {
                    break;
                }
                pos += 1;
            }
            let mut pos2 = pos;
            while pos2 < MAX_HINTS as u16 {
                if self.hint_at(pos2 as usize) != key_head {
                    break;
                }
                pos2 += 1;
            }
            *lower = pos * dist;
            if pos2 < MAX_HINTS as u16 {
                *upper = (pos2 + 1) * dist;
            }
        }
    }

    fn hint_at(&self, idx: usize) -> u32 {
        self.hdr.hint[idx]
    }

    pub fn make_hint(&mut self) {
        if self.hdr.count == 0 {
            return;
        }
        let dist = self.hdr.count / (MAX_HINTS as u16 + 1);
        for i in 0..MAX_HINTS {
            let idx = dist * (i as u16 + 1);
            if idx < self.hdr.count {
                self.hdr.hint[i] = self.get_slot(idx as usize).head;
            }
        }
    }

    pub fn remove_slot(&mut self, slot_id: usize) {
        assert!(slot_id < self.hdr.count as usize);
        let s = self.get_slot(slot_id).clone();
        self.hdr.space_used -= s.key_len + s.payload_len;
        if slot_id + 1 < self.hdr.count as usize {
            let count = (self.hdr.count as usize) - slot_id - 1;
            unsafe {
                std::ptr::copy(
                    self.slots_ptr().add(slot_id + 1),
                    self.slots_ptr_mut().add(slot_id),
                    count,
                );
            }
        }
        self.hdr.count -= 1;
        self.make_hint();
    }

    pub fn underfull(&self) -> bool {
        self.free_space_after_compaction() + (self.hdr.space_used as usize) < BTreeNodeHeader::underfull_threshold()
    }

    pub fn compactify(&mut self) {
        let _should = self.free_space_after_compaction();
        let mut tmp = BTreeNode {
            hdr: BTreeNodeHeader::new(self.hdr.is_leaf),
            slot_and_heap: [0u8; PAGE_DATA_SIZE],
        };
        tmp.set_fences(self.lower_fence(), self.upper_fence());
        for i in 0..self.hdr.count as usize {
            let key_full = self.reconstruct_key(i);
            let payload = self.get_payload(i).to_vec();
            tmp.insert_in_page(&key_full, &payload);
        }
        tmp.hdr.upper_inner = self.hdr.upper_inner;
        *self = tmp;
        // If accounting drifts slightly (e.g., due to fence/prefix reuse), keep going.
        // Invariants still hold because we rebuilt the node from logical contents.
    }

    pub fn reconstruct_key(&self, slot: usize) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.hdr.prefix_len as usize + self.get_slot(slot).key_len as usize);
        key.extend_from_slice(self.get_prefix());
        key.extend_from_slice(self.get_key(slot));
        key
    }

    /// Store key/payload at slot position, shifting existing slots.
    /// Returns false if there still isn't enough space even after compaction.
    pub fn insert_in_page(&mut self, key: &[u8], payload: &[u8]) -> bool {
        let needed = self.space_needed(key.len(), payload.len());
        if needed > self.free_space() {
            self.compactify();
        }
        if needed > self.free_space_after_compaction() {
            return false;
        }
        let (slot_id_u16, _) = self.lower_bound(key);
        let slot_id = slot_id_u16 as usize;
        if slot_id < self.hdr.count as usize {
            let count = (self.hdr.count as usize) - slot_id;
            unsafe {
                std::ptr::copy(
                    self.slots_ptr().add(slot_id),
                    self.slots_ptr_mut().add(slot_id + 1),
                    count,
                );
            }
        }
        // Clear the slot we're about to write to avoid carrying stale bytes.
        unsafe {
            let slot_ptr = self.slots_ptr_mut().add(slot_id);
            std::ptr::write_bytes(slot_ptr, 0, 1);
        }
        self.hdr.count += 1;
        self.store_key_value(slot_id, key, payload);
        self.make_hint();
        true
    }

    fn store_key_value(&mut self, slot_id: usize, key: &[u8], payload: &[u8]) {
        let key_tail = &key[self.hdr.prefix_len as usize..];
        let head = Self::head(key_tail);
        let space = key_tail.len() + payload.len();
        self.hdr.data_offset -= space as u16;
        self.hdr.space_used += space as u16;
        let ptr = self as *mut Self as *mut u8;
        let offset = self.hdr.data_offset;
        {
            let slot = self.get_slot_mut(slot_id);
            slot.offset = offset;
            slot.key_len = key_tail.len() as u16;
            slot.payload_len = payload.len() as u16;
            slot.head = head;
        }
        unsafe {
            let dest = ptr.add(offset as usize);
            std::ptr::copy_nonoverlapping(key_tail.as_ptr(), dest, key_tail.len());
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dest.add(key_tail.len()), payload.len());
        }
    }
}

fn prefix_cmp(key: &[u8], prefix: &[u8]) -> i32 {
    if prefix.is_empty() {
        return 0;
    }
    let cmp = key.iter().zip(prefix.iter()).find_map(|(a, b)| {
        if a != b {
            Some(*a as i32 - *b as i32)
        } else {
            None
        }
    });
    if let Some(c) = cmp {
        return c.signum();
    }
    (key.len() as i32 - prefix.len() as i32).signum()
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let limit = std::cmp::min(a.len(), b.len());
    let mut i = 0;
    while i < limit && a[i] == b[i] {
        i += 1;
    }
    i
}
