use super::node::BTreeNode;
use crate::Result;
use crate::buffer_manager::BufferManager;
use crate::guard::{GuardO, GuardX};
use std::sync::Arc;

pub const METADATA_PID: u64 = 0;

#[repr(C)]
pub struct MetaDataPage {
    pub dirty: bool,
    pub root: u64,
}

pub struct BTree {
    bm: Arc<BufferManager>,
    root_pid: Arc<std::sync::atomic::AtomicU64>,
}

// BTree holds an Arc<BufferManager> (Send + Sync) and an AtomicU64; safe to share.
unsafe impl Send for BTree {}
unsafe impl Sync for BTree {}

impl Clone for BTree {
    fn clone(&self) -> Self {
        Self {
            bm: self.bm.clone(),
            root_pid: self.root_pid.clone(),
        }
    }
}

impl BTree {
    pub fn new(bm: Arc<BufferManager>) -> Result<Self> {
        // Initialize metadata/root lazily
        let mut meta = GuardX::<MetaDataPage>::new_from_pid(bm.as_ref(), METADATA_PID);
        if meta.ptr().root == 0 {
            // allocate a new leaf
            let pid = bm.alloc_page()?;
            // safety: we overlay BTreeNode on the page bytes
            let node_ptr = bm.to_ptr(pid) as *mut BTreeNode;
            unsafe {
                node_ptr.write(BTreeNode {
                    hdr: super::node::BTreeNodeHeader::new(true),
                    slot_and_heap: [0u8; super::node::PAGE_DATA_SIZE],
                });
            }
            meta.ptr().root = pid;
        }
        let root = meta.ptr().root;
        drop(meta);
        Ok(Self {
            bm,
            root_pid: Arc::new(std::sync::atomic::AtomicU64::new(root)),
        })
    }

    fn validate_leaf(&self, node: &mut GuardX<BTreeNode>) -> Result<()> {
        let _pid = node.pid;
        let slot_count = node.ptr().hdr.count as usize;
        for i in 0..slot_count {
            let s = node.ptr().get_slot(i);
            let end = (s.offset as usize)
                .saturating_add(s.key_len as usize)
                .saturating_add(s.payload_len as usize);
            if end > crate::memory::PAGE_SIZE
                || s.offset as usize + s.key_len as usize > crate::memory::PAGE_SIZE
            {
                return Err("leaf slot corrupt".into());
            }
        }
        Ok(())
    }

    fn validate_inner(&self, node: &mut GuardX<BTreeNode>) -> Result<()> {
        let _pid = node.pid;
        if node.ptr().hdr.is_leaf {
            return Err("validate_inner called on leaf".into());
        }
        if node.ptr().hdr.upper_inner == u64::MAX && node.ptr().hdr.count > 0 {
            // Attempt to repair using first child pointer.
            let child0 = node.ptr().get_child(0);
            if child0 != u64::MAX {
                node.ptr().hdr.upper_inner = child0;
            }
        }
        if node.ptr().hdr.upper_inner == u64::MAX {
            return Err("invalid upper_inner".into());
        }
        let children = node.ptr().children();
        if children.len() != node.ptr().hdr.count as usize + 1 {
            return Err("child count mismatch".into());
        }
        for c in children.iter() {
            if *c == u64::MAX {
                return Err("invalid child pid".into());
            }
        }
        Ok(())
    }

    /// Ascending scan starting from a given key, invoking callback for each payload; stop when callback returns false.
    pub fn scan_asc<F: FnMut(&[u8], &[u8]) -> bool>(&self, key: &[u8], mut f: F) {
        loop {
            match self.scan_inner(key, &mut f) {
                Ok(done) => {
                    if done {
                        return;
                    }
                }
                Err(_) => continue, // restart on optimistic restart
            }
        }
    }

    fn scan_inner<F: FnMut(&[u8], &[u8]) -> bool>(&self, key: &[u8], f: &mut F) -> Result<bool> {
        let meta = GuardO::<MetaDataPage>::new(self.bm.as_ref(), METADATA_PID)?;
        let mut node = GuardO::<BTreeNode>::new(self.bm.as_ref(), meta.ptr().root)?;
        // descend to leaf
        loop {
            if node.ptr().hdr.is_leaf {
                break;
            }
            let (pos, found) = node.ptr().lower_bound(key);
            let child_idx = if found { pos + 1 } else { pos };
            let child_pid = node.ptr().child_pid(child_idx as usize)?;
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }
        // iterate leaf chain
        let mut current = node;
        loop {
            let (mut pos, _) = current.ptr().lower_bound(key);
            while pos < current.ptr().hdr.count {
                let k = current.ptr().reconstruct_key(pos as usize);
                let payload = current.ptr().get_payload(pos as usize);
                if !f(&k, payload) {
                    return Ok(true);
                }
                pos += 1;
            }
            if current.ptr().hdr.upper_inner == u64::MAX {
                return Ok(true);
            }
            current = GuardO::<BTreeNode>::new(self.bm.as_ref(), current.ptr().hdr.upper_inner)?;
        }
    }

    pub fn lookup<F: FnMut(&[u8])>(&self, key: &[u8], mut f: F) -> bool {
        let found = loop {
            match self.lookup_inner(key, &mut f) {
                Ok(found) => break found,
                Err(_) => continue, // restart
            }
        };
        found
    }

    fn lookup_inner<F: FnMut(&[u8])>(&self, key: &[u8], f: &mut F) -> Result<bool> {
        let mut node = GuardO::<BTreeNode>::new(
            self.bm.as_ref(),
            self.root_pid.load(std::sync::atomic::Ordering::Acquire),
        )?;
        loop {
            if node.ptr().hdr.is_leaf {
                let (pos, found) = node.ptr().lower_bound(key);
                if !found {
                    return Ok(false);
                }
                f(node.ptr().get_payload(pos as usize));
                return Ok(true);
            }
            let (pos, found) = node.ptr().lower_bound(key);
            let child_idx = if found { pos + 1 } else { pos };
            let child_pid = node.ptr().child_pid(child_idx as usize)?;
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }
    }

    /// Insert or replace by key (duplicates overwrite).
    pub fn insert(&self, key: &[u8], payload: &[u8]) -> Result<()> {
        loop {
            match self.insert_inner(key, payload) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Delete a key from the tree. Returns true if removed.
    pub fn delete(&self, key: &[u8]) -> bool {
        let res = self.delete_inner(key); // best effort
        res.unwrap_or(false)
    }

    fn insert_inner(&self, key: &[u8], payload: &[u8]) -> Result<()> {
        let meta = GuardO::<MetaDataPage>::new(self.bm.as_ref(), METADATA_PID)?;
        let root_pid = if meta.ptr().root == 0 {
            let pid = self.bm.alloc_page()?;
            let node_ptr = self.bm.to_ptr(pid) as *mut BTreeNode;
            unsafe {
                node_ptr.write(BTreeNode {
                    hdr: super::node::BTreeNodeHeader::new(true),
                    slot_and_heap: [0u8; super::node::PAGE_DATA_SIZE],
                });
            }
            let mut meta_x = GuardX::<MetaDataPage>::new_from_pid(self.bm.as_ref(), METADATA_PID);
            meta_x.ptr().root = pid;
            self.root_pid
                .store(pid, std::sync::atomic::Ordering::Release);
            pid
        } else {
            meta.ptr().root
        };
        {
            let mut root_check = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), root_pid);
            if !root_check.ptr().hdr.is_leaf {
                self.validate_inner(&mut root_check)?;
            }
        }
        let mut node = GuardO::<BTreeNode>::new(self.bm.as_ref(), root_pid)?;
        while !node.ptr().hdr.is_leaf {
            let (pos, found) = node.ptr().lower_bound(key);
            let child_idx = if found { pos + 1 } else { pos };
            let child_pid = node.ptr().child_pid(child_idx as usize)?;
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }
        let mut leaf_x = GuardX::<BTreeNode>::new_from_optimistic(node)?;
        self.validate_leaf(&mut leaf_x)?;
        let entries = self.leaf_entries_with(&mut leaf_x, key, payload);
        if self.rebuild_leaf_with_entries(&mut leaf_x, &entries) {
            return Ok(());
        }
        let left_pid = leaf_x.pid;
        let (sep_key, new_pid) = self.split_leaf_from_entries(&mut leaf_x, entries)?;
        drop(leaf_x); // release X lock before updating parent
        self.insert_into_parent(left_pid, sep_key, new_pid)?;
        Ok(())
    }

    fn delete_inner(&self, key: &[u8]) -> Result<bool> {
        let root_pid = self.root_pid.load(std::sync::atomic::Ordering::Acquire);
        if root_pid == 0 {
            return Ok(false);
        }
        // descend to leaf
        let mut path: Vec<(GuardO<BTreeNode>, u16)> = Vec::new();
        let mut node = GuardO::<BTreeNode>::new(self.bm.as_ref(), root_pid)?;
        loop {
            if node.ptr().hdr.is_leaf {
                break;
            }
            let (pos, found) = node.ptr().lower_bound(key);
            let child_idx = if found { pos + 1 } else { pos };
            let child_pid = node.ptr().child_pid(child_idx as usize)?;
            path.push((node, child_idx));
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }
        let (pos, found) = node.ptr().lower_bound(key);
        if !found {
            return Ok(false);
        }
        let mut leaf_x = GuardX::new_from_optimistic(node)?;
        leaf_x.ptr().remove_slot(pos as usize);
        if leaf_x.ptr().hdr.count == 0
            && leaf_x.pid != self.root_pid.load(std::sync::atomic::Ordering::Acquire)
        {
            self.rebalance_after_delete_leaf(leaf_x.pid, path)?;
        }
        Ok(true)
    }

    fn rebalance_after_delete_leaf(
        &self,
        leaf_pid: u64,
        mut path: Vec<(GuardO<BTreeNode>, u16)>,
    ) -> Result<()> {
        // For now, attempt to borrow/merge with right sibling if parent info exists.
        if let Some((parent_o, pos)) = path.pop() {
            let mut parent_x = GuardX::new_from_optimistic(parent_o)?;
            // Prefer left sibling if exists, otherwise right.
            let left_sibling_pid = if pos > 0 {
                parent_x
                    .ptr()
                    .child_pid((pos - 1) as usize)
                    .unwrap_or(u64::MAX)
            } else {
                u64::MAX
            };
            let right_pid = if (pos as usize) < parent_x.ptr().hdr.count as usize {
                parent_x
                    .ptr()
                    .child_pid((pos + 1) as usize)
                    .unwrap_or(u64::MAX)
            } else {
                u64::MAX
            };

            if left_sibling_pid != u64::MAX && left_sibling_pid != leaf_pid {
                // merge leaf into left sibling
                let mut left =
                    GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), left_sibling_pid);
                let mut leaf = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), leaf_pid);
                for i in 0..leaf.ptr().hdr.count as usize {
                    let k = leaf.ptr().reconstruct_key(i);
                    let p = leaf.ptr().get_payload(i).to_vec();
                    let ok = left.ptr().insert_in_page(&k, &p);
                    if !ok {
                        return Err("merge into left sibling failed".into());
                    }
                }
                left.ptr().hdr.upper_inner = leaf.ptr().hdr.upper_inner;
                left.ptr().recompute_fences();
                left.ptr().make_hint();
                parent_x.ptr().remove_slot((pos - 1) as usize);
                self.bm.free_page(leaf.pid);
                self.handle_parent_underflow(parent_x, path)?;
                return Ok(());
            }

            if right_pid != leaf_pid && right_pid != u64::MAX {
                let mut right = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), right_pid);
                let mut left = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), leaf_pid);
                // merge all from right into left, update links
                for i in 0..right.ptr().hdr.count as usize {
                    let k = right.ptr().reconstruct_key(i);
                    let p = right.ptr().get_payload(i).to_vec();
                    let ok = left.ptr().insert_in_page(&k, &p);
                    if !ok {
                        return Err("merge from right failed".into());
                    }
                }
                left.ptr().hdr.upper_inner = right.ptr().hdr.upper_inner;
                left.ptr().recompute_fences();
                left.ptr().make_hint();
                // remove separator from parent
                if pos < parent_x.ptr().hdr.count {
                    parent_x.ptr().remove_slot(pos as usize);
                } else if parent_x.ptr().hdr.count > 0 {
                    let last = (parent_x.ptr().hdr.count - 1) as usize;
                    parent_x.ptr().remove_slot(last);
                }
                self.bm.free_page(right.pid);
                self.handle_parent_underflow(parent_x, path)?;
                return Ok(());
            }
        }
        // fallback: no merge
        Ok(())
    }

    fn handle_parent_underflow(
        &self,
        mut parent_x: GuardX<BTreeNode>,
        _path: Vec<(GuardO<BTreeNode>, u16)>,
    ) -> Result<()> {
        // root shrink
        if parent_x.pid == self.root_pid.load(std::sync::atomic::Ordering::Acquire)
            && parent_x.ptr().hdr.count == 0
        {
            // promote only child
            let new_root = parent_x.ptr().hdr.upper_inner;
            if new_root != u64::MAX {
                self.root_pid
                    .store(new_root, std::sync::atomic::Ordering::Release);
                let mut meta = GuardX::<MetaDataPage>::new_from_pid(self.bm.as_ref(), METADATA_PID);
                meta.ptr().root = new_root;
            }
            return Ok(());
        }
        if parent_x.ptr().hdr.count > 0 && !parent_x.ptr().underfull() {
            return Ok(());
        }
        // For now tolerate underfull internal nodes; structure remains correct though less balanced.
        Ok(())
    }

    fn insert_into_parent(&self, left_pid: u64, sep_key: Vec<u8>, right_pid: u64) -> Result<()> {
        // Walk from root with optimistic reads.
        let meta = GuardO::<MetaDataPage>::new(self.bm.as_ref(), METADATA_PID)?;
        let mut path: Vec<(GuardO<BTreeNode>, u16)> = Vec::new();
        let mut node = GuardO::<BTreeNode>::new(self.bm.as_ref(), meta.ptr().root)?;
        loop {
            if node.ptr().hdr.is_leaf || node.pid == left_pid {
                break;
            }
            let (pos, found) = node.ptr().lower_bound(&sep_key);
            let child_idx = if found { pos + 1 } else { pos };
            let child_pid = node.ptr().child_pid(child_idx as usize)?;
            path.push((node, child_idx));
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }
        self.insert_into_parent_from_path(path, left_pid, sep_key, right_pid)
    }

    fn insert_into_parent_from_path(
        &self,
        mut path: Vec<(GuardO<BTreeNode>, u16)>,
        left_pid: u64,
        sep_key: Vec<u8>,
        right_pid: u64,
    ) -> Result<()> {
        while let Some((parent_o, pos)) = path.pop() {
            let mut parent_x = GuardX::new_from_optimistic(parent_o)?;
            // Duplicate separator: replace existing mapping to point to new right child.
            if pos > 0 {
                let slot_idx = (pos - 1) as usize;
                let existing_key = parent_x.ptr().reconstruct_key(slot_idx);
                if existing_key == sep_key {
                    parent_x.ptr().remove_slot(slot_idx);
                    let ok = parent_x
                        .ptr()
                        .insert_in_page(&sep_key, &right_pid.to_le_bytes());
                    if ok {
                        self.validate_inner(&mut parent_x)?;
                        return Ok(());
                    } else {
                        return Err("replace existing separator failed".into());
                    }
                }
            }
            if parent_x.ptr().has_space_for(sep_key.len(), 8) {
                let ok = parent_x
                    .ptr()
                    .insert_in_page(&sep_key, &right_pid.to_le_bytes());
                if ok {
                    self.validate_inner(&mut parent_x)?;
                    return Ok(());
                }
            }
            // split inner node
            let (new_sep, new_inner_pid) =
                self.split_inner(&mut parent_x, sep_key.clone(), right_pid, pos as usize)?;
            // propagate upward
            let parent_pid = parent_x.pid;
            drop(parent_x); // release before recursing to avoid self-deadlock
            return self.insert_into_parent_from_path(path, parent_pid, new_sep, new_inner_pid);
        }

        // parent is root
        if self.root_pid.load(std::sync::atomic::Ordering::Acquire) == left_pid {
            let new_root_pid = self.bm.alloc_page()?;
            let new_root_ptr = self.bm.to_ptr(new_root_pid) as *mut BTreeNode;
            unsafe {
                new_root_ptr.write(BTreeNode {
                    hdr: super::node::BTreeNodeHeader::new(false),
                    slot_and_heap: [0u8; super::node::PAGE_DATA_SIZE],
                });
            }
            let mut root_x = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), new_root_pid);
            root_x.ptr().hdr.upper_inner = left_pid;
            root_x.ptr().set_fences(&[], &[]);
            let ok = root_x
                .ptr()
                .insert_in_page(&sep_key, &right_pid.to_le_bytes());
            if !ok {
                return Err("root insert failed".into());
            }
            let mut meta_x = GuardX::<MetaDataPage>::new_from_pid(self.bm.as_ref(), METADATA_PID);
            meta_x.ptr().root = new_root_pid;
            self.root_pid
                .store(new_root_pid, std::sync::atomic::Ordering::Release);
            self.validate_inner(&mut root_x)?;
            return Ok(());
        }
        Err("failed to insert separator into parent".into())
    }

    /// Split an inner node inserting sep_key/right_pid; returns (separator to propagate, new inner PID).
    fn split_inner(
        &self,
        node: &mut GuardX<BTreeNode>,
        sep_key: Vec<u8>,
        right_pid: u64,
        insert_pos: usize,
    ) -> Result<(Vec<u8>, u64)> {
        // Collect existing keys/children.
        let count = node.ptr().hdr.count as usize;
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(count + 1);
        for i in 0..count {
            keys.push(node.ptr().reconstruct_key(i));
        }
        let mut children: Vec<u64> = Vec::with_capacity(count + 2);
        children.push(node.ptr().hdr.upper_inner);
        for i in 0..count {
            children.push(node.ptr().get_child(i));
        }
        assert!(insert_pos <= keys.len(), "insert_pos out of range");
        // Insert new separator/right child at the known position.
        keys.insert(insert_pos, sep_key.clone());
        children.insert(insert_pos + 1, right_pid);

        let new_count = keys.len();
        let sep_slot = new_count / 2;
        let propagate_key = keys[sep_slot].clone();

        // Split keys/children around the separator slot.
        let left_keys = keys[..sep_slot].to_vec();
        let right_keys = keys[(sep_slot + 1)..].to_vec();
        let left_children = children[..=sep_slot].to_vec(); // len = left_keys + 1
        let right_children = children[(sep_slot + 1)..].to_vec(); // len = right_keys + 1
        let left_child0 = left_children[0];
        let right_child0 = right_children[0];

        let new_pid = self.bm.alloc_page().expect("alloc new inner");
        let new_ptr = self.bm.to_ptr(new_pid) as *mut BTreeNode;
        unsafe {
            new_ptr.write(BTreeNode {
                hdr: super::node::BTreeNodeHeader::new(false),
                slot_and_heap: [0u8; super::node::PAGE_DATA_SIZE],
            });
        }
        let mut new_node = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), new_pid);
        // rebuild right
        new_node.ptr().hdr.upper_inner = right_child0;
        let old_lower = node.ptr().lower_fence().to_vec();
        let old_upper = node.ptr().upper_fence().to_vec();
        let right_lower = propagate_key.clone();
        let right_upper = if old_upper.is_empty() {
            right_keys
                .last()
                .cloned()
                .unwrap_or_else(|| propagate_key.clone())
        } else {
            old_upper.clone()
        };
        new_node.ptr().set_fences(&right_lower, &right_upper);
        for (k, child) in right_keys.iter().zip(right_children.iter().skip(1)) {
            let ok = new_node.ptr().insert_in_page(k, &child.to_le_bytes());
            if !ok {
                return Err("split inner right insert failed".into());
            }
        }
        new_node.ptr().make_hint();

        // rebuild left (original node)
        let left_lower = if old_lower.is_empty() {
            left_keys.first().cloned().unwrap_or_else(Vec::new)
        } else {
            old_lower.clone()
        };
        let left_upper = propagate_key.clone();
        node.ptr().hdr = super::node::BTreeNodeHeader::new(false);
        node.ptr().hdr.upper_inner = left_child0;
        node.ptr().set_fences(&left_lower, &left_upper);
        for (k, child) in left_keys.iter().zip(left_children.iter().skip(1)) {
            let ok = node.ptr().insert_in_page(k, &child.to_le_bytes());
            if !ok {
                return Err("split inner left insert failed".into());
            }
        }
        node.ptr().make_hint();

        // Ensure children are valid
        self.validate_inner(node)?;
        self.validate_inner(&mut new_node)?;

        Ok((propagate_key, new_pid))
    }

    fn leaf_entries_with(
        &self,
        node: &mut GuardX<BTreeNode>,
        key: &[u8],
        payload: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> =
            Vec::with_capacity((node.ptr().hdr.count as usize) + 1);
        for i in 0..node.ptr().hdr.count as usize {
            entries.push((
                node.ptr().reconstruct_key(i),
                node.ptr().get_payload(i).to_vec(),
            ));
        }
        match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(idx) => entries[idx].1 = payload.to_vec(),
            Err(pos) => entries.insert(pos, (key.to_vec(), payload.to_vec())),
        }
        // Reject clearly corrupt payloads to surface the root cause.
        for (k, p) in entries.iter() {
            if p.len() > crate::memory::PAGE_SIZE {
                return vec![];
            }
            if k.is_empty() && p.is_empty() {
                return vec![];
            }
        }
        entries
    }

    fn rebuild_leaf_with_entries(
        &self,
        node: &mut GuardX<BTreeNode>,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> bool {
        let next = node.ptr().hdr.upper_inner;
        node.ptr().hdr = super::node::BTreeNodeHeader::new(true);
        node.ptr().hdr.upper_inner = next;
        let lower = entries.first().map(|e| e.0.clone()).unwrap_or_default();
        let upper = entries.last().map(|e| e.0.clone()).unwrap_or_default();
        node.ptr().set_fences(&lower, &upper);
        for (k, p) in entries.iter() {
            if !node.ptr().has_space_for(k.len(), p.len()) {
                return false;
            }
            let ok = node.ptr().insert_in_page(k, p);
            if !ok {
                return false;
            }
        }
        node.ptr().make_hint();
        true
    }

    fn split_leaf_from_entries(
        &self,
        node: &mut GuardX<BTreeNode>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(Vec<u8>, u64)> {
        self.validate_leaf(node)?;
        let mid = entries.len() / 2;
        let right_entries = entries[mid..].to_vec();
        let left_entries = entries[..mid].to_vec();
        let sep_key = right_entries
            .first()
            .expect("split leaf non-empty right")
            .0
            .clone();

        let new_pid = self.bm.alloc_page()?;
        let new_ptr = self.bm.to_ptr(new_pid) as *mut BTreeNode;
        unsafe {
            new_ptr.write(BTreeNode {
                hdr: super::node::BTreeNodeHeader::new(true),
                slot_and_heap: [0u8; super::node::PAGE_DATA_SIZE],
            });
        }
        let next = node.ptr().hdr.upper_inner;
        let left_lower = left_entries.first().unwrap().0.clone();
        let left_upper = left_entries.last().unwrap().0.clone();
        let right_lower = sep_key.clone();
        let right_upper = right_entries.last().unwrap().0.clone();
        // rebuild left
        node.ptr().hdr = super::node::BTreeNodeHeader::new(true);
        node.ptr().hdr.upper_inner = new_pid;
        node.ptr().set_fences(&left_lower, &left_upper);
        for (k, p) in left_entries.iter() {
            let ok = node.ptr().insert_in_page(k, p);
            if !ok {
                return Err("split leaf left insert failed".into());
            }
        }
        // rebuild right
        let mut new_node = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), new_pid);
        new_node.ptr().hdr.upper_inner = next;
        new_node.ptr().set_fences(&right_lower, &right_upper);
        for (k, p) in right_entries.iter() {
            let ok = new_node.ptr().insert_in_page(k, p);
            if !ok {
                return Err("split leaf right insert failed".into());
            }
        }
        node.ptr().make_hint();
        new_node.ptr().make_hint();
        Ok((sep_key, new_pid))
    }
}

/// Test-only helpers to introspect leaf fence/prefix state.
#[cfg(test)]
impl BTree {
    pub fn debug_leaves(&self) -> Result<Vec<(u64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, u16)>> {
        let mut out = Vec::new();
        let mut node = GuardO::<BTreeNode>::new(
            self.bm.as_ref(),
            self.root_pid.load(std::sync::atomic::Ordering::Acquire),
        )?;
        while !node.ptr().hdr.is_leaf {
            let child_pid = node.ptr().child_pid(0)?;
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }
        loop {
            if node.ptr().hdr.count > 0 {
                let first = node.ptr().reconstruct_key(0);
                let last = node
                    .ptr()
                    .reconstruct_key((node.ptr().hdr.count - 1) as usize);
                out.push((
                    node.pid,
                    node.ptr().lower_fence().to_vec(),
                    node.ptr().upper_fence().to_vec(),
                    first,
                    last,
                    node.ptr().hdr.prefix_len,
                ));
            }
            if node.ptr().hdr.upper_inner == u64::MAX {
                break;
            }
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), node.ptr().hdr.upper_inner)?;
        }
        Ok(out)
    }
}
