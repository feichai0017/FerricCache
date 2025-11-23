use crate::buffer_manager::BufferManager;
use crate::guard::{GuardO, GuardX};
use crate::Result;
use super::node::BTreeNode;
use crate::memory::PAGE_SIZE;
use std::sync::Arc;

pub const METADATA_PID: u64 = 0;

#[repr(C)]
pub struct MetaDataPage {
    pub dirty: bool,
    pub root: u64,
}

/// Minimal BTree wrapper: single root/leaf, no splits yet.
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
        Ok(Self { bm, root_pid: Arc::new(std::sync::atomic::AtomicU64::new(root)) })
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
            let (pos, _) = node.ptr().lower_bound(key);
            let child_pid = if pos == node.ptr().hdr.count {
                node.ptr().hdr.upper_inner
            } else {
                node.ptr().get_child(pos as usize)
            };
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
        loop {
            match self.lookup_inner(key, &mut f) {
                Ok(found) => return found,
                Err(_) => continue, // restart
            }
        }
    }

    fn lookup_inner<F: FnMut(&[u8])>(&self, key: &[u8], f: &mut F) -> Result<bool> {
        let root_pid = self.root_pid.load(std::sync::atomic::Ordering::Acquire);
        let node = GuardO::<BTreeNode>::new(self.bm.as_ref(), root_pid)?;
        let (pos, found) = node.ptr().lower_bound(key);
        if !found {
            return Ok(false);
        }
        f(node.ptr().get_payload(pos as usize));
        Ok(true)
    }

    /// Insert or replace by key (duplicates overwrite).
    pub fn insert(&self, key: &[u8], payload: &[u8]) -> Result<()> {
        loop {
            match self.insert_inner(key, payload) {
                Ok(_) => return Ok(()),
                Err(_) => continue, // restart
            }
        }
    }

    /// Delete a key from the tree. Returns true if removed.
    pub fn delete(&self, key: &[u8]) -> bool {
        loop {
            match self.delete_inner(key) {
                Ok(res) => return res,
                Err(_) => continue, // restart
            }
        }
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
            // safety: meta is GuardO; upgrade to X to set root
            let mut meta_x = GuardX::<MetaDataPage>::new_from_pid(self.bm.as_ref(), METADATA_PID);
            meta_x.ptr().root = pid;
            self.root_pid.store(pid, std::sync::atomic::Ordering::Release);
            pid
        } else {
            meta.ptr().root
        };
        let mut node = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), root_pid);
        // duplicate: replace in place
        let (pos, found) = node.ptr().lower_bound(key);
        if found {
            let buf = node.ptr().get_payload_mut(pos as usize);
            let len = buf.len().min(payload.len());
            buf[..len].copy_from_slice(&payload[..len]);
            return Ok(());
        }
        if node.ptr().has_space_for(key.len(), payload.len()) {
            node.ptr().insert_in_page(key, payload);
            return Ok(());
        }
        // split leaf
        let (sep_key, new_pid) = self.split_leaf(&mut node)?;
        self.insert_into_parent(root_pid, sep_key, new_pid)?;
        // retry the original insert after split
        self.insert_inner(key, payload)
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
            let (pos, _) = node.ptr().lower_bound(key);
            let child_pid = if pos == node.ptr().hdr.count {
                node.ptr().hdr.upper_inner
            } else {
                node.ptr().get_child(pos as usize)
            };
            path.push((node, pos));
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }
        let (pos, found) = node.ptr().lower_bound(key);
        if !found {
            return Ok(false);
        }
        let mut leaf_x = GuardX::new_from_optimistic(node)?;
        leaf_x.ptr().remove_slot(pos as usize);
        if leaf_x.ptr().hdr.count == 0 && leaf_x.pid != self.root_pid.load(std::sync::atomic::Ordering::Acquire) {
            self.rebalance_after_delete_leaf(leaf_x.pid, path)?;
        }
        Ok(true)
    }

    fn rebalance_after_delete_leaf(&self, leaf_pid: u64, mut path: Vec<(GuardO<BTreeNode>, u16)>) -> Result<()> {
        // For now, attempt to borrow/merge with right sibling if parent info exists.
        if let Some((parent_o, pos)) = path.pop() {
            let mut parent_x = GuardX::new_from_optimistic(parent_o)?;
            // Prefer left sibling if exists, otherwise right.
            let left_sibling_pid = if pos > 0 {
                parent_x.ptr().get_child((pos - 1) as usize)
            } else { u64::MAX };
            let right_pid = if pos < parent_x.ptr().hdr.count {
                parent_x.ptr().get_child(pos as usize)
            } else {
                parent_x.ptr().hdr.upper_inner
            };

            if left_sibling_pid != u64::MAX && left_sibling_pid != leaf_pid {
                // merge leaf into left sibling
                let mut left = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), left_sibling_pid);
                let mut leaf = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), leaf_pid);
                for i in 0..leaf.ptr().hdr.count as usize {
                    let k = leaf.ptr().reconstruct_key(i);
                    let p = leaf.ptr().get_payload(i).to_vec();
                    left.ptr().insert_in_page(&k, &p);
                }
                left.ptr().hdr.upper_inner = leaf.ptr().hdr.upper_inner;
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
                    left.ptr().insert_in_page(&k, &p);
                }
                left.ptr().hdr.upper_inner = right.ptr().hdr.upper_inner;
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

    fn handle_parent_underflow(&self, mut parent_x: GuardX<BTreeNode>, mut path: Vec<(GuardO<BTreeNode>, u16)>) -> Result<()> {
        // root shrink
        if parent_x.pid == self.root_pid.load(std::sync::atomic::Ordering::Acquire) && parent_x.ptr().hdr.count == 0 {
            // promote only child
            let new_root = parent_x.ptr().hdr.upper_inner;
            if new_root != u64::MAX {
                self.root_pid.store(new_root, std::sync::atomic::Ordering::Release);
                let mut meta = GuardX::<MetaDataPage>::new_from_pid(self.bm.as_ref(), METADATA_PID);
                meta.ptr().root = new_root;
            }
            return Ok(());
        }
        if parent_x.ptr().hdr.count > 0 && !parent_x.ptr().underfull() {
            return Ok(());
        }
        // Merge/borrow parent into sibling using recorded path.
        if let Some((grand_o, grand_pos)) = path.pop() {
            let mut grand_x = GuardX::new_from_optimistic(grand_o)?;
            let left_pid = if grand_pos > 0 {
                grand_x.ptr().get_child((grand_pos - 1) as usize)
            } else { u64::MAX };
            let right_pid = if grand_pos < grand_x.ptr().hdr.count {
                grand_x.ptr().get_child(grand_pos as usize)
            } else { grand_x.ptr().hdr.upper_inner };
            // Try borrow from left sibling: move its last entry into parent, adjust fences.
            if left_pid != u64::MAX && left_pid != parent_x.pid {
                let mut left = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), left_pid);
                if left.ptr().hdr.count > 0 && parent_x.ptr().has_space_for(left.ptr().hdr.prefix_len as usize, 8) {
                    let borrow_idx = (left.ptr().hdr.count - 1) as usize;
                    let k = left.ptr().reconstruct_key(borrow_idx);
                    let p = left.ptr().get_payload(borrow_idx).to_vec();
                    left.ptr().remove_slot(borrow_idx);
                    parent_x.ptr().insert_in_page(&k, &p);
                    // Recompute fences based on children bounds.
                    let lower = left.ptr().lower_fence().to_vec();
                    let upper = parent_x.ptr().upper_fence().to_vec();
                    parent_x.ptr().set_fences(&lower, &upper);
                    grand_x.ptr().remove_slot((grand_pos - 1) as usize);
                    left.ptr().recompute_fences();
                    parent_x.ptr().recompute_fences();
                    grand_x.ptr().recompute_fences();
                    return Ok(());
                }
            }
            // Try borrow from right sibling: move its first entry into parent.
            if right_pid != u64::MAX && right_pid != parent_x.pid {
                let mut right = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), right_pid);
                if right.ptr().hdr.count > 0 && parent_x.ptr().has_space_for(right.ptr().hdr.prefix_len as usize, 8) {
                    let k = right.ptr().reconstruct_key(0);
                    let p = right.ptr().get_payload(0).to_vec();
                    right.ptr().remove_slot(0);
                    parent_x.ptr().insert_in_page(&k, &p);
                    let lower = parent_x.ptr().lower_fence().to_vec();
                    let upper = right.ptr().upper_fence().to_vec();
                    parent_x.ptr().set_fences(&lower, &upper);
                    if grand_pos < grand_x.ptr().hdr.count {
                        grand_x.ptr().remove_slot(grand_pos as usize);
                    } else if grand_x.ptr().hdr.count > 0 {
                        let last = (grand_x.ptr().hdr.count - 1) as usize;
                        grand_x.ptr().remove_slot(last);
                    }
                    right.ptr().recompute_fences();
                    parent_x.ptr().recompute_fences();
                    grand_x.ptr().recompute_fences();
                    return Ok(());
                }
            }
            // Merge into left if possible
            if left_pid != u64::MAX && left_pid != parent_x.pid {
                let mut left = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), left_pid);
                let tail = parent_x.ptr().hdr.upper_inner;
                for i in 0..parent_x.ptr().hdr.count as usize {
                    let k = parent_x.ptr().reconstruct_key(i);
                    let p = parent_x.ptr().get_payload(i).to_vec();
                    left.ptr().insert_in_page(&k, &p);
                }
                if tail != u64::MAX {
                    let sep = left.ptr().upper_fence().to_vec();
                    left.ptr().insert_in_page(&sep, &tail.to_le_bytes());
                }
                left.ptr().hdr.upper_inner = parent_x.ptr().hdr.upper_inner;
                grand_x.ptr().remove_slot((grand_pos - 1) as usize);
                self.bm.free_page(parent_x.pid);
                return self.handle_parent_underflow(grand_x, path);
            }
            // Merge into right
            if right_pid != u64::MAX && right_pid != parent_x.pid {
                let mut right = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), right_pid);
                let tail = parent_x.ptr().hdr.upper_inner;
                for i in 0..parent_x.ptr().hdr.count as usize {
                    let k = parent_x.ptr().reconstruct_key(i);
                    let p = parent_x.ptr().get_payload(i).to_vec();
                    right.ptr().insert_in_page(&k, &p);
                }
                if tail != u64::MAX {
                    let sep = right.ptr().lower_fence().to_vec();
                    right.ptr().insert_in_page(&sep, &tail.to_le_bytes());
                }
                if grand_pos < grand_x.ptr().hdr.count {
                    grand_x.ptr().remove_slot(grand_pos as usize);
                } else if grand_x.ptr().hdr.count > 0 {
                    let last = (grand_x.ptr().hdr.count - 1) as usize;
                    grand_x.ptr().remove_slot(last);
                }
                self.bm.free_page(parent_x.pid);
                return self.handle_parent_underflow(grand_x, path);
            }
        }
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
            let (pos, _) = node.ptr().lower_bound(&sep_key);
            let child_pid = if pos == node.ptr().hdr.count {
                node.ptr().hdr.upper_inner
            } else {
                node.ptr().get_child(pos as usize)
            };
            path.push((node, pos));
            node = GuardO::<BTreeNode>::new(self.bm.as_ref(), child_pid)?;
        }

        // Try to insert separator into parent (top of path or root if empty).
        while let Some((parent_o, pos)) = path.pop() {
            let mut parent_x = GuardX::new_from_optimistic(parent_o)?;
            if parent_x.ptr().has_space_for(sep_key.len(), 8) {
                parent_x.ptr().insert_in_page(&sep_key, &right_pid.to_le_bytes());
                return Ok(());
            }
            // split inner node
            let (new_sep, new_inner_pid) = self.split_inner(&mut parent_x, sep_key.clone(), right_pid, pos as usize);
            // propagate upward
            return self.insert_into_parent(parent_x.pid, new_sep, new_inner_pid);
        }

        // parent is root
        let meta = GuardO::<MetaDataPage>::new(self.bm.as_ref(), METADATA_PID)?;
        if meta.ptr().root == left_pid {
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
            root_x.ptr().insert_in_page(&sep_key, &right_pid.to_le_bytes());
            let mut meta_x = GuardX::<MetaDataPage>::new_from_pid(self.bm.as_ref(), METADATA_PID);
            meta_x.ptr().root = new_root_pid;
            self.root_pid.store(new_root_pid, std::sync::atomic::Ordering::Release);
            return Ok(());
        }
        Err("failed to insert separator into parent".into())
    }

    fn split_leaf(&self, node: &mut GuardX<BTreeNode>) -> Result<(Vec<u8>, u64)> {
        let count = node.ptr().hdr.count as usize;
        if count < 2 {
            return Err("cannot split leaf with <2 entries".into());
        }
        let sep_slot = count / 2;
        let sep_key = node.ptr().reconstruct_key(sep_slot);

        let new_pid = self.bm.alloc_page()?;
        let new_ptr = self.bm.to_ptr(new_pid) as *mut BTreeNode;
        unsafe {
            new_ptr.write(BTreeNode {
                hdr: super::node::BTreeNodeHeader::new(true),
                slot_and_heap: [0u8; super::node::PAGE_DATA_SIZE],
            });
        }
        let mut new_node = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), new_pid);
        new_node.ptr().set_fences(&sep_key, node.ptr().upper_fence());

        // move second half to new leaf
        for i in sep_slot..count {
            let k = node.ptr().reconstruct_key(i);
            let p = node.ptr().get_payload(i).to_vec();
            new_node.ptr().insert_in_page(&k, &p);
        }
        // trim old node to first half
        node.ptr().hdr.count = sep_slot as u16;
        node.ptr().hdr.data_offset = PAGE_SIZE as u16;
        node.ptr().hdr.space_used = 0;
        let lower = node.ptr().lower_fence().to_vec();
        node.ptr().set_fences(&lower, &sep_key);
        node.ptr().compactify();
        node.ptr().make_hint();
        new_node.ptr().make_hint();
        // link leaves
        new_node.ptr().hdr.upper_inner = node.ptr().hdr.upper_inner;
        node.ptr().hdr.upper_inner = new_pid;
        Ok((sep_key, new_pid))
    }

    /// Split an inner node inserting sep_key/right_pid; returns (separator to propagate, new inner PID).
    // TODO: support duplicates by defining a policy (e.g., replace or allow multiple).
    fn split_inner(&self, node: &mut GuardX<BTreeNode>, sep_key: Vec<u8>, right_pid: u64, _insert_pos: usize) -> (Vec<u8>, u64) {
        // Insert separator to make splitting easier (may compactify internally)
        node.ptr().insert_in_page(&sep_key, &right_pid.to_le_bytes());
        let count = node.ptr().hdr.count as usize;
        let sep_slot = count / 2;
        let propagate_key = node.ptr().reconstruct_key(sep_slot);

        let new_pid = self.bm.alloc_page().expect("alloc new inner");
        let new_ptr = self.bm.to_ptr(new_pid) as *mut BTreeNode;
        unsafe {
            new_ptr.write(BTreeNode {
                hdr: super::node::BTreeNodeHeader::new(false),
                slot_and_heap: [0u8; super::node::PAGE_DATA_SIZE],
            });
        }
        let mut new_node = GuardX::<BTreeNode>::new_from_pid(self.bm.as_ref(), new_pid);
        // left: slots [0, sep_slot)
        // sep_slot is propagated
        // right: slots [sep_slot+1, end)
        new_node.ptr().set_fences(&propagate_key, node.ptr().upper_fence());
        for i in (sep_slot + 1)..count {
            let k = node.ptr().reconstruct_key(i);
            let p = node.ptr().get_payload(i).to_vec();
            new_node.ptr().insert_in_page(&k, &p);
        }
        new_node.ptr().hdr.upper_inner = node.ptr().hdr.upper_inner;
        node.ptr().hdr.count = sep_slot as u16;
        node.ptr().hdr.data_offset = PAGE_SIZE as u16;
        node.ptr().hdr.space_used = 0;
        let lower = node.ptr().lower_fence().to_vec();
        node.ptr().set_fences(&lower, &propagate_key);
        node.ptr().compactify();
        node.ptr().make_hint();
        new_node.ptr().make_hint();
        (propagate_key, new_pid)
    }
}
