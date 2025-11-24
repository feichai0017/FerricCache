use std::sync::atomic::{AtomicU64, Ordering};

/// Open-addressing hash set that tracks resident page IDs and supports a clock
/// pointer iteration pattern for eviction.
pub struct ResidentPageSet {
    entries: Vec<AtomicU64>,
    mask: u64,
    pub clock_pos: AtomicU64,
}

impl ResidentPageSet {
    pub const EMPTY: u64 = u64::MAX;
    pub const TOMBSTONE: u64 = u64::MAX - 1;

    pub fn new(max_count: u64) -> Self {
        let capacity = next_pow2((max_count as f64 * 1.5).ceil() as u64);
        let mut entries = Vec::with_capacity(capacity as usize);
        for _ in 0..capacity {
            entries.push(AtomicU64::new(Self::EMPTY));
        }
        Self {
            entries,
            mask: capacity - 1,
            clock_pos: AtomicU64::new(0),
        }
    }

    pub fn capacity(&self) -> u64 {
        self.entries.len() as u64
    }

    fn hash(k: u64) -> u64 {
        const M: u64 = 0xc6a4a7935bd1e995;
        const R: u32 = 47;
        let mut h = 0x8445d61a4e774912 ^ (8u64.wrapping_mul(M));
        let mut v = k.wrapping_mul(M);
        v ^= v >> R;
        v = v.wrapping_mul(M);
        h ^= v;
        h = h.wrapping_mul(M);
        h ^= h >> R;
        h = h.wrapping_mul(M);
        h ^= h >> R;
        h
    }

    /// Insert a PID; panics if already present.
    pub fn insert(&self, pid: u64) {
        let mut pos = Self::hash(pid) & self.mask;
        loop {
            let curr = self.entries[pos as usize].load(Ordering::Acquire);
            assert!(curr != pid, "duplicate insert of pid {}", pid);
            if (curr == Self::EMPTY || curr == Self::TOMBSTONE)
                && self.entries[pos as usize]
                    .compare_exchange(curr, pid, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    /// Remove a PID; returns true if removed.
    pub fn remove(&self, pid: u64) -> bool {
        let mut pos = Self::hash(pid) & self.mask;
        loop {
            let curr = self.entries[pos as usize].load(Ordering::Acquire);
            if curr == Self::EMPTY {
                return false;
            }
            if curr == pid {
                return self.entries[pos as usize]
                    .compare_exchange(curr, Self::TOMBSTONE, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
            }
            pos = (pos + 1) & self.mask;
        }
    }

    /// Iterate over a batch of slots starting at the clock position.
    pub fn iterate_clock_batch<F: FnMut(u64)>(&self, batch: u64, mut f: F) {
        let mut pos;
        let mut new_pos;
        loop {
            pos = self.clock_pos.load(Ordering::Acquire);
            new_pos = (pos + batch) & self.mask;
            if self
                .clock_pos
                .compare_exchange(pos, new_pos, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        let mut p = pos;
        for _ in 0..batch {
            let curr = self.entries[p as usize].load(Ordering::Acquire);
            if curr != Self::EMPTY && curr != Self::TOMBSTONE {
                f(curr);
            }
            p = (p + 1) & self.mask;
        }
    }
}

fn next_pow2(x: u64) -> u64 {
    1 << (64 - (x - 1).leading_zeros() as u64)
}
