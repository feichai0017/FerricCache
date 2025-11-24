use std::sync::atomic::{AtomicU64, Ordering};

/// Page lock state bits stored in the top 8 bits of the 64-bit word.
pub mod state {
    pub const UNLOCKED: u8 = 0;
    pub const MAX_SHARED: u8 = 252;
    pub const LOCKED: u8 = 253;
    pub const MARKED: u8 = 254;
    pub const EVICTED: u8 = 255;
}

/// Combined state + version word (state in the high 8 bits, version in low 56).
#[derive(Debug)]
pub struct PageState {
    state_and_version: AtomicU64,
}

impl PageState {
    pub fn new() -> Self {
        Self {
            state_and_version: AtomicU64::new(0),
        }
    }

    /// Initialize to evicted with version zero.
    pub fn init_evicted(&self) {
        self.state_and_version
            .store(Self::pack(state::EVICTED, 0), Ordering::Release);
    }

    pub fn load(&self) -> u64 {
        self.state_and_version.load(Ordering::Acquire)
    }

    pub fn state(v: u64) -> u8 {
        (v >> 56) as u8
    }

    pub fn version(v: u64) -> u64 {
        v & ((1u64 << 56) - 1)
    }

    pub fn with_same_version(old: u64, new_state: u8) -> u64 {
        let ver = Self::version(old);
        Self::pack(new_state, ver)
    }

    pub fn with_next_version(old: u64, new_state: u8) -> u64 {
        let ver = Self::version(old).wrapping_add(1);
        Self::pack(new_state, ver)
    }

    fn pack(state: u8, version: u64) -> u64 {
        ((state as u64) << 56) | (version & ((1u64 << 56) - 1))
    }

    /// Attempt to transition to X-locked while keeping version.
    pub fn try_lock_x(&self, expected: u64) -> bool {
        self.state_and_version
            .compare_exchange(expected, Self::with_same_version(expected, state::LOCKED), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn unlock_x(&self) {
        let current = self.load();
        debug_assert_eq!(Self::state(current), state::LOCKED);
        self.state_and_version
            .store(Self::with_next_version(current, state::UNLOCKED), Ordering::Release);
    }

    pub fn unlock_x_evicted(&self) {
        let current = self.load();
        debug_assert_eq!(Self::state(current), state::LOCKED);
        self.state_and_version
            .store(Self::with_next_version(current, state::EVICTED), Ordering::Release);
    }

    /// Downgrade X lock to a single shared holder.
    pub fn downgrade_lock(&self) {
        let current = self.load();
        debug_assert_eq!(Self::state(current), state::LOCKED);
        self.state_and_version
            .store(Self::with_next_version(current, 1), Ordering::Release);
    }

    /// Attempt to take a shared lock (S) based on current state.
    ///
    /// - UNLOCKED: increment to 1
    /// - existing shared count < MAX_SHARED: increment
    /// - MARKED: allow shared access, set to 1
    /// Returns true on success.
    pub fn try_lock_s(&self, expected: u64) -> bool {
        let s = Self::state(expected);
        if s < state::MAX_SHARED {
            return self
                .state_and_version
                .compare_exchange(
                    expected,
                    Self::with_same_version(expected, s.saturating_add(1)),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
        }
        if s == state::MARKED {
            return self
                .state_and_version
                .compare_exchange(
                    expected,
                    Self::with_same_version(expected, 1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
        }
        false
    }

    /// Release a shared lock (decrement shared count).
    pub fn unlock_s(&self) {
        loop {
            let current = self.load();
            let s = Self::state(current);
            debug_assert!(s > 0 && s <= state::MAX_SHARED);
            let target = Self::with_same_version(current, s - 1);
            if self
                .state_and_version
                .compare_exchange(current, target, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // If we just released the last shared lock, notify potential parkers.
                if s == 1 {
                    std::thread::yield_now();
                }
                return;
            }
        }
    }

    /// Try to transition from UNLOCKED to MARKED.
    pub fn try_mark(&self, expected: u64) -> bool {
        debug_assert_eq!(Self::state(expected), state::UNLOCKED);
        self.state_and_version
            .compare_exchange(
                expected,
                Self::with_same_version(expected, state::MARKED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    #[inline]
    pub fn get_state(&self) -> u8 {
        Self::state(self.load())
    }
}

impl Default for PageState {
    fn default() -> Self {
        Self::new()
    }
}
