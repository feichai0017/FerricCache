use std::cell::Cell;

thread_local! {
    static WORKER_ID: Cell<u16> = const { Cell::new(0) };
}

pub fn set_worker_id(id: u16) {
    WORKER_ID.with(|w| w.set(id));
}

pub fn get_worker_id() -> u16 {
    WORKER_ID.with(|w| w.get())
}

/// Execute a closure with a temporary worker id, restoring the previous value.
pub fn with_worker_id<F, T>(id: u16, f: F) -> T
where
    F: FnOnce() -> T,
{
    let prev = get_worker_id();
    set_worker_id(id);
    let res = f();
    set_worker_id(prev);
    res
}
