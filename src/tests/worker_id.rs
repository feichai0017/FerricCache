use crate::thread_local::{set_worker_id, get_worker_id};

#[test]
fn worker_id_roundtrip() {
    set_worker_id(42);
    assert_eq!(get_worker_id(), 42);

    // temporary override with restoration
    let r = crate::thread_local::with_worker_id(7, || get_worker_id());
    assert_eq!(r, 7);
    assert_eq!(get_worker_id(), 42);
}
