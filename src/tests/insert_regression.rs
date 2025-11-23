use crate::btree::tree::BTree;
use crate::buffer_manager::BufferManager;
use std::sync::Arc;

#[test]
fn inserts_preserve_all_keys() {
    let bm = Arc::new(BufferManager::new_with_pages(
        "/tmp/ferric_btree_insert_reg".to_string(),
        256,
        128,
        4,
    ).unwrap());
    let tree = BTree::new(bm.clone()).unwrap();
    for i in 0u64..200 {
        let key = i.to_le_bytes();
        let val = (i + 1).to_le_bytes();
        tree.insert(&key, &val).unwrap();
    }
    let mut missing = Vec::new();
    for i in 0u64..200 {
        let key = i.to_le_bytes();
        let ok = tree.lookup(&key, |_p| {});
        if !ok {
            missing.push(i);
        }
    }
    assert!(missing.is_empty(), "missing keys after insert: {:?}", missing);
}
