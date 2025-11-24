use crate::btree::tree::BTree;
use crate::buffer_manager::BufferManager;
use rand::{Rng, seq::SliceRandom};
use std::sync::Arc;

#[test]
fn mixed_insert_delete_scan_stress() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_mixed".to_string(), 128, 64, 4).unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    let mut keys: Vec<u8> = (0u8..100).collect();
    let mut rng = rand::thread_rng();
    keys.shuffle(&mut rng);
    for &k in &keys {
        tree.insert(&[k], &[k + 1]).unwrap();
    }
    // random deletes
    for _ in 0..50 {
        let k = rng.gen_range(0u8..100);
        let _ = tree.delete(&[k]);
    }
    // random inserts again
    for _ in 0..30 {
        let k = rng.gen_range(0u8..100);
        tree.insert(&[k], &[k + 1]).unwrap();
    }
    // scan and count remaining
    let mut seen = 0;
    tree.scan_asc(&[0], |_k, _v| {
        seen += 1;
        true
    });
    assert!(seen <= 100);
}
