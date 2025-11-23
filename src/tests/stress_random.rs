use crate::btree::tree::BTree;
use crate::buffer_manager::BufferManager;
use std::sync::Arc;
use rand::{seq::SliceRandom, Rng};

#[test]
fn random_insert_delete_scan_stability() {
    let bm = Arc::new(BufferManager::new_with_pages("/tmp/ferric_btree_stress".to_string(), 128, 64, 8).unwrap());
    let tree = BTree::new(bm.clone()).unwrap();
    let mut keys: Vec<u8> = (0u8..80).collect();
    let mut rng = rand::thread_rng();
    keys.shuffle(&mut rng);
    for &k in &keys {
        tree.insert(&[k], &[k + 1]).unwrap();
    }
    // delete half at random
    let mut to_delete = keys.clone();
    to_delete.shuffle(&mut rng);
    for &k in to_delete.iter().take(40) {
        let _ = tree.delete(&[k]);
    }
    // scan and count remaining
    let mut remaining = 0;
    tree.scan_asc(&[0], |_k, _v| {
        remaining += 1;
        true
    });
    assert!(remaining >= 40);
}
