use crate::btree::tree::BTree;
use crate::buffer_manager::BufferManager;
use std::sync::Arc;

/// Basic insert/lookup in single-leaf tree (no splits).
#[test]
fn btree_single_leaf_insert_lookup() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_test".to_string(), 4, 4, 2).unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    let k = b"abc";
    let v = b"payload";
    tree.insert(k, v).unwrap();
    let mut found = false;
    let ok = tree.lookup(k, |p| {
        found = true;
        assert_eq!(p, v);
    });
    assert!(ok);
    assert!(found);
}

/// Insert enough keys to force a leaf split and ensure lookup works across leaves.
#[test]
fn btree_split_and_lookup() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_split".to_string(), 32, 16, 4).unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    // Insert non-sequential keys to force multiple splits.
    let keys: Vec<u8> = (0u8..50).rev().collect();
    for &i in &keys {
        let k = [i];
        let v = [i + 1];
        tree.insert(&k, &v).unwrap();
    }
    for &i in &keys {
        let k = [i];
        let expect = i + 1;
        let mut seen = false;
        let ok = tree.lookup(&k, |p| {
            seen = true;
            assert_eq!(p[0], expect);
        });
        assert!(ok);
        assert!(seen);
    }
}
