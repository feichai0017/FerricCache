use crate::btree::tree::BTree;
use crate::buffer_manager::BufferManager;
use std::sync::Arc;

#[test]
fn duplicate_overwrites() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_dup".to_string(), 8, 8, 2).unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    tree.insert(b"k1", b"v1").unwrap();
    tree.insert(b"k1", b"v2").unwrap(); // overwrite
    let mut got = None;
    let ok = tree.lookup(b"k1", |p| {
        got = Some(p.to_vec());
    });
    assert!(ok);
    assert_eq!(got.unwrap(), b"v2");
}

#[test]
fn delete_single_leaf() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_delete".to_string(), 8, 8, 2).unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    tree.insert(b"k1", b"v1").unwrap();
    tree.insert(b"k2", b"v2").unwrap();
    assert!(tree.delete(b"k1"));
    assert!(!tree.lookup(b"k1", |_p| {}));
    assert!(tree.lookup(b"k2", |p| assert_eq!(p, b"v2")));
}

#[test]
fn delete_triggers_merge() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_delete_merge".to_string(), 32, 16, 4)
            .unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    // Insert enough to split
    for i in 0u8..20 {
        let k = [i];
        let v = [i + 1];
        tree.insert(&k, &v).unwrap();
    }
    // Delete almost all to underflow left leaf
    for i in 0u8..10 {
        let k = [i];
        assert!(tree.delete(&k));
    }
    // Remaining keys should still be findable
    for i in 10u8..20 {
        let k = [i];
        let expect = i + 1;
        assert!(tree.lookup(&k, |p| assert_eq!(p[0], expect)));
    }
}

#[test]
fn delete_merge_with_left_sibling() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_delete_merge_left".to_string(), 32, 16, 4)
            .unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    for i in 0u8..30 {
        let k = [i];
        tree.insert(&k, &[i + 1]).unwrap();
    }
    // Delete higher keys to make right leaf empty, forcing merge into left sibling
    for i in 20u8..30 {
        assert!(tree.delete(&[i]));
    }
    // Remaining keys still present
    for i in 0u8..20 {
        let expect = i + 1;
        assert!(tree.lookup(&[i], |p| assert_eq!(p[0], expect)));
    }
}

#[test]
fn deep_delete_separators_correct() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_delete_deep".to_string(), 64, 32, 4)
            .unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    for i in 0u8..60 {
        tree.insert(&[i], &[i + 1]).unwrap();
    }
    for i in (10u8..50).rev() {
        assert!(tree.delete(&[i]));
    }
    // Verify remaining keys (0..10 and 50..60)
    for i in 0u8..10 {
        let expect = i + 1;
        assert!(tree.lookup(&[i], |p| assert_eq!(p[0], expect)));
    }
    for i in 50u8..60 {
        let expect = i + 1;
        assert!(tree.lookup(&[i], |p| assert_eq!(p[0], expect)));
    }
}
