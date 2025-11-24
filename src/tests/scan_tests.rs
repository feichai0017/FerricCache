use crate::btree::tree::BTree;
use crate::buffer_manager::BufferManager;
use std::sync::Arc;

#[test]
fn scan_ascending_iterates_all_keys() {
    let bm = Arc::new(
        BufferManager::new_with_pages("/tmp/ferric_btree_scan".to_string(), 32, 16, 4).unwrap(),
    );
    let tree = BTree::new(bm.clone()).unwrap();
    for i in 0u8..30 {
        let k = [i];
        let v = [i + 1];
        tree.insert(&k, &v).unwrap();
    }
    let mut seen = 0;
    tree.scan_asc(&[0], |k, v| {
        assert_eq!(k[0] + 1, v[0]);
        seen += 1;
        true
    });
    assert_eq!(seen, 30);
}
