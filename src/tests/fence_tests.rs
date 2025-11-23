use crate::btree::tree::BTree;
use crate::buffer_manager::BufferManager;

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let limit = std::cmp::min(a.len(), b.len());
    let mut i = 0;
    while i < limit && a[i] == b[i] {
        i += 1;
    }
    i
}

#[test]
fn fences_and_prefix_maintained_after_splits_and_merges() {
    let bm = BufferManager::new_with_pages("/tmp/ferric_fence_test".to_string(), 256, 32, 4).unwrap();
    let tree = BTree::new(std::sync::Arc::new(bm)).unwrap();
    // insert enough keys to force multiple leaf splits
    for i in 0..200u64 {
        let key = i.to_le_bytes();
        tree.insert(&key, &key).unwrap();
    }
    // delete some keys to trigger merges
    for i in (0..200u64).step_by(4) {
        let key = i.to_le_bytes();
        tree.delete(&key);
    }
    let leaves = tree.debug_leaves().unwrap();
    assert!(!leaves.is_empty());
    for (_pid, lower, upper, first, last, prefix_len) in leaves {
        assert!(!lower.is_empty() && !upper.is_empty(), "fences should be set");
        assert_eq!(lower, first, "lower fence must match first key");
        assert_eq!(upper, last, "upper fence must match last key");
        let expected_prefix = common_prefix_len(&lower, &upper) as u16;
        assert_eq!(prefix_len, expected_prefix, "prefix length should match common prefix");
    }
}
