use treecrdt_core::order_key::allocate_between;

#[test]
fn allocates_between_adjacent_prefixes_with_opposite_suffix_order() {
    // Once fffd < fffe, later digits must not be compared across the bounds, even when the right
    // suffix is smaller.
    let left = [0xff, 0xfd, 0xff, 0xfe];
    let right = [0xff, 0xfe, 0xff, 0xfc];

    let allocated = allocate_between(Some(&left), Some(&right), b"em-undo-redo").unwrap();

    assert!(allocated.as_slice() > left.as_slice());
    assert!(allocated.as_slice() < right.as_slice());
}

#[test]
fn allocates_after_maximum_digit_without_overflow() {
    let left = [0xff, 0xff];

    let allocated = allocate_between(Some(&left), None, b"append-after-ffff").unwrap();

    assert!(allocated.as_slice() > left.as_slice());
}
