use proptest::prelude::*;
use treecrdt_core::order_key::allocate_between;
use treecrdt_core::NodeId;

fn allocate(left: Option<&[u8]>, right: Option<&[u8]>, counter: u64) -> Vec<u8> {
    let seed = [b"replica".as_slice(), &counter.to_be_bytes()].concat();
    let key = allocate_between(left, right, &seed, NodeId(counter.into())).unwrap();
    assert!(left.is_none_or(|left| key.as_slice() > left));
    assert!(right.is_none_or(|right| key.as_slice() < right));
    key
}

fn assert_keys_short(case: &str, first: &[u8], mut next: impl FnMut(&[u8], u64) -> Vec<u8>) {
    let mut key = first.to_vec();
    for counter in 1..=10_000 {
        key = next(&key, counter);
        // Every key ends with an 18-byte node suffix; inserts after one anchor also copy the
        // anchor's suffix.
        assert!(key.len() <= 16 + 2 * 18, "{case}: {} bytes", key.len());
    }
}

#[test]
fn repeated_placements_keep_keys_short() {
    let start = allocate(None, None, 0);
    let end = allocate(Some(&start), None, 1);
    let long: Vec<u8> =
        [0xfffe].into_iter().chain(1..=500_u16).flat_map(u16::to_be_bytes).collect();
    let append = |last: &[u8], counter| allocate(Some(last), None, counter);

    assert_keys_short("appends", &start, append);
    assert_keys_short("appends after a 1 KB key", &long, append);
    assert_keys_short("prepends", &start, |first, counter| {
        allocate(None, Some(first), counter)
    });
    assert_keys_short("typing between siblings", &start, |previous, counter| {
        allocate(Some(previous), Some(&end), counter)
    });
    assert_keys_short("inserts after one anchor", &end, |previous, counter| {
        allocate(Some(&start), Some(previous), counter)
    });
}

fn order_key() -> impl Strategy<Value = Vec<u8>> {
    // Extreme digits make adjacent and saturated bounds common.
    let digit = prop_oneof![3 => 0..=2_u16, 3 => 0xfffd..=u16::MAX, 1 => any::<u16>()];
    prop::collection::vec(digit, 1..6)
        .prop_filter("non-zero last digit", |digits| digits.last() != Some(&0))
        .prop_map(|digits| digits.into_iter().flat_map(u16::to_be_bytes).collect())
}

proptest! {
    #[test]
    fn allocates_distinct_keys_strictly_between_bounds(
        a in order_key(),
        b in order_key(),
        nest in any::<bool>(),
        open_left in any::<bool>(),
        open_right in any::<bool>(),
        seed in any::<[u8; 8]>(),
        nodes in any::<[u128; 2]>(),
    ) {
        let b = if nest { [a.as_slice(), &b].concat() } else { b };
        prop_assume!(a != b && nodes[0] != nodes[1]);
        let (low, high) = if a < b { (a, b) } else { (b, a) };
        let left = (!open_left).then_some(low.as_slice());
        let right = (!open_right).then_some(high.as_slice());

        // With one seed both nodes get the same position, as concurrent placements in one gap can.
        let keys = nodes.map(|node| allocate_between(left, right, &seed, NodeId(node)).unwrap());

        prop_assert_ne!(&keys[0], &keys[1]);
        for key in &keys {
            prop_assert!(key.len().is_multiple_of(2) && key[key.len() - 2..] != [0, 0]);
            prop_assert!(left.is_none_or(|left| key.as_slice() > left));
            prop_assert!(right.is_none_or(|right| key.as_slice() < right));
        }
    }
}
