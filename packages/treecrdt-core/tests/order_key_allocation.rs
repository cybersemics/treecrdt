use proptest::prelude::*;
use treecrdt_core::order_key::allocate_between;

// Every run stays at 4-6 bytes; the previous allocator passed this within a few dozen inserts.
const MAX_KEY_BYTES: usize = 16;

/// Allocates with a `TreeCrdt`-style seed and checks that the key sorts between its bounds.
fn allocate(left: Option<&[u8]>, right: Option<&[u8]>, counter: u64) -> Vec<u8> {
    let seed = [b"replica".as_slice(), &counter.to_be_bytes()].concat();
    let key = allocate_between(left, right, &seed).unwrap();
    assert!(left.is_none_or(|left| key.as_slice() > left));
    assert!(right.is_none_or(|right| key.as_slice() < right));
    key
}

/// Allocates 10,000 keys, each placed relative to the previous one, and returns the longest.
fn longest_chained(first: &[u8], mut next: impl FnMut(&[u8], u64) -> Vec<u8>) -> usize {
    let mut previous = first.to_vec();
    let mut longest = 0;
    for counter in 1..=10_000 {
        previous = next(&previous, counter);
        longest = longest.max(previous.len());
    }
    longest
}

#[test]
fn repeated_placements_keep_keys_short() {
    let start = allocate(None, None, 0);
    let end = allocate(Some(&start), None, 1);
    // The previous allocator nested each append under the previous key.
    let long: Vec<u8> =
        [0xfffe].into_iter().chain(1..=500_u16).flat_map(u16::to_be_bytes).collect();
    let append = |last: &[u8], counter| allocate(Some(last), None, counter);

    let longest = [
        ("appends", longest_chained(&start, append)),
        ("appends after a 1 KB key", longest_chained(&long, append)),
        (
            "prepends",
            longest_chained(&start, |first, counter| {
                allocate(None, Some(first), counter)
            }),
        ),
        (
            "typing between siblings",
            longest_chained(&start, |previous, counter| {
                allocate(Some(previous), Some(&end), counter)
            }),
        ),
        (
            "inserts after one anchor",
            longest_chained(&end, |previous, counter| {
                allocate(Some(&start), Some(previous), counter)
            }),
        ),
    ];
    assert!(
        longest.iter().all(|(_, bytes)| *bytes <= MAX_KEY_BYTES),
        "{longest:?}"
    );
}

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

fn digit() -> impl Strategy<Value = u16> {
    // Extreme digits make adjacent and saturated bounds common.
    prop_oneof![
        Just(0),
        Just(1),
        Just(2),
        Just(0xfffd),
        Just(0xfffe),
        Just(0xffff),
        any::<u16>(),
    ]
}

fn order_key() -> impl Strategy<Value = Vec<u8>> {
    (
        prop::collection::vec(digit(), 0..5),
        digit().prop_filter("final digit is non-zero", |digit| *digit != 0),
    )
        .prop_map(|(mut digits, last)| {
            digits.push(last);
            digits.iter().flat_map(|digit| digit.to_be_bytes()).collect()
        })
}

proptest! {
    #[test]
    fn allocates_strictly_between_bounds(
        a in order_key(),
        b in order_key(),
        nest in any::<bool>(),
        open_left in any::<bool>(),
        open_right in any::<bool>(),
        seed in any::<[u8; 8]>(),
    ) {
        let b = if nest { [a.as_slice(), &b].concat() } else { b };
        prop_assume!(a != b);
        let (low, high) = if a < b { (a, b) } else { (b, a) };
        let left = (!open_left).then_some(low.as_slice());
        let right = (!open_right).then_some(high.as_slice());

        let key = allocate_between(left, right, &seed).unwrap();

        prop_assert!(key.len().is_multiple_of(2) && key[key.len() - 2..] != [0, 0]);
        prop_assert!(left.is_none_or(|left| key.as_slice() > left));
        prop_assert!(right.is_none_or(|right| key.as_slice() < right));
    }
}
