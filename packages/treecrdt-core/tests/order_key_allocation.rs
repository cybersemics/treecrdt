use std::collections::VecDeque;

use proptest::prelude::*;
use treecrdt_core::order_key::allocate_between;

const RUN: u64 = 10_000;
// Every run below stays at 4-6 bytes. The previous allocator crossed this bound within the first
// few dozen inserts of each run.
const MAX_KEY_BYTES: usize = 16;

/// Allocates with a `TreeCrdt`-style seed and checks that the key sorts between its bounds.
fn allocate(left: Option<&[u8]>, right: Option<&[u8]>, counter: u64) -> Vec<u8> {
    let seed = [b"replica".as_slice(), &counter.to_be_bytes()].concat();
    let key = allocate_between(left, right, &seed).unwrap();
    assert!(left.is_none_or(|left| key.as_slice() > left));
    assert!(right.is_none_or(|right| key.as_slice() < right));
    key
}

/// Allocates `RUN` keys, each placed relative to the previous one, and returns the longest.
/// Counters 0 and 1 are left for the keys a test allocates first.
fn longest_chained(first: Vec<u8>, mut next: impl FnMut(&[u8], u64) -> Vec<u8>) -> usize {
    let mut previous = first;
    let mut longest = 0;
    for counter in 2..RUN + 2 {
        previous = next(&previous, counter);
        longest = longest.max(previous.len());
    }
    longest
}

fn assert_short(longest: usize) {
    assert!(
        longest <= MAX_KEY_BYTES,
        "longest order key is {longest} bytes"
    );
}

#[test]
fn appends_keep_keys_short() {
    let first = allocate(None, None, 0);
    assert_short(longest_chained(first, |last, counter| {
        allocate(Some(last), None, counter)
    }));
}

#[test]
fn prepends_keep_keys_short() {
    let last = allocate(None, None, 0);
    assert_short(longest_chained(last, |first, counter| {
        allocate(None, Some(first), counter)
    }));
}

#[test]
fn typing_between_siblings_keeps_keys_short() {
    // Each insert follows the previous one, before a fixed next sibling.
    let start = allocate(None, None, 0);
    let end = allocate(Some(&start), None, 1);
    assert_short(longest_chained(start, |previous, counter| {
        allocate(Some(previous), Some(&end), counter)
    }));
}

#[test]
fn repeated_inserts_after_one_anchor_keep_keys_short() {
    // Each insert lands directly after the anchor, before the previous insert.
    let anchor = allocate(None, None, 0);
    let end = allocate(Some(&anchor), None, 1);
    assert_short(longest_chained(end, |previous, counter| {
        allocate(Some(&anchor), Some(previous), counter)
    }));
}

#[test]
fn moving_last_child_to_front_keeps_keys_short() {
    let mut siblings = VecDeque::new();
    for counter in 0..100 {
        let key = allocate(siblings.back().map(Vec::as_slice), None, counter);
        siblings.push_back(key);
    }

    let mut longest = 0;
    for counter in 100..100 + RUN {
        siblings.pop_back();
        let key = allocate(None, siblings.front().map(Vec::as_slice), counter);
        longest = longest.max(key.len());
        siblings.push_front(key);
    }
    assert_short(longest);
}

#[test]
fn appends_after_long_existing_keys_are_short() {
    // Earlier allocators nested each append under the previous key, leaving keys like this one.
    let existing = [0xfffe].into_iter().chain(1..=500_u16).flat_map(u16::to_be_bytes).collect();
    assert_short(longest_chained(existing, |last, counter| {
        allocate(Some(last), None, counter)
    }));
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
    // Canonical keys end in a non-zero digit.
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
