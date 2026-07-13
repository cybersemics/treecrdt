//! Deterministic allocation of sibling ordering keys.
//!
//! Keys are non-empty sequences of big-endian `u16` digits. The final digit is non-zero, which
//! keeps the valid key space dense under the bytewise ordering used by storage backends.

use crate::error::{Error, Result};

const ORDER_KEY_DOMAIN: &[u8] = b"treecrdt/order_key/v1";
const DIGIT_BYTES: usize = 2;
const ENTROPY_DIGITS: usize = 4;
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Validate the canonical encoding used by structural insert and move operations.
pub(crate) fn validate_order_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidOperation(
            "structural order_key must be non-empty".into(),
        ));
    }
    if !key.len().is_multiple_of(DIGIT_BYTES) {
        return Err(Error::InvalidOperation(
            "structural order_key must contain complete big-endian u16 digits".into(),
        ));
    }
    if key[key.len() - DIGIT_BYTES..] == [0, 0] {
        return Err(Error::InvalidOperation(
            "structural order_key must end in a non-zero u16 digit".into(),
        ));
    }
    Ok(())
}

fn decode_digits(bytes: &[u8]) -> Result<Vec<u16>> {
    validate_order_key(bytes)?;
    Ok(bytes
        .chunks_exact(DIGIT_BYTES)
        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
        .collect())
}

fn encode_digits(digits: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(digits.len() * DIGIT_BYTES);
    for digit in digits {
        out.extend_from_slice(&digit.to_be_bytes());
    }
    out
}

fn hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn sample_u64(seed: &[u8], depth: usize) -> u64 {
    let mut hash = hash_bytes(FNV_OFFSET_BASIS, ORDER_KEY_DOMAIN);
    hash = hash_bytes(hash, &(seed.len() as u64).to_be_bytes());
    hash = hash_bytes(hash, seed);
    hash = hash_bytes(hash, &(depth as u64).to_be_bytes());

    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^ (hash >> 33)
}

fn append_entropy(digits: &mut Vec<u16>, seed: &[u8], depth: usize) {
    let sample = sample_u64(seed, depth);
    for index in 0..ENTROPY_DIGITS {
        let shift = (ENTROPY_DIGITS - index - 1) * u16::BITS as usize;
        let raw = ((sample >> shift) & u64::from(u16::MAX)) as u16;
        digits.push(if index + 1 == ENTROPY_DIGITS {
            (u32::from(raw) % u32::from(u16::MAX) + 1) as u16
        } else {
            raw
        });
    }
}

fn allocate_after(left: &[u16], seed: &[u8], depth: usize) -> Vec<u16> {
    let mut out = left.to_vec();
    append_entropy(&mut out, seed, depth + left.len());
    out
}

fn allocate_before(right: &[u16], seed: &[u8], depth: usize) -> Vec<u16> {
    let offset = right
        .iter()
        .position(|digit| *digit != 0)
        .expect("validated key has a non-zero final digit");
    let mut out = right[..offset].to_vec();
    out.push(right[offset] / 2);
    append_entropy(&mut out, seed, depth + offset + 1);
    out
}

fn allocate_between_digits(left: &[u16], right: &[u16], seed: &[u8]) -> Vec<u16> {
    let common = left.iter().zip(right).take_while(|(a, b)| a == b).count();

    if common == left.len() {
        let mut out = left.to_vec();
        out.extend(allocate_before(&right[common..], seed, common));
        return out;
    }

    let left_digit = left[common];
    let right_digit = right[common];
    let mut out = left[..common].to_vec();
    if u32::from(right_digit) > u32::from(left_digit) + 1 {
        out.push(((u32::from(left_digit) + u32::from(right_digit)) / 2) as u16);
        append_entropy(&mut out, seed, common + 1);
    } else {
        out.push(left_digit);
        out.extend(allocate_after(&left[common + 1..], seed, common + 1));
    }
    out
}

/// Allocate a deterministic key strictly between `left` and `right` in bytewise order.
///
/// `None` opens that side of the key space. Every provided bound must be a canonical structural
/// key: non-empty, even-length, and ending in a non-zero `u16` digit.
pub fn allocate_between(left: Option<&[u8]>, right: Option<&[u8]>, seed: &[u8]) -> Result<Vec<u8>> {
    let left_digits = left.map(decode_digits).transpose()?;
    let right_digits = right.map(decode_digits).transpose()?;

    if let (Some(left), Some(right)) = (left, right) {
        if left >= right {
            return Err(Error::InvalidOperation(
                "cannot allocate order_key: bounds must be in strictly increasing order".into(),
            ));
        }
    }

    let digits = match (left_digits.as_deref(), right_digits.as_deref()) {
        (None, None) => {
            let mut out = Vec::with_capacity(ENTROPY_DIGITS);
            append_entropy(&mut out, seed, 0);
            out
        }
        (Some(left), None) => allocate_after(left, seed, 0),
        (None, Some(right)) => allocate_before(right, seed, 0),
        (Some(left), Some(right)) => allocate_between_digits(left, right, seed),
    };

    let encoded = encode_digits(&digits);
    validate_order_key(&encoded)?;
    if left.is_some_and(|bound| encoded.as_slice() <= bound)
        || right.is_some_and(|bound| encoded.as_slice() >= bound)
    {
        return Err(Error::InvalidOperation(
            "cannot allocate order_key strictly between bounds".into(),
        ));
    }
    Ok(encoded)
}
