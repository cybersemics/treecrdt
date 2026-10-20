use crate::error::{Error, Result};

const ORDER_KEY_DOMAIN: &[u8] = b"treecrdt/order_key/v0";
const DIGIT_BYTES: usize = 2;
const DEFAULT_BOUNDARY: u16 = 10;
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn decode_digits(bytes: &[u8]) -> Result<Vec<u16>> {
    if !bytes.len().is_multiple_of(DIGIT_BYTES) {
        return Err(Error::InvalidOperation(
            "order_key must have even length (u16 big-endian digits)".into(),
        ));
    }
    Ok(bytes
        .as_chunks::<DIGIT_BYTES>()
        .0
        .iter()
        .map(|chunk| u16::from_be_bytes(*chunk))
        .collect())
}

fn encode_digits(digits: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(digits.len() * DIGIT_BYTES);
    for d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

fn sample_u64(seed: &[u8], depth: usize) -> u64 {
    let mut h = FNV_OFFSET_BASIS;
    for b in ORDER_KEY_DOMAIN {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    for b in &(seed.len() as u32).to_be_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    for b in seed {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    for b in &(depth as u32).to_be_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

fn choose_side(seed: &[u8], depth: usize) -> bool {
    // true = choose near left, false = choose near right
    (sample_u64(seed, depth) & 1) == 0
}

fn choose_in_range(seed: &[u8], depth: usize, lo: u16, hi: u16) -> u16 {
    debug_assert!(lo <= hi);
    if lo == hi {
        return lo;
    }
    let span = (hi - lo) as u32 + 1;
    let n = (sample_u64(seed, depth) % (span as u64)) as u16;
    lo + n
}

/// Allocate a stable ordering key strictly between `left` and `right` (lexicographic order).
///
/// Keys are encoded as a variable-length sequence of big-endian `u16` digits, compared
/// lexicographically. The generator is LSEQ-inspired: it allocates within a bounded interval
/// next to one bound so that runs of appends, prepends, or inserts at one anchor reuse a digit
/// level instead of adding one per insert.
pub fn allocate_between(left: Option<&[u8]>, right: Option<&[u8]>, seed: &[u8]) -> Result<Vec<u8>> {
    let left_digits = decode_digits(left.unwrap_or_default())?;
    let right_digits = decode_digits(right.unwrap_or_default())?;

    let mut out: Vec<u16> = Vec::new();
    let mut depth: usize = 0;
    // `right` bounds the next digit only while `out` is still a prefix of it.
    let mut right_bounded = true;

    loop {
        let left_digit = left_digits.get(depth).copied();
        let right_digit = right_digits.get(depth).copied().filter(|_| right_bounded);
        let ld = left_digit.unwrap_or(0);
        let rd = right_digit.unwrap_or(u16::MAX);
        if rd < ld {
            return Err(Error::InvalidOperation(
                "cannot allocate order_key: right < left".into(),
            ));
        }

        if u32::from(rd) > u32::from(ld) + 1 {
            let gap = rd - ld - 1;
            let boundary = DEFAULT_BOUNDARY.min(gap);
            let near_left = (ld + 1, ld + boundary);
            let near_right = (rd - boundary, rd - 1);

            // Allocate next to the bound that has a digit at this depth and leave the open side
            // free: appends and typing advance from the left, while prepends and repeated inserts
            // after one anchor move down from the right. Without such a bound, start in the
            // middle; with both, the seed picks a side.
            let (lo, hi) = match (left_digit, right_digit) {
                (Some(_), None) => near_left,
                (None, Some(_)) => near_right,
                (None, None) => {
                    let lo = ld + 1 + (gap - boundary) / 2;
                    (lo, lo + boundary - 1)
                }
                (Some(_), Some(_)) if choose_side(seed, depth) => near_left,
                (Some(_), Some(_)) => near_right,
            };

            out.push(choose_in_range(seed, depth, lo, hi));
            break;
        }

        // No room at this level; extend the prefix and continue deeper. Taking `ld < rd` here
        // already orders every deeper suffix below `right`.
        if rd > ld {
            right_bounded = false;
        }
        out.push(ld);
        depth += 1;
    }

    Ok(encode_digits(&out))
}
