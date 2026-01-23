use crate::error::{Error, Result};

const ORDER_KEY_DOMAIN: &[u8] = b"treecrdt/order_key/v0";
const DIGIT_BYTES: usize = 2;
const DEFAULT_BOUNDARY: u16 = 10;
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn decode_digits(bytes: &[u8]) -> Result<Vec<u16>> {
    if bytes.len() % DIGIT_BYTES != 0 {
        return Err(Error::InvalidOperation(
            "order_key must have even length (u16 big-endian digits)".into(),
        ));
    }
    Ok(bytes
        .chunks_exact(DIGIT_BYTES)
        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
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
/// lexicographically. The generator is LSEQ-inspired: it prefers allocating within a bounded
/// interval near one side to reduce expected key growth in repeated "insert between the same
/// neighbors" workloads.
pub fn allocate_between(left: Option<&[u8]>, right: Option<&[u8]>, seed: &[u8]) -> Result<Vec<u8>> {
    let left_digits = decode_digits(left.unwrap_or_default())?;
    let right_digits = decode_digits(right.unwrap_or_default())?;

    let mut out: Vec<u16> = Vec::new();
    let mut depth: usize = 0;

    loop {
        let ld = left_digits.get(depth).copied().unwrap_or(0);
        let rd = right_digits.get(depth).copied().unwrap_or(u16::MAX);
        if rd < ld {
            return Err(Error::InvalidOperation(
                "cannot allocate order_key: right < left".into(),
            ));
        }

        if rd > ld + 1 {
            let gap = rd - ld - 1;
            let boundary = DEFAULT_BOUNDARY.min(gap);
            let choose_left = choose_side(seed, depth);

            let (lo, hi) = if gap > boundary {
                if choose_left {
                    (ld + 1, ld + boundary)
                } else {
                    (rd - boundary, rd - 1)
                }
            } else {
                (ld + 1, rd - 1)
            };

            out.push(choose_in_range(seed, depth, lo, hi));
            break;
        }

        // No room at this level; extend the prefix and continue deeper.
        out.push(ld);
        depth += 1;
    }

    Ok(encode_digits(&out))
}
