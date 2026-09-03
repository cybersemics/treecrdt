# TreeCRDT VersionVector v0 encoding

VersionVector v0 is the canonical binary encoding of a gap-aware version vector. Fields specified to
contain VersionVector v0 bytes carry this encoding directly. Transports may carry it as an opaque byte
string, but must not re-encode the value as JSON or another representation.

## Layout

All integers are unsigned and big-endian, and quoted strings are ASCII bytes. `||` means
concatenation, `bytes[n]` means exactly `n` bytes, and `value[n]` means exactly `n` repetitions.

```text
vector = "TCVV" || u8(0) || u32(n) || entry[n]
entry  = u32(m) || bytes[m] || u64(frontier) || u32(r) || range[r]
range  = u64(start) || u64(end)
```

`frontier` is the highest counter for which every value in `1..=frontier` was observed. Ranges are
inclusive additional observations after gaps.

## Canonical validity

- Replica IDs MUST be unique and sorted in unsigned bytewise lexicographic order. If one ID is a
  prefix of another, the shorter ID sorts first. The format permits a zero-length ID; higher-level
  profiles may impose narrower constraints.
- An entry with `frontier = 0` and no ranges is invalid and MUST be omitted.
- The canonical empty vector contains no entries and is nine bytes long. A zero-length byte string is
  not a VersionVector v0 encoding.
- For each range, `1 <= start <= end`, and `start` MUST be at least two greater than `previous_end`.
  `previous_end` is the frontier for the first range and the prior range's end thereafter.
- Counts and lengths MUST consume exactly the input. Truncation, trailing bytes, and versions other
  than `0` are invalid.

The [shared test vectors](../fixtures/version-vector-v0.json) are normative: implementations MUST
produce the exact valid encodings and reject every invalid value.
