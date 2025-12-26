#![forbid(unsafe_code)]
//! WASM bridge for the `riblt` crate.
//!
//! This wrapper exposes a small, fixed-width (16-byte) symbol API suitable for
//! set reconciliation over `OpRef`-like identifiers.

use riblt::{CodedSymbol, Decoder, Encoder, Error as RibltError, HashedSymbol, Symbol};
use serde::{Deserialize, Serialize};
use serde_wasm_bindgen::{from_value, to_value};
use wasm_bindgen::prelude::*;

#[derive(Clone, Copy, Eq, PartialEq)]
struct Bytes16([u8; 16]);

impl Symbol for Bytes16 {
    fn zero() -> Self {
        Self([0u8; 16])
    }

    fn xor(&self, other: &Self) -> Self {
        let mut out = [0u8; 16];
        for i in 0..16 {
            out[i] = self.0[i] ^ other.0[i];
        }
        Self(out)
    }

    fn hash(&self) -> u64 {
        // Non-linear 64-bit hash used both as the RIBLT checksum and the PRNG seed for the
        // symbol-to-codeword mapping. This MUST NOT be XOR-linear w.r.t. symbol bytes, or the
        // checksum degenerates (e.g. `hash_sum == hash(xor_sum)` becomes tautological), causing
        // the decoder to incorrectly peel and fail.
        let hi = u64::from_be_bytes(self.0[0..8].try_into().unwrap());
        let lo = u64::from_be_bytes(self.0[8..16].try_into().unwrap());
        splitmix64(hi ^ splitmix64(lo ^ 0x9e37_79b9_7f4a_7c15))
    }
}

// Simple, deterministic, non-cryptographic mixing function (SplitMix64 finalizer).
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn parse_16(bytes: &[u8]) -> Result<Bytes16, JsValue> {
    if bytes.len() != 16 {
        return Err(JsValue::from_str("expected 16 bytes"));
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Ok(Bytes16(buf))
}

fn u64_to_be_bytes(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn u64_from_be_bytes(bytes: &[u8]) -> Result<u64, JsValue> {
    if bytes.len() != 8 {
        return Err(JsValue::from_str("expected 8 bytes"));
    }
    Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodewordJs {
    // Signed count for the codeword.
    count: i32,
    // XOR of hashes for symbols in this codeword (u64 as big-endian bytes).
    key_sum: Vec<u8>,
    // XOR of symbol bytes in this codeword (16 bytes).
    value_sum: Vec<u8>,
}

fn coded_to_js(c: &CodedSymbol<Bytes16>) -> CodewordJs {
    CodewordJs {
        count: c.count as i32,
        key_sum: u64_to_be_bytes(c.hash).to_vec(),
        value_sum: c.symbol.0.to_vec(),
    }
}

fn js_to_coded(js: CodewordJs) -> Result<CodedSymbol<Bytes16>, JsValue> {
    let symbol = parse_16(&js.value_sum)?;
    let hash = u64_from_be_bytes(&js.key_sum)?;
    Ok(CodedSymbol::<Bytes16> {
        symbol,
        hash,
        count: js.count as i64,
    })
}

fn hashed_symbols_to_js(values: &[HashedSymbol<Bytes16>]) -> Vec<Vec<u8>> {
    values.iter().map(|s| s.symbol.0.to_vec()).collect()
}

#[wasm_bindgen]
pub struct RibltEncoder16 {
    inner: Encoder<Bytes16>,
}

#[wasm_bindgen]
impl RibltEncoder16 {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Encoder::<Bytes16>::new(),
        }
    }

    pub fn reset(&mut self) {
        self.inner.reset();
    }

    #[wasm_bindgen(js_name = addSymbol)]
    pub fn add_symbol(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        let sym = parse_16(bytes)?;
        self.inner.add_symbol(&sym);
        Ok(())
    }

    /// Produce the next coded symbol to stream to a decoder.
    #[wasm_bindgen(js_name = nextCodeword)]
    pub fn next_codeword(&mut self) -> Result<JsValue, JsValue> {
        let coded = self.inner.produce_next_coded_symbol();
        to_value(&coded_to_js(&coded)).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

#[wasm_bindgen]
pub struct RibltDecoder16 {
    inner: Decoder<Bytes16>,
    codewords_received: u64,
}

#[wasm_bindgen]
impl RibltDecoder16 {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Decoder::<Bytes16>::new(),
            codewords_received: 0,
        }
    }

    pub fn reset(&mut self) {
        self.inner.reset();
        self.codewords_received = 0;
    }

    #[wasm_bindgen(js_name = addLocalSymbol)]
    pub fn add_local_symbol(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        let sym = parse_16(bytes)?;
        self.inner.add_symbol(&sym);
        Ok(())
    }

    #[wasm_bindgen(js_name = addCodeword)]
    pub fn add_codeword(&mut self, codeword: JsValue) -> Result<(), JsValue> {
        let js: CodewordJs =
            from_value(codeword).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let coded = js_to_coded(js)?;
        self.inner.add_coded_symbol(&coded);
        self.codewords_received += 1;
        Ok(())
    }

    #[wasm_bindgen(js_name = codewordsReceived)]
    pub fn codewords_received(&self) -> u64 {
        self.codewords_received
    }

    /// Attempt to decode with the codewords received so far.
    ///
    /// Returns `true` if fully decoded, `false` if not decoded yet.
    /// Throws if the underlying decoder reports a fatal error.
    #[wasm_bindgen(js_name = tryDecode)]
    pub fn try_decode(&mut self) -> Result<bool, JsValue> {
        match self.inner.try_decode() {
            Ok(()) => Ok(self.inner.decoded()),
            Err(RibltError::InvalidDegree) => Err(JsValue::from_str("riblt: invalid degree")),
            Err(RibltError::InvalidSize) => Err(JsValue::from_str("riblt: invalid size")),
            Err(RibltError::DecodeFailed) => Err(JsValue::from_str("riblt: decode failed")),
        }
    }

    #[wasm_bindgen(js_name = decoded)]
    pub fn decoded(&self) -> bool {
        self.inner.decoded()
    }

    /// Symbols present only on the encoder side (i.e. missing locally).
    #[wasm_bindgen(js_name = remoteMissing)]
    pub fn remote_missing(&self) -> Result<JsValue, JsValue> {
        to_value(&hashed_symbols_to_js(&self.inner.get_remote_symbols()))
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Symbols present only on the decoder local set (i.e. missing on the encoder side).
    #[wasm_bindgen(js_name = localMissing)]
    pub fn local_missing(&self) -> Result<JsValue, JsValue> {
        to_value(&hashed_symbols_to_js(&self.inner.get_local_symbols()))
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}
