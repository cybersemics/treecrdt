//! Input-validation failures cross the bridge as strings; runtime failures remain errors.

use serde::Serialize;
use treecrdt_core::VersionVector;
use wasm_bindgen::prelude::*;

/// Validate the structured input before encoding; deserialization never normalizes it.
#[wasm_bindgen(js_name = encodeVersionVectorV0)]
pub fn encode_version_vector_v0(value: JsValue) -> Result<Vec<u8>, JsValue> {
    let vector: VersionVector = serde_wasm_bindgen::from_value(value)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    vector.encode_v0().map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = decodeVersionVectorV0)]
pub fn decode_version_vector_v0(bytes: &[u8]) -> Result<JsValue, JsValue> {
    let vector =
        VersionVector::decode_v0(bytes).map_err(|error| JsValue::from_str(&error.to_string()))?;
    // Even small counters stay bigint in JavaScript, preserving the full u64 domain.
    vector
        .serialize(
            &serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true),
        )
        .map_err(Into::into)
}
