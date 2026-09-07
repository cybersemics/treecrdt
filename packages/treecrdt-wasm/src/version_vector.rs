use serde::Serialize;
use treecrdt_core::VersionVector;
use wasm_bindgen::prelude::*;

/// Validate the structured input before encoding; deserialization never normalizes it.
#[wasm_bindgen(js_name = encodeVersionVectorV0)]
pub fn encode_version_vector_v0(value: JsValue) -> Result<Vec<u8>, JsError> {
    let vector: VersionVector = serde_wasm_bindgen::from_value(value)?;
    Ok(vector.encode_v0()?)
}

#[wasm_bindgen(js_name = decodeVersionVectorV0)]
pub fn decode_version_vector_v0(bytes: &[u8]) -> Result<JsValue, JsError> {
    let vector = VersionVector::decode_v0(bytes)?;
    // Even small counters stay bigint in JavaScript, preserving the full u64 domain.
    Ok(vector.serialize(
        &serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true),
    )?)
}
