use serde::Serialize;
use treecrdt_core::VersionVector;
use wasm_bindgen::prelude::*;

/// Validate the structured input before encoding; deserialization never normalizes it.
#[wasm_bindgen(js_name = encodeVersionVector)]
pub fn encode_version_vector(value: JsValue) -> Result<Vec<u8>, JsError> {
    let vector: VersionVector = serde_wasm_bindgen::from_value(value)?;
    Ok(vector.encode()?)
}

#[wasm_bindgen(js_name = decodeVersionVector)]
pub fn decode_version_vector(bytes: &[u8]) -> Result<JsValue, JsError> {
    let vector = VersionVector::decode(bytes)?;
    // Even small counters stay bigint in JavaScript, preserving the full u64 domain.
    Ok(vector.serialize(
        &serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true),
    )?)
}
