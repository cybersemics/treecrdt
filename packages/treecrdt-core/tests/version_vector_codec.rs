use treecrdt_core::{Error, ReplicaId, VersionVector};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("../../../fixtures/version-vector-v0.json"))
        .expect("parse shared VersionVector v0 fixture")
}

fn fixture_hex(name: &str) -> String {
    fixture()["validEncodedHex"][name]
        .as_str()
        .expect("fixture encoded hex")
        .to_owned()
}

fn from_hex(value: &str) -> Vec<u8> {
    assert!(
        value.is_ascii() && value.len().is_multiple_of(2),
        "invalid fixture hex"
    );
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).expect("fixture hex byte"))
        .collect()
}

#[test]
fn v0_empty_vector_has_stable_bytes() {
    let bytes = VersionVector::new().encode_v0().expect("encode empty vector");
    assert_eq!(hex(&bytes), fixture_hex("empty"));
    assert_eq!(
        VersionVector::decode_v0(&bytes).expect("decode empty vector"),
        VersionVector::new()
    );
}

#[test]
fn v0_prefix_order_gaps_and_max_counter_have_stable_cross_runtime_bytes() {
    let short_replica = ReplicaId::new([0]);
    let prefixed_replica = ReplicaId::new([0, 1]);
    let mut vector = VersionVector::new();
    for counter in [1, 3] {
        vector.observe(&prefixed_replica, counter);
    }
    for counter in [1, 2, 4, 5, u64::MAX] {
        vector.observe(&short_replica, counter);
    }

    let bytes = vector.encode_v0().expect("encode fixture vector");
    assert_eq!(hex(&bytes), fixture_hex("prefixOrderGapsAndMaxCounter"));
    let decoded = VersionVector::decode_v0(&bytes).expect("decode fixture vector");
    assert_eq!(decoded, vector);
    assert_eq!(decoded.get(&short_replica), u64::MAX);
}

#[test]
fn zero_observations_do_not_create_semantic_entries() {
    let mut vector = VersionVector::new();
    vector.observe(&ReplicaId::new(b"replica"), 0);

    assert!(vector.is_empty());
}

#[test]
fn v0_preserves_zero_length_replica_id() {
    let replica = ReplicaId::new([]);
    let mut vector = VersionVector::new();
    vector.observe(&replica, 1);

    let bytes = vector.encode_v0().expect("encode zero-length replica ID");
    let decoded = VersionVector::decode_v0(&bytes).expect("decode zero-length replica ID");
    assert_eq!(decoded, vector);
}

#[test]
fn v0_rejects_shared_invalid_encodings() {
    let fixture = fixture();
    let invalid = fixture["invalidEncodedHex"]
        .as_object()
        .expect("invalidEncodedHex fixture object");

    for (name, encoded_hex) in invalid {
        let bytes = from_hex(encoded_hex.as_str().expect("invalid fixture encoded hex"));
        let error = VersionVector::decode_v0(&bytes).expect_err(name);
        assert!(
            matches!(error, Error::InvalidVersionVector(_)),
            "{name}: {error}"
        );
    }
}

proptest::proptest! {
    #[test]
    fn v0_roundtrips_observed_counters(
        observations in proptest::collection::vec(
            (proptest::collection::vec(proptest::num::u8::ANY, 1..=32), 1u64..=10_000),
            0..200,
        )
    ) {
        let mut vector = VersionVector::new();
        for (replica, counter) in observations {
            vector.observe(&ReplicaId::new(replica), counter);
        }
        let bytes = vector.encode_v0().expect("encode vector");
        let decoded = VersionVector::decode_v0(&bytes).expect("decode vector");
        proptest::prop_assert_eq!(decoded, vector);
    }


    #[test]
    fn v0_decoder_never_panics_on_arbitrary_bytes(
        bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..4096),
    ) {
        let _ = VersionVector::decode_v0(&bytes);
    }
}
