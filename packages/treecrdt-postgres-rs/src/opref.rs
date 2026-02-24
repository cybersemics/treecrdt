use blake3::Hasher;

pub const OPREF_V0_WIDTH: usize = 16;
const OPREF_V0_DOMAIN: &[u8] = b"treecrdt/opref/v0";

pub fn derive_op_ref_v0(doc_id: &str, replica: &[u8], counter: u64) -> [u8; OPREF_V0_WIDTH] {
    let mut hasher = Hasher::new();
    hasher.update(OPREF_V0_DOMAIN);
    hasher.update(doc_id.as_bytes());
    hasher.update(&(replica.len() as u32).to_be_bytes());
    hasher.update(replica);
    hasher.update(&counter.to_be_bytes());
    let hash = hasher.finalize();
    let mut out = [0u8; OPREF_V0_WIDTH];
    out.copy_from_slice(&hash.as_bytes()[0..OPREF_V0_WIDTH]);
    out
}

