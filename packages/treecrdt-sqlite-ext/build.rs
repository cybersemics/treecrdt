fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();
    if target == "wasm32-unknown-emscripten" {
        // When building the `cdylib` artifact for Emscripten we intentionally leave SQLite symbols
        // unresolved so the static library can be linked into wa-sqlite later.
        println!("cargo:rustc-link-arg=-sERROR_ON_UNDEFINED_SYMBOLS=0");
    }
}
