use std::env;
use std::path::Path;

fn main() {
    // Get the directory where the output files will be placed
    let out_dir = env::var("OUT_DIR").unwrap();

    // Generate the header file using cbindgen
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let header_path = Path::new(&out_dir).join("parquet_bindings.h");

    cbindgen::generate(crate_dir)
        .expect("Unable to generate bindings")
        .write_to_file(header_path);

    println!("cargo:rerun-if-changed=src/lib.rs");
}
