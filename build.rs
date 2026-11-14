//! The offline tools' requests, the Nexus's Memory, its operations and its
//! emissions, and the bootstrap's journal and emissions, are generated from
//! ethos/ingress.ethos, ethos/memory.ethos, ethos/operation.ethos,
//! ethos/emission.ethos, ethos/bootstrap_journal.ethos and
//! ethos/bootstrap_emission.ethos by ethos-zero; the committed Rust must equal
//! fresh generation.

use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    for (source_path, generated_path) in [
        ("ethos/ingress.ethos", "src/ingress.rs"),
        ("ethos/memory.ethos", "src/memory.rs"),
        ("ethos/operation.ethos", "src/operation.rs"),
        ("ethos/emission.ethos", "src/emission.rs"),
        ("ethos/bootstrap_journal.ethos", "src/bootstrap_journal.rs"),
        (
            "ethos/bootstrap_emission.ethos",
            "src/bootstrap_emission.rs",
        ),
    ] {
        println!("cargo:rerun-if-changed={source_path}");
        println!("cargo:rerun-if-changed={generated_path}");
        let source = std::fs::read_to_string(root.join(source_path)).expect("source");
        let file = Potential::<File>::from(source)
            .actualize()
            .unwrap_or_else(|_| panic!("read {source_path}"));
        let generated = file
            .generate()
            .unwrap_or_else(|_| panic!("generate {source_path}"));
        assert_eq!(
            generated,
            std::fs::read_to_string(root.join(generated_path)).expect("generated"),
            "{generated_path} is stale; regenerate it from {source_path}"
        );
    }
}
