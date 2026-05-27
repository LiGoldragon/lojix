//! Architectural-truth test — every `fn` in `src/` lives inside an
//! `impl` block (or `fn main` in a binary). Per `skills/rust/methods.md`
//! and the AGENTS.md hard override (intent record 712).
//!
//! The check walks `src/` and asserts that every line beginning with
//! `fn ` or `pub fn ` or `async fn ` or `pub async fn ` is preceded
//! by an indent OR comes from an exempted file (`bin/*.rs` for
//! `fn main`). Inside `impl` blocks, function declarations are
//! indented, so the indent check catches them as method-on-type;
//! free functions appear at column zero and trip the assertion.

use std::fs;
use std::path::{Path, PathBuf};

struct SourceTree {
    root: PathBuf,
}

impl SourceTree {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn walk(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        self.recurse(&self.root, &mut paths);
        paths
    }

    fn recurse(&self, directory: &Path, paths: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(directory) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    self.recurse(&path, paths);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    paths.push(path);
                }
            }
        }
    }
}

struct ProductionRustFile {
    path: PathBuf,
    source: String,
}

impl ProductionRustFile {
    fn new(path: PathBuf) -> std::io::Result<Self> {
        let source = fs::read_to_string(&path)?;
        Ok(Self { path, source })
    }

    fn is_binary_main(&self) -> bool {
        self.path
            .components()
            .any(|component| component.as_os_str() == "bin")
    }

    /// Search for free function declarations (lines starting with
    /// `fn` / `pub fn` / `async fn` / etc at column zero).
    fn free_function_lines(&self) -> Vec<(usize, String)> {
        let mut violations = Vec::new();
        let mut in_cfg_test_block = false;
        let mut brace_depth: i64 = 0;
        let mut cfg_test_start_depth: Option<i64> = None;
        for (index, line) in self.source.lines().enumerate() {
            let trimmed_open = line.trim_start();
            // Track cfg(test) brace blocks. The line "#[cfg(test)]"
            // immediately precedes a `mod tests { ... }` or `fn ...`.
            if trimmed_open.starts_with("#[cfg(test)]") {
                in_cfg_test_block = true;
                cfg_test_start_depth = Some(brace_depth);
                continue;
            }
            if in_cfg_test_block {
                let next_brace = brace_depth + line.matches('{').count() as i64
                    - line.matches('}').count() as i64;
                if Some(brace_depth) == cfg_test_start_depth && line.matches('{').count() > 0 {
                    // The cfg(test) construct opens a new block.
                    brace_depth = next_brace;
                    continue;
                }
                if Some(brace_depth) > cfg_test_start_depth
                    && next_brace <= cfg_test_start_depth.unwrap_or(0)
                {
                    in_cfg_test_block = false;
                    cfg_test_start_depth = None;
                }
                brace_depth = next_brace;
                continue;
            }
            // Update brace depth for ordinary lines.
            brace_depth += line.matches('{').count() as i64;
            brace_depth -= line.matches('}').count() as i64;

            // Column-zero `fn` / `pub fn` / `async fn` / `pub async fn` /
            // `const fn` / `pub const fn` declarations.
            let candidates = [
                "fn ",
                "pub fn ",
                "async fn ",
                "pub async fn ",
                "const fn ",
                "pub const fn ",
            ];
            if candidates.iter().any(|prefix| line.starts_with(prefix)) {
                // `fn main` is the allowed exemption when this file is a binary.
                if self.is_binary_main() && line.starts_with("fn main") {
                    continue;
                }
                if self.is_binary_main() && line.starts_with("async fn main") {
                    continue;
                }
                violations.push((index + 1, line.to_owned()));
            }
        }
        violations
    }
}

#[test]
fn lojix_next_no_free_functions_outside_main_and_tests() {
    let crate_root = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let src = PathBuf::from(&crate_root).join("src");
    let tree = SourceTree::new(src);
    let mut violations = Vec::new();
    for path in tree.walk() {
        let file = ProductionRustFile::new(path.clone()).expect("read source file");
        for (line_number, line) in file.free_function_lines() {
            violations.push(format!("{}:{}  {}", path.display(), line_number, line));
        }
    }
    assert!(
        violations.is_empty(),
        "found free functions outside fn main / #[cfg(test)]:\n{}",
        violations.join("\n")
    );
}
