//! The one place lojix judges raw text before it is trusted, kept, or
//! published.
//!
//! Three copies of "is this a canonical store item root?" and four copies of
//! "does this name credential material?" used to sit in `adapters`, `lib`,
//! `schema_runtime` and `bootstrap`. A predicate repeated is a noun missing:
//! the nouns are the text under inspection and, more specifically, the store
//! path offered as one. The verbs are trait-borne so the percent-encoded form
//! is a second implementation rather than a fifth copy.

/// The terms whose presence in text is taken as evidence that the text names
/// credential material. Matching is case-insensitive and substring-wise: a
/// false positive costs a redacted line, a false negative leaks a secret.
const CREDENTIAL_TERMS: [&str; 9] = [
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "apikey",
    "api-key",
    "api_key",
    "auth",
];

/// Whether a piece of text names credential material. Implemented once per
/// encoding the text can arrive in, so every caller asks the same question of
/// the same vocabulary.
pub(crate) trait CredentialBearing {
    /// True when the text names credential material, and true when the text
    /// cannot be decoded far enough to tell.
    fn names_credential_material(&self) -> bool;
}

/// Text taken as it stands: a failure-log line, a store path, a literal value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InspectedText<'text>(&'text str);

impl<'text> InspectedText<'text> {
    pub(crate) fn new(text: &'text str) -> Self {
        Self(text)
    }
}

impl CredentialBearing for InspectedText<'_> {
    fn names_credential_material(&self) -> bool {
        let lowered = self.0.to_ascii_lowercase();
        CREDENTIAL_TERMS
            .into_iter()
            .any(|term| lowered.contains(term))
    }
}

/// Text that arrived percent-encoded — a query value off the wire. It is
/// decoded exactly once before it is judged; text still holding a `%` after
/// that decode would need a second pass to read, which is ambiguous, so it is
/// judged credential-bearing rather than normalized.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PercentEncodedText<'text>(&'text str);

impl<'text> PercentEncodedText<'text> {
    pub(crate) fn new(text: &'text str) -> Self {
        Self(text)
    }

    /// Decode percent escapes exactly once. `None` when the text is not
    /// singly-decodable: a malformed escape, a non-UTF-8 result, or a `%`
    /// surviving the decode.
    pub(crate) fn decoded_once(&self) -> Option<String> {
        let bytes = self.0.as_bytes();
        let mut decoded = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'%' => {
                    if index + 2 >= bytes.len() {
                        return None;
                    }
                    let high = (bytes[index + 1] as char).to_digit(16)?;
                    let low = (bytes[index + 2] as char).to_digit(16)?;
                    decoded.push(((high << 4) | low) as u8);
                    index += 3;
                }
                byte => {
                    decoded.push(byte);
                    index += 1;
                }
            }
        }
        let decoded = String::from_utf8(decoded).ok()?;
        (!decoded.contains('%')).then_some(decoded)
    }
}

impl CredentialBearing for PercentEncodedText<'_> {
    fn names_credential_material(&self) -> bool {
        let Some(decoded) = self.decoded_once() else {
            return true;
        };
        InspectedText::new(&decoded).names_credential_material()
    }
}

/// Text offered as a path under `/nix/store`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NixStorePath<'text>(&'text str);

impl<'text> NixStorePath<'text> {
    pub(crate) fn new(text: &'text str) -> Self {
        Self(text)
    }
}

/// What lojix asks of a store path before it will persist or act on one.
pub(crate) trait StoreItemShape {
    /// True when the text is exactly a store item root: `/nix/store/` followed
    /// by a 32-character nix base-32 hash, a `-`, and a non-empty traversal-free
    /// name. Shape only — it says nothing about what the item is named after.
    fn is_canonical_item(&self) -> bool;

    /// True when the text is a canonical item root *and* its name does not
    /// advertise credential material, so it is safe to record and project.
    fn is_canonical_item_root(&self) -> bool;
}

impl StoreItemShape for NixStorePath<'_> {
    fn is_canonical_item(&self) -> bool {
        let Some(item) = self.0.strip_prefix("/nix/store/") else {
            return false;
        };
        let Some((hash, name)) = item.split_once('-') else {
            return false;
        };
        hash.len() == 32
            && hash.bytes().all(|byte| {
                matches!(byte, b'0'..=b'9' | b'a'..=b'z')
                    && !matches!(byte, b'e' | b'o' | b't' | b'u')
            })
            && !name.is_empty()
            && !name.contains("..")
            && name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'_' | b'-')
            })
    }

    fn is_canonical_item_root(&self) -> bool {
        self.is_canonical_item() && !self.names_credential_material()
    }
}

impl CredentialBearing for NixStorePath<'_> {
    fn names_credential_material(&self) -> bool {
        InspectedText::new(self.0).names_credential_material()
    }
}

/// Why text offered as a path was not admitted.
#[derive(Debug)]
pub(crate) enum PathFault {
    /// Empty, control-bearing, relative, or carrying a `.`/`..`/prefix
    /// component. Nothing was touched on disk.
    Malformed,
    /// A component of the path is a symbolic link. Admitting it would let the
    /// link be re-pointed between the check and the use.
    Symlinked,
    /// The path exists but is not the kind of entry that was required.
    WrongKind,
    /// The path could not be read to decide.
    Unreadable(std::io::Error),
}

/// Text offered as a filesystem path, before anything is done with it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OfferedPath<'text>(&'text str);

impl<'text> OfferedPath<'text> {
    pub(crate) fn new(text: &'text str) -> Self {
        Self(text)
    }
}

/// The path admissions lojix shares. Every one of them decides through
/// `symlink_metadata` rather than letting a filesystem operation resolve a
/// link after admission, so a link cannot be swapped in behind the check.
pub(crate) trait PathAdmission {
    /// Absolute, non-empty, control-free, and built only of root and normal
    /// components. Nothing is read from disk.
    fn absolute_normal(&self) -> Result<std::path::PathBuf, PathFault>;

    /// `absolute_normal`, and no component of it is a symbolic link. Every
    /// component must exist.
    fn existing_symlink_free(&self) -> Result<std::path::PathBuf, PathFault>;

    /// `existing_symlink_free`, and the entry itself is a directory.
    fn existing_directory(&self) -> Result<std::path::PathBuf, PathFault>;

    /// `existing_symlink_free`, and the entry itself is a regular file bearing
    /// exactly `required_file_name`.
    fn existing_regular_file(
        &self,
        required_file_name: &str,
    ) -> Result<std::path::PathBuf, PathFault>;
}

impl PathAdmission for OfferedPath<'_> {
    fn absolute_normal(&self) -> Result<std::path::PathBuf, PathFault> {
        use std::path::{Component, PathBuf};

        if self.0.is_empty() || self.0.chars().any(char::is_control) {
            return Err(PathFault::Malformed);
        }
        let path = PathBuf::from(self.0);
        if !path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
        {
            return Err(PathFault::Malformed);
        }
        Ok(path)
    }

    fn existing_symlink_free(&self) -> Result<std::path::PathBuf, PathFault> {
        use std::path::{Component, PathBuf};

        let path = self.absolute_normal()?;
        let mut prefix = PathBuf::from("/");
        for component in path.components() {
            let Component::Normal(component) = component else {
                continue;
            };
            prefix.push(component);
            let metadata = std::fs::symlink_metadata(&prefix).map_err(PathFault::Unreadable)?;
            if metadata.file_type().is_symlink() {
                return Err(PathFault::Symlinked);
            }
        }
        Ok(path)
    }

    fn existing_directory(&self) -> Result<std::path::PathBuf, PathFault> {
        let path = self.existing_symlink_free()?;
        let metadata = std::fs::symlink_metadata(&path).map_err(PathFault::Unreadable)?;
        if metadata.file_type().is_dir() {
            Ok(path)
        } else {
            Err(PathFault::WrongKind)
        }
    }

    fn existing_regular_file(
        &self,
        required_file_name: &str,
    ) -> Result<std::path::PathBuf, PathFault> {
        let path = self.existing_symlink_free()?;
        let metadata = std::fs::symlink_metadata(&path).map_err(PathFault::Unreadable)?;
        if metadata.file_type().is_file()
            && path.file_name().and_then(|value| value.to_str()) == Some(required_file_name)
        {
            Ok(path)
        } else {
            Err(PathFault::WrongKind)
        }
    }
}
