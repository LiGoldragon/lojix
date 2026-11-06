//! A store Lojix wrote before 0.10.0, converted into the store this Lojix
//! opens.
//!
//! Lojix up to 9.0.2 kept its state in `lojix.sema`, written by sema-engine.
//! From 0.10.0 the store is `lojix.memory`, written by memory-engine. When the
//! state directory holds `lojix.sema` and no `lojix.memory`, the Nexus copies
//! `lojix.sema` beside itself (same owner, mode 0600), converts the copy with
//! memory-engine's `LegacySemaStore`, opens the converted copy the way it opens
//! any store, and only then renames it to `lojix.memory`, refusing to replace a
//! file already there. `lojix.sema` is only ever read: it stays as it was, the
//! store an earlier Lojix reads back if this one is rolled back.
//!
//! Any failure removes the copy and stops startup with
//! [`Error::LegacyStoreConversion`], naming the stage that failed.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use memory_engine::{LegacySemaStore, SemaConversion};
use rustix::fs::{CWD, RenameFlags};

use crate::quarantine::OpeningReport as _;
use crate::{
    Error, LojixNexusConfigurable as _, NexusConfiguration, NexusPersistable as _, Result, Store,
};

static CONVERSION_COPY_IDENTIFIER: AtomicU64 = AtomicU64::new(0);

/// The two names of one state directory's store: the legacy file read, and
/// the current file the converted copy becomes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyStoreConversion {
    /// `lojix.sema`, read and never written.
    pub legacy: PathBuf,
    /// `lojix.memory`, created by the conversion and absent before it.
    pub current: PathBuf,
}

/// The step of a conversion that failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyConversionStage {
    /// Copying the legacy store into a fresh file beside it.
    CopyingLegacyStore,
    /// memory-engine rewriting the copy into the memory format.
    ConvertingCopy,
    /// Opening the converted copy as this Lojix opens its store.
    VerifyingConvertedCopy,
    /// Renaming the converted copy to the current store name.
    InstallingConvertedCopy,
}

impl std::fmt::Display for LegacyConversionStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::CopyingLegacyStore => "copying the legacy store",
            Self::ConvertingCopy => "converting the copy",
            Self::VerifyingConvertedCopy => "opening the converted copy",
            Self::InstallingConvertedCopy => "renaming the converted copy into place",
        })
    }
}

/// The conversion copy on disk. Dropping it removes the file unless it was
/// renamed into place, so an error or a panic in any stage leaves no copy.
struct ConversionCopy {
    path: PathBuf,
    installed: bool,
}

impl Drop for ConversionCopy {
    fn drop(&mut self) {
        if !self.installed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Converting a legacy store into the current store, leaving the legacy file
/// untouched whatever happens.
pub trait LegacyStoreConverting {
    /// Convert a copy of the legacy store and install it as the current store.
    /// On `Err` the copy is gone, the legacy store is as it was, and no
    /// current store was created.
    fn convert(&self) -> Result<SemaConversion>;

    /// The fresh file beside the current store that the copy is made in.
    fn copy_path(&self) -> PathBuf;

    /// Fill the copy file with the legacy store's bytes, owned like it, mode
    /// 0600.
    fn fill_copy(&self, file: File) -> std::io::Result<()>;

    /// Run one stage, turning its error, or a panic inside it (a damaged file
    /// can panic the storage library), into the typed failure of that stage.
    fn stage<Value>(
        &self,
        stage: LegacyConversionStage,
        work: impl FnOnce() -> Result<Value>,
    ) -> Result<Value>;

    /// The typed failure of one stage.
    fn failure(&self, stage: LegacyConversionStage, source: Error) -> Error;
}

impl LegacyStoreConverting for LegacyStoreConversion {
    fn convert(&self) -> Result<SemaConversion> {
        let path = self.copy_path();
        let file = self.stage(LegacyConversionStage::CopyingLegacyStore, || {
            Ok(OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?)
        })?;
        let mut copy = ConversionCopy {
            path,
            installed: false,
        };
        self.stage(LegacyConversionStage::CopyingLegacyStore, || {
            Ok(self.fill_copy(file)?)
        })?;
        let conversion = self.stage(LegacyConversionStage::ConvertingCopy, || {
            Ok(LegacySemaStore::new(&copy.path).convert()?)
        })?;
        self.stage(LegacyConversionStage::VerifyingConvertedCopy, || {
            let store = Store::open_unchecked(&copy.path, NexusConfiguration::built_in(), false)?;
            for line in store.opening.report_lines() {
                eprintln!("lojix-nexus: {line}");
            }
            Ok(())
        })?;
        self.stage(LegacyConversionStage::InstallingConvertedCopy, || {
            rustix::fs::renameat_with(CWD, &copy.path, CWD, &self.current, RenameFlags::NOREPLACE)
                .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?;
            Ok(())
        })?;
        copy.installed = true;
        if let Some(directory) = self.current.parent()
            && let Err(error) = File::open(directory).and_then(|directory| directory.sync_all())
        {
            eprintln!(
                "lojix-nexus: LegacyStoreDirectoryNotSynced.{{ {} «{error}» }}",
                directory.display()
            );
        }
        Ok(conversion)
    }

    fn copy_path(&self) -> PathBuf {
        let identifier = CONVERSION_COPY_IDENTIFIER.fetch_add(1, Ordering::Relaxed);
        let name = self
            .current
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| String::from("lojix.memory"));
        self.current.with_file_name(format!(
            "{name}.converting-{}-{identifier}",
            std::process::id()
        ))
    }

    fn fill_copy(&self, mut file: File) -> std::io::Result<()> {
        let legacy = File::open(&self.legacy)?;
        let owner = legacy.metadata()?;
        std::os::unix::fs::fchown(&file, Some(owner.uid()), Some(owner.gid()))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        std::io::copy(&mut &legacy, &mut file)?;
        file.sync_all()
    }

    fn stage<Value>(
        &self,
        stage: LegacyConversionStage,
        work: impl FnOnce() -> Result<Value>,
    ) -> Result<Value> {
        std::panic::catch_unwind(AssertUnwindSafe(work))
            .unwrap_or_else(|panic| {
                let message = panic
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| String::from("a panic without a message"));
                Err(Error::Invariant(format!("panicked: {message}")))
            })
            .map_err(|error| self.failure(stage, error))
    }

    fn failure(&self, stage: LegacyConversionStage, source: Error) -> Error {
        Error::LegacyStoreConversion {
            legacy: self.legacy.clone(),
            current: self.current.clone(),
            stage,
            source: Box::new(source),
        }
    }
}

/// The line the Nexus logs once a legacy store is converted.
pub struct LegacyStoreConverted<'conversion> {
    pub conversion: &'conversion LegacyStoreConversion,
    pub receipt: &'conversion SemaConversion,
}

impl std::fmt::Display for LegacyStoreConverted<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "LegacyStoreConverted.{{ {} {} {} {} {} {} }}",
            self.conversion.legacy.display(),
            self.conversion.current.display(),
            self.receipt.entries(),
            self.receipt.checkpoints(),
            self.receipt.segments(),
            self.receipt.kept_digests().len(),
        )
    }
}
