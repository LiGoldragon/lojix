//! Hermetic bootstrap witnesses.  The executor records command values but
//! never starts a body, so these tests cannot build, copy, SSH, or activate.

use std::ffi::OsString;
use std::fs;
use std::path::Path;
use std::process::Command;

use lojix::bootstrap::{
    BootstrapActivationBackend, BootstrapBootOnce, BootstrapBuildOnly, BootstrapBuilder,
    BootstrapCliInput, BootstrapCommand, BootstrapDirectInput, BootstrapEffectStage,
    BootstrapExecutor, BootstrapGcRootPath, BootstrapHermeticTest, BootstrapJournalParent,
    BootstrapLocalBootstrapV1, BootstrapMode, BootstrapNixStoreUri, BootstrapNixSystem,
    BootstrapOutputSelector, BootstrapRemoteNixosSystemdBootV1, BootstrapRequestId, BootstrapRun,
    BootstrapSshDestination, BootstrapSystemProfilePath, BootstrapTerminalEvidence,
    BootstrapTerminalEvidencePath, BootstrapTestPlan, ProcessBootstrapExecutor,
    decode_single_inline, run_with_executor,
};

const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const DERIVATION: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-fixture.drv";
const CLOSURE: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-fixture-system";

#[derive(Default)]
struct SuppressedExecutor {
    commands: Vec<BootstrapCommand>,
    fail_program: Option<&'static str>,
    fail_subcommand: Option<&'static str>,
}

impl BootstrapExecutor for SuppressedExecutor {
    fn run(
        &mut self,
        command: BootstrapCommand,
    ) -> Result<String, lojix::bootstrap::BootstrapError> {
        let should_fail = self.fail_program == Some(command.program.as_str())
            && command
                .arguments
                .first()
                .is_some_and(|argument| self.fail_subcommand == Some(argument.as_str()));
        self.commands.push(command.clone());
        if should_fail {
            return Err(lojix::bootstrap::BootstrapError::Effect(
                BootstrapEffectStage::Built,
            ));
        }
        match (
            command.program.as_str(),
            command.arguments.first().map(String::as_str),
        ) {
            ("nix", Some("eval")) => Ok(format!("{DERIVATION}\n")),
            ("nix", Some("build")) => Ok(format!("{CLOSURE}\n")),
            ("nix-store", Some("--add-root")) => Ok(format!("{CLOSURE}\n")),
            ("nix", Some("copy")) | ("ssh", _) | ("systemd-run", _) => Ok(String::new()),
            other => panic!("unexpected suppressed effect {other:?}"),
        }
    }
}

fn direct_input() -> lojix::bootstrap::BootstrapInput {
    lojix::bootstrap::BootstrapInput::Direct(BootstrapDirectInput {
        flake_reference: lojix::bootstrap::BootstrapFlakeReference(FLAKE.to_string()),
        nix_system: BootstrapNixSystem("x86_64-linux".to_string()),
        output_selector: BootstrapOutputSelector(
            "nixosConfigurations.target.config.system.build.toplevel".to_string(),
        ),
    })
}

fn paths(
    directory: &Path,
) -> (
    BootstrapJournalParent,
    BootstrapGcRootPath,
    BootstrapTerminalEvidencePath,
) {
    (
        BootstrapJournalParent(directory.display().to_string()),
        BootstrapGcRootPath(directory.join("generation-root").display().to_string()),
        BootstrapTerminalEvidencePath(directory.join("terminal.rkyv").display().to_string()),
    )
}

fn build_only(directory: &Path) -> BootstrapRun {
    let (journal_parent, gc_root_path, terminal_evidence_path) = paths(directory);
    BootstrapRun {
        request_id: BootstrapRequestId("fixture-build-only".to_string()),
        mode: BootstrapMode::BuildOnly(BootstrapBuildOnly {
            input: direct_input(),
            builder: BootstrapBuilder::NoBuilder,
            journal_parent,
            gc_root_path,
            terminal_evidence_path,
        }),
    }
}

fn local_boot_once(directory: &Path) -> BootstrapRun {
    let (journal_parent, gc_root_path, terminal_evidence_path) = paths(directory);
    BootstrapRun {
        request_id: BootstrapRequestId("fixture-local".to_string()),
        mode: BootstrapMode::BootOnce(BootstrapBootOnce {
            input: direct_input(),
            builder: BootstrapBuilder::NoBuilder,
            test_plan: BootstrapTestPlan::NoTest,
            activation_backend: BootstrapActivationBackend::LocalBootstrapV1(
                BootstrapLocalBootstrapV1 {
                    system_profile_path: BootstrapSystemProfilePath(
                        directory.join("system-profile").display().to_string(),
                    ),
                    boot_entries_directory: lojix::bootstrap::BootstrapBootEntriesDirectory(
                        directory.display().to_string(),
                    ),
                },
            ),
            journal_parent,
            gc_root_path,
            terminal_evidence_path,
        }),
    }
}

fn remote_boot_once(directory: &Path) -> BootstrapRun {
    let (journal_parent, gc_root_path, terminal_evidence_path) = paths(directory);
    BootstrapRun {
        request_id: BootstrapRequestId("fixture-remote".to_string()),
        mode: BootstrapMode::BootOnce(BootstrapBootOnce {
            input: direct_input(),
            builder: BootstrapBuilder::NixBuilder(lojix::bootstrap::BootstrapBuilderSpec(
                "ssh-ng://builder.invalid x86_64-linux - 4 1 - - -".to_string(),
            )),
            test_plan: BootstrapTestPlan::RunHermeticTest(BootstrapHermeticTest {
                flake_reference: lojix::bootstrap::BootstrapFlakeReference(FLAKE.to_string()),
                nix_system: BootstrapNixSystem("x86_64-linux".to_string()),
                output_selector: BootstrapOutputSelector("checks.bootstrap".to_string()),
            }),
            activation_backend: BootstrapActivationBackend::RemoteNixosSystemdBootV1(
                BootstrapRemoteNixosSystemdBootV1 {
                    nix_store_uri: BootstrapNixStoreUri("ssh-ng://copy.invalid".to_string()),
                    ssh_destination: BootstrapSshDestination("root@activate.invalid".to_string()),
                    system_profile_path: BootstrapSystemProfilePath(
                        "/nix/var/nix/profiles/system".to_string(),
                    ),
                    boot_entries_directory: lojix::bootstrap::BootstrapBootEntriesDirectory(
                        "/boot/loader/entries".to_string(),
                    ),
                },
            ),
            journal_parent,
            gc_root_path,
            terminal_evidence_path,
        }),
    }
}

fn evidence(path: &Path) -> BootstrapTerminalEvidence {
    rkyv::from_bytes::<BootstrapTerminalEvidence, rkyv::rancor::Error>(
        &fs::read(path).expect("read terminal evidence"),
    )
    .expect("decode terminal evidence")
}

#[test]
fn build_only_is_body_suppressed_and_cannot_activate() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut executor = SuppressedExecutor::default();

    let terminal = run_with_executor(build_only(directory.path()), &mut executor)
        .expect("body-suppressed build-only pipeline");

    assert_eq!(terminal.status, "Succeeded");
    assert_eq!(
        executor
            .commands
            .iter()
            .map(|command| (
                command.program.as_str(),
                command.arguments.first().map(String::as_str)
            ))
            .collect::<Vec<_>>(),
        vec![
            ("nix", Some("eval")),
            ("nix", Some("build")),
            ("nix-store", Some("--add-root")),
        ]
    );
    assert!(executor.commands.iter().all(|command| {
        command.program != "ssh"
            && command.program != "systemd-run"
            && command
                .arguments
                .first()
                .is_none_or(|argument| argument != "copy")
    }));
    let artifact = evidence(&directory.path().join("terminal.rkyv"));
    assert_eq!(
        artifact.status,
        lojix::bootstrap::BootstrapEvidenceStatus::Succeeded
    );
    assert!(
        artifact
            .effects
            .iter()
            .any(|effect| effect.stage == BootstrapEffectStage::GcRooted)
    );
    assert!(
        fs::read_dir(directory.path())
            .expect("read journal parent")
            .all(|entry| !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".lojix-bootstrap-v4-")),
        "journal cleanup must remove only the child after evidence exists"
    );
}

#[test]
fn remote_effect_order_uses_every_request_owned_value() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut executor = SuppressedExecutor::default();
    let terminal = run_with_executor(remote_boot_once(directory.path()), &mut executor)
        .expect("body-suppressed remote pipeline");

    assert_eq!(terminal.status, "Succeeded");
    let labels = executor
        .commands
        .iter()
        .map(|command| format!("{}:{}", command.program, command.arguments[0]))
        .collect::<Vec<_>>();
    assert_eq!(
        labels,
        vec![
            "nix:build",
            "nix:eval",
            "nix:build",
            "nix-store:--add-root",
            "nix:copy",
            "ssh:root@activate.invalid",
        ]
    );
    let build = &executor.commands[2];
    assert!(build.arguments.windows(2).any(|arguments| arguments
        == [
            "--builders",
            "ssh-ng://builder.invalid x86_64-linux - 4 1 - - -"
        ]));
    let copy = &executor.commands[4];
    assert!(
        copy.arguments
            .contains(&"ssh-ng://copy.invalid".to_string())
    );
    assert_eq!(executor.commands[5].arguments[0], "root@activate.invalid");
    let evidence_bytes = fs::read(directory.path().join("terminal.rkyv")).expect("evidence bytes");
    assert!(
        !String::from_utf8_lossy(&evidence_bytes).contains("activate.invalid")
            && !String::from_utf8_lossy(&evidence_bytes).contains("fixture-flake"),
        "terminal evidence must not disclose request-owned routing or input values"
    );
}

#[test]
fn local_bootstrap_is_explicit_and_never_elides_transport() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut executor = SuppressedExecutor::default();
    run_with_executor(local_boot_once(directory.path()), &mut executor)
        .expect("body-suppressed local pipeline");
    assert!(
        executor
            .commands
            .iter()
            .any(|command| command.program == "systemd-run")
    );
    assert!(executor.commands.iter().all(|command| {
        command.program != "ssh"
            && !(command.program == "nix" && command.arguments.first() == Some(&"copy".to_string()))
    }));
}

#[test]
fn failure_persists_terminal_evidence_before_journal_cleanup() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut executor = SuppressedExecutor {
        fail_program: Some("nix"),
        fail_subcommand: Some("build"),
        ..Default::default()
    };
    let terminal = run_with_executor(build_only(directory.path()), &mut executor)
        .expect("terminal evidence should convert pipeline failure into a terminal result");
    assert_eq!(terminal.status, "Failed");
    let artifact = evidence(&directory.path().join("terminal.rkyv"));
    assert_eq!(
        artifact.status,
        lojix::bootstrap::BootstrapEvidenceStatus::Failed
    );
    assert!(
        artifact
            .effects
            .iter()
            .any(|effect| effect.stage == BootstrapEffectStage::Built && !effect.succeeded)
    );
    assert!(
        fs::read_dir(directory.path())
            .expect("read journal parent")
            .all(|entry| !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".lojix-bootstrap-v4-"))
    );
}

#[test]
fn root_evidence_and_symlink_collisions_fail_before_any_effect() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("generation-root");
    fs::write(&root, "existing root").expect("existing root");
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(build_only(directory.path()), &mut executor).is_err());
    assert!(executor.commands.is_empty());

    fs::remove_file(&root).expect("remove root fixture");
    fs::write(directory.path().join("terminal.rkyv"), "existing evidence")
        .expect("existing evidence");
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(build_only(directory.path()), &mut executor).is_err());
    assert!(executor.commands.is_empty());

    fs::remove_file(directory.path().join("terminal.rkyv")).expect("remove evidence fixture");
    #[cfg(unix)]
    std::os::unix::fs::symlink(directory.path().join("missing"), &root).expect("symlink root");
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(build_only(directory.path()), &mut executor).is_err());
    assert!(executor.commands.is_empty());
}

#[test]
fn cli_requires_exactly_one_inline_bootstrap_object() {
    let directory = tempfile::tempdir().expect("tempdir");
    let request_file = directory.path().join("request.dotos");
    fs::write(
        &request_file,
        "BootstrapRun.{fixture BuildOnly.{Direct.{x y z} NoBuilder /a /b /c}}",
    )
    .expect("request fixture");
    for arguments in [
        Vec::<OsString>::new(),
        vec![request_file.into_os_string()],
        vec![OsString::from("--help")],
        vec![OsString::from("BootstrapRun.{}"), OsString::from("extra")],
    ] {
        assert!(decode_single_inline(arguments).is_err());
    }

    let status = Command::new(env!("CARGO_BIN_EXE_lojix-bootstrap"))
        .arg("--help")
        .status()
        .expect("bootstrap cli")
        .success();
    assert!(!status, "the binary rejects flags before any effect");

    let parsed = decode_single_inline([OsString::from(format!(
        "BootstrapRun.{{fixture BuildOnly.{{Direct.{{{FLAKE} x86_64-linux nixosConfigurations.target.config.system.build.toplevel}} NoBuilder {} {} {}}}}}",
        directory.path().display(),
        directory.path().join("root").display(),
        directory.path().join("evidence").display(),
    ))])
    .expect("one inline bootstrap object");
    assert!(matches!(
        BootstrapCliInput::BootstrapRun(parsed),
        BootstrapCliInput::BootstrapRun(_)
    ));
}

#[test]
fn production_executor_is_a_distinct_body_boundary() {
    let _ = ProcessBootstrapExecutor;
}
