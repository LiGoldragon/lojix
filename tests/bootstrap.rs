//! Hermetic bootstrap witnesses.  Every command body is suppressed; the only
//! filesystem mutations are private temporary-directory receipts that model
//! Nix's GC-root link and durable evidence file.

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::Command;

use lojix::bootstrap::{
    BootstrapActivationBackend, BootstrapBootOnce, BootstrapBuildOnly, BootstrapBuilder,
    BootstrapCliInput, BootstrapCommand, BootstrapCrashInjector, BootstrapCrashPoint,
    BootstrapDirectInput, BootstrapEffectStage, BootstrapExecutor, BootstrapGcRootPath,
    BootstrapHermeticTest, BootstrapJournalParent, BootstrapLocalBootstrapV1, BootstrapMode,
    BootstrapNixStoreUri, BootstrapNixSystem, BootstrapOutputSelector,
    BootstrapRemoteNixosSystemdBootV1, BootstrapRequestId, BootstrapRun, BootstrapSshDestination,
    BootstrapSshIdentityFile, BootstrapSshKnownHostsFile, BootstrapSshPolicy,
    BootstrapStrictHostKeyMode, BootstrapSystemProfilePath, BootstrapTerminalEvidencePath,
    BootstrapTestPlan, decode_single_inline, run_with_executor, run_with_executor_and_crash,
};

const FLAKE: &str = "github:fixture-owner/fixture-flake/0123456789abcdef0123456789abcdef01234567";
const DERIVATION: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-fixture.drv";
const CLOSURE: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-fixture-system";

#[derive(Default)]
struct SuppressedExecutor {
    commands: Vec<BootstrapCommand>,
    copied: bool,
    dispatched: bool,
    fail_program: Option<&'static str>,
    fail_subcommand: Option<&'static str>,
}

impl SuppressedExecutor {
    fn with_remote_receipts() -> Self {
        Self {
            copied: true,
            dispatched: true,
            ..Self::default()
        }
    }

    fn count(&self, program: &str, first: &str) -> usize {
        self.commands
            .iter()
            .filter(|command| {
                command.program == program
                    && command
                        .arguments
                        .first()
                        .is_some_and(|value| value == first)
            })
            .count()
    }
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
                .is_some_and(|value| self.fail_subcommand == Some(value));
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
            ("nix-store", Some("--add-root")) => {
                let root = command.arguments.get(1).expect("root argument");
                symlink(CLOSURE, root).expect("model Nix's root receipt");
                Ok(format!("{CLOSURE}\n"))
            }
            ("nix", Some("copy")) => {
                self.copied = true;
                Ok(String::new())
            }
            (program, _) if program.ends_with("/ssh") => {
                let body = command.arguments.last().expect("remote command");
                if body.contains("nix-store --query") {
                    Ok(if self.copied { "Present" } else { "Absent" }.to_string())
                } else if body.contains("systemctl show") {
                    Ok(if self.dispatched {
                        "LoadState=loaded\nActiveState=active\nResult=success\n".to_string()
                    } else {
                        "LoadState=not-found\n".to_string()
                    })
                } else if body.contains("systemd-run") {
                    self.dispatched = true;
                    Ok(String::new())
                } else {
                    panic!("unexpected remote command {body}")
                }
            }
            ("/bin/sh", Some("-eu")) => {
                let body = command.arguments.last().expect("local query");
                Ok(if body.contains("systemctl show") {
                    if self.dispatched {
                        "LoadState=loaded\nActiveState=active\nResult=success\n".to_string()
                    } else {
                        "LoadState=not-found\n".to_string()
                    }
                } else {
                    panic!("unexpected local shell command {body}")
                })
            }
            ("/run/current-system/sw/bin/systemd-run", _) => {
                self.dispatched = true;
                Ok(String::new())
            }
            other => panic!("unexpected suppressed effect {other:?}"),
        }
    }
}

struct CrashOnce(BootstrapCrashPoint);

impl BootstrapCrashInjector for CrashOnce {
    fn after(
        &mut self,
        point: BootstrapCrashPoint,
    ) -> Result<(), lojix::bootstrap::BootstrapError> {
        if point == self.0 {
            Err(lojix::bootstrap::BootstrapError::InjectedCrash)
        } else {
            Ok(())
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

fn private(directory: &Path) {
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
        .expect("private fixture parent");
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
    let identity_file = directory.join("ssh-identity");
    let known_hosts_file = directory.join("known-hosts");
    fs::write(&identity_file, "fixture private key").expect("identity fixture");
    fs::write(&known_hosts_file, "activate.invalid fixture-key").expect("known-hosts fixture");
    fs::set_permissions(&identity_file, fs::Permissions::from_mode(0o600)).expect("identity mode");
    fs::set_permissions(&known_hosts_file, fs::Permissions::from_mode(0o600))
        .expect("known-hosts mode");
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
                    nix_store_uri: BootstrapNixStoreUri(
                        "ssh-ng://root@activate.invalid:2222".to_string(),
                    ),
                    ssh_destination: BootstrapSshDestination(
                        "root@activate.invalid:2222".to_string(),
                    ),
                    ssh_policy: BootstrapSshPolicy {
                        identity_file: BootstrapSshIdentityFile(
                            identity_file.display().to_string(),
                        ),
                        known_hosts_file: BootstrapSshKnownHostsFile(
                            known_hosts_file.display().to_string(),
                        ),
                        strict_host_key_mode: BootstrapStrictHostKeyMode::RequireKnownHost,
                    },
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

#[test]
fn build_only_has_no_transport_or_activation_body() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let mut executor = SuppressedExecutor::default();
    let terminal =
        run_with_executor(build_only(directory.path()), &mut executor).expect("build only");
    assert_eq!(terminal.status, "Succeeded");
    assert_eq!(executor.count("nix-store", "--add-root"), 1);
    assert!(executor.commands.iter().all(|command| {
        !command.program.ends_with("/ssh")
            && command.program != "/run/current-system/sw/bin/systemd-run"
    }));
    assert_eq!(
        fs::symlink_metadata(directory.path().join("terminal.rkyv"))
            .expect("evidence")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn remote_dispatch_is_no_block_and_identity_is_explicit() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let mut executor = SuppressedExecutor::default();
    run_with_executor(remote_boot_once(directory.path()), &mut executor).expect("remote pipeline");
    let dispatch = executor
        .commands
        .iter()
        .find(|command| {
            command.program.ends_with("/ssh")
                && command
                    .arguments
                    .last()
                    .is_some_and(|body| body.contains("systemd-run"))
        })
        .expect("remote dispatch");
    assert!(
        dispatch
            .arguments
            .windows(3)
            .any(|arguments| arguments == ["-p", "2222", "--"])
    );
    assert!(
        dispatch
            .arguments
            .windows(2)
            .any(|arguments| arguments[0] == "-F" && arguments[1].ends_with("/ssh-config"))
    );
    assert!(dispatch.arguments.last().is_some_and(|body| {
        body.contains("--no-block")
            && body.contains("RemainAfterExit=yes")
            && body.contains("--unit=")
    }));
    assert!(executor.commands.iter().any(|command| {
        command.program.ends_with("/ssh")
            && command
                .arguments
                .last()
                .is_some_and(|body| body.contains("systemctl show"))
    }));
    let copy = executor
        .commands
        .iter()
        .find(|command| {
            command.program == "nix" && command.arguments.first() == Some(&"copy".to_string())
        })
        .expect("copy");
    assert!(copy.environment.iter().any(|(key, value)| {
        key == "NIX_SSHOPTS"
            && value.contains("ssh-config")
            && value.contains("IdentitiesOnly=yes")
            && value.contains("ProxyCommand=none")
    }));
    let ssh_config = fs::read_dir(directory.path())
        .expect("journal parent")
        .map(|entry| entry.expect("entry").path().join("ssh-config"))
        .find(|path| path.exists())
        .expect("private generated SSH config");
    let config = fs::read_to_string(ssh_config).expect("read config");
    assert!(config.contains("StrictHostKeyChecking yes"));
    assert!(config.contains("IdentitiesOnly yes"));
    assert!(config.contains("IdentityAgent none"));
    assert!(config.contains("ProxyCommand none"));
    assert!(config.contains("ControlMaster no"));
    let bytes = fs::read(directory.path().join("terminal.rkyv")).expect("evidence");
    let evidence_text = String::from_utf8_lossy(&bytes);
    assert!(!evidence_text.contains("activate.invalid"));
    assert!(!evidence_text.contains("fixture-flake"));
    assert!(!evidence_text.contains("fixture-remote"));
    assert!(!evidence_text.contains(CLOSURE));
    assert!(!evidence_text.contains("generation-root"));
}

#[test]
fn local_backend_has_explicit_systemd_and_path_environment() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let mut executor = SuppressedExecutor::default();
    run_with_executor(local_boot_once(directory.path()), &mut executor).expect("local pipeline");
    let dispatch = executor
        .commands
        .iter()
        .find(|command| command.program == "/run/current-system/sw/bin/systemd-run")
        .expect("local systemd dispatch");
    assert!(
        dispatch
            .arguments
            .iter()
            .any(|argument| argument.starts_with("--setenv=PATH="))
    );
    assert!(
        executor
            .commands
            .iter()
            .all(|command| !command.program.ends_with("/ssh"))
    );
}

#[test]
fn crash_receipts_resume_without_repeating_prior_effects() {
    for point in [
        BootstrapCrashPoint::AfterGcRoot,
        BootstrapCrashPoint::AfterCopy,
        BootstrapCrashPoint::AfterDispatch,
        BootstrapCrashPoint::AfterActivation,
        BootstrapCrashPoint::AfterEvidence,
    ] {
        let directory = tempfile::tempdir().expect("tempdir");
        private(directory.path());
        let request = remote_boot_once(directory.path());
        let mut initial = SuppressedExecutor::default();
        let mut crash = CrashOnce(point);
        assert!(matches!(
            run_with_executor_and_crash(request.clone(), &mut initial, &mut crash),
            Err(lojix::bootstrap::BootstrapError::InjectedCrash)
        ));

        let initial_root = initial.count("nix-store", "--add-root");
        let initial_copy = initial.count("nix", "copy");
        let initial_dispatch = initial
            .commands
            .iter()
            .filter(|command| {
                command.program.ends_with("/ssh")
                    && command
                        .arguments
                        .last()
                        .is_some_and(|body| body.contains("systemd-run"))
            })
            .count();
        let mut resumed = SuppressedExecutor::with_remote_receipts();
        run_with_executor(request, &mut resumed).expect("resumed journal");
        assert_eq!(resumed.count("nix-store", "--add-root"), 0, "{point:?}");
        if initial_copy > 0 {
            assert_eq!(resumed.count("nix", "copy"), 0, "{point:?}");
        }
        if initial_dispatch > 0 {
            assert_eq!(
                resumed
                    .commands
                    .iter()
                    .filter(|command| command.program.ends_with("/ssh")
                        && command
                            .arguments
                            .last()
                            .is_some_and(|body| body.contains("systemd-run")))
                    .count(),
                0,
                "{point:?}"
            );
        }
        assert_eq!(initial_root, 1, "{point:?}");
    }
}

#[test]
fn root_command_crash_reconciles_private_staging_for_every_mode() {
    for mode in ["build", "local", "remote"] {
        let directory = tempfile::tempdir().expect("tempdir");
        private(directory.path());
        let request = match mode {
            "build" => build_only(directory.path()),
            "local" => local_boot_once(directory.path()),
            "remote" => remote_boot_once(directory.path()),
            _ => unreachable!(),
        };
        let mut initial = SuppressedExecutor::default();
        let mut crash = CrashOnce(BootstrapCrashPoint::AfterGcRootCommand);
        assert!(matches!(
            run_with_executor_and_crash(request.clone(), &mut initial, &mut crash),
            Err(lojix::bootstrap::BootstrapError::InjectedCrash)
        ));
        let staging = fs::read_dir(directory.path())
            .expect("journal parent")
            .map(|entry| entry.expect("entry").path().join("gc-root-staging"))
            .find(|path| fs::symlink_metadata(path).is_ok())
            .expect("staging root survives crash");
        assert_eq!(
            fs::read_link(&staging).expect("staging target"),
            Path::new(CLOSURE)
        );

        let mut resumed = SuppressedExecutor::default();
        run_with_executor(request, &mut resumed).expect("reconcile root staging");
        assert_eq!(resumed.count("nix-store", "--add-root"), 0, "{mode}");
        assert_eq!(
            fs::read_link(directory.path().join("generation-root")).expect("final root"),
            Path::new(CLOSURE)
        );
        assert!(fs::symlink_metadata(&staging).is_err(), "{mode}");
    }
}

#[test]
fn wrong_root_staging_after_crash_fails_without_deleting_it() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let request = build_only(directory.path());
    let mut initial = SuppressedExecutor::default();
    let mut crash = CrashOnce(BootstrapCrashPoint::AfterGcRootCommand);
    assert!(run_with_executor_and_crash(request.clone(), &mut initial, &mut crash).is_err());
    let staging = fs::read_dir(directory.path())
        .expect("journal parent")
        .map(|entry| entry.expect("entry").path().join("gc-root-staging"))
        .find(|path| fs::symlink_metadata(path).is_ok())
        .expect("staging root");
    fs::remove_file(&staging).expect("replace fixture link");
    symlink(
        "/nix/store/cccccccccccccccccccccccccccccccc-wrong",
        &staging,
    )
    .expect("wrong link");
    let mut resumed = SuppressedExecutor::default();
    assert!(run_with_executor(request, &mut resumed).is_err());
    assert_eq!(
        fs::read_link(staging).expect("wrong staging remains"),
        Path::new("/nix/store/cccccccccccccccccccccccccccccccc-wrong")
    );
}

#[test]
fn nonprivate_ssh_policy_files_are_rejected_before_any_effect() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let mut request = remote_boot_once(directory.path());
    let BootstrapMode::BootOnce(boot) = &mut request.mode else {
        unreachable!()
    };
    let BootstrapActivationBackend::RemoteNixosSystemdBootV1(remote) = &mut boot.activation_backend
    else {
        unreachable!()
    };
    let bad_identity = directory.path().join("bad-identity");
    fs::write(&bad_identity, "bad").expect("identity");
    fs::set_permissions(&bad_identity, fs::Permissions::from_mode(0o644)).expect("mode");
    remote.ssh_policy.identity_file = BootstrapSshIdentityFile(bad_identity.display().to_string());
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(request, &mut executor).is_err());
    assert!(executor.commands.is_empty());
}

#[test]
fn mutable_flakes_and_ambiguous_transport_fail_before_effects() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let mut mutable = build_only(directory.path());
    let BootstrapMode::BuildOnly(build) = &mut mutable.mode else {
        unreachable!()
    };
    let lojix::bootstrap::BootstrapInput::Direct(direct) = &mut build.input else {
        unreachable!()
    };
    direct.flake_reference.0 =
        "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567"
            .to_string();
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(mutable, &mut executor).is_err());
    assert!(executor.commands.is_empty());

    let mut mismatched = remote_boot_once(directory.path());
    let BootstrapMode::BootOnce(boot) = &mut mismatched.mode else {
        unreachable!()
    };
    let BootstrapActivationBackend::RemoteNixosSystemdBootV1(remote) = &mut boot.activation_backend
    else {
        unreachable!()
    };
    remote.ssh_destination.0 = "root@other.invalid:2222".to_string();
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(mismatched, &mut executor).is_err());
    assert!(executor.commands.is_empty());
}

#[test]
fn transport_identity_rejection_table_never_reaches_a_body() {
    for (store_uri, destination) in [
        ("ssh://root@activate.invalid", "root@activate.invalid"),
        (
            "ssh-ng://root:password@activate.invalid",
            "root@activate.invalid",
        ),
        ("ssh-ng://root@activate.invalid", "activate.invalid"),
        ("ssh-ng://root@activate.invalid", "-oProxyCommand=x"),
        (
            "ssh-ng://root@activate.invalid:22",
            "root@activate.invalid:23",
        ),
        ("ssh-ng://Root@activate.invalid", "Root@activate.invalid"),
    ] {
        let directory = tempfile::tempdir().expect("tempdir");
        private(directory.path());
        let mut request = remote_boot_once(directory.path());
        let BootstrapMode::BootOnce(boot) = &mut request.mode else {
            unreachable!()
        };
        let BootstrapActivationBackend::RemoteNixosSystemdBootV1(remote) =
            &mut boot.activation_backend
        else {
            unreachable!()
        };
        remote.nix_store_uri.0 = store_uri.to_string();
        remote.ssh_destination.0 = destination.to_string();
        let mut executor = SuppressedExecutor::default();
        assert!(
            run_with_executor(request, &mut executor).is_err(),
            "{store_uri} / {destination}"
        );
        assert!(executor.commands.is_empty(), "{store_uri} / {destination}");
    }
}

#[test]
fn private_output_parents_and_existing_collisions_fail_closed() {
    let directory = tempfile::tempdir().expect("tempdir");
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))
        .expect("make parent nonprivate");
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(build_only(directory.path()), &mut executor).is_err());
    assert!(executor.commands.is_empty());
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .expect("restore parent");
    fs::write(directory.path().join("terminal.rkyv"), "collision").expect("collision");
    let mut executor = SuppressedExecutor::default();
    assert!(run_with_executor(build_only(directory.path()), &mut executor).is_err());
    assert!(executor.commands.is_empty());
}

#[test]
fn failed_effect_writes_private_terminal_evidence_before_cleanup() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let mut executor = SuppressedExecutor {
        fail_program: Some("nix"),
        fail_subcommand: Some("build"),
        ..Default::default()
    };
    let terminal = run_with_executor(build_only(directory.path()), &mut executor)
        .expect("failure becomes durable terminal evidence");
    assert_eq!(terminal.status, "Failed");
    let artifact = fs::symlink_metadata(directory.path().join("terminal.rkyv")).expect("evidence");
    assert_eq!(artifact.permissions().mode() & 0o777, 0o600);
    assert!(
        fs::read_dir(directory.path())
            .expect("parent")
            .any(|entry| entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".lojix-bootstrap-v5-")),
        "finalized private journal is retained safely"
    );
}

#[test]
fn substituted_journal_child_is_never_cleaned_recursively() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    let request = build_only(directory.path());
    let mut executor = SuppressedExecutor::default();
    let mut crash = CrashOnce(BootstrapCrashPoint::AfterGcRoot);
    assert!(matches!(
        run_with_executor_and_crash(request.clone(), &mut executor, &mut crash),
        Err(lojix::bootstrap::BootstrapError::InjectedCrash)
    ));
    let child = fs::read_dir(directory.path())
        .expect("journal parent")
        .map(|entry| entry.expect("entry").path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".lojix-bootstrap-v5-"))
        })
        .expect("journal child");
    let displaced = directory.path().join("displaced-journal");
    fs::rename(&child, &displaced).expect("move original child");
    fs::create_dir(&child).expect("substituted child");
    private(&child);
    fs::write(child.join("not-ours"), "retain").expect("replacement marker");

    let mut resumed = SuppressedExecutor::default();
    assert!(run_with_executor(request, &mut resumed).is_err());
    assert!(
        child.join("not-ours").exists(),
        "replacement survives fail-closed cleanup"
    );
    assert!(
        displaced.exists(),
        "original durable journal is retained for inspection"
    );
}

#[test]
fn cli_remains_exactly_one_inline_object() {
    let directory = tempfile::tempdir().expect("tempdir");
    private(directory.path());
    for arguments in [
        Vec::<OsString>::new(),
        vec![OsString::from("--help")],
        vec![OsString::from("BootstrapRun.{}"), OsString::from("extra")],
    ] {
        assert!(decode_single_inline(arguments).is_err());
    }
    let parsed = decode_single_inline([OsString::from(format!(
        "BootstrapRun.{{fixture BuildOnly.{{Direct.{{{FLAKE} x86_64-linux nixosConfigurations.target.config.system.build.toplevel}} NoBuilder {} {} {}}}}}",
        directory.path().display(),
        directory.path().join("root").display(),
        directory.path().join("evidence").display(),
    ))]).expect("one inline request");
    assert!(matches!(
        BootstrapCliInput::BootstrapRun(parsed),
        BootstrapCliInput::BootstrapRun(_)
    ));
    assert!(
        !Command::new(env!("CARGO_BIN_EXE_lojix-bootstrap"))
            .arg("--help")
            .status()
            .expect("cli")
            .success()
    );
}
