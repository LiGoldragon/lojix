//! The argv every bootstrap emission runs, for four runs: a direct build, a
//! Horizon build with a secrets directory and a builder, a local boot-once
//! and a remote boot-once with a hermetic test. The fixture was recorded from
//! the bootstrap at af392ec, before its process calls became emissions, with
//! the temporary directory, the journal name and the boot-once unit name,
//! each derived from the request's fingerprint, written as placeholders. The
//! generated Horizon inputs are compared by name and size.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use lojix::bootstrap::{
    BootstrapCommand, BootstrapExecutor, BootstrapInvocation as _, BootstrapRun,
};
use lojix::emission::ProcessFailure;
use lojix::ingress::{
    BootstrapActivationBackend, BootstrapBootEntriesDirectory, BootstrapBootOnce,
    BootstrapBuildOnly, BootstrapBuilder, BootstrapBuilderSpec, BootstrapClusterName,
    BootstrapDirectInput, BootstrapFlakeReference, BootstrapGcRootPath, BootstrapHermeticTest,
    BootstrapHorizonInput, BootstrapInput, BootstrapJournalParent, BootstrapLocalBootstrapV1,
    BootstrapMaterializationShape, BootstrapMode, BootstrapNixStoreUri, BootstrapNixSystem,
    BootstrapNodeName, BootstrapOutputSelector, BootstrapProposalSource,
    BootstrapRemoteNixosSystemdBootV1, BootstrapRequestId, BootstrapSecretsDirectory,
    BootstrapSecretsInput, BootstrapSshDestination, BootstrapSshIdentityFile,
    BootstrapSshKnownHostsFile, BootstrapSshPolicy, BootstrapSystemProfilePath,
    BootstrapTerminalEvidencePath, BootstrapTestPlan,
};

const FLAKE: &str = "github:fixture-owner/fixture-flake/0123456789abcdef0123456789abcdef01234567";
const SELECTOR: &str = "nixosConfigurations.target.config.system.build.toplevel";

#[derive(Default)]
struct Recorder {
    lines: Vec<String>,
    copied: bool,
    dispatched: bool,
}

impl BootstrapExecutor for Recorder {
    fn run(&mut self, command: &BootstrapCommand) -> Result<String, ProcessFailure> {
        self.lines.push(format!(
            "{}\t{:?}\t{:?}",
            command.program, command.arguments, command.environment
        ));
        Ok(answer(
            &command.program,
            &command.arguments,
            &mut self.copied,
            &mut self.dispatched,
        ))
    }
}

fn normalized(text: &str, directory: &std::path::Path) -> String {
    let mut text = text.replace(&directory.display().to_string(), "<dir>");
    for (prefix, length, name) in [
        (".lojix-bootstrap-v5-", 64, "<journal>"),
        (".lojix-bootstrap-v6-", 64, "<journal>"),
        ("lojix-bootstrap-boot-once-", 24, "<unit>"),
    ] {
        while let Some(start) = text.find(prefix) {
            let end = start + prefix.len() + length;
            text.replace_range(start..end, name);
        }
    }
    text
}

fn answer(program: &str, arguments: &[String], copied: &mut bool, dispatched: &mut bool) -> String {
    match (program, arguments.first().map(String::as_str)) {
        ("nix", Some("eval")) => "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-fixture.drv\n".into(),
        ("nix", Some("build")) => {
            "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-fixture-system\n".into()
        }
        ("nix", Some("hash")) => "sha256-AbC+/d=\n".into(),
        ("nix-store", Some("--add-root")) => {
            std::os::unix::fs::symlink(
                "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-fixture-system",
                &arguments[1],
            )
            .expect("root receipt");
            String::new()
        }
        ("nix", Some("copy")) => {
            *copied = true;
            String::new()
        }
        (program, _) if program.ends_with("/ssh") || program == "/bin/sh" => {
            let body = arguments.last().expect("body");
            if body.contains("nix-store --query") {
                if *copied { "Present" } else { "Absent" }.into()
            } else if body.contains("systemctl show") {
                if *dispatched {
                    "LoadState=loaded\nActiveState=active\nResult=success\n".into()
                } else {
                    "LoadState=not-found\n".into()
                }
            } else {
                *dispatched = true;
                String::new()
            }
        }
        ("/run/current-system/sw/bin/systemd-run", _) => {
            *dispatched = true;
            String::new()
        }
        other => panic!("unexpected {other:?}"),
    }
}

fn direct() -> BootstrapInput {
    BootstrapInput::Direct(BootstrapDirectInput {
        bootstrap_flake_reference: BootstrapFlakeReference(FLAKE.into()),
        bootstrap_nix_system: BootstrapNixSystem("x86_64-linux".into()),
        bootstrap_output_selector: BootstrapOutputSelector(SELECTOR.into()),
    })
}

fn scenario(name: &str, directory: &Path) -> BootstrapRun {
    let journal_parent = BootstrapJournalParent(directory.display().to_string());
    let gc_root_path = BootstrapGcRootPath(directory.join("generation-root").display().to_string());
    let terminal_evidence_path =
        BootstrapTerminalEvidencePath(directory.join("terminal.rkyv").display().to_string());
    let identity = directory.join("ssh-identity");
    let known = directory.join("known-hosts");
    fs::write(&identity, "key").expect("identity");
    fs::write(&known, "hosts").expect("known hosts");
    fs::set_permissions(&identity, fs::Permissions::from_mode(0o600)).expect("identity mode");
    fs::set_permissions(&known, fs::Permissions::from_mode(0o600)).expect("known hosts mode");
    let mode = match name {
        "build-only" => BootstrapMode::BuildOnly(BootstrapBuildOnly {
            bootstrap_input: direct(),
            bootstrap_builder: BootstrapBuilder::NoBuilder,
            bootstrap_journal_parent: journal_parent,
            bootstrap_gc_root_path: gc_root_path,
            bootstrap_terminal_evidence_path: terminal_evidence_path,
        }),
        "horizon" => {
            let source = directory.join("proposal");
            fs::create_dir(&source).expect("proposal directory");
            fs::write(
                source.join("horizon-definition.datom"),
                include_str!("fixtures/horizon-definition.datom"),
            )
            .expect("proposal");
            let secrets = directory.join("secrets-source");
            fs::create_dir(&secrets).expect("secrets directory");
            fs::write(secrets.join("alpha_key.sops"), "sealed").expect("secret");
            BootstrapMode::BuildOnly(BootstrapBuildOnly {
                bootstrap_input: BootstrapInput::Horizon(BootstrapHorizonInput {
                    bootstrap_proposal_source: BootstrapProposalSource(
                        source
                            .join("horizon-definition.datom")
                            .display()
                            .to_string(),
                    ),
                    bootstrap_cluster_name: BootstrapClusterName("alpha".into()),
                    bootstrap_node_name: BootstrapNodeName("node-1".into()),
                    bootstrap_materialization_shape: BootstrapMaterializationShape::CompleteHost,
                    bootstrap_secrets_input: BootstrapSecretsInput::SecretsDirectory(
                        BootstrapSecretsDirectory(secrets.display().to_string()),
                    ),
                    bootstrap_flake_reference: BootstrapFlakeReference(FLAKE.into()),
                    bootstrap_nix_system: BootstrapNixSystem("x86_64-linux".into()),
                    bootstrap_output_selector: BootstrapOutputSelector(SELECTOR.into()),
                }),
                bootstrap_builder: BootstrapBuilder::NixBuilder(BootstrapBuilderSpec(
                    "@/etc/nix/machines".into(),
                )),
                bootstrap_journal_parent: journal_parent,
                bootstrap_gc_root_path: gc_root_path,
                bootstrap_terminal_evidence_path: terminal_evidence_path,
            })
        }
        "local" => BootstrapMode::BootOnce(BootstrapBootOnce {
            bootstrap_input: direct(),
            bootstrap_builder: BootstrapBuilder::NoBuilder,
            bootstrap_test_plan: BootstrapTestPlan::NoTest,
            bootstrap_activation_backend: BootstrapActivationBackend::LocalBootstrapV1(
                BootstrapLocalBootstrapV1 {
                    bootstrap_system_profile_path: BootstrapSystemProfilePath(
                        directory.join("system-profile").display().to_string(),
                    ),
                    bootstrap_boot_entries_directory: BootstrapBootEntriesDirectory(
                        directory.display().to_string(),
                    ),
                },
            ),
            bootstrap_journal_parent: journal_parent,
            bootstrap_gc_root_path: gc_root_path,
            bootstrap_terminal_evidence_path: terminal_evidence_path,
        }),
        _ => BootstrapMode::BootOnce(BootstrapBootOnce {
            bootstrap_input: direct(),
            bootstrap_builder: BootstrapBuilder::NixBuilder(BootstrapBuilderSpec(
                "ssh-ng://builder.invalid x86_64-linux - 4 1 - - -".into(),
            )),
            bootstrap_test_plan: BootstrapTestPlan::RunHermeticTest(BootstrapHermeticTest {
                bootstrap_flake_reference: BootstrapFlakeReference(FLAKE.into()),
                bootstrap_nix_system: BootstrapNixSystem("x86_64-linux".into()),
                bootstrap_output_selector: BootstrapOutputSelector("checks.bootstrap".into()),
            }),
            bootstrap_activation_backend: BootstrapActivationBackend::RemoteNixosSystemdBootV1(
                BootstrapRemoteNixosSystemdBootV1 {
                    bootstrap_nix_store_uri: BootstrapNixStoreUri(
                        "ssh-ng://root@activate.invalid:2222".into(),
                    ),
                    bootstrap_ssh_destination: BootstrapSshDestination(
                        "root@activate.invalid:2222".into(),
                    ),
                    bootstrap_ssh_policy: BootstrapSshPolicy {
                        bootstrap_ssh_identity_file: BootstrapSshIdentityFile(
                            identity.display().to_string(),
                        ),
                        bootstrap_ssh_known_hosts_file: BootstrapSshKnownHostsFile(
                            known.display().to_string(),
                        ),
                    },
                    bootstrap_system_profile_path: BootstrapSystemProfilePath(
                        "/nix/var/nix/profiles/system".into(),
                    ),
                    bootstrap_boot_entries_directory: BootstrapBootEntriesDirectory(
                        "/boot/loader/entries".into(),
                    ),
                },
            ),
            bootstrap_journal_parent: journal_parent,
            bootstrap_gc_root_path: gc_root_path,
            bootstrap_terminal_evidence_path: terminal_evidence_path,
        }),
    };
    BootstrapRun {
        bootstrap_request_id: BootstrapRequestId(format!("probe-{name}")),
        bootstrap_mode: mode,
    }
}

#[test]
fn every_bootstrap_emission_runs_the_argv_af392ec_ran() {
    let mut emitted = String::new();
    for name in ["build-only", "horizon", "local", "remote"] {
        let directory = tempfile::tempdir().expect("tempdir");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private parent");
        let mut recorder = Recorder::default();
        scenario(name, directory.path())
            .run_with_executor(&mut recorder)
            .expect(name);
        for line in &recorder.lines {
            emitted.push_str(&format!("{name}\t{}\n", normalized(line, directory.path())));
        }
        if name == "horizon" {
            let generated = fs::read_dir(directory.path())
                .expect("journal parent")
                .map(|entry| entry.expect("entry").path())
                .find(|path| {
                    path.file_name()
                        .is_some_and(|name| name.to_string_lossy().starts_with(".lojix-bootstrap-"))
                })
                .expect("journal")
                .join("generated-inputs");
            for input in ["deployment", "horizon", "secrets", "system"] {
                for entry in fs::read_dir(generated.join(input)).expect("generated input") {
                    let entry = entry.expect("entry");
                    let bytes = fs::read(entry.path()).expect("generated file");
                    emitted.push_str(&format!(
                        "horizon-file\t{input}/{}\t{}\n",
                        entry.file_name().to_string_lossy(),
                        bytes.len()
                    ));
                }
            }
        }
    }
    let recorded = include_str!("fixtures/bootstrap-argv.txt");
    for (recorded_line, emitted_line) in recorded.lines().zip(emitted.lines()) {
        assert_eq!(recorded_line, emitted_line);
    }
    assert_eq!(emitted, recorded);
}
