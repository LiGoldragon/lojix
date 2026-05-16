use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use horizon_lib::address::{YggAddress, YggSubnet};
use horizon_lib::disk::{DevicePath, Disk, FsType, MountPath};
use horizon_lib::magnitude::Magnitude;
use horizon_lib::name::{
    ClusterDomain as HorizonClusterDomain, Keygrip, NodeName as HorizonNodeName,
    UserName as HorizonUserName,
};
use horizon_lib::proposal::{
    ClusterProposal, ClusterTrust, Io, Machine, NodePlacement, NodeProposal, NodePubKeys,
    NodeServices, UserProposal, UserPubKeyEntry, YggPubKeyEntry,
};
use horizon_lib::pub_key::{NixPubKey, SshPubKey, YggPubKey};
use horizon_lib::species::{Arch, Bootloader, Keyboard, NodeSpecies, Style, UserSpecies};
use horizon_nota_codec::{Encoder as HorizonNotaEncoder, NotaEncode as HorizonNotaEncode};
use kameo::actor::{ActorRef, Spawn};
use lojix::process::ProcessToolchain;
use lojix::wire::{
    BuildLocally, BuilderSelection, ClusterName, DeploymentObservationSubscription,
    DeploymentPhase, DeploymentPlan, DeploymentRejected, DeploymentRejectionReason,
    DeploymentSubmission, DispatcherChoosesBuilder, FlakeReference, FullOsDeployment, NodeName,
    ProposalSource, RealizedStorePath, Reply, Request, SystemAction,
};
use lojix::{RuntimeConfiguration, RuntimeRequest, RuntimeRoot};

const NIX_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const FAKE_NAR_HASH: &str = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const FAKE_STORE_PATH: &str = "/nix/store/00000000000000000000000000000000-built-system";

#[tokio::test]
async fn build_only_deployment_accepts_before_remote_build_completes() {
    let fixture = BuildPipelineFixture::create();
    let root = fixture.runtime_root();

    let accepted = tokio::time::timeout(
        Duration::from_millis(500),
        ask_runtime(&root, Request::DeploymentSubmission(fixture.submission())),
    )
    .await
    .expect("build submission returned before fake remote build completed");

    let deployment = match accepted {
        Reply::DeploymentAccepted(accepted) => accepted.deployment,
        other => panic!("expected deployment acceptance, got {other:?}"),
    };

    let observations = wait_for_built_observation(&root, &deployment).await;
    assert!(
        observations.iter().any(|observation| matches!(
            observation.phase,
            DeploymentPhase::DeploymentSubmitted(_)
        ))
    );
    assert!(
        observations
            .iter()
            .any(|observation| matches!(observation.phase, DeploymentPhase::DeploymentBuilding(_)))
    );
    assert!(
        observations
            .iter()
            .any(|observation| matches!(observation.phase, DeploymentPhase::DeploymentBuilt(_)))
    );
    assert!(observations.iter().any(|observation| matches!(
        &observation.phase,
        DeploymentPhase::DeploymentBuilt(built)
            if matches!(
                &built.result,
                lojix::wire::BuildResult::RealizedStorePath(RealizedStorePath { store_path })
                    if store_path.as_str() == FAKE_STORE_PATH
            )
    )));

    let log = fixture.tool_log();
    assert!(log.contains("root@ouranos.goldragon.criome"));
    assert!(log.contains("--override-input horizon"));
    assert!(log.contains("--override-input system"));
    assert!(log.contains("--override-input deployment"));
    assert!(log.contains("path:/var/tmp/lojix/generated-inputs/"));
    assert!(log.contains("/horizon?narHash="));
    assert!(log.contains("/system?narHash="));
    assert!(log.contains("/deployment?narHash="));
    assert!(log.contains("github:LiGoldragon/CriomOS/horizon-re-engineering#nixosConfigurations.target.config.system.build.toplevel"));
}

#[tokio::test]
async fn build_locally_is_rejected_before_any_tool_runs() {
    let fixture = BuildPipelineFixture::create();
    let root = fixture.runtime_root();

    let reply = ask_runtime(
        &root,
        Request::DeploymentSubmission(DeploymentSubmission {
            builder: BuilderSelection::BuildLocally(BuildLocally {}),
            ..fixture.submission()
        }),
    )
    .await;

    assert_rejected_with(reply, "local builds are disabled");
    assert_eq!(fixture.tool_log(), "");
}

#[tokio::test]
async fn activation_actions_are_rejected_before_any_tool_runs() {
    let fixture = BuildPipelineFixture::create();
    let root = fixture.runtime_root();

    let reply = ask_runtime(
        &root,
        Request::DeploymentSubmission(DeploymentSubmission {
            plan: DeploymentPlan::FullOsDeployment(FullOsDeployment {
                action: SystemAction::Switch,
            }),
            ..fixture.submission()
        }),
    )
    .await;

    assert_rejected_with(reply, "build-only deployments");
    assert_eq!(fixture.tool_log(), "");
}

async fn ask_runtime(root: &ActorRef<RuntimeRoot>, request: Request) -> Reply {
    root.ask(RuntimeRequest { request })
        .await
        .expect("runtime actor replied")
}

async fn wait_for_built_observation(
    root: &ActorRef<RuntimeRoot>,
    deployment: &lojix::wire::DeploymentId,
) -> Vec<lojix::wire::DeploymentObservation> {
    for _ in 0..100 {
        let reply = ask_runtime(
            root,
            Request::DeploymentObservationSubscription(DeploymentObservationSubscription {
                cluster: None,
                node: None,
                deployment: Some(deployment.clone()),
            }),
        )
        .await;

        let Reply::DeploymentObservationSubscriptionOpened(opened) = reply else {
            panic!("expected observation subscription snapshot");
        };
        if opened
            .observations
            .iter()
            .any(|observation| matches!(observation.phase, DeploymentPhase::DeploymentBuilt(_)))
        {
            return opened.observations;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("build observation did not appear");
}

fn assert_rejected_with(reply: Reply, expected_detail: &str) {
    let Reply::DeploymentRejected(DeploymentRejected {
        reason,
        detail: Some(detail),
    }) = reply
    else {
        panic!("expected deployment rejection, got {reply:?}");
    };
    assert_eq!(reason, DeploymentRejectionReason::InvalidRequest);
    assert!(
        detail.as_str().contains(expected_detail),
        "expected rejection detail to contain {expected_detail:?}, got {detail}"
    );
}

struct BuildPipelineFixture {
    root: PathBuf,
    proposal_path: PathBuf,
    log_path: PathBuf,
    process_toolchain: ProcessToolchain,
}

impl BuildPipelineFixture {
    fn create() -> Self {
        let root = unique_temporary_directory("lojix-build-pipeline");
        let tool_directory = root.join("tools");
        std::fs::create_dir_all(&tool_directory).expect("create fake tool directory");
        let log_path = root.join("tool.log");
        std::fs::write(&log_path, "").expect("initialize fake tool log");

        let nix_path = tool_directory.join("nix");
        let ssh_path = tool_directory.join("ssh");
        let rsync_path = tool_directory.join("rsync");
        FakeTool::new(&nix_path).write(&fake_nix_script(&log_path));
        FakeTool::new(&ssh_path).write(&fake_ssh_script(&log_path));
        FakeTool::new(&rsync_path).write(&fake_rsync_script(&log_path));

        let proposal_path = root.join("datom.nota");
        write_proposal(&proposal_path);

        Self {
            root,
            proposal_path,
            log_path,
            process_toolchain: ProcessToolchain::new(
                nix_path.display().to_string(),
                ssh_path.display().to_string(),
                rsync_path.display().to_string(),
            ),
        }
    }

    fn runtime_root(&self) -> ActorRef<RuntimeRoot> {
        RuntimeRoot::spawn(RuntimeRoot::with_configuration(
            RuntimeConfiguration::for_in_process_tests()
                .with_process_toolchain(self.process_toolchain.clone()),
        ))
    }

    fn submission(&self) -> DeploymentSubmission {
        DeploymentSubmission {
            cluster: ClusterName::from_text("goldragon").expect("cluster name"),
            node: NodeName::from_text("zeus").expect("node name"),
            source: ProposalSource::from_text(self.proposal_path.display().to_string())
                .expect("proposal source"),
            flake: FlakeReference::from_text("github:LiGoldragon/CriomOS/horizon-re-engineering")
                .expect("flake reference"),
            plan: DeploymentPlan::FullOsDeployment(FullOsDeployment {
                action: SystemAction::Build,
            }),
            builder: BuilderSelection::DispatcherChoosesBuilder(DispatcherChoosesBuilder {}),
            substituters: Vec::new(),
        }
    }

    fn tool_log(&self) -> String {
        std::fs::read_to_string(&self.log_path).expect("read fake tool log")
    }
}

impl Drop for BuildPipelineFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct FakeTool {
    path: PathBuf,
}

impl FakeTool {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    fn write(&self, script: &str) {
        std::fs::write(&self.path, script).expect("write fake tool");
        let mut permissions = std::fs::metadata(&self.path)
            .expect("fake tool metadata")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&self.path, permissions).expect("fake tool executable");
    }
}

fn fake_nix_script(log_path: &Path) -> String {
    format!(
        r#"#!/bin/sh
set -eu
printf 'nix' >> {log}
for argument in "$@"; do
  printf ' <%s>' "$argument" >> {log}
done
printf '\n' >> {log}
if [ "${{1:-}}" = hash ]; then
  printf '%s\n' '{hash}'
  exit 0
fi
printf '%s\n' "unexpected nix invocation" >&2
exit 11
"#,
        log = shell_quote(log_path),
        hash = FAKE_NAR_HASH,
    )
}

fn fake_ssh_script(log_path: &Path) -> String {
    format!(
        r#"#!/bin/sh
set -eu
printf 'ssh' >> {log}
last_argument=
for argument in "$@"; do
  printf ' <%s>' "$argument" >> {log}
  last_argument="$argument"
done
printf '\n' >> {log}
case "$last_argument" in
  mkdir\ -p*)
    exit 0
    ;;
  *"nix build"*)
    sleep 1
    printf '%s\n' '{store_path}'
    exit 0
    ;;
esac
printf '%s\n' "unexpected ssh invocation: $last_argument" >&2
exit 12
"#,
        log = shell_quote(log_path),
        store_path = FAKE_STORE_PATH,
    )
}

fn fake_rsync_script(log_path: &Path) -> String {
    format!(
        r#"#!/bin/sh
set -eu
printf 'rsync' >> {log}
for argument in "$@"; do
  printf ' <%s>' "$argument" >> {log}
done
printf '\n' >> {log}
exit 0
"#,
        log = shell_quote(log_path),
    )
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

fn unique_temporary_directory(prefix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{prefix}-{}-{timestamp}", std::process::id()));
    std::fs::create_dir_all(&path).expect("create temporary directory");
    path
}

fn write_proposal(path: &Path) {
    let mut encoder = HorizonNotaEncoder::new();
    cluster_proposal()
        .encode(&mut encoder)
        .expect("encode horizon proposal");
    std::fs::write(path, encoder.into_string()).expect("write horizon proposal");
}

fn cluster_proposal() -> ClusterProposal {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        HorizonNodeName::try_new("ouranos").unwrap(),
        node_proposal(NodeSpecies::EdgeTesting, Magnitude::Large, true),
    );
    nodes.insert(
        HorizonNodeName::try_new("zeus").unwrap(),
        node_proposal(NodeSpecies::Edge, Magnitude::Min, false),
    );

    let mut users = BTreeMap::new();
    users.insert(
        HorizonUserName::try_new("li").unwrap(),
        user_proposal(UserSpecies::Unlimited),
    );

    let mut node_trust = BTreeMap::new();
    node_trust.insert(HorizonNodeName::try_new("ouranos").unwrap(), Magnitude::Max);
    node_trust.insert(HorizonNodeName::try_new("zeus").unwrap(), Magnitude::Max);

    let mut user_trust = BTreeMap::new();
    user_trust.insert(HorizonUserName::try_new("li").unwrap(), Magnitude::Max);

    ClusterProposal {
        nodes,
        users,
        domains: BTreeMap::new(),
        trust: ClusterTrust {
            cluster: Magnitude::Max,
            clusters: BTreeMap::new(),
            nodes: node_trust,
            users: user_trust,
        },
        secret_bindings: Vec::new(),
        lan: None,
        resolver: None,
        tailnet: None,
        ai_providers: Vec::new(),
        vpn_profiles: Vec::new(),
        domain: HorizonClusterDomain::try_new("criome").unwrap(),
        public_domain: "criome.net".to_string(),
    }
}

fn node_proposal(species: NodeSpecies, size: Magnitude, full_keys: bool) -> NodeProposal {
    NodeProposal {
        species,
        size,
        trust: Magnitude::Max,
        machine: Machine {
            arch: Some(Arch::X86_64),
            cores: 4,
            model: None,
            mother_board: None,
            chip_gen: None,
            ram_gb: None,
        },
        io: Io {
            keyboard: Keyboard::Colemak,
            bootloader: Bootloader::Uefi,
            disks: root_disks(),
            swap_devices: Vec::new(),
        },
        pub_keys: NodePubKeys {
            ssh: SshPubKey::try_new("AAA=").unwrap(),
            nix: full_keys.then(|| NixPubKey::try_new(NIX_KEY).unwrap()),
            yggdrasil: full_keys.then(|| YggPubKeyEntry {
                pub_key: YggPubKey::try_new("a".repeat(64)).unwrap(),
                address: YggAddress::try_new("200::1").unwrap(),
                subnet: YggSubnet::try_new("300:ca41:6b12:fba").unwrap(),
            }),
        },
        link_local_ips: Vec::new(),
        node_ip: None,
        wireguard_pub_key: None,
        nordvpn: false,
        wifi_cert: false,
        wireguard_untrusted_proxies: Vec::new(),
        wants_printing: false,
        wants_hw_video_accel: false,
        router_interfaces: None,
        online: None,
        number_of_build_cores: None,
        services: NodeServices::default(),
        placement: NodePlacement::Metal {},
    }
}

fn root_disks() -> BTreeMap<MountPath, Disk> {
    let mut disks = BTreeMap::new();
    disks.insert(
        MountPath::new("/"),
        Disk {
            device: DevicePath::new("/dev/sda1"),
            fs_type: FsType::Ext4,
            options: Vec::new(),
        },
    );
    disks
}

fn user_proposal(species: UserSpecies) -> UserProposal {
    let mut pub_keys = BTreeMap::new();
    pub_keys.insert(
        HorizonNodeName::try_new("zeus").unwrap(),
        UserPubKeyEntry {
            ssh: SshPubKey::try_new("AAAAC3NzaC1lZDI1NTE5AAAA").unwrap(),
            keygrip: Keygrip::try_new("0123456789ABCDEF0123456789ABCDEF01234567").unwrap(),
        },
    );
    UserProposal {
        species,
        size: Magnitude::Max,
        keyboard: Keyboard::Colemak,
        style: Style::Emacs,
        github_id: None,
        fast_repeat: None,
        pub_keys,
        editor: None,
        text_size: None,
    }
}
