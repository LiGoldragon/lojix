//! Sandbox-OS witness — activation against a simulated nspawn-style
//! sandbox container.
//!
//! Designed to run cleanly under `nix flake check`. The pilot
//! cannot invoke a real `systemd-nspawn` from inside the Nix sandbox
//! (it requires root + cgroup access), so this test exercises the
//! same Activator + ObservationFan code path with a sandbox-mode
//! ProcessToolchain whose `activation_command` is a marker string
//! `nspawn-sandbox`. The activation invocation is observed end-to-end
//! through the schema-emitted ObservationRecord stream.
//!
//! The full nspawn-dune-on-prometheus shape is the operator's job to
//! land when amalgamating (per skills/double-implementation-strategy.md);
//! this pilot proves the actor wiring up to that boundary.

use lojix_next::runtime::authorization::AuthorizationPolicy;
use lojix_next::runtime::engine::Engine;
use lojix_next::runtime::observation::Subscribe;
use lojix_next::runtime::toolchain::ToolchainMode;
use lojix_next::{
    ActivationCommand, BuildCommand, CopyCommand, CriomeAuthorization, DeploymentRequest,
    HorizonView, Input, Output, Phase, Status, TargetNode, Toolchain,
};

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_activation_on_nspawn_sandbox() {
    let toolchain = Toolchain {
        build_command: BuildCommand("nspawn-sandbox-build".to_owned()),
        copy_command: CopyCommand("nspawn-sandbox-copy".to_owned()),
        activation_command: ActivationCommand("nspawn-sandbox-activate".to_owned()),
    };
    let temp = tempfile::tempdir().expect("temp dir");
    let database_path = temp.path().join("sema.redb");
    let engine = Engine::spawn(
        database_path,
        toolchain,
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
    )
    .await
    .expect("engine spawn");

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    engine
        .children()
        .fan
        .ask(Subscribe(sender))
        .await
        .expect("subscribe");

    let output = engine
        .handle(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: nspawn".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("submit + activate");

    assert!(matches!(output, Output::Accepted(_)));

    let mut activation_complete = false;
    let mut deploy_observed = false;
    let mut iterations = 0_u32;
    while iterations < 32 {
        match receiver.try_recv() {
            Ok(record) => {
                if record.phase == Phase::Activating && record.status == Status::Complete {
                    activation_complete = true;
                }
                if record.phase == Phase::Observed && record.status == Status::Complete {
                    deploy_observed = true;
                }
            }
            Err(_) => break,
        }
        iterations += 1;
    }
    assert!(
        activation_complete,
        "observation stream should report Activating Complete"
    );
    assert!(
        deploy_observed,
        "observation stream should report Observed Complete"
    );
}
