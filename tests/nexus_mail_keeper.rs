//! Nexus mail keeper tests (iteration 2).
//!
//! Two of the six new tests this iteration witnesses:
//!
//! - `lojix_next_nexus_is_mail_keeper` — assert NexusMailKeeper
//!   holds a `MailEntry` while SEMA is processing, and the lifecycle
//!   path traversed Sent -> Queued -> Processing -> Replied.
//! - `lojix_next_message_lifecycle_hooks_fire` — attach a test
//!   `MessageSentHook` to the Nexus, send a Submit, assert the hook
//!   fired with the right correlation ID.

use std::sync::{Arc, Mutex};

use kameo::error::Infallible;
use lojix_next::generated::{MailLifecycle, MessageSent, MessageSentHook};
use lojix_next::runtime::authorization::AuthorizationPolicy;
use lojix_next::runtime::engine::Engine;
use lojix_next::runtime::nexus::{AttachSentHook, MailLog};
use lojix_next::runtime::toolchain::ToolchainMode;
use lojix_next::runtime::trace::{Plane, Snapshot, TraceWitness};
use lojix_next::{
    CriomeAuthorization, DeploymentRequest, HorizonView, Input, TargetNode, Toolchain,
};

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_nexus_is_mail_keeper() {
    let temp = tempfile::tempdir().expect("temp dir");
    let database_path = temp.path().join("sema.redb");
    let engine = Engine::spawn(
        database_path,
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
    )
    .await
    .expect("engine spawn");

    let _ = engine
        .handle(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: mail".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("submit");

    // Inspect the mail log on the Nexus actor — the completed mail
    // should have walked through Sent -> Queued -> Processing -> Replied.
    let mail_log = engine
        .children()
        .nexus
        .ask(MailLog)
        .await
        .expect("mail log");
    assert_eq!(
        mail_log.in_flight().len(),
        0,
        "no mail should be in flight after a completed Submit"
    );
    assert_eq!(
        mail_log.completed().len(),
        1,
        "exactly one completed mail expected"
    );
    let completed = &mail_log.completed()[0];
    assert_eq!(completed.lifecycle(), MailLifecycle::Replied);
    assert_eq!(
        completed.lifecycle_path(),
        &[
            MailLifecycle::Sent,
            MailLifecycle::Queued,
            MailLifecycle::Processing,
            MailLifecycle::Replied,
        ]
    );

    // The trace log should witness the matching lifecycle events.
    let trace = engine
        .children()
        .trace
        .ask(Snapshot)
        .await
        .expect("snapshot trace");
    let identifier = completed.identifier();
    let saw_sent = trace.iter().any(|witness| {
        matches!(witness, TraceWitness::MailSent { identifier: id, plane: Plane::NexusMailKeeper } if *id == identifier)
    });
    let saw_queued = trace.iter().any(|witness| {
        matches!(witness, TraceWitness::MailQueued { identifier: id, plane: Plane::NexusMailKeeper } if *id == identifier)
    });
    let saw_processing = trace.iter().any(|witness| {
        matches!(witness, TraceWitness::MailProcessing { identifier: id, plane: Plane::NexusMailKeeper } if *id == identifier)
    });
    let saw_replied = trace.iter().any(|witness| {
        matches!(witness, TraceWitness::MailReplied { identifier: id, plane: Plane::NexusMailKeeper } if *id == identifier)
    });
    assert!(saw_sent, "trace must witness MailSent");
    assert!(saw_queued, "trace must witness MailQueued");
    assert!(saw_processing, "trace must witness MailProcessing");
    assert!(saw_replied, "trace must witness MailReplied");
}

/// Test hook that pushes each `MessageSent` event into a shared
/// vec. The hook attaches through the schema-emitted
/// `MessageSentHook` trait; the shared vec stays accessible to the
/// test for assertion. State carries (a) the shared vec, and (b) is
/// itself a non-ZST type — methods live on the hook noun.
struct RecordingSentHook {
    captured: Arc<Mutex<Vec<MessageSent>>>,
}

impl RecordingSentHook {
    fn new(captured: Arc<Mutex<Vec<MessageSent>>>) -> Self {
        Self { captured }
    }
}

impl MessageSentHook for RecordingSentHook {
    type Error = Infallible;

    fn message_sent(&mut self, event: MessageSent) -> std::result::Result<(), Self::Error> {
        self.captured.lock().expect("captured lock").push(event);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_message_lifecycle_hooks_fire() {
    let temp = tempfile::tempdir().expect("temp dir");
    let database_path = temp.path().join("sema.redb");
    let engine = Engine::spawn(
        database_path,
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
    )
    .await
    .expect("engine spawn");

    let captured: Arc<Mutex<Vec<MessageSent>>> = Arc::new(Mutex::new(Vec::new()));
    let hook: Arc<Mutex<dyn MessageSentHook<Error = Infallible> + Send>> =
        Arc::new(Mutex::new(RecordingSentHook::new(captured.clone())));
    engine
        .children()
        .nexus
        .ask(AttachSentHook(hook))
        .await
        .expect("attach hook");

    let _ = engine
        .handle(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: hook".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("submit");

    let mail_log = engine
        .children()
        .nexus
        .ask(MailLog)
        .await
        .expect("mail log");
    let identifier = mail_log.completed()[0].identifier();

    let events = captured.lock().expect("captured lock");
    assert_eq!(events.len(), 1, "hook should have fired exactly once");
    assert_eq!(events[0].identifier, identifier);
}
