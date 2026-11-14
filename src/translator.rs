//! The translator: the one place that holds the words of every external call
//! Lojix makes.
//!
//! Each emission generated from `ethos/emission.ethos` names what is asked;
//! here it becomes the call — a program and its argv under the
//! [`EffectExecution`] that says where a program comes from, or a systemd
//! D-Bus method on one [`SystemdBus`] session — and what came back becomes
//! its typed outcome. `emit` is synchronous: an attempt awaits it on a
//! blocking thread through [`Emitting::emitted`], so a build or an
//! activation that runs for minutes holds a blocking thread and never a
//! runtime worker.

use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use zbus::blocking::proxy::{Builder as ProxyBuilder, PropertyIterator, SignalIterator};
use zbus::blocking::{Connection, Proxy};
use zbus::proxy::CacheProperties;
use zbus::zvariant::OwnedObjectPath;

use crate::emission::{
    ActiveState, BootEntry, BootEntryClearing, BuildPlace, BusCallOutcome, BusFault, CallOutcome,
    CheckBuild, ClosureBuild, ClosureCopy, ClosurePresence, CollectedPaths, ConfigurationSwitch,
    DerivationCopy, DerivationEvaluation, Emittable, EvaluationFreshness, ExitStatus,
    FlakeMetadata, FlakeMetadataOutcome, FlakeMetadataQuery, GarbageCollection, GarbageOutcome,
    GcRootDirectory, HashOutcome, HostNameOutcome, InputHashing, JournalOutcome, JournalText,
    LocalHomeActivation, LocalHomeProfile, MetadataFault, OsFault, PathOutcome, PrintedPath,
    ProcessEnd, ProcessEnding, ProcessFailure, ProcessFault, PropertyReading, ReadShortHostName,
    RemoteHomeActivation, RemoteHomeLogin, RemoteHomeProfile, RetainedUnitDispatch, ServiceResult,
    SignalEnd, SignalNumber, SriHash, Stderr, Stdout, SubscribeUnitNew, SwitchAction,
    TargetStoreBuild, TargetStoreRooting, TestVmBringUp, TestVmTearDown, TransientScript,
    TransientUnitRun, UnitArrival, UnitArrivalOutcome, UnitJournalQuery, UnitLookup,
    UnitLookupOutcome, UnitState, UnitStateQuery, UnitStateWatch, UnitStateWatchOutcome, UnitStop,
    WaitStatus,
};
use crate::schema_runtime::{EffectExecution, ProgramResolution as _};

/// How many journal lines are read back for a transient unit an activation
/// ran in. The durable bound on failure detail is 4096 bytes; this only has to
/// be enough lines to fill it.
const UNIT_JOURNAL_LINES: usize = 200;

/// Home Manager's activation driver protocol: at version 1 the activation
/// script leaves the Home Manager profile alone, so the profile names a
/// generation only after that generation's activation has succeeded.
const HOME_MANAGER_DRIVER_VERSION_FLAG: &str = "--driver-version";
const HOME_MANAGER_DRIVER_VERSION: &str = "1";

/// Awaiting one emission on a blocking thread. The translator is cloned onto
/// the thread with the emission, and what the call observed comes back.
pub(crate) trait Emitting: Clone + Send + Sync + 'static {
    async fn emitted<Call>(&self, emission: Call) -> Call::Outcome
    where
        Call: Emittable<Self> + Send + 'static,
        Call::Outcome: Send + 'static,
    {
        let translator = self.clone();
        match tokio::task::spawn_blocking(move || emission.emit(&translator)).await {
            Ok(outcome) => outcome,
            Err(stopped) => std::panic::resume_unwind(stopped.into_panic()),
        }
    }
}

impl Emitting for EffectExecution {}
impl Emitting for SystemdBus {}

/// One program and its argument vector: the words of one process emission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommandLine {
    pub(crate) program: String,
    pub(crate) arguments: Vec<String>,
}

/// The command line a process emission becomes.
pub(crate) trait Commanded {
    fn command_line(&self) -> CommandLine;
}

/// Running one command line to its end and reporting what it printed, or how
/// it failed. The process reads nothing and inherits the daemon's
/// environment; both of its streams are kept whole.
pub(crate) trait ProcessRunning {
    fn run(&self, command_line: &CommandLine) -> Result<String, ProcessFailure>;
}

impl ProcessRunning for EffectExecution {
    fn run(&self, command_line: &CommandLine) -> Result<String, ProcessFailure> {
        let failure = |process_fault| ProcessFailure {
            command_program: signal_lojix::CommandProgram(command_line.program.clone()),
            command_argument_vector: command_line
                .arguments
                .iter()
                .map(|argument| signal_lojix::CommandArgument(argument.clone()))
                .collect(),
            process_fault,
        };
        let child = std::process::Command::new(self.program(&command_line.program))
            .args(&command_line.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| failure(ProcessFault::Unstarted(OsFault(error.to_string()))))?;
        let output = child
            .wait_with_output()
            .map_err(|error| failure(ProcessFault::Unfinished(OsFault(error.to_string()))))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(failure(ProcessFault::Ended(ProcessEnding {
                process_end: ProcessEnd::from(output.status),
                stdout: Stdout(String::from_utf8_lossy(&output.stdout).into_owned()),
                stderr: Stderr(String::from_utf8_lossy(&output.stderr).into_owned()),
            })))
        }
    }
}

impl From<std::process::ExitStatus> for ProcessEnd {
    fn from(status: std::process::ExitStatus) -> Self {
        use std::os::unix::process::ExitStatusExt as _;
        match (status.code(), status.signal()) {
            (Some(code), _) => Self::Exited(ExitStatus(i64::from(code))),
            (None, Some(signal)) => Self::Signalled(SignalEnd {
                signal_number: SignalNumber(i64::from(signal)),
                core_dumped: crate::emission::CoreDumped(status.core_dumped()),
            }),
            (None, None) => Self::Unreported(WaitStatus(i64::from(status.into_raw()))),
        }
    }
}

/// What a process printed, read as the emission's outcome.
trait PrintedReading {
    /// The first line, trimmed.
    fn first_line(&self) -> String;

    /// The first line as a path, absent when nothing was printed.
    fn printed_path(&self) -> PrintedPath;
}

impl PrintedReading for String {
    fn first_line(&self) -> String {
        self.lines().next().unwrap_or("").trim().to_string()
    }

    fn printed_path(&self) -> PrintedPath {
        let line = self.first_line();
        PrintedPath {
            closure_path_option: (!line.is_empty()).then_some(signal_lojix::ClosurePath(line)),
        }
    }
}

/// A single argument rendered safe for a remote `/bin/sh -c` body: bare when
/// the text is wholly shell-safe, single-quoted (with `'\''` escaping)
/// otherwise.
pub(crate) struct ShellArgument<'text>(pub(crate) &'text str);

/// One argument as it must appear inside a shell command line.
pub(crate) trait ShellQuoting {
    fn to_command_text(&self) -> String;
}

impl ShellQuoting for ShellArgument<'_> {
    fn to_command_text(&self) -> String {
        let safe = !self.0.is_empty()
            && self.0.bytes().all(|byte| {
                matches!(
                    byte,
                    b'a'..=b'z'
                        | b'A'..=b'Z'
                        | b'0'..=b'9'
                        | b'-'
                        | b'_'
                        | b'.'
                        | b'/'
                        | b'='
                        | b':'
                        | b'#'
                        | b'+'
                        | b','
                )
            });
        if safe {
            return self.0.to_string();
        }
        format!("'{}'", self.0.replace('\'', "'\\''"))
    }
}

/// `ssh -o BatchMode=yes <destination> <remote shell body>`.
trait RemoteShell {
    fn remote(&self, body: String) -> CommandLine;
}

impl RemoteShell for signal_lojix::SshDestination {
    fn remote(&self, body: String) -> CommandLine {
        CommandLine {
            program: "ssh".into(),
            arguments: vec!["-o".into(), "BatchMode=yes".into(), self.0.clone(), body],
        }
    }
}

/// The words a Nix command line is assembled from.
trait NixWords {
    /// `nix <arguments>`.
    fn nix(arguments: Vec<String>) -> Self;

    /// The `--option extra-substituters / extra-trusted-public-keys`
    /// arguments for a deploy's extra substituters; none when there are none.
    fn substituter_options(substituters: &[meta_signal_lojix::ExtraSubstituter]) -> Vec<String>;

    /// Select a derivation's outputs with the `^*` installable suffix, so
    /// `--print-out-paths` prints the realised output rather than the `.drv`.
    fn output_installable(derivation: &signal_lojix::ClosurePath) -> String;

    /// The `<flake>#<selector>` installable a deploy or check names.
    fn installable(
        flake_reference: &signal_lojix::FlakeReference,
        selector: &signal_lojix::DeploymentOutputSelector,
    ) -> String;
}

impl NixWords for CommandLine {
    fn nix(arguments: Vec<String>) -> Self {
        Self {
            program: "nix".into(),
            arguments,
        }
    }

    fn substituter_options(substituters: &[meta_signal_lojix::ExtraSubstituter]) -> Vec<String> {
        if substituters.is_empty() {
            return Vec::new();
        }
        let urls = substituters
            .iter()
            .map(|substituter| substituter.first_string.clone())
            .collect::<Vec<_>>()
            .join(" ");
        let public_keys = substituters
            .iter()
            .map(|substituter| substituter.second_string.clone())
            .collect::<Vec<_>>()
            .join(" ");
        vec![
            "--option".into(),
            "extra-substituters".into(),
            urls,
            "--option".into(),
            "extra-trusted-public-keys".into(),
            public_keys,
        ]
    }

    fn output_installable(derivation: &signal_lojix::ClosurePath) -> String {
        format!("{}^*", derivation.0)
    }

    fn installable(
        flake_reference: &signal_lojix::FlakeReference,
        selector: &signal_lojix::DeploymentOutputSelector,
    ) -> String {
        format!("{}#{}", flake_reference.0, selector.flake_attribute.0)
    }
}

impl Commanded for FlakeMetadataQuery {
    fn command_line(&self) -> CommandLine {
        CommandLine::nix(vec![
            "flake".into(),
            "metadata".into(),
            "--no-write-lock-file".into(),
            "--json".into(),
            self.flake_reference.0.clone(),
        ])
    }
}

impl Commanded for DerivationEvaluation {
    /// Resolve the toplevel `.drvPath` in the daemon's own Nix context. No
    /// `--store` redirect is ever added: an ssh-ng evaluation can deadlock
    /// in remote transport (lojix 0.3.9).
    fn command_line(&self) -> CommandLine {
        let mut arguments = vec!["eval".to_string()];
        if matches!(self.evaluation_freshness, EvaluationFreshness::Refreshed) {
            arguments.push("--refresh".into());
        }
        arguments.push("--raw".into());
        for input in &self.persisted_flake_input_override_vector {
            arguments.push("--override-input".into());
            arguments.push(input.flake_input_name.0.clone());
            arguments.push(format!(
                "{}?narHash={}",
                input.persisted_flake_input_reference.flake_input_url.0,
                input.persisted_flake_input_reference.nix_archive_hash.0
            ));
        }
        arguments.push(format!(
            "{}.drvPath",
            CommandLine::installable(&self.flake_reference, &self.deployment_output_selector)
        ));
        CommandLine::nix(arguments)
    }
}

impl Commanded for InputHashing {
    fn command_line(&self) -> CommandLine {
        CommandLine::nix(vec![
            "hash".into(),
            "path".into(),
            "--type".into(),
            "sha256".into(),
            "--sri".into(),
            self.input_directory.0.clone(),
        ])
    }
}

impl Commanded for ClosureBuild {
    fn command_line(&self) -> CommandLine {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".into(),
            "--print-out-paths".into(),
        ];
        if let BuildPlace::Offloaded(builder) = &self.build_place {
            arguments.extend([
                "--option".into(),
                "max-jobs".into(),
                "0".into(),
                "--builders".into(),
                builder.0.clone(),
            ]);
        }
        arguments.push(CommandLine::output_installable(&self.closure_path));
        arguments.extend(CommandLine::substituter_options(
            &self.extra_substituter_vector,
        ));
        CommandLine::nix(arguments)
    }
}

impl Commanded for CheckBuild {
    fn command_line(&self) -> CommandLine {
        CommandLine::nix(vec![
            "build".into(),
            "--no-link".into(),
            "--print-out-paths".into(),
            CommandLine::installable(&self.flake_reference, &self.deployment_output_selector),
        ])
    }
}

impl Commanded for DerivationCopy {
    fn command_line(&self) -> CommandLine {
        CommandLine::nix(vec![
            "copy".into(),
            "--derivation".into(),
            "--to".into(),
            self.nix_store_uri.0.clone(),
            self.closure_path.0.clone(),
        ])
    }
}

impl Commanded for TargetStoreBuild {
    fn command_line(&self) -> CommandLine {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".into(),
            "--print-out-paths".into(),
            "--store".into(),
            self.nix_store_uri.0.clone(),
            CommandLine::output_installable(&self.closure_path),
        ];
        arguments.extend(CommandLine::substituter_options(
            &self.extra_substituter_vector,
        ));
        CommandLine::nix(arguments)
    }
}

/// Where the target-side GC root for one generation lives, as remote shell
/// text and as readable text for the daemon log.
pub(crate) trait GcRootNaming {
    fn directory_text(&self) -> String;

    fn display_text(&self) -> String;
}

impl GcRootNaming for TargetStoreRooting {
    fn directory_text(&self) -> String {
        let operator = ShellArgument(&self.operator_node.0.0).to_command_text();
        match self.gc_root_directory {
            GcRootDirectory::SystemRoots => format!("/nix/var/nix/gcroots/lojix/{operator}"),
            GcRootDirectory::LoginHome => {
                format!("\"$HOME\"/.local/state/lojix/gcroots/{operator}")
            }
        }
    }

    fn display_text(&self) -> String {
        let directory = match self.gc_root_directory {
            GcRootDirectory::SystemRoots => "/nix/var/nix/gcroots/lojix",
            GcRootDirectory::LoginHome => "$HOME/.local/state/lojix/gcroots",
        };
        format!(
            "{directory}/{}/generation-{}",
            self.operator_node.0.0, self.generation_identifier.0
        )
    }
}

impl Commanded for TargetStoreRooting {
    fn command_line(&self) -> CommandLine {
        let directory = self.directory_text();
        let output = ShellArgument(&self.closure_path.0).to_command_text();
        self.ssh_destination.remote(format!(
            "export PATH=/run/current-system/sw/bin:/nix/var/nix/profiles/default/bin:$PATH \
             && mkdir -p {directory} \
             && nix-store --add-root {directory}/generation-{generation} --realise {output}",
            generation = self.generation_identifier.0,
        ))
    }
}

impl Commanded for ClosurePresence {
    fn command_line(&self) -> CommandLine {
        CommandLine::nix(vec![
            "path-info".into(),
            "--store".into(),
            self.nix_store_uri.0.clone(),
            self.closure_path.0.clone(),
        ])
    }
}

impl Commanded for ClosureCopy {
    fn command_line(&self) -> CommandLine {
        CommandLine::nix(vec![
            "copy".into(),
            "--substitute-on-destination".into(),
            "--to".into(),
            self.nix_store_uri.0.clone(),
            self.closure_path.0.clone(),
        ])
    }
}

impl Commanded for ConfigurationSwitch {
    fn command_line(&self) -> CommandLine {
        let store = &self.closure_path.0;
        let body = match self.switch_action {
            SwitchAction::Test => format!("{store}/bin/switch-to-configuration test"),
            SwitchAction::Boot | SwitchAction::Switch => {
                let action = match self.switch_action {
                    SwitchAction::Boot => "boot",
                    _ => "switch",
                };
                format!(
                    "nix-env -p /nix/var/nix/profiles/system --set {store} \
                     && {store}/bin/switch-to-configuration {action}"
                )
            }
        };
        self.ssh_destination.remote(body)
    }
}

impl Commanded for BootEntryClearing {
    fn command_line(&self) -> CommandLine {
        self.ssh_destination.remote(
            match self.boot_entry {
                BootEntry::Default => "bootctl set-default ''",
                BootEntry::Oneshot => "bootctl set-oneshot ''",
            }
            .into(),
        )
    }
}

/// The scripts a PID-1-owned transient unit runs.
pub(crate) trait ActivationScript {
    fn script(&self) -> String;
}

impl ActivationScript for TransientScript {
    /// `BootOnce`: `OLD` (the rollback target) is read from `bootctl status`'s
    /// `Current Entry`; after the candidate has generated its boot
    /// configuration, `NEW` is read from its generated `loader.conf`. Reboot 1
    /// lands NEW; reboot 2+ returns to OLD. `SelfSwitch`: set the system
    /// profile, `switch-to-configuration switch`, then clear both EFI
    /// overrides so the declarative `loader.conf` is the sole authority.
    fn script(&self) -> String {
        match self {
            Self::BootOnce(closure) => format!(
                "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
                 set -eu\n\
                 CLOSURE='{store}'\n\
                 OLD=$(bootctl status | awk -F': *' '/Current Entry:/ {{print $2}}')\n\
                 [ -n \"$OLD\" ]\n\
                 nix-env -p /nix/var/nix/profiles/system --set \"$CLOSURE\"\n\
                 \"$CLOSURE/bin/switch-to-configuration\" boot\n\
                 NEW=$(awk '$1 == \"default\" {{print $2; exit}}' /boot/loader/loader.conf)\n\
                 [ -n \"$NEW\" ]\n\
                 [ -f \"/boot/loader/entries/$NEW\" ]\n\
                 [ \"$NEW\" != \"$OLD\" ]\n\
                 bootctl set-default \"$OLD\"\n\
                 bootctl set-oneshot \"$NEW\"\n\
                 echo \"boot-once: oneshot=$NEW persistent-default=$OLD (=running generation)\"\n",
                store = closure.0,
            ),
            Self::SelfSwitch(closure) => format!(
                "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
                 set -eu\n\
                 nix-env -p /nix/var/nix/profiles/system --set {store}\n\
                 {store}/bin/switch-to-configuration switch\n\
                 bootctl set-default ''\n\
                 bootctl set-oneshot ''\n",
                store = closure.0,
            ),
        }
    }
}

impl Commanded for TransientUnitRun {
    /// `systemd-run --wait`: ssh holds open while PID 1 runs the script in its
    /// own unit, so a dropped ssh leaves the unit running to completion.
    fn command_line(&self) -> CommandLine {
        self.ssh_destination.remote(format!(
            "systemd-run \
             --unit={unit_name} \
             --collect \
             --wait \
             --service-type=oneshot \
             /bin/sh -c {script}",
            unit_name = self.unit_name.0,
            script = ShellArgument(&self.transient_script.script()).to_command_text(),
        ))
    }
}

impl Commanded for RetainedUnitDispatch {
    /// A same-host test activation, handed to PID 1 without tying its
    /// lifetime to the daemon: `--remain-after-exit` keeps PID 1's terminal
    /// receipt until the successor has read it and stops the unit.
    fn command_line(&self) -> CommandLine {
        let script = format!(
            "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
             set -eu\n\
             {store}/bin/switch-to-configuration test\n",
            store = self.closure_path.0,
        );
        self.ssh_destination.remote(format!(
            "systemd-run --unit={unit_name} --no-block --service-type=oneshot --remain-after-exit /bin/sh -c {script}",
            unit_name = self.unit_name.0,
            script = ShellArgument(&script).to_command_text(),
        ))
    }
}

impl Commanded for UnitJournalQuery {
    fn command_line(&self) -> CommandLine {
        self.ssh_destination.remote(format!(
            "journalctl --unit={} --lines={UNIT_JOURNAL_LINES} --no-pager --output=cat",
            ShellArgument(&self.unit_name.0).to_command_text(),
        ))
    }
}

impl Commanded for LocalHomeProfile {
    fn command_line(&self) -> CommandLine {
        CommandLine {
            program: "nix-env".into(),
            arguments: vec![
                "-p".into(),
                Path::new(&self.home_directory.0)
                    .join(".local/state/nix/profiles/home-manager")
                    .display()
                    .to_string(),
                "--set".into(),
                self.closure_path.0.clone(),
            ],
        }
    }
}

impl Commanded for LocalHomeActivation {
    /// Home Manager's activation in a private transient user service, waited
    /// on, its output piped back.
    fn command_line(&self) -> CommandLine {
        CommandLine {
            program: "/run/current-system/sw/bin/systemd-run".into(),
            arguments: vec![
                "--user".into(),
                "--wait".into(),
                "--pipe".into(),
                "--collect".into(),
                "--quiet".into(),
                "--service-type=exec".into(),
                "--expand-environment=no".into(),
                "--property=PrivateTmp=yes".into(),
                "--setenv=TMPDIR=/tmp".into(),
                "--".into(),
                format!("{}/activate", self.closure_path.0),
                HOME_MANAGER_DRIVER_VERSION_FLAG.into(),
                HOME_MANAGER_DRIVER_VERSION.into(),
            ],
        }
    }
}

/// A remote Home Manager command, run as the SSH login when that login is
/// the target user, or through `runuser --login` for the target user when
/// the login is root: a login rebuilds the environment from the target
/// account, so activation runs as a clean session of that user.
trait RemoteHomeCommand {
    fn remote_home(&self, destination: &signal_lojix::SshDestination, body: String) -> CommandLine;
}

impl RemoteHomeCommand for RemoteHomeLogin {
    fn remote_home(&self, destination: &signal_lojix::SshDestination, body: String) -> CommandLine {
        match self {
            Self::AsTargetUser => destination.remote(body),
            Self::MediatedFor(user) => destination.remote(format!(
                "runuser --login --command {command} {user}",
                command = ShellArgument(&body).to_command_text(),
                user = ShellArgument(&user.0).to_command_text(),
            )),
        }
    }
}

impl Commanded for RemoteHomeProfile {
    fn command_line(&self) -> CommandLine {
        self.remote_home_login.remote_home(
            &self.ssh_destination,
            format!(
                "nix-env -p \"$HOME/.local/state/nix/profiles/home-manager\" --set {}",
                ShellArgument(&self.closure_path.0).to_command_text(),
            ),
        )
    }
}

impl Commanded for RemoteHomeActivation {
    fn command_line(&self) -> CommandLine {
        let local = LocalHomeActivation {
            closure_path: self.closure_path.clone(),
        }
        .command_line();
        let body = std::iter::once(local.program)
            .chain(local.arguments)
            .map(|argument| ShellArgument(&argument).to_command_text())
            .collect::<Vec<_>>()
            .join(" ");
        self.remote_home_login
            .remote_home(&self.ssh_destination, body)
    }
}

impl Commanded for ReadShortHostName {
    fn command_line(&self) -> CommandLine {
        CommandLine {
            program: "hostname".into(),
            arguments: vec!["-s".into()],
        }
    }
}

impl Commanded for GarbageCollection {
    fn command_line(&self) -> CommandLine {
        CommandLine {
            program: "ssh".into(),
            arguments: vec![self.node_name.0.clone(), "nix-store --gc".into()],
        }
    }
}

impl Commanded for TestVmBringUp {
    /// A `--user` systemd-run unit that `unshare -rn`s a private network
    /// namespace, brings up the additive tap inside it, and runs the built
    /// runner: no sudo, no switch-to-configuration, the host's namespace
    /// untouched.
    fn command_line(&self) -> CommandLine {
        let body = format!(
            "ip tuntap add dev vmt0 mode tap; \
             ip addr add 169.254.100.1/32 dev vmt0; \
             ip link set vmt0 up; \
             ip route add {guest_ip} dev vmt0; \
             exec {runner}",
            guest_ip = self.guest_address.0,
            runner = self.closure_path.0,
        );
        self.ssh_destination.remote(format!(
            "set -eu\n\
             systemd-run --user --unit={unit} --collect --service-type=notify \
             unshare -rn /bin/sh -c {body}\n",
            unit = self.guest_node.test_vm_unit(),
            body = ShellArgument(&body).to_command_text(),
        ))
    }
}

impl Commanded for TestVmTearDown {
    fn command_line(&self) -> CommandLine {
        self.ssh_destination.remote(format!(
            "systemctl --user stop {unit} || true",
            unit = self.guest_node.test_vm_unit(),
        ))
    }
}

/// The durable `--user` unit a guest's namespace runs in.
trait TestVmUnit {
    fn test_vm_unit(&self) -> String;
}

impl TestVmUnit for crate::emission::GuestNode {
    fn test_vm_unit(&self) -> String {
        format!("lojix-test-vm-{}", self.0.0)
    }
}

/// A process emission whose outcome says only whether the call completed.
macro_rules! completed_on {
    ($($emission:ty),* $(,)?) => {$(
        impl Emittable<EffectExecution> for $emission {
            fn emit(&self, translator: &EffectExecution) -> CallOutcome {
                match translator.run(&self.command_line()) {
                    Ok(_) => CallOutcome::Completed,
                    Err(failure) => CallOutcome::Failed(failure),
                }
            }
        }
    )*};
}

completed_on!(
    DerivationCopy,
    TargetStoreRooting,
    ClosurePresence,
    ClosureCopy,
    ConfigurationSwitch,
    BootEntryClearing,
    TransientUnitRun,
    RetainedUnitDispatch,
    LocalHomeProfile,
    LocalHomeActivation,
    RemoteHomeProfile,
    RemoteHomeActivation,
    TestVmBringUp,
    TestVmTearDown,
);

/// A process emission whose outcome is the path it printed first.
macro_rules! printed_path_on {
    ($($emission:ty),* $(,)?) => {$(
        impl Emittable<EffectExecution> for $emission {
            fn emit(&self, translator: &EffectExecution) -> PathOutcome {
                match translator.run(&self.command_line()) {
                    Ok(printed) => PathOutcome::Printed(printed.printed_path()),
                    Err(failure) => PathOutcome::Failed(failure),
                }
            }
        }
    )*};
}

printed_path_on!(
    DerivationEvaluation,
    ClosureBuild,
    CheckBuild,
    TargetStoreBuild
);

impl Emittable<EffectExecution> for FlakeMetadataQuery {
    /// Nix's metadata, read where Nix writes the locked source: the locked
    /// URL (`url`, then `resolvedUrl`) and the revision (`locked.rev`,
    /// `revision`, `rev`, then the NAR hash a path source is identified by).
    fn emit(&self, translator: &EffectExecution) -> FlakeMetadataOutcome {
        let printed = match translator.run(&self.command_line()) {
            Ok(printed) => printed,
            Err(failure) => return FlakeMetadataOutcome::Failed(failure),
        };
        let value: serde_json::Value = match serde_json::from_str(&printed) {
            Ok(value) => value,
            Err(error) => return FlakeMetadataOutcome::Malformed(MetadataFault(error.to_string())),
        };
        let string_at = |path: &[&str]| {
            let mut found = &value;
            for key in path {
                found = found.get(*key)?;
            }
            found.as_str().map(ToOwned::to_owned)
        };
        FlakeMetadataOutcome::Read(FlakeMetadata {
            flake_reference_option: string_at(&["url"])
                .or_else(|| string_at(&["resolvedUrl"]))
                .map(signal_lojix::FlakeReference),
            resolved_revision_option: string_at(&["locked", "rev"])
                .or_else(|| string_at(&["revision"]))
                .or_else(|| string_at(&["rev"]))
                .or_else(|| string_at(&["locked", "narHash"]))
                .or_else(|| string_at(&["narHash"]))
                .map(crate::memory::ResolvedRevision),
        })
    }
}

impl Emittable<EffectExecution> for InputHashing {
    fn emit(&self, translator: &EffectExecution) -> HashOutcome {
        match translator.run(&self.command_line()) {
            Ok(printed) => HashOutcome::Hashed(SriHash(printed.first_line())),
            Err(failure) => HashOutcome::Failed(failure),
        }
    }
}

impl Emittable<EffectExecution> for UnitJournalQuery {
    fn emit(&self, translator: &EffectExecution) -> JournalOutcome {
        match translator.run(&self.command_line()) {
            Ok(printed) => JournalOutcome::Read(JournalText(printed)),
            Err(failure) => JournalOutcome::Failed(failure),
        }
    }
}

impl Emittable<EffectExecution> for ReadShortHostName {
    fn emit(&self, translator: &EffectExecution) -> HostNameOutcome {
        match translator.run(&self.command_line()) {
            Ok(printed) => HostNameOutcome::Named(signal_lojix::NodeName(printed.trim().into())),
            Err(failure) => HostNameOutcome::Failed(failure),
        }
    }
}

impl Emittable<EffectExecution> for GarbageCollection {
    fn emit(&self, translator: &EffectExecution) -> GarbageOutcome {
        match translator.run(&self.command_line()) {
            Ok(printed) => GarbageOutcome::Collected(CollectedPaths(
                printed
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .count() as i64,
            )),
            Err(failure) => GarbageOutcome::Failed(failure),
        }
    }
}

/// One session on the system bus with systemd: the connection, the
/// Manager.UnitNew subscription, and the unit last found, its properties
/// cached and its `ActiveState` changes subscribed. Cloning shares the
/// session, so the emissions that watch one detached activation see the
/// subscriptions the earlier ones made.
#[derive(Clone, Default)]
pub(crate) struct SystemdBus {
    session: Arc<Mutex<BusSession>>,
}

#[derive(Default)]
struct BusSession {
    connection_option: Option<Connection>,
    unit_new_option: Option<SignalIterator<'static>>,
    watched_unit_option: Option<WatchedUnit>,
}

struct WatchedUnit {
    name: String,
    unit: Proxy<'static>,
    service: Proxy<'static>,
    active_state_changes: PropertyIterator<'static, String>,
}

impl std::fmt::Debug for SystemdBus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SystemdBus")
    }
}

/// The system-bus session's own steps, each behind one emission.
trait BusSessionOpening {
    fn connection(&mut self) -> Result<Connection, String>;

    fn manager(connection: &Connection) -> Result<Proxy<'static>, String>;

    fn unit_proxy(
        connection: &Connection,
        path: OwnedObjectPath,
        interface: &'static str,
        what: &str,
    ) -> Result<Proxy<'static>, String>;
}

impl BusSessionOpening for BusSession {
    fn connection(&mut self) -> Result<Connection, String> {
        if let Some(connection) = &self.connection_option {
            return Ok(connection.clone());
        }
        let connection = Connection::system()
            .map_err(|error| format!("connect to systemd for detached activation: {error}"))?;
        self.connection_option = Some(connection.clone());
        Ok(connection)
    }

    fn manager(connection: &Connection) -> Result<Proxy<'static>, String> {
        Proxy::new(
            connection,
            "org.freedesktop.systemd1",
            "/org/freedesktop/systemd1",
            "org.freedesktop.systemd1.Manager",
        )
        .map_err(|error| format!("open systemd manager: {error}"))
    }

    fn unit_proxy(
        connection: &Connection,
        path: OwnedObjectPath,
        interface: &'static str,
        what: &str,
    ) -> Result<Proxy<'static>, String> {
        ProxyBuilder::new(connection)
            .destination("org.freedesktop.systemd1")
            .map_err(|error| format!("address detached activation {what}: {error}"))?
            .path(path)
            .map_err(|error| format!("address detached activation {what} path: {error}"))?
            .interface(interface)
            .map_err(|error| format!("address detached activation {what} interface: {error}"))?
            .cache_properties(CacheProperties::Yes)
            .build()
            .map_err(|error| format!("open detached activation {what}: {error}"))
    }
}

impl Emittable<SystemdBus> for SubscribeUnitNew {
    /// Subscribe to Manager.UnitNew, so a unit PID 1 registers after this
    /// cannot be missed by a later lookup.
    fn emit(&self, translator: &SystemdBus) -> BusCallOutcome {
        let mut session = translator.session.lock().expect("bus session");
        let subscribed = session.connection().and_then(|connection| {
            BusSession::manager(&connection)?
                .receive_signal("UnitNew")
                .map_err(|error| format!("subscribe to systemd UnitNew: {error}"))
        });
        match subscribed {
            Ok(unit_new) => {
                session.unit_new_option = Some(unit_new);
                BusCallOutcome::Answered
            }
            Err(fault) => BusCallOutcome::Refused(BusFault(fault)),
        }
    }
}

impl Emittable<SystemdBus> for UnitLookup {
    /// Manager.GetUnit; a found unit's Unit and Service properties are
    /// cached and its `ActiveState` changes subscribed before anything is read
    /// from it, so no transition falls between a read and the subscription.
    fn emit(&self, translator: &SystemdBus) -> UnitLookupOutcome {
        let mut session = translator.session.lock().expect("bus session");
        let name = self.unit_name.0.clone();
        let found = session.connection().and_then(|connection| {
            let manager = BusSession::manager(&connection)?;
            let path: OwnedObjectPath = match manager.call("GetUnit", &(name.as_str(),)) {
                Ok(path) => path,
                Err(error) if error.to_string().contains("NoSuchUnit") => return Ok(None),
                Err(error) => return Err(format!("find detached activation unit {name}: {error}")),
            };
            let unit = BusSession::unit_proxy(
                &connection,
                path.clone(),
                "org.freedesktop.systemd1.Unit",
                "unit",
            )?;
            let service = BusSession::unit_proxy(
                &connection,
                path,
                "org.freedesktop.systemd1.Service",
                "service",
            )?;
            let active_state_changes = unit.receive_property_changed::<String>("ActiveState");
            Ok(Some(WatchedUnit {
                name: name.clone(),
                unit,
                service,
                active_state_changes,
            }))
        });
        match found {
            Ok(Some(watched)) => {
                session.watched_unit_option = Some(watched);
                UnitLookupOutcome::Found
            }
            Ok(None) => UnitLookupOutcome::NoSuchUnit,
            Err(fault) => UnitLookupOutcome::Refused(BusFault(fault)),
        }
    }
}

/// Whether a registered unit is the one awaited: its canonical name exactly,
/// so an unrelated transient never wakes this deployment.
pub(crate) trait ArrivalMatching {
    fn names(&self, registered: &str) -> bool;
}

impl ArrivalMatching for UnitArrival {
    fn names(&self, registered: &str) -> bool {
        registered == self.unit_name.0
    }
}

impl Emittable<SystemdBus> for UnitArrival {
    /// Wait on the UnitNew subscription until this unit is registered.
    fn emit(&self, translator: &SystemdBus) -> UnitArrivalOutcome {
        let mut session = translator.session.lock().expect("bus session");
        let Some(unit_new) = session.unit_new_option.as_mut() else {
            return UnitArrivalOutcome::Refused(BusFault(
                "no systemd UnitNew subscription to wait on".into(),
            ));
        };
        loop {
            let Some(signal) = unit_new.next() else {
                return UnitArrivalOutcome::StreamEnded;
            };
            let (name, _path): (String, OwnedObjectPath) = match signal.body().deserialize() {
                Ok(body) => body,
                Err(error) => {
                    return UnitArrivalOutcome::Refused(BusFault(format!(
                        "decode systemd UnitNew for detached activation: {error}"
                    )));
                }
            };
            if self.names(&name) {
                return UnitArrivalOutcome::Arrived;
            }
        }
    }
}

impl Emittable<SystemdBus> for UnitStateQuery {
    /// The found unit's `ActiveState`, its service `Result`, and how its main
    /// process ended (`ExecMainCode`/`ExecMainStatus`), read from the cache
    /// the subscription keeps current.
    fn emit(&self, translator: &SystemdBus) -> UnitState {
        let session = translator.session.lock().expect("bus session");
        let Some(watched) = session
            .watched_unit_option
            .as_ref()
            .filter(|watched| watched.name == self.unit_name.0)
        else {
            let unknown = || {
                PropertyReading::Unreadable(BusFault(format!(
                    "{} was not found on the bus",
                    self.unit_name.0
                )))
            };
            return UnitState {
                active_state: ActiveState(unknown()),
                service_result: ServiceResult(unknown()),
                process_end_option: None,
            };
        };
        let reading =
            |proxy: &Proxy<'static>, property: &str| match proxy.get_property::<String>(property) {
                Ok(value) => PropertyReading::Read(value),
                Err(error) => PropertyReading::Unreadable(BusFault(error.to_string())),
            };
        let main_code = watched.service.get_property::<i32>("ExecMainCode");
        let main_status = watched.service.get_property::<i32>("ExecMainStatus");
        UnitState {
            active_state: ActiveState(reading(&watched.unit, "ActiveState")),
            service_result: ServiceResult(reading(&watched.service, "Result")),
            process_end_option: match (main_code, main_status) {
                (Ok(code), Ok(status)) => ProcessEnd::from_main_process(code, status),
                _ => None,
            },
        }
    }
}

impl Emittable<SystemdBus> for UnitStateWatch {
    /// Wait for the found unit's next `ActiveState` change.
    fn emit(&self, translator: &SystemdBus) -> UnitStateWatchOutcome {
        let mut session = translator.session.lock().expect("bus session");
        match session
            .watched_unit_option
            .as_mut()
            .filter(|watched| watched.name == self.unit_name.0)
            .and_then(|watched| watched.active_state_changes.next())
        {
            Some(_) => UnitStateWatchOutcome::Changed,
            None => UnitStateWatchOutcome::StreamEnded,
        }
    }
}

impl Emittable<SystemdBus> for UnitStop {
    /// Manager.StopUnit, mode `replace`.
    fn emit(&self, translator: &SystemdBus) -> BusCallOutcome {
        let mut session = translator.session.lock().expect("bus session");
        let stopped = session.connection().and_then(|connection| {
            BusSession::manager(&connection)?
                .call::<_, _, OwnedObjectPath>("StopUnit", &(self.unit_name.0.as_str(), "replace"))
                .map(|_| ())
                .map_err(|error| format!("stop {}: {error}", self.unit_name.0))
        });
        match stopped {
            Ok(()) => BusCallOutcome::Answered,
            Err(fault) => BusCallOutcome::Refused(BusFault(fault)),
        }
    }
}

/// Reading PID 1's `ExecMainCode`/`ExecMainStatus` pair: `CLD_EXITED` (1)
/// carries an exit status, `CLD_KILLED` (2) and `CLD_DUMPED` (3) a signal, and
/// 0 means no main process ended.
pub(crate) trait MainProcessReading {
    fn from_main_process(code: i32, status: i32) -> Option<Self>
    where
        Self: Sized;
}

impl MainProcessReading for ProcessEnd {
    fn from_main_process(code: i32, status: i32) -> Option<Self> {
        let status = i64::from(status);
        match code {
            1 => Some(Self::Exited(ExitStatus(status))),
            2 | 3 => Some(Self::Signalled(SignalEnd {
                signal_number: SignalNumber(status),
                core_dumped: crate::emission::CoreDumped(code == 3),
            })),
            _ => None,
        }
    }
}
