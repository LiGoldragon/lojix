//! The bootstrap translator: the one place that holds the words of every
//! process `lojix-bootstrap` starts.
//!
//! Each emission generated from `ethos/bootstrap_emission.ethos` names what is
//! asked; here it becomes a [`BootstrapCommand`], a program with its argv and
//! environment, run by the [`BootstrapExecutor`] the translator carries, and
//! what the process printed, or how it failed, becomes the emission's typed
//! outcome. The executor that starts real processes is
//! [`ProcessBootstrapExecutor`]; a test supplies one that records each
//! command and answers it from fixtures.

use std::cell::RefCell;
use std::process::Stdio;

use crate::bootstrap_emission::BootstrapBuild;
use crate::bootstrap_emission::{
    BootOnceDispatch, BootOnceRoute, BootOnceScript, BootOnceUnitOutcome, BootOnceUnitQuery,
    BootOnceUnitState, BootstrapCheckBuild, BootstrapCopy, BootstrapEvaluation, BootstrapRooting,
    ClosurePresenceOutcome, Emittable, GeneratedInputHashing, PrintedPathOutcome, PrintedStorePath,
    RemoteClosureQuery, SshConfigPath, SshRoute,
};
use crate::emission::{
    CallOutcome, HashOutcome, OsFault, ProcessEnd, ProcessEnding, ProcessFailure, ProcessFault,
    SriHash, Stderr, Stdout,
};

/// The search path every command run on a node, and every boot-once unit,
/// is given: the node's own Nix and system profiles before the base system.
const NODE_PATH: &str =
    "/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin";

/// Where `systemd-run` is on a NixOS node.
const SYSTEMD_RUN: &str = "/run/current-system/sw/bin/systemd-run";

/// Where the ssh binary is named when the flake wrapper names none: a
/// deterministic absolute path that does not exist, so an unwrapped run never
/// consults an ambient `PATH` executable.
const SSH_PROGRAM_UNWRAPPED: &str = "/__lojix-bootstrap-wrapper-required__/bin/ssh";

/// The variable the flake wrapper sets to its openssh closure's `ssh`.
const SSH_PROGRAM_VARIABLE: &str = "LOJIX_BOOTSTRAP_OPENSSH";

/// One process a bootstrap emission becomes: its program, its argv, and the
/// variables added to its environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapCommand {
    pub program: String,
    pub arguments: Vec<String>,
    pub environment: Vec<(String, String)>,
}

/// Running one command to its end: what it printed on stdout, or the
/// failure, with its program, argv and how it ended. Tests supply an executor
/// that records each command and answers it from fixtures; production is the
/// only implementation that starts a process.
pub trait BootstrapExecutor {
    fn run(&mut self, command: &BootstrapCommand) -> Result<String, ProcessFailure>;
}

#[derive(Debug, Default)]
pub struct ProcessBootstrapExecutor;

impl BootstrapExecutor for ProcessBootstrapExecutor {
    fn run(&mut self, command: &BootstrapCommand) -> Result<String, ProcessFailure> {
        let failure = |process_fault| ProcessFailure {
            command_program: signal_lojix::CommandProgram(command.program.clone()),
            command_argument_vector: command
                .arguments
                .iter()
                .map(|argument| signal_lojix::CommandArgument(argument.clone()))
                .collect(),
            process_fault,
        };
        let output = std::process::Command::new(&command.program)
            .args(&command.arguments)
            .envs(command.environment.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| failure(ProcessFault::Unstarted(OsFault(error.to_string()))))?
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

/// The translator one bootstrap run emits through: the executor its commands
/// run on, and the ssh binary a remote command is carried by.
pub struct BootstrapTranslator<'executor, Executor> {
    executor: RefCell<&'executor mut Executor>,
    ssh_program: String,
}

/// Making a translator, and the two things every emission asks of it.
pub trait BootstrapTranslating<'executor, Executor> {
    /// A translator over `executor`, carrying remote commands by the ssh the
    /// flake wrapper names.
    fn over(executor: &'executor mut Executor) -> Self;

    /// Run one command.
    fn run(&self, command: &BootstrapCommand) -> Result<String, ProcessFailure>;

    /// The command that runs `body` on the node `route` reaches, through
    /// the run's private ssh configuration and nothing ambient.
    fn over_ssh(&self, route: &SshRoute, body: String) -> BootstrapCommand;
}

impl<'executor, Executor: BootstrapExecutor> BootstrapTranslating<'executor, Executor>
    for BootstrapTranslator<'executor, Executor>
{
    fn over(executor: &'executor mut Executor) -> Self {
        Self {
            executor: RefCell::new(executor),
            ssh_program: std::env::var(SSH_PROGRAM_VARIABLE)
                .unwrap_or_else(|_| SSH_PROGRAM_UNWRAPPED.to_string()),
        }
    }

    fn run(&self, command: &BootstrapCommand) -> Result<String, ProcessFailure> {
        self.executor.borrow_mut().run(command)
    }

    fn over_ssh(&self, route: &SshRoute, body: String) -> BootstrapCommand {
        let identity = &route.ssh_identity;
        let mut arguments = vec![
            "-F".to_string(),
            route.ssh_config_path.0.clone(),
            "-o".to_string(),
            "ProxyCommand=none".to_string(),
            "-o".to_string(),
            "ProxyJump=none".to_string(),
            "-o".to_string(),
            "ControlMaster=no".to_string(),
            "-o".to_string(),
            "ControlPath=none".to_string(),
        ];
        if let Some(port) = &identity.ssh_port_option {
            arguments.extend(["-p".to_string(), port.0.to_string()]);
        }
        arguments.extend([
            "--".to_string(),
            format!("{}@{}", identity.ssh_user.0, identity.ssh_host.0),
            body,
        ]);
        BootstrapCommand {
            program: self.ssh_program.clone(),
            arguments,
            environment: Vec::new(),
        }
    }
}

/// What a bootstrap asks of a piece of text it puts in a command.
pub(crate) trait BootstrapText {
    /// The first non-blank line, trimmed; `None` when there is none.
    fn first_printed_line(&self) -> Option<String>;

    /// The text as one shell word, safe inside single quotes.
    fn shell_quoted(&self) -> String;

    /// The text as one `ssh_config` value, safe inside double quotes.
    fn ssh_config_quoted(&self) -> String;
}

impl BootstrapText for str {
    fn first_printed_line(&self) -> Option<String> {
        self.lines()
            .find(|line| !line.trim().is_empty())
            .map(|line| line.trim().to_string())
    }

    fn shell_quoted(&self) -> String {
        format!("'{}'", self.replace('\'', "'\\''"))
    }

    fn ssh_config_quoted(&self) -> String {
        format!("\"{}\"", self.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

/// The command a process emission becomes.
trait BootstrapCommanded<'executor, Executor> {
    fn command(&self, translator: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand;
}

/// What a command printed, read as an emission's outcome.
trait PrintedOutcome {
    fn printed_path(self) -> PrintedPathOutcome;

    fn called(self) -> CallOutcome;
}

impl PrintedOutcome for Result<String, ProcessFailure> {
    fn printed_path(self) -> PrintedPathOutcome {
        match self {
            Ok(printed) => match printed.first_printed_line() {
                Some(line) => PrintedPathOutcome::Printed(PrintedStorePath(line)),
                None => PrintedPathOutcome::Unprinted,
            },
            Err(failure) => PrintedPathOutcome::Failed(failure),
        }
    }

    fn called(self) -> CallOutcome {
        match self {
            Ok(_) => CallOutcome::Completed,
            Err(failure) => CallOutcome::Failed(failure),
        }
    }
}

/// A `nix` command with its argv.
trait NixCommand {
    fn nix(arguments: Vec<String>) -> Self;
}

impl NixCommand for BootstrapCommand {
    fn nix(arguments: Vec<String>) -> Self {
        Self {
            program: "nix".to_string(),
            arguments,
            environment: Vec::new(),
        }
    }
}

impl<'executor, Executor> BootstrapCommanded<'executor, Executor> for BootstrapEvaluation {
    fn command(&self, _: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        let mut arguments = vec![
            "eval".to_string(),
            "--raw".to_string(),
            "--system".to_string(),
            self.bootstrap_nix_system.0.clone(),
        ];
        for input in &self.persisted_flake_input_override_vector {
            arguments.extend([
                "--override-input".to_string(),
                input.flake_input_name.0.clone(),
                format!(
                    "{}?narHash={}",
                    input.persisted_flake_input_reference.flake_input_url.0,
                    input.persisted_flake_input_reference.nix_archive_hash.0
                ),
            ]);
        }
        arguments.push(format!(
            "{}#{}.drvPath",
            self.bootstrap_flake_reference.0, self.bootstrap_output_selector.0
        ));
        BootstrapCommand::nix(arguments)
    }
}

impl<'executor, Executor> BootstrapCommanded<'executor, Executor> for BootstrapBuild {
    fn command(&self, _: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--system".to_string(),
            self.bootstrap_nix_system.0.clone(),
        ];
        if let Some(builder) = &self.bootstrap_builder_spec_option {
            arguments.extend([
                "--option".to_string(),
                "max-jobs".to_string(),
                "0".to_string(),
                "--builders".to_string(),
                builder.0.clone(),
            ]);
        }
        arguments.push(format!("{}^*", self.derivation_path.0));
        BootstrapCommand::nix(arguments)
    }
}

impl<'executor, Executor> BootstrapCommanded<'executor, Executor> for BootstrapCheckBuild {
    fn command(&self, _: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        BootstrapCommand::nix(vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--system".to_string(),
            self.bootstrap_nix_system.0.clone(),
            format!(
                "{}#{}",
                self.bootstrap_flake_reference.0, self.bootstrap_output_selector.0
            ),
        ])
    }
}

impl<'executor, Executor> BootstrapCommanded<'executor, Executor> for GeneratedInputHashing {
    fn command(&self, _: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        BootstrapCommand::nix(vec![
            "hash".to_string(),
            "path".to_string(),
            "--type".to_string(),
            "sha256".to_string(),
            "--sri".to_string(),
            self.generated_input_directory.0.clone(),
        ])
    }
}

impl<'executor, Executor> BootstrapCommanded<'executor, Executor> for BootstrapRooting {
    fn command(&self, _: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        BootstrapCommand {
            program: "nix-store".to_string(),
            arguments: vec![
                "--add-root".to_string(),
                self.root_staging_path.0.clone(),
                "--realise".to_string(),
                self.closure_path.0.clone(),
            ],
            environment: Vec::new(),
        }
    }
}

impl<'executor, Executor: BootstrapExecutor> BootstrapCommanded<'executor, Executor>
    for RemoteClosureQuery
{
    fn command(&self, translator: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        translator.over_ssh(
            &self.ssh_route,
            format!(
                "PATH={NODE_PATH}; export PATH; if nix-store --query --references {} >/dev/null 2>&1; then printf Present; else printf Absent; fi",
                self.closure_path.0.shell_quoted()
            ),
        )
    }
}

impl<'executor, Executor> BootstrapCommanded<'executor, Executor> for BootstrapCopy {
    fn command(&self, _: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        BootstrapCommand {
            program: "nix".to_string(),
            arguments: vec![
                "copy".to_string(),
                "--substitute-on-destination".to_string(),
                "--to".to_string(),
                self.bootstrap_nix_store_uri.0.clone(),
                self.closure_path.0.clone(),
            ],
            environment: vec![(
                "NIX_SSHOPTS".to_string(),
                self.ssh_config_path.nix_ssh_options(),
            )],
        }
    }
}

/// The options Nix's own ssh is given, pointing it at the run's private ssh
/// configuration and nothing ambient.
trait NixSshOptions {
    fn nix_ssh_options(&self) -> String;
}

impl NixSshOptions for SshConfigPath {
    fn nix_ssh_options(&self) -> String {
        format!(
            "-F {} -o BatchMode=yes -o IdentitiesOnly=yes -o IdentityAgent=none -o ProxyCommand=none -o ProxyJump=none -o ControlMaster=no -o ControlPath=none",
            self.0.shell_quoted()
        )
    }
}

impl<'executor, Executor: BootstrapExecutor> BootstrapCommanded<'executor, Executor>
    for BootOnceUnitQuery
{
    fn command(&self, translator: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        let query = format!(
            "PATH={NODE_PATH}; export PATH; systemctl show --property=LoadState --property=ActiveState --property=Result {} 2>/dev/null || printf 'LoadState=not-found\\n'",
            self.unit_name.0.shell_quoted()
        );
        match &self.boot_once_route {
            BootOnceRoute::Remote(route) => translator.over_ssh(route, query),
            BootOnceRoute::Local => BootstrapCommand {
                program: "/bin/sh".to_string(),
                arguments: vec!["-eu".to_string(), "-c".to_string(), query],
                environment: Vec::new(),
            },
        }
    }
}

impl<'executor, Executor: BootstrapExecutor> BootstrapCommanded<'executor, Executor>
    for BootOnceDispatch
{
    fn command(&self, translator: &BootstrapTranslator<'executor, Executor>) -> BootstrapCommand {
        let script = self.boot_once_script.script();
        let unit = &self.boot_once_script.unit_name.0;
        match &self.boot_once_route {
            BootOnceRoute::Remote(route) => translator.over_ssh(
                route,
                format!(
                    "exec {SYSTEMD_RUN} --unit={} --no-block --service-type=oneshot --property=RemainAfterExit=yes --setenv=PATH={NODE_PATH} /bin/sh -eu -c {}",
                    unit.shell_quoted(),
                    script.shell_quoted()
                ),
            ),
            BootOnceRoute::Local => BootstrapCommand {
                program: SYSTEMD_RUN.to_string(),
                arguments: vec![
                    format!("--unit={unit}"),
                    "--no-block".to_string(),
                    "--service-type=oneshot".to_string(),
                    "--property=RemainAfterExit=yes".to_string(),
                    format!("--setenv=PATH={NODE_PATH}"),
                    "/bin/sh".to_string(),
                    "-eu".to_string(),
                    "-c".to_string(),
                    script,
                ],
                environment: Vec::new(),
            },
        }
    }
}

/// The `/bin/sh` script a boot-once unit runs: set the profile, switch to the
/// closure for boot, then arm exactly one boot into the new entry while
/// leaving the old entry as the default to fall back to.
trait BootOnceScripting {
    fn script(&self) -> String;
}

impl BootOnceScripting for BootOnceScript {
    fn script(&self) -> String {
        let profile = self.system_profile_path.0.shell_quoted();
        let entries = self.boot_entries_directory.0.shell_quoted();
        let closure = self.closure_path.0.shell_quoted();
        let unit = self.unit_name.0.shell_quoted();
        format!(
            "set -eu\n\
             PATH={NODE_PATH}\n\
             export PATH\n\
             CLOSURE={closure}\n\
             PROFILE={profile}\n\
             ENTRIES={entries}\n\
             UNIT={unit}\n\
             OLD=$(bootctl status | awk -F': *' '/Current Entry:/ {{print $2}}')\n\
             [ -n \"$OLD\" ]\n\
             nix-env -p \"$PROFILE\" --set \"$CLOSURE\"\n\
             \"$CLOSURE/bin/switch-to-configuration\" boot\n\
             LOADER_CONF=$(dirname \"$ENTRIES\")/loader.conf\n\
             NEW=$(awk '$1 == \"default\" {{print $2; exit}}' \"$LOADER_CONF\")\n\
             [ -n \"$NEW\" ]\n\
             [ -f \"$ENTRIES/$NEW\" ]\n\
             [ \"$NEW\" != \"$OLD\" ]\n\
             bootctl set-default \"$OLD\"\n\
             bootctl set-oneshot \"$NEW\"\n\
             printf '%s\\n' \"$UNIT boot-once prepared\"\n"
        )
    }
}

/// One `systemctl show` reading. A unit that is loaded but neither finished
/// nor failed is still Waiting, so a resumed run keeps reading it rather than
/// dispatching it again.
trait UnitStateReading {
    fn unit_state(&self) -> BootOnceUnitState;
}

impl UnitStateReading for str {
    fn unit_state(&self) -> BootOnceUnitState {
        let says = |property: &str| self.lines().any(|line| line.trim() == property);
        if says("LoadState=not-found") {
            BootOnceUnitState::NotFound
        } else if says("ActiveState=failed") || says("Result=failed") {
            BootOnceUnitState::Failed
        } else if says("ActiveState=active") && says("Result=success") {
            BootOnceUnitState::Ready
        } else {
            BootOnceUnitState::Waiting
        }
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for BootstrapEvaluation
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> PrintedPathOutcome {
        translator.run(&self.command(translator)).printed_path()
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for BootstrapBuild
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> PrintedPathOutcome {
        translator.run(&self.command(translator)).printed_path()
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for BootstrapCheckBuild
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> CallOutcome {
        translator.run(&self.command(translator)).called()
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for GeneratedInputHashing
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> HashOutcome {
        match translator.run(&self.command(translator)) {
            Ok(printed) => {
                HashOutcome::Hashed(SriHash(printed.first_printed_line().unwrap_or_default()))
            }
            Err(failure) => HashOutcome::Failed(failure),
        }
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for BootstrapRooting
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> CallOutcome {
        translator.run(&self.command(translator)).called()
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for RemoteClosureQuery
{
    fn emit(
        &self,
        translator: &BootstrapTranslator<'executor, Executor>,
    ) -> ClosurePresenceOutcome {
        match translator.run(&self.command(translator)) {
            Ok(printed) if printed.trim() == "Present" => ClosurePresenceOutcome::Present,
            Ok(_) => ClosurePresenceOutcome::Absent,
            Err(failure) => ClosurePresenceOutcome::Failed(failure),
        }
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for BootstrapCopy
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> CallOutcome {
        translator.run(&self.command(translator)).called()
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for BootOnceUnitQuery
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> BootOnceUnitOutcome {
        match translator.run(&self.command(translator)) {
            Ok(printed) => BootOnceUnitOutcome::Read(printed.unit_state()),
            Err(failure) => BootOnceUnitOutcome::Failed(failure),
        }
    }
}

impl<'executor, Executor: BootstrapExecutor> Emittable<BootstrapTranslator<'executor, Executor>>
    for BootOnceDispatch
{
    fn emit(&self, translator: &BootstrapTranslator<'executor, Executor>) -> CallOutcome {
        translator.run(&self.command(translator)).called()
    }
}
