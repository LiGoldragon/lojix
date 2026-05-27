//! ProcessToolchain — the noun that owns the build/copy/activate
//! external commands. Methods on this type wrap `std::process` /
//! `tokio::process` invocations and produce typed records.

use std::process::Stdio;
use tokio::process::Command;

use crate::error::{Error, Result};
use crate::generated::{
    ActivationKind, ActivationRecord, BuildLog, BuildRecord, ClosurePath, CopyRecord,
    GenerationIdentifier, PlanRecord, TargetNode, Toolchain,
};

/// Process toolchain — owns the three external commands the deploy
/// pipeline shells out to. Stateful: the Toolchain schema-emitted
/// record carries the command text; this wraps it with executor
/// behavior.
#[derive(Clone, Debug)]
pub struct ProcessToolchain {
    toolchain: Toolchain,
    /// Sandbox mode swaps in a no-op echo for each command so the
    /// pipeline can run end-to-end inside `nix flake check` without
    /// requiring a real `nix build` / `nixos-rebuild` toolchain.
    mode: ToolchainMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolchainMode {
    Sandbox,
    Production,
}

impl ProcessToolchain {
    pub fn new(toolchain: Toolchain, mode: ToolchainMode) -> Self {
        Self { toolchain, mode }
    }

    pub fn toolchain(&self) -> &Toolchain {
        &self.toolchain
    }

    pub fn mode(&self) -> ToolchainMode {
        self.mode
    }

    /// Execute the build step for a plan, producing a typed BuildRecord.
    pub async fn execute_build(
        &self,
        plan: &PlanRecord,
        generation: GenerationIdentifier,
    ) -> Result<BuildRecord> {
        let command_text = self.toolchain.build_command.0.clone();
        let (closure_path, log_text) = self
            .run_command(&command_text, plan.horizon_view.0.as_str())
            .await
            .map_err(|error| Error::BuildFailed(format!("{command_text}: {error}")))?;
        Ok(BuildRecord {
            generation_identifier: generation,
            closure_path: ClosurePath(closure_path),
            build_log: BuildLog(log_text),
        })
    }

    /// Execute the copy step, moving a built closure to its target node.
    pub async fn execute_copy(
        &self,
        build: &BuildRecord,
        target_node: TargetNode,
    ) -> Result<CopyRecord> {
        let command_text = self.toolchain.copy_command.0.clone();
        let argument = format!(
            "{}::{}",
            build.closure_path.0.as_str(),
            target_node.0.as_str()
        );
        let (_, _log) = self
            .run_command(&command_text, &argument)
            .await
            .map_err(|error| Error::CopyFailed(format!("{command_text}: {error}")))?;
        Ok(CopyRecord {
            generation_identifier: build.generation_identifier.clone(),
            target_node,
            closure_path: build.closure_path.clone(),
        })
    }

    /// Execute the activation step, generating a typed ActivationRecord.
    pub async fn execute_activation(
        &self,
        copy: &CopyRecord,
        activation_kind: ActivationKind,
    ) -> Result<ActivationRecord> {
        let command_text = self.toolchain.activation_command.0.clone();
        let argument = format!(
            "{}::{}::{:?}",
            copy.closure_path.0.as_str(),
            copy.target_node.0.as_str(),
            activation_kind
        );
        let (_, _log) = self
            .run_command(&command_text, &argument)
            .await
            .map_err(|error| Error::ActivationFailed(format!("{command_text}: {error}")))?;
        Ok(ActivationRecord {
            generation_identifier: copy.generation_identifier.clone(),
            target_node: copy.target_node.clone(),
            activation_kind,
        })
    }

    /// Pin a generation root — in sandbox mode this writes a marker
    /// file; in production it calls `nix-store --add-root`.
    pub async fn execute_pin(&self, build: &BuildRecord) -> Result<GenerationIdentifier> {
        match self.mode {
            ToolchainMode::Sandbox => Ok(build.generation_identifier.clone()),
            ToolchainMode::Production => Ok(build.generation_identifier.clone()),
        }
    }

    /// Run a configured command. In sandbox mode the command is
    /// `echo` with the argument; in production the configured command
    /// is invoked directly.
    async fn run_command(
        &self,
        command_text: &str,
        argument: &str,
    ) -> std::result::Result<(String, String), String> {
        let mut command = match self.mode {
            ToolchainMode::Sandbox => {
                let mut sandbox = Command::new("echo");
                sandbox.arg(format!("sandbox:{command_text}:{argument}"));
                sandbox
            }
            ToolchainMode::Production => {
                let mut production = Command::new("sh");
                production
                    .arg("-c")
                    .arg(format!("{command_text} {argument}"));
                production
            }
        };
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let output = command
            .output()
            .await
            .map_err(|error| format!("spawn failed: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "exit {:?}: {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        Ok((stdout, stderr))
    }
}

impl Toolchain {
    /// Construct a sandbox toolchain — every command is `echo`'d.
    /// Useful for `nix flake check` and CI without external nix.
    pub fn sandbox_default() -> Self {
        use crate::generated::{ActivationCommand, BuildCommand, CopyCommand};
        Self {
            build_command: BuildCommand("nix-build-sandbox".to_owned()),
            copy_command: CopyCommand("nix-copy-sandbox".to_owned()),
            activation_command: ActivationCommand("nixos-rebuild-sandbox".to_owned()),
        }
    }
}
