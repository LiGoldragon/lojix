use std::process::Stdio;

use tokio::process::Command;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessInvocation {
    program: String,
    arguments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessToolchain {
    nix: String,
    ssh: String,
    rsync: String,
}

impl ProcessToolchain {
    pub fn production() -> Self {
        Self {
            nix: "nix".to_string(),
            ssh: "ssh".to_string(),
            rsync: "rsync".to_string(),
        }
    }

    pub fn new(nix: impl Into<String>, ssh: impl Into<String>, rsync: impl Into<String>) -> Self {
        Self {
            nix: nix.into(),
            ssh: ssh.into(),
            rsync: rsync.into(),
        }
    }

    pub fn nix(&self) -> &str {
        &self.nix
    }

    pub fn ssh(&self) -> &str {
        &self.ssh
    }

    pub fn rsync(&self) -> &str {
        &self.rsync
    }
}

impl ProcessInvocation {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            arguments: Vec::new(),
        }
    }

    pub fn with_argument(mut self, argument: impl Into<String>) -> Self {
        self.arguments.push(argument.into());
        self
    }

    pub fn with_arguments<I, S>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.arguments.extend(arguments.into_iter().map(Into::into));
        self
    }

    pub fn program(&self) -> &str {
        &self.program
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn to_shell_command(&self) -> ShellCommand {
        ShellCommand::from_invocation(self)
    }

    pub async fn capture_stdout(&self) -> Result<ProcessOutput> {
        let mut command = Command::new(&self.program);
        command
            .args(&self.arguments)
            .stdin(Stdio::null())
            .kill_on_drop(true);
        let output = command.output().await?;

        if !output.status.success() {
            return Err(Error::ProcessFailed {
                program: self.program.clone(),
                status: output.status.code().unwrap_or(-1),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
        Ok(ProcessOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        })
    }

    pub async fn expect_success(&self) -> Result<()> {
        self.capture_stdout().await.map(|_| ())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessOutput {
    stdout: String,
}

impl ProcessOutput {
    pub fn stdout(&self) -> &str {
        &self.stdout
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellCommand(String);

impl ShellCommand {
    pub fn from_invocation(invocation: &ProcessInvocation) -> Self {
        let mut command = ShellArgument::new(invocation.program()).to_command_text();
        for argument in invocation.arguments() {
            command.push(' ');
            command.push_str(&ShellArgument::new(argument).to_command_text());
        }
        Self(command)
    }

    pub fn from_raw(script: impl Into<String>) -> Self {
        Self(script.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub struct ShellArgument<'argument> {
    text: &'argument str,
}

impl<'argument> ShellArgument<'argument> {
    pub fn new(text: &'argument str) -> Self {
        Self { text }
    }

    pub fn to_command_text(&self) -> String {
        let text = self.text;
        let safe = !text.is_empty()
            && text.bytes().all(|byte| {
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
                        | b'@'
                )
            });
        if safe {
            return text.to_string();
        }
        format!("'{}'", text.replace('\'', "'\\''"))
    }
}
