//! Read-only post-activation evidence collection for a user-environment deploy.
//!
//! This is intentionally an operator diagnostic rather than a daemon operation:
//! it joins the daemon's typed generation query to target-host observations and
//! refuses to call an unhealthy or unattributed deployment healthy.

use std::collections::BTreeMap;
use std::process::{Command, Output, Stdio};

use crate::{Error, Result};

const ROOT_OBSERVATION_SCRIPT: &str = r#"
set -eu
user="$1"
shift
field() { printf '%s\t%s\n' "$1" "$2"; }
field system_closure "$(readlink -f /run/current-system 2>/dev/null || true)"
field system_profile "$(readlink -f /nix/var/nix/profiles/system 2>/dev/null || true)"
field profile_closure "$(readlink -f /nix/var/nix/profiles/per-user/$user/profile 2>/dev/null || true)"
field home_manager_closure "$(readlink -f /home/$user/.local/state/nix/profiles/home-manager 2>/dev/null || true)"
field home_manager_result "$(systemctl show home-manager-$user.service -p Result --value 2>/dev/null || true)"
for unit in $(systemctl --machine="$user@" --user --failed --no-legend --plain 2>/dev/null | awk '{print $1}'); do
  field failed_unit "$unit"
done
for unit in "$@"; do
  field "unit:$unit" "$(systemctl --machine="$user@" --user is-active "$unit" 2>/dev/null || true)"
done
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerArguments {
    cluster: String,
    node: String,
    user: String,
    root_host: String,
    managed_units: Vec<String>,
}

impl LedgerArguments {
    pub fn from_environment() -> Result<Self> {
        Self::from_values(std::env::args().skip(1).collect())
    }

    pub fn from_values(values: Vec<String>) -> Result<Self> {
        if values.len() < 4 || values.iter().any(|value| value.starts_with('-')) {
            return Err(Error::ExpectedSingleArgument);
        }
        Ok(Self {
            cluster: values[0].clone(),
            node: values[1].clone(),
            user: values[2].clone(),
            root_host: values[3].clone(),
            managed_units: values[4..].to_vec(),
        })
    }
}

pub struct PostActivationLedger {
    arguments: LedgerArguments,
}

impl PostActivationLedger {
    pub fn new(arguments: LedgerArguments) -> Self {
        Self { arguments }
    }

    pub fn run(&self) -> Result<LedgerResult> {
        let query = self.lojix_query()?;
        let home_pin = HomeInputPin::from_query(&query).resolve();
        let remote = self.remote_observation()?;
        Ok(LedgerResult::new(
            self.arguments.clone(),
            query,
            home_pin,
            remote,
        ))
    }

    fn lojix_query(&self) -> Result<String> {
        let executable = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|parent| parent.join("lojix")))
            .unwrap_or_else(|| "lojix".into());
        let request = format!(
            "(Query (ByNode ({} {} None)))",
            self.arguments.cluster, self.arguments.node
        );
        self.output(Command::new(executable).arg(request))
    }

    fn remote_observation(&self) -> Result<RemoteObservation> {
        let mut command = Command::new("ssh");
        command
            .arg("-o")
            .arg("BatchMode=yes")
            .arg(format!("root@{}", self.arguments.root_host))
            .arg("/bin/sh")
            .arg("-s")
            .arg("--")
            .arg(&self.arguments.user)
            .args(&self.arguments.managed_units)
            .stdin(Stdio::piped());
        let mut child = command.spawn()?;
        use std::io::Write;
        child
            .stdin
            .take()
            .ok_or_else(|| Error::Io(std::io::Error::other("missing SSH stdin")))?
            .write_all(ROOT_OBSERVATION_SCRIPT.as_bytes())?;
        let output = child.wait_with_output()?;
        RemoteObservation::from_output(output)
    }

    fn output(&self, command: &mut Command) -> Result<String> {
        let output = command.output()?;
        if !output.status.success() {
            return Err(Error::Io(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteObservation {
    values: BTreeMap<String, Vec<String>>,
}

impl RemoteObservation {
    fn from_output(output: Output) -> Result<Self> {
        if !output.status.success() {
            return Err(Error::Io(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )));
        }
        Self::from_text(&String::from_utf8_lossy(&output.stdout))
    }

    pub fn from_text(text: &str) -> Result<Self> {
        let mut values = BTreeMap::<String, Vec<String>>::new();
        for line in text.lines().filter(|line| !line.is_empty()) {
            let Some((name, value)) = line.split_once('\t') else {
                return Err(Error::Io(std::io::Error::other(format!(
                    "ledger observation was not tab-delimited: {line}"
                ))));
            };
            values
                .entry(name.to_string())
                .or_default()
                .push(value.to_string());
        }
        Ok(Self { values })
    }

    fn value(&self, name: &str) -> Option<&str> {
        self.values
            .get(name)
            .and_then(|values| values.first())
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    fn values(&self, name: &str) -> impl Iterator<Item = &str> {
        self.values
            .get(name)
            .into_iter()
            .flatten()
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }
}

pub struct HomeInputPin {
    criomos_revision: Option<String>,
}

impl HomeInputPin {
    fn from_query(query: &str) -> Self {
        let marker = "github:LiGoldragon/CriomOS?rev=";
        let criomos_revision = query
            .split(marker)
            .nth(1)
            .map(|tail| {
                tail.chars()
                    .take_while(|character| character.is_ascii_hexdigit())
                    .collect::<String>()
            })
            .filter(|revision| revision.len() == 40);
        Self { criomos_revision }
    }

    fn resolve(&self) -> String {
        let Some(revision) = self.criomos_revision.as_ref() else {
            return "UNKNOWN (Lojix query has no immutable CriomOS revision)".to_string();
        };
        let reference = format!("github:LiGoldragon/CriomOS?rev={revision}");
        let output = Command::new("nix")
            .args([
                "flake",
                "metadata",
                "--json",
                "--no-write-lock-file",
                &reference,
            ])
            .output();
        let Ok(output) = output else {
            return "UNKNOWN (nix flake metadata was unavailable)".to_string();
        };
        if !output.status.success() {
            return "UNKNOWN (nix could not read the immutable source lock)".to_string();
        }
        serde_json::from_slice::<serde_json::Value>(&output.stdout)
            .ok()
            .and_then(|metadata| {
                metadata
                    .pointer("/locks/nodes/criomos-home/locked/rev")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "UNKNOWN (CriomOS source has no criomos-home lock node)".to_string())
    }
}

pub struct LedgerResult {
    arguments: LedgerArguments,
    lojix_query: String,
    home_pin: String,
    remote: RemoteObservation,
}

impl LedgerResult {
    pub fn new(
        arguments: LedgerArguments,
        lojix_query: String,
        home_pin: String,
        remote: RemoteObservation,
    ) -> Self {
        Self {
            arguments,
            lojix_query,
            home_pin,
            remote,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.problems().is_empty()
    }

    pub fn problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        let system = self.remote.value("system_closure");
        if system.is_none() || !system.is_some_and(|closure| self.lojix_query.contains(closure)) {
            problems.push(
                "active system closure is not present in Lojix current-generation evidence"
                    .to_string(),
            );
        }
        if self.remote.value("home_manager_result") != Some("success") {
            problems
                .push("Home Manager activation has no successful system-unit result".to_string());
        }
        for unit in self.remote.values("failed_unit") {
            problems.push(format!("target user has failed unit {unit}"));
        }
        for unit in &self.arguments.managed_units {
            if self.remote.value(&format!("unit:{unit}")) != Some("active") {
                problems.push(format!("managed activation service {unit} is not active"));
            }
        }
        // Lojix's current generation record deliberately has no user field.
        // A matching closure is evidence of a node generation, not user identity.
        problems.push("home profile cannot be attributed directly to the requested user from the current Lojix contract".to_string());
        problems
    }

    pub fn render(&self) -> String {
        let mut output = String::new();
        output.push_str("POST_ACTIVATION_LEDGER\nExpected:\n");
        output.push_str(&format!(
            "  target = {}/{}/{}\n",
            self.arguments.cluster, self.arguments.node, self.arguments.user
        ));
        output.push_str(&format!(
            "  lojix_current_generations = {}\n",
            self.lojix_query
        ));
        output.push_str(&format!("  criomos_home_pin = {}\n", self.home_pin));
        output.push_str("Observed:\n");
        for name in [
            "system_closure",
            "system_profile",
            "profile_closure",
            "home_manager_closure",
            "home_manager_result",
        ] {
            output.push_str(&format!(
                "  {name} = {}\n",
                self.remote.value(name).unwrap_or("MISSING")
            ));
        }
        for unit in self.remote.values("failed_unit") {
            output.push_str(&format!("  failed_unit = {unit}\n"));
        }
        for unit in &self.arguments.managed_units {
            output.push_str(&format!(
                "  unit:{unit} = {}\n",
                self.remote
                    .value(&format!("unit:{unit}"))
                    .unwrap_or("MISSING")
            ));
        }
        output.push_str(
            "Unknown:\n  profile_to_lojix_user_link = no user field exists on LiveGeneration\n",
        );
        output.push_str("Health:\n");
        for problem in self.problems() {
            output.push_str(&format!("  unhealthy = {problem}\n"));
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::{LedgerArguments, LedgerResult, RemoteObservation};

    #[test]
    fn failed_units_and_missing_attribution_make_the_ledger_unhealthy() {
        let arguments = LedgerArguments::from_values(vec![
            "cluster".into(),
            "node".into(),
            "bird".into(),
            "node.example".into(),
            "spirit-daemon.service".into(),
        ])
        .expect("arguments");
        let remote = RemoteObservation::from_text("system_closure\t/nix/store/system\nhome_manager_result\tsuccess\nfailed_unit\tspirit-daemon.service\nunit:spirit-daemon.service\tfailed\n").expect("observation");
        let ledger = LedgerResult::new(
            arguments,
            "(Current /nix/store/system)".into(),
            "UNKNOWN".into(),
            remote,
        );
        assert!(!ledger.is_healthy());
        assert!(
            ledger
                .render()
                .contains("failed_unit = spirit-daemon.service")
        );
        assert!(
            ledger
                .problems()
                .iter()
                .any(|problem| problem.contains("cannot be attributed"))
        );
    }

    #[test]
    fn malformed_remote_observation_is_rejected() {
        assert!(RemoteObservation::from_text("not a ledger field\n").is_err());
    }
}
