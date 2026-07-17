//! Read-only post-activation evidence collection for a user-environment deploy.
//!
//! This operator diagnostic joins the durable Lojix generation query to target
//! observations. It deliberately distinguishes a source pin from an evaluated
//! closure and an evaluated closure from the active closure: none is allowed to
//! stand in for another.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
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
        let attribution = EvaluatedClosureAttribution::from_query(&query).resolve();
        let remote = self.remote_observation()?;
        Ok(LedgerResult::new(
            self.arguments.clone(),
            query,
            attribution,
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
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

/// Immutable source identities selected by the evaluated top-level CriomOS
/// flake. `criomos_home_revision` is resolved from that top-level lock, never
/// from a separately supplied home pin that the system evaluation may override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluatedClosureAttribution {
    criomos_revision: Option<String>,
    criomos_home_revision: Option<String>,
    resolution_failure: Option<String>,
}

impl EvaluatedClosureAttribution {
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
        Self {
            criomos_revision,
            criomos_home_revision: None,
            resolution_failure: None,
        }
    }

    fn resolve(mut self) -> Self {
        let Some(revision) = self.criomos_revision.as_ref() else {
            self.resolution_failure =
                Some("Lojix generation has no immutable top-level CriomOS revision".to_string());
            return self;
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
            self.resolution_failure = Some("nix flake metadata was unavailable".to_string());
            return self;
        };
        if !output.status.success() {
            self.resolution_failure =
                Some("nix could not read the immutable top-level source lock".to_string());
            return self;
        }
        self.criomos_home_revision = serde_json::from_slice::<serde_json::Value>(&output.stdout)
            .ok()
            .and_then(|metadata| {
                metadata
                    .pointer("/locks/nodes/criomos-home/locked/rev")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
        if self.criomos_home_revision.is_none() {
            self.resolution_failure =
                Some("evaluated CriomOS source has no criomos-home lock node".to_string());
        }
        self
    }

    #[cfg(test)]
    fn fixture(criomos_revision: &str, criomos_home_revision: &str) -> Self {
        Self {
            criomos_revision: Some(criomos_revision.to_string()),
            criomos_home_revision: Some(criomos_home_revision.to_string()),
            resolution_failure: None,
        }
    }

    fn display_value(value: Option<&str>) -> &str {
        value.unwrap_or("UNKNOWN")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthFailure {
    SourceClosureAttribution(String),
    EvaluatedSystemClosureAttribution,
    EvaluatedHomeClosureAttribution,
    Activation(String),
    UnexpectedFailedUserUnit(String),
    ManagedUserUnitHealth(String),
}

impl Display for HealthFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceClosureAttribution(reason) => {
                write!(formatter, "source closure attribution failed: {reason}")
            }
            Self::EvaluatedSystemClosureAttribution => formatter.write_str(
                "active system closure is not the exact evaluated Lojix system closure",
            ),
            Self::EvaluatedHomeClosureAttribution => formatter.write_str(
                "active home closure is not the exact evaluated Lojix home closure; a source pin alone is not active-closure evidence",
            ),
            Self::Activation(reason) => write!(formatter, "activation failed: {reason}"),
            Self::UnexpectedFailedUserUnit(unit) => {
                write!(formatter, "Bird user-unit health failed: unexpected failed unit {unit}")
            }
            Self::ManagedUserUnitHealth(unit) => {
                write!(formatter, "Bird user-unit health failed: managed unit {unit} is not active")
            }
        }
    }
}

pub struct LedgerResult {
    arguments: LedgerArguments,
    lojix_query: String,
    attribution: EvaluatedClosureAttribution,
    remote: RemoteObservation,
}

impl LedgerResult {
    pub fn new(
        arguments: LedgerArguments,
        lojix_query: String,
        attribution: EvaluatedClosureAttribution,
        remote: RemoteObservation,
    ) -> Self {
        Self {
            arguments,
            lojix_query,
            attribution,
            remote,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.failures().is_empty()
    }

    pub fn failures(&self) -> Vec<HealthFailure> {
        let mut failures = Vec::new();
        if let Some(reason) = &self.attribution.resolution_failure {
            failures.push(HealthFailure::SourceClosureAttribution(reason.clone()));
        }
        let system = self.remote.value("system_closure");
        if system.is_none() || !system.is_some_and(|closure| self.lojix_query.contains(closure)) {
            failures.push(HealthFailure::EvaluatedSystemClosureAttribution);
        }
        let home = self.remote.value("home_manager_closure");
        if home.is_none() || !home.is_some_and(|closure| self.lojix_query.contains(closure)) {
            failures.push(HealthFailure::EvaluatedHomeClosureAttribution);
        }
        if self.remote.value("home_manager_result") != Some("success") {
            failures.push(HealthFailure::Activation(
                "Home Manager system unit did not report success".to_string(),
            ));
        }
        for unit in self.remote.values("failed_unit") {
            failures.push(HealthFailure::UnexpectedFailedUserUnit(unit.to_string()));
        }
        for unit in &self.arguments.managed_units {
            if self.remote.value(&format!("unit:{unit}")) != Some("active") {
                failures.push(HealthFailure::ManagedUserUnitHealth(unit.clone()));
            }
        }
        failures
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
        output.push_str(&format!(
            "  evaluated_top_level_criomos_revision = {}\n",
            EvaluatedClosureAttribution::display_value(
                self.attribution.criomos_revision.as_deref()
            )
        ));
        output.push_str(&format!(
            "  evaluated_top_level_criomos_home_revision = {}\n",
            EvaluatedClosureAttribution::display_value(
                self.attribution.criomos_home_revision.as_deref()
            )
        ));
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
        output.push_str("Health:\n");
        for failure in self.failures() {
            output.push_str(&format!("  unhealthy = {failure}\n"));
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EvaluatedClosureAttribution, HealthFailure, LedgerArguments, LedgerResult,
        RemoteObservation,
    };

    fn arguments() -> LedgerArguments {
        LedgerArguments::from_values(vec![
            "cluster".into(),
            "node".into(),
            "bird".into(),
            "node.example".into(),
            "spirit-daemon.service".into(),
        ])
        .expect("arguments")
    }

    fn attribution() -> EvaluatedClosureAttribution {
        EvaluatedClosureAttribution::fixture(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
    }

    #[test]
    fn exact_active_closures_and_zero_failed_bird_units_are_healthy() {
        let query = "(Current /nix/store/system /nix/store/home github:LiGoldragon/CriomOS?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)";
        let remote = RemoteObservation::from_text("system_closure\t/nix/store/system\nhome_manager_closure\t/nix/store/home\nhome_manager_result\tsuccess\nunit:spirit-daemon.service\tactive\n").expect("observation");
        let ledger = LedgerResult::new(arguments(), query.into(), attribution(), remote);
        assert!(ledger.is_healthy(), "{:?}", ledger.failures());
        assert!(ledger.render().contains(
            "evaluated_top_level_criomos_home_revision = bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
    }

    #[test]
    fn a_home_pin_without_the_exact_active_home_closure_is_not_health_evidence() {
        let query = "(Current /nix/store/system github:LiGoldragon/CriomOS?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa criomos-home-rev=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)";
        let remote = RemoteObservation::from_text("system_closure\t/nix/store/system\nhome_manager_closure\t/nix/store/different-home\nhome_manager_result\tsuccess\nunit:spirit-daemon.service\tactive\n").expect("observation");
        let ledger = LedgerResult::new(arguments(), query.into(), attribution(), remote);
        assert!(
            ledger
                .failures()
                .contains(&HealthFailure::EvaluatedHomeClosureAttribution)
        );
    }

    #[test]
    fn every_unexpected_failed_bird_unit_makes_health_fail() {
        let query = "(Current /nix/store/system /nix/store/home github:LiGoldragon/CriomOS?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)";
        let remote = RemoteObservation::from_text("system_closure\t/nix/store/system\nhome_manager_closure\t/nix/store/home\nhome_manager_result\tsuccess\nfailed_unit\tunexpected-one.service\nfailed_unit\tunexpected-two.service\nunit:spirit-daemon.service\tactive\n").expect("observation");
        let ledger = LedgerResult::new(arguments(), query.into(), attribution(), remote);
        let failures = ledger.failures();
        assert!(failures.contains(&HealthFailure::UnexpectedFailedUserUnit(
            "unexpected-one.service".to_string()
        )));
        assert!(failures.contains(&HealthFailure::UnexpectedFailedUserUnit(
            "unexpected-two.service".to_string()
        )));
    }

    #[test]
    fn malformed_remote_observation_is_rejected() {
        assert!(RemoteObservation::from_text("not a ledger field\n").is_err());
    }
}
