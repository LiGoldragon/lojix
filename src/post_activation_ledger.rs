//! Read-only post-activation evidence collection for a user-environment deploy.
//!
//! This operator diagnostic joins the typed durable Lojix generation query to
//! target observations. It deliberately distinguishes an immutable source, an
//! evaluated closure, and an active closure: none is allowed to stand in for
//! another. A user-environment generation carries its user identity, so the
//! ledger cannot accidentally attribute Bird's home closure to a peer user on
//! the same node.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::process::{Command, Output, Stdio};

use signal_lojix::schema::lib as ordinary;

use crate::{Error, Result, client::OrdinaryClient};

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
        let generations = self.lojix_generations()?;
        let attribution =
            CurrentClosureAttribution::from_generations(&self.arguments, &generations).resolve();
        let remote = self.remote_observation()?;
        Ok(LedgerResult::new(
            self.arguments.clone(),
            generations,
            attribution,
            remote,
        ))
    }

    fn lojix_generations(&self) -> Result<ordinary::GenerationListing> {
        let query = ordinary::Input::Query(ordinary::QueryPayload::new(
            ordinary::Selection::ByNode(ordinary::NodeSelector {
                cluster_name: ordinary::ClusterName::new(self.arguments.cluster.clone()),
                node_name: ordinary::NodeName::new(self.arguments.node.clone()),
                optional_generation_artifact: None,
            }),
        ));
        match OrdinaryClient::from_input(query).run()? {
            ordinary::Output::Queried(payload) => Ok(payload.into_payload()),
            ordinary::Output::QueryRejected(payload) => {
                Err(Error::LedgerQueryRejected(format!("{payload:?}")))
            }
            output => Err(Error::LedgerQueryUnexpectedOutput(format!("{output:?}"))),
        }
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

/// Immutable source identities selected by the active top-level CriomOS
/// generation. `criomos_home_revision` is resolved from that generation's
/// locked CriomOS source, never from a separately supplied home pin that the
/// system evaluation may override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluatedClosureAttribution {
    criomos_reference: Option<String>,
    criomos_revision: Option<String>,
    criomos_home_revision: Option<String>,
    resolution_failure: Option<String>,
}

impl EvaluatedClosureAttribution {
    fn from_system_generation(generation: Option<&ordinary::Generation>) -> Self {
        let Some(generation) = generation else {
            return Self {
                criomos_reference: None,
                criomos_revision: None,
                criomos_home_revision: None,
                resolution_failure: Some(
                    "Lojix has no unique current complete-host generation".to_string(),
                ),
            };
        };
        let Some(source) = generation.optional_source_revision_record.as_ref() else {
            return Self {
                criomos_reference: None,
                criomos_revision: None,
                criomos_home_revision: None,
                resolution_failure: Some(
                    "current complete-host generation has no resolved source record".to_string(),
                ),
            };
        };
        let reference = source.resolved_ref.payload().clone();
        if !reference.starts_with("github:LiGoldragon/CriomOS?") || source.string.is_empty() {
            return Self {
                criomos_reference: None,
                criomos_revision: None,
                criomos_home_revision: None,
                resolution_failure: Some(
                    "current complete-host generation does not name an immutable top-level CriomOS source"
                        .to_string(),
                ),
            };
        }
        Self {
            criomos_reference: Some(reference),
            criomos_revision: Some(source.string.clone()),
            criomos_home_revision: None,
            resolution_failure: None,
        }
    }

    fn resolve(mut self) -> Self {
        let Some(reference) = self.criomos_reference.as_ref() else {
            return self;
        };
        let output = Command::new("nix")
            .args([
                "flake",
                "metadata",
                "--json",
                "--no-write-lock-file",
                reference,
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
            criomos_reference: Some(format!("github:LiGoldragon/CriomOS?rev={criomos_revision}")),
            criomos_revision: Some(criomos_revision.to_string()),
            criomos_home_revision: Some(criomos_home_revision.to_string()),
            resolution_failure: None,
        }
    }

    fn display_value(value: Option<&str>) -> &str {
        value.unwrap_or("UNKNOWN")
    }
}

/// The exact current Lojix generations that the remote system and home
/// pointers must equal. The selected home generation is keyed by its target
/// user, not merely by the shared node, so separate users cannot satisfy each
/// other's health evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentClosureAttribution {
    system_generation: Option<ordinary::Generation>,
    home_generation: Option<ordinary::Generation>,
    system_selection_failure: Option<String>,
    home_selection_failure: Option<String>,
    home_source_failure: Option<String>,
    evaluated_source: EvaluatedClosureAttribution,
}

impl CurrentClosureAttribution {
    fn from_generations(
        arguments: &LedgerArguments,
        listing: &ordinary::GenerationListing,
    ) -> Self {
        let (system_generation, system_selection_failure) = Self::select_generation(
            &listing.generation_vector,
            ordinary::GenerationArtifact::CompleteHost,
            None,
            "complete-host system",
        );
        let (home_generation, home_selection_failure) = Self::select_generation(
            &listing.generation_vector,
            ordinary::GenerationArtifact::UserEnvironment,
            Some(arguments.user.as_str()),
            "user-environment home",
        );
        let evaluated_source =
            EvaluatedClosureAttribution::from_system_generation(system_generation.as_ref());
        let home_source_failure =
            Self::home_source_failure(system_generation.as_ref(), home_generation.as_ref());
        Self {
            system_generation,
            home_generation,
            system_selection_failure,
            home_selection_failure,
            home_source_failure,
            evaluated_source,
        }
    }

    fn select_generation(
        generations: &[ordinary::Generation],
        artifact: ordinary::GenerationArtifact,
        user: Option<&str>,
        label: &str,
    ) -> (Option<ordinary::Generation>, Option<String>) {
        let selected = generations
            .iter()
            .filter(|generation| {
                generation.generation_slot == ordinary::GenerationSlot::Current
                    && generation.generation_artifact == artifact
                    && generation
                        .optional_user_name
                        .as_ref()
                        .map(|name| name.payload().as_str())
                        == user
            })
            .cloned()
            .collect::<Vec<_>>();
        match selected.as_slice() {
            [generation] => (Some(generation.clone()), None),
            [] => (
                None,
                Some(format!(
                    "no current {label} generation was returned by Lojix"
                )),
            ),
            _ => (
                None,
                Some(format!(
                    "multiple current {label} generations were returned by Lojix"
                )),
            ),
        }
    }

    fn home_source_failure(
        system: Option<&ordinary::Generation>,
        home: Option<&ordinary::Generation>,
    ) -> Option<String> {
        let system = system?;
        let home = home?;
        let system_source = system.optional_source_revision_record.as_ref()?;
        let Some(home_source) = home.optional_source_revision_record.as_ref() else {
            return Some("current Bird home generation has no resolved source record".to_string());
        };
        if system_source.resolved_ref != home_source.resolved_ref
            || system_source.string != home_source.string
        {
            return Some(
                "current Bird home generation was not evaluated from the exact active CriomOS source"
                    .to_string(),
            );
        }
        None
    }

    fn resolve(mut self) -> Self {
        self.evaluated_source = self.evaluated_source.clone().resolve();
        self
    }

    fn system_closure(&self) -> Option<&str> {
        self.system_generation
            .as_ref()
            .map(|generation| generation.closure_path.payload().as_str())
    }

    fn home_closure(&self) -> Option<&str> {
        self.home_generation
            .as_ref()
            .map(|generation| generation.closure_path.payload().as_str())
    }

    #[cfg(test)]
    fn fixture(
        system: &str,
        home: &str,
        criomos_revision: &str,
        criomos_home_revision: &str,
    ) -> Self {
        Self {
            system_generation: Some(Self::fixture_generation(
                ordinary::GenerationArtifact::CompleteHost,
                None,
                system,
                criomos_revision,
            )),
            home_generation: Some(Self::fixture_generation(
                ordinary::GenerationArtifact::UserEnvironment,
                Some("bird"),
                home,
                criomos_revision,
            )),
            system_selection_failure: None,
            home_selection_failure: None,
            home_source_failure: None,
            evaluated_source: EvaluatedClosureAttribution::fixture(
                criomos_revision,
                criomos_home_revision,
            ),
        }
    }

    #[cfg(test)]
    fn fixture_generation(
        artifact: ordinary::GenerationArtifact,
        user: Option<&str>,
        closure: &str,
        criomos_revision: &str,
    ) -> ordinary::Generation {
        ordinary::Generation {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            deployment_identifier: ordinary::DeploymentIdentifier::new(1),
            cluster_name: ordinary::ClusterName::new("cluster"),
            node_name: ordinary::NodeName::new("node"),
            generation_artifact: artifact,
            optional_user_name: user.map(ordinary::UserName::new),
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: ordinary::ClosurePath::new(closure),
            optional_source_revision_record: Some(ordinary::SourceRevisionRecord {
                source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
                requested_ref: ordinary::FlakeReference::new(format!(
                    "github:LiGoldragon/CriomOS?rev={criomos_revision}"
                )),
                resolved_ref: ordinary::FlakeReference::new(format!(
                    "github:LiGoldragon/CriomOS?rev={criomos_revision}"
                )),
                string: criomos_revision.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthFailure {
    SourceClosureAttribution(String),
    CurrentSystemGenerationAttribution(String),
    CurrentHomeGenerationAttribution(String),
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
            Self::CurrentSystemGenerationAttribution(reason) => {
                write!(formatter, "current system generation attribution failed: {reason}")
            }
            Self::CurrentHomeGenerationAttribution(reason) => {
                write!(formatter, "current home generation attribution failed: {reason}")
            }
            Self::EvaluatedSystemClosureAttribution => formatter.write_str(
                "active system closure is not the exact current Lojix complete-host closure",
            ),
            Self::EvaluatedHomeClosureAttribution => formatter.write_str(
                "active home closure is not the exact current Lojix user-environment closure for Bird",
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
    generations: ordinary::GenerationListing,
    attribution: CurrentClosureAttribution,
    remote: RemoteObservation,
}

impl LedgerResult {
    pub fn new(
        arguments: LedgerArguments,
        generations: ordinary::GenerationListing,
        attribution: CurrentClosureAttribution,
        remote: RemoteObservation,
    ) -> Self {
        Self {
            arguments,
            generations,
            attribution,
            remote,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.failures().is_empty()
    }

    pub fn failures(&self) -> Vec<HealthFailure> {
        let mut failures = Vec::new();
        if let Some(reason) = &self.attribution.system_selection_failure {
            failures.push(HealthFailure::CurrentSystemGenerationAttribution(
                reason.clone(),
            ));
        }
        if let Some(reason) = &self.attribution.home_selection_failure {
            failures.push(HealthFailure::CurrentHomeGenerationAttribution(
                reason.clone(),
            ));
        }
        if let Some(reason) = &self.attribution.home_source_failure {
            failures.push(HealthFailure::SourceClosureAttribution(reason.clone()));
        }
        if let Some(reason) = &self.attribution.evaluated_source.resolution_failure {
            failures.push(HealthFailure::SourceClosureAttribution(reason.clone()));
        }
        if self
            .attribution
            .system_closure()
            .is_none_or(|closure| self.remote.value("system_closure") != Some(closure))
        {
            failures.push(HealthFailure::EvaluatedSystemClosureAttribution);
        }
        if self
            .attribution
            .home_closure()
            .is_none_or(|closure| self.remote.value("home_manager_closure") != Some(closure))
        {
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
            "  lojix_generation_count = {}\n",
            self.generations.generation_vector.len()
        ));
        output.push_str(&format!(
            "  lojix_current_system_closure = {}\n",
            self.attribution.system_closure().unwrap_or("MISSING")
        ));
        output.push_str(&format!(
            "  lojix_current_bird_home_closure = {}\n",
            self.attribution.home_closure().unwrap_or("MISSING")
        ));
        output.push_str(&format!(
            "  evaluated_top_level_criomos_revision = {}\n",
            EvaluatedClosureAttribution::display_value(
                self.attribution
                    .evaluated_source
                    .criomos_revision
                    .as_deref()
            )
        ));
        output.push_str(&format!(
            "  evaluated_top_level_criomos_home_revision = {}\n",
            EvaluatedClosureAttribution::display_value(
                self.attribution
                    .evaluated_source
                    .criomos_home_revision
                    .as_deref()
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
        CurrentClosureAttribution, HealthFailure, LedgerArguments, LedgerResult, RemoteObservation,
    };
    use signal_lojix::schema::lib as ordinary;

    const CRIOMOS_REVISION: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CRIOMOS_HOME_REVISION: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

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

    fn generations() -> ordinary::GenerationListing {
        ordinary::GenerationListing {
            generation_vector: vec![
                CurrentClosureAttribution::fixture_generation(
                    ordinary::GenerationArtifact::CompleteHost,
                    None,
                    "/nix/store/system",
                    CRIOMOS_REVISION,
                ),
                CurrentClosureAttribution::fixture_generation(
                    ordinary::GenerationArtifact::UserEnvironment,
                    Some("bird"),
                    "/nix/store/home",
                    CRIOMOS_REVISION,
                ),
                CurrentClosureAttribution::fixture_generation(
                    ordinary::GenerationArtifact::UserEnvironment,
                    Some("another-user"),
                    "/nix/store/another-home",
                    CRIOMOS_REVISION,
                ),
            ],
            database_marker: ordinary::DatabaseMarker {
                commit_sequence: ordinary::CommitSequence::new(1),
                state_digest: ordinary::StateDigest::new(1),
            },
        }
    }

    fn attribution() -> CurrentClosureAttribution {
        CurrentClosureAttribution::fixture(
            "/nix/store/system",
            "/nix/store/home",
            CRIOMOS_REVISION,
            CRIOMOS_HOME_REVISION,
        )
    }

    #[test]
    fn exact_active_closures_and_zero_failed_bird_units_are_healthy() {
        let remote = RemoteObservation::from_text("system_closure\t/nix/store/system\nhome_manager_closure\t/nix/store/home\nhome_manager_result\tsuccess\nunit:spirit-daemon.service\tactive\n").expect("observation");
        let ledger = LedgerResult::new(arguments(), generations(), attribution(), remote);
        assert!(ledger.is_healthy(), "{:?}", ledger.failures());
        assert!(ledger.render().contains(
            "evaluated_top_level_criomos_home_revision = bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
    }

    #[test]
    fn a_home_pin_without_the_exact_active_home_closure_is_not_health_evidence() {
        let remote = RemoteObservation::from_text("system_closure\t/nix/store/system\nhome_manager_closure\t/nix/store/different-home\nhome_manager_result\tsuccess\nunit:spirit-daemon.service\tactive\n").expect("observation");
        let ledger = LedgerResult::new(arguments(), generations(), attribution(), remote);
        assert!(
            ledger
                .failures()
                .contains(&HealthFailure::EvaluatedHomeClosureAttribution)
        );
    }

    #[test]
    fn every_unexpected_failed_bird_unit_makes_health_fail() {
        let remote = RemoteObservation::from_text("system_closure\t/nix/store/system\nhome_manager_closure\t/nix/store/home\nhome_manager_result\tsuccess\nfailed_unit\tunexpected-one.service\nfailed_unit\tunexpected-two.service\nunit:spirit-daemon.service\tactive\n").expect("observation");
        let ledger = LedgerResult::new(arguments(), generations(), attribution(), remote);
        let failures = ledger.failures();
        assert!(failures.contains(&HealthFailure::UnexpectedFailedUserUnit(
            "unexpected-one.service".to_string()
        )));
        assert!(failures.contains(&HealthFailure::UnexpectedFailedUserUnit(
            "unexpected-two.service".to_string()
        )));
    }

    #[test]
    fn bird_home_attribution_ignores_other_users_on_the_same_node() {
        let attribution = CurrentClosureAttribution::from_generations(&arguments(), &generations());
        assert_eq!(attribution.system_closure(), Some("/nix/store/system"));
        assert_eq!(attribution.home_closure(), Some("/nix/store/home"));
        assert!(attribution.home_selection_failure.is_none());
    }

    #[test]
    fn duplicate_current_bird_generations_are_a_typed_health_failure() {
        let mut listing = generations();
        listing
            .generation_vector
            .push(CurrentClosureAttribution::fixture_generation(
                ordinary::GenerationArtifact::UserEnvironment,
                Some("bird"),
                "/nix/store/second-bird-home",
                CRIOMOS_REVISION,
            ));
        let attribution = CurrentClosureAttribution::from_generations(&arguments(), &listing);
        assert!(attribution.home_generation.is_none());
        assert!(attribution.home_selection_failure.is_some());
    }

    #[test]
    fn malformed_remote_observation_is_rejected() {
        assert!(RemoteObservation::from_text("not a ledger field\n").is_err());
    }
}
