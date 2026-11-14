#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootstrapEvaluation {
    pub bootstrap_nix_system: crate::ingress::BootstrapNixSystem,
    pub persisted_flake_input_override_vector: std::vec::Vec<
        crate::memory::PersistedFlakeInputOverride,
    >,
    pub bootstrap_flake_reference: crate::ingress::BootstrapFlakeReference,
    pub bootstrap_output_selector: crate::ingress::BootstrapOutputSelector,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootstrapBuild {
    pub bootstrap_nix_system: crate::ingress::BootstrapNixSystem,
    pub bootstrap_builder_spec_option: std::option::Option<
        crate::ingress::BootstrapBuilderSpec,
    >,
    pub derivation_path: DerivationPath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DerivationPath(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootstrapCheckBuild {
    pub bootstrap_nix_system: crate::ingress::BootstrapNixSystem,
    pub bootstrap_flake_reference: crate::ingress::BootstrapFlakeReference,
    pub bootstrap_output_selector: crate::ingress::BootstrapOutputSelector,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct GeneratedInputDirectory(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct GeneratedInputHashing {
    pub generated_input_directory: GeneratedInputDirectory,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RootStagingPath(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootstrapRooting {
    pub root_staging_path: RootStagingPath,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RemoteClosureQuery {
    pub ssh_route: SshRoute,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SshRoute {
    pub ssh_config_path: SshConfigPath,
    pub ssh_identity: SshIdentity,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SshConfigPath(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SshUser(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SshHost(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SshIdentity {
    pub ssh_user: SshUser,
    pub ssh_host: SshHost,
    pub ssh_port_option: std::option::Option<SshPort>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SshPort(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootstrapCopy {
    pub bootstrap_nix_store_uri: crate::ingress::BootstrapNixStoreUri,
    pub ssh_config_path: SshConfigPath,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootOnceUnitQuery {
    pub boot_once_route: BootOnceRoute,
    pub unit_name: crate::emission::UnitName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BootOnceRoute {
    Remote(SshRoute),
    Local,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootOnceDispatch {
    pub boot_once_route: BootOnceRoute,
    pub boot_once_script: BootOnceScript,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SystemProfilePath(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootEntriesDirectory(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootOnceScript {
    pub unit_name: crate::emission::UnitName,
    pub closure_path: signal_lojix::ClosurePath,
    pub system_profile_path: SystemProfilePath,
    pub boot_entries_directory: BootEntriesDirectory,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum PrintedPathOutcome {
    Printed(PrintedStorePath),
    Unprinted,
    Failed(crate::emission::ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PrintedStorePath(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ClosurePresenceOutcome {
    Present,
    Absent,
    Failed(crate::emission::ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BootOnceUnitOutcome {
    Read(BootOnceUnitState),
    Failed(crate::emission::ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BootOnceUnitState {
    NotFound,
    Ready,
    Waiting,
    Failed,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Emission {
    EvaluateBootstrapDerivation(BootstrapEvaluation),
    BuildBootstrapDerivation(BootstrapBuild),
    BuildBootstrapCheck(BootstrapCheckBuild),
    HashGeneratedInput(GeneratedInputHashing),
    RootBootstrapClosure(BootstrapRooting),
    ReadRemoteClosure(RemoteClosureQuery),
    CopyBootstrapClosure(BootstrapCopy),
    ReadBootOnceUnit(BootOnceUnitQuery),
    DispatchBootOnceUnit(BootOnceDispatch),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    BootstrapDerivationEvaluated(PrintedPathOutcome),
    BootstrapDerivationBuilt(PrintedPathOutcome),
    BootstrapCheckBuilt(crate::emission::CallOutcome),
    GeneratedInputHashed(crate::emission::HashOutcome),
    BootstrapClosureRooted(crate::emission::CallOutcome),
    RemoteClosureRead(ClosurePresenceOutcome),
    BootstrapClosureCopied(crate::emission::CallOutcome),
    BootOnceUnitRead(BootOnceUnitOutcome),
    BootOnceUnitDispatched(crate::emission::CallOutcome),
}
#[rustfmt::skip]
pub trait Emitted {
    type Outcome;
}
#[rustfmt::skip]
pub trait Emittable<Translator>: Emitted {
    fn emit(&self, translator: &Translator) -> Self::Outcome;
}
#[rustfmt::skip]
impl Emitted for BootstrapEvaluation {
    type Outcome = PrintedPathOutcome;
}
#[rustfmt::skip]
impl Emitted for BootstrapBuild {
    type Outcome = PrintedPathOutcome;
}
#[rustfmt::skip]
impl Emitted for BootstrapCheckBuild {
    type Outcome = crate::emission::CallOutcome;
}
#[rustfmt::skip]
impl Emitted for GeneratedInputHashing {
    type Outcome = crate::emission::HashOutcome;
}
#[rustfmt::skip]
impl Emitted for BootstrapRooting {
    type Outcome = crate::emission::CallOutcome;
}
#[rustfmt::skip]
impl Emitted for RemoteClosureQuery {
    type Outcome = ClosurePresenceOutcome;
}
#[rustfmt::skip]
impl Emitted for BootstrapCopy {
    type Outcome = crate::emission::CallOutcome;
}
#[rustfmt::skip]
impl Emitted for BootOnceUnitQuery {
    type Outcome = BootOnceUnitOutcome;
}
#[rustfmt::skip]
impl Emitted for BootOnceDispatch {
    type Outcome = crate::emission::CallOutcome;
}
