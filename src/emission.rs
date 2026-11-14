#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlakeMetadataQuery {
    pub flake_reference: signal_lojix::FlakeReference,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DerivationEvaluation {
    pub flake_reference: signal_lojix::FlakeReference,
    pub deployment_output_selector: signal_lojix::DeploymentOutputSelector,
    pub evaluation_freshness: EvaluationFreshness,
    pub persisted_flake_input_override_vector: std::vec::Vec<
        crate::memory::PersistedFlakeInputOverride,
    >,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum EvaluationFreshness {
    Refreshed,
    TrustingPin,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct InputDirectory(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct InputHashing {
    pub input_directory: InputDirectory,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ClosureBuild {
    pub closure_path: signal_lojix::ClosurePath,
    pub build_place: BuildPlace,
    pub extra_substituter_vector: std::vec::Vec<meta_signal_lojix::ExtraSubstituter>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BuildPlace {
    DaemonStore,
    Offloaded(signal_lojix::NixBuilderSpec),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CheckBuild {
    pub flake_reference: signal_lojix::FlakeReference,
    pub deployment_output_selector: signal_lojix::DeploymentOutputSelector,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DerivationCopy {
    pub nix_store_uri: signal_lojix::NixStoreUri,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TargetStoreBuild {
    pub nix_store_uri: signal_lojix::NixStoreUri,
    pub closure_path: signal_lojix::ClosurePath,
    pub extra_substituter_vector: std::vec::Vec<meta_signal_lojix::ExtraSubstituter>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TargetStoreRooting {
    pub ssh_destination: signal_lojix::SshDestination,
    pub gc_root_directory: GcRootDirectory,
    pub operator_node: OperatorNode,
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum GcRootDirectory {
    SystemRoots,
    LoginHome,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OperatorNode(pub signal_lojix::NodeName);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ClosurePresence {
    pub nix_store_uri: signal_lojix::NixStoreUri,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ClosureCopy {
    pub nix_store_uri: signal_lojix::NixStoreUri,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigurationSwitch {
    pub ssh_destination: signal_lojix::SshDestination,
    pub closure_path: signal_lojix::ClosurePath,
    pub switch_action: SwitchAction,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SwitchAction {
    Boot,
    Switch,
    Test,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LoaderInterfaceQuery {
    pub ssh_destination: signal_lojix::SshDestination,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LoaderInterface {
    Implemented,
    NotImplemented,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootEntryClearing {
    pub ssh_destination: signal_lojix::SshDestination,
    pub boot_entry: BootEntry,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BootEntry {
    Default,
    Oneshot,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TransientUnitRun {
    pub ssh_destination: signal_lojix::SshDestination,
    pub unit_name: UnitName,
    pub transient_script: TransientScript,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum TransientScript {
    BootOnce(signal_lojix::ClosurePath),
    SelfSwitch(SelfSwitchScript),
    SelfTest(signal_lojix::ClosurePath),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SelfSwitchScript {
    pub closure_path: signal_lojix::ClosurePath,
    pub loader_interface: LoaderInterface,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RetainedUnitDispatch {
    pub ssh_destination: signal_lojix::SshDestination,
    pub unit_name: UnitName,
    pub transient_script: TransientScript,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitJournalQuery {
    pub ssh_destination: signal_lojix::SshDestination,
    pub unit_name: UnitName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HomeDirectory(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LocalHomeProfile {
    pub home_directory: HomeDirectory,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LocalHomeActivation {
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RemoteHomeProfile {
    pub ssh_destination: signal_lojix::SshDestination,
    pub remote_home_login: RemoteHomeLogin,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RemoteHomeActivation {
    pub ssh_destination: signal_lojix::SshDestination,
    pub remote_home_login: RemoteHomeLogin,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RemoteHomeLogin {
    AsTargetUser,
    MediatedFor(signal_lojix::UserName),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct GarbageCollection {
    pub node_name: signal_lojix::NodeName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct GuestAddress(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TestVmBringUp {
    pub ssh_destination: signal_lojix::SshDestination,
    pub guest_node: GuestNode,
    pub closure_path: signal_lojix::ClosurePath,
    pub guest_address: GuestAddress,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TestVmTearDown {
    pub ssh_destination: signal_lojix::SshDestination,
    pub guest_node: GuestNode,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct GuestNode(pub signal_lojix::NodeName);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitName(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitLookup {
    pub unit_name: UnitName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitArrival {
    pub unit_name: UnitName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitStateQuery {
    pub unit_name: UnitName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitStateWatch {
    pub unit_name: UnitName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitStop {
    pub unit_name: UnitName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CallOutcome {
    Completed,
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum PathOutcome {
    Printed(PrintedPath),
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PrintedPath {
    pub closure_path_option: std::option::Option<signal_lojix::ClosurePath>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HashOutcome {
    Hashed(SriHash),
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SriHash(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlakeMetadataOutcome {
    Read(FlakeMetadata),
    Malformed(MetadataFault),
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlakeMetadata {
    pub flake_reference_option: std::option::Option<signal_lojix::FlakeReference>,
    pub resolved_revision_option: std::option::Option<crate::memory::ResolvedRevision>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct MetadataFault(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum JournalOutcome {
    Read(JournalText),
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LoaderInterfaceOutcome {
    Read(LoaderInterface),
    Unrecognized(Stdout),
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct JournalText(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HostNameOutcome {
    Named(signal_lojix::NodeName),
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum GarbageOutcome {
    Collected(CollectedPaths),
    Failed(ProcessFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CollectedPaths(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ProcessFailure {
    pub command_program: signal_lojix::CommandProgram,
    pub command_argument_vector: std::vec::Vec<signal_lojix::CommandArgument>,
    pub process_fault: ProcessFault,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ProcessFault {
    Unstarted(OsFault),
    Unfinished(OsFault),
    Ended(ProcessEnding),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OsFault(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Stdout(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Stderr(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ProcessEnding {
    pub process_end: ProcessEnd,
    pub stdout: Stdout,
    pub stderr: Stderr,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ProcessEnd {
    Exited(ExitStatus),
    Signalled(SignalEnd),
    Unreported(WaitStatus),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ExitStatus(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SignalNumber(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CoreDumped(pub bool);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SignalEnd {
    pub signal_number: SignalNumber,
    pub core_dumped: CoreDumped,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WaitStatus(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BusCallOutcome {
    Answered,
    Refused(BusFault),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BusFault(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum UnitLookupOutcome {
    Found,
    NoSuchUnit,
    Refused(BusFault),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum UnitArrivalOutcome {
    Arrived,
    StreamEnded,
    Refused(BusFault),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UnitState {
    pub active_state_reading: ActiveStateReading,
    pub service_result_reading: ServiceResultReading,
    pub process_end_option: std::option::Option<ProcessEnd>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ActiveStateReading {
    Read(ActiveState),
    Unreadable(BusFault),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ServiceResultReading {
    Read(ServiceResult),
    Unreadable(BusFault),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ActiveState {
    Active,
    Reloading,
    Inactive,
    Failed,
    Activating,
    Deactivating,
    Maintenance,
    Refreshing,
    Unrecognized(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ServiceResult {
    Success,
    Resources,
    Protocol,
    Timeout,
    ExitCode,
    Signal,
    CoreDump,
    Watchdog,
    StartLimitHit,
    OomKill,
    ExecCondition,
    Unrecognized(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum UnitStateWatchOutcome {
    Changed,
    StreamEnded,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReadShortHostName {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SubscribeUnitNew {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Emission {
    ReadFlakeMetadata(FlakeMetadataQuery),
    EvaluateDerivation(DerivationEvaluation),
    HashInput(InputHashing),
    BuildClosure(ClosureBuild),
    BuildCheck(CheckBuild),
    CopyDerivation(DerivationCopy),
    BuildInTargetStore(TargetStoreBuild),
    RootInTargetStore(TargetStoreRooting),
    CheckClosurePresence(ClosurePresence),
    CopyClosure(ClosureCopy),
    SwitchConfiguration(ConfigurationSwitch),
    ReadLoaderInterface(LoaderInterfaceQuery),
    ClearBootEntry(BootEntryClearing),
    RunTransientUnit(TransientUnitRun),
    DispatchRetainedUnit(RetainedUnitDispatch),
    ReadUnitJournal(UnitJournalQuery),
    SetLocalHomeProfile(LocalHomeProfile),
    ActivateLocalHome(LocalHomeActivation),
    SetRemoteHomeProfile(RemoteHomeProfile),
    ActivateRemoteHome(RemoteHomeActivation),
    ReadShortHostName(ReadShortHostName),
    CollectGarbage(GarbageCollection),
    BringUpTestVm(TestVmBringUp),
    TearDownTestVm(TestVmTearDown),
    SubscribeUnitNew(SubscribeUnitNew),
    FindUnit(UnitLookup),
    AwaitUnitNew(UnitArrival),
    ReadUnitState(UnitStateQuery),
    AwaitUnitStateChange(UnitStateWatch),
    StopUnit(UnitStop),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    FlakeMetadataRead(FlakeMetadataOutcome),
    DerivationEvaluated(PathOutcome),
    InputHashed(HashOutcome),
    ClosureBuilt(PathOutcome),
    CheckBuilt(PathOutcome),
    DerivationCopied(CallOutcome),
    TargetStoreBuilt(PathOutcome),
    TargetStoreRooted(CallOutcome),
    ClosurePresenceChecked(CallOutcome),
    ClosureCopied(CallOutcome),
    ConfigurationSwitched(CallOutcome),
    LoaderInterfaceRead(LoaderInterfaceOutcome),
    BootEntryCleared(CallOutcome),
    TransientUnitRan(CallOutcome),
    RetainedUnitDispatched(CallOutcome),
    UnitJournalRead(JournalOutcome),
    LocalHomeProfileSet(CallOutcome),
    LocalHomeActivated(CallOutcome),
    RemoteHomeProfileSet(CallOutcome),
    RemoteHomeActivated(CallOutcome),
    ShortHostNameRead(HostNameOutcome),
    GarbageCollected(GarbageOutcome),
    TestVmBroughtUp(CallOutcome),
    TestVmTornDown(CallOutcome),
    UnitNewSubscribed(BusCallOutcome),
    UnitFound(UnitLookupOutcome),
    UnitNewAwaited(UnitArrivalOutcome),
    UnitStateRead(UnitState),
    UnitStateChangeAwaited(UnitStateWatchOutcome),
    UnitStopped(BusCallOutcome),
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
impl Emitted for FlakeMetadataQuery {
    type Outcome = FlakeMetadataOutcome;
}
#[rustfmt::skip]
impl Emitted for DerivationEvaluation {
    type Outcome = PathOutcome;
}
#[rustfmt::skip]
impl Emitted for InputHashing {
    type Outcome = HashOutcome;
}
#[rustfmt::skip]
impl Emitted for ClosureBuild {
    type Outcome = PathOutcome;
}
#[rustfmt::skip]
impl Emitted for CheckBuild {
    type Outcome = PathOutcome;
}
#[rustfmt::skip]
impl Emitted for DerivationCopy {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for TargetStoreBuild {
    type Outcome = PathOutcome;
}
#[rustfmt::skip]
impl Emitted for TargetStoreRooting {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for ClosurePresence {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for ClosureCopy {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for ConfigurationSwitch {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for LoaderInterfaceQuery {
    type Outcome = LoaderInterfaceOutcome;
}
#[rustfmt::skip]
impl Emitted for BootEntryClearing {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for TransientUnitRun {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for RetainedUnitDispatch {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for UnitJournalQuery {
    type Outcome = JournalOutcome;
}
#[rustfmt::skip]
impl Emitted for LocalHomeProfile {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for LocalHomeActivation {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for RemoteHomeProfile {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for RemoteHomeActivation {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for ReadShortHostName {
    type Outcome = HostNameOutcome;
}
#[rustfmt::skip]
impl Emitted for GarbageCollection {
    type Outcome = GarbageOutcome;
}
#[rustfmt::skip]
impl Emitted for TestVmBringUp {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for TestVmTearDown {
    type Outcome = CallOutcome;
}
#[rustfmt::skip]
impl Emitted for SubscribeUnitNew {
    type Outcome = BusCallOutcome;
}
#[rustfmt::skip]
impl Emitted for UnitLookup {
    type Outcome = UnitLookupOutcome;
}
#[rustfmt::skip]
impl Emitted for UnitArrival {
    type Outcome = UnitArrivalOutcome;
}
#[rustfmt::skip]
impl Emitted for UnitStateQuery {
    type Outcome = UnitState;
}
#[rustfmt::skip]
impl Emitted for UnitStateWatch {
    type Outcome = UnitStateWatchOutcome;
}
#[rustfmt::skip]
impl Emitted for UnitStop {
    type Outcome = BusCallOutcome;
}
