#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum InspectionRequest {
    InspectStore(InspectStore),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct InspectStore {
    pub string: String,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum ResetStoreRequest {
    ResetStore,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapRequest {
    BootstrapRun(BootstrapRun),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapRun {
    pub bootstrap_request_id: BootstrapRequestId,
    pub bootstrap_mode: BootstrapMode,
}
#[rustfmt::skip]
pub type BootstrapRequestId = String;
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapMode {
    BuildOnly(BootstrapBuildOnly),
    BootOnce(BootstrapBootOnce),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapBuildOnly {
    pub bootstrap_input: BootstrapInput,
    pub bootstrap_builder: BootstrapBuilder,
    pub bootstrap_journal_parent: BootstrapJournalParent,
    pub bootstrap_gc_root_path: BootstrapGcRootPath,
    pub bootstrap_terminal_evidence_path: BootstrapTerminalEvidencePath,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapBootOnce {
    pub bootstrap_input: BootstrapInput,
    pub bootstrap_builder: BootstrapBuilder,
    pub bootstrap_test_plan: BootstrapTestPlan,
    pub bootstrap_activation_backend: BootstrapActivationBackend,
    pub bootstrap_journal_parent: BootstrapJournalParent,
    pub bootstrap_gc_root_path: BootstrapGcRootPath,
    pub bootstrap_terminal_evidence_path: BootstrapTerminalEvidencePath,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapInput {
    Direct(BootstrapDirectInput),
    Horizon(BootstrapHorizonInput),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapDirectInput {
    pub bootstrap_flake_reference: BootstrapFlakeReference,
    pub bootstrap_nix_system: BootstrapNixSystem,
    pub bootstrap_output_selector: BootstrapOutputSelector,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapHorizonInput {
    pub bootstrap_proposal_source: BootstrapProposalSource,
    pub bootstrap_cluster_name: BootstrapClusterName,
    pub bootstrap_node_name: BootstrapNodeName,
    pub bootstrap_materialization_shape: BootstrapMaterializationShape,
    pub bootstrap_secrets_input: BootstrapSecretsInput,
    pub bootstrap_flake_reference: BootstrapFlakeReference,
    pub bootstrap_nix_system: BootstrapNixSystem,
    pub bootstrap_output_selector: BootstrapOutputSelector,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapMaterializationShape {
    CompleteHost,
    BaseHost,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapSecretsInput {
    NoSecrets,
    SecretsDirectory(BootstrapSecretsDirectory),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapBuilder {
    NoBuilder,
    NixBuilder(BootstrapBuilderSpec),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapTestPlan {
    NoTest,
    RunHermeticTest(BootstrapHermeticTest),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapHermeticTest {
    pub bootstrap_flake_reference: BootstrapFlakeReference,
    pub bootstrap_nix_system: BootstrapNixSystem,
    pub bootstrap_output_selector: BootstrapOutputSelector,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapActivationBackend {
    RemoteNixosSystemdBootV1(BootstrapRemoteNixosSystemdBootV1),
    LocalBootstrapV1(BootstrapLocalBootstrapV1),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapRemoteNixosSystemdBootV1 {
    pub bootstrap_nix_store_uri: BootstrapNixStoreUri,
    pub bootstrap_ssh_destination: BootstrapSshDestination,
    pub bootstrap_ssh_policy: BootstrapSshPolicy,
    pub bootstrap_system_profile_path: BootstrapSystemProfilePath,
    pub bootstrap_boot_entries_directory: BootstrapBootEntriesDirectory,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapSshPolicy {
    pub bootstrap_ssh_identity_file: BootstrapSshIdentityFile,
    pub bootstrap_ssh_known_hosts_file: BootstrapSshKnownHostsFile,
    pub bootstrap_strict_host_key_mode: BootstrapStrictHostKeyMode,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum BootstrapStrictHostKeyMode {
    RequireKnownHost,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct BootstrapLocalBootstrapV1 {
    pub bootstrap_system_profile_path: BootstrapSystemProfilePath,
    pub bootstrap_boot_entries_directory: BootstrapBootEntriesDirectory,
}
#[rustfmt::skip]
pub type BootstrapFlakeReference = String;
#[rustfmt::skip]
pub type BootstrapNixSystem = String;
#[rustfmt::skip]
pub type BootstrapOutputSelector = String;
#[rustfmt::skip]
pub type BootstrapProposalSource = String;
#[rustfmt::skip]
pub type BootstrapClusterName = String;
#[rustfmt::skip]
pub type BootstrapNodeName = String;
#[rustfmt::skip]
pub type BootstrapSecretsDirectory = String;
#[rustfmt::skip]
pub type BootstrapBuilderSpec = String;
#[rustfmt::skip]
pub type BootstrapJournalParent = String;
#[rustfmt::skip]
pub type BootstrapGcRootPath = String;
#[rustfmt::skip]
pub type BootstrapTerminalEvidencePath = String;
#[rustfmt::skip]
pub type BootstrapNixStoreUri = String;
#[rustfmt::skip]
pub type BootstrapSshDestination = String;
#[rustfmt::skip]
pub type BootstrapSshIdentityFile = String;
#[rustfmt::skip]
pub type BootstrapSshKnownHostsFile = String;
#[rustfmt::skip]
pub type BootstrapSystemProfilePath = String;
#[rustfmt::skip]
pub type BootstrapBootEntriesDirectory = String;
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum ConfigurationWriterInput {
    ConfigurationWriteRequest(ConfigurationWriteRequest),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct ConfigurationWriteRequest {
    pub first_writer_path: WriterPath,
    pub first_writer_mode: WriterMode,
    pub second_writer_path: WriterPath,
    pub second_writer_mode: WriterMode,
    pub third_writer_path: WriterPath,
    pub fourth_writer_path: WriterPath,
    pub writer_cluster: WriterCluster,
    pub writer_test_defaults_choice: WriterTestDefaultsChoice,
    pub fifth_writer_path: WriterPath,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum WriterTestDefaultsChoice {
    NoTestDefaults,
    TestDefaults(WriterTestDefaults),
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct WriterTestDefaults {
    pub first_writer_cluster: WriterCluster,
    pub second_writer_cluster: WriterCluster,
    pub writer_test_mode: WriterTestMode,
    pub third_writer_cluster: WriterCluster,
    pub fourth_writer_cluster: WriterCluster,
    pub fifth_writer_cluster: WriterCluster,
    pub writer_path: WriterPath,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum WriterTestMode {
    Hermetic,
    Live,
}
#[rustfmt::skip]
pub type WriterPath = String;
#[rustfmt::skip]
pub type WriterMode = i64;
#[rustfmt::skip]
pub type WriterCluster = String;
