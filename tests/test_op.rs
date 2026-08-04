//! The test operation is represented by the current local SEMA runtime. More
//! detailed dispatch coverage stays in the runtime unit tests; this target
//! guards that the public test path does not require a legacy text protocol.

use lojix::schema_runtime::SchemaRuntime;

#[test]
fn runtime_initializes_without_a_legacy_text_protocol() {
    let _runtime = SchemaRuntime::new();
}
