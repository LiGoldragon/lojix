//! Build-time witness: the schema-next macro registry covered the
//! nested struct-field and enum-variant macros. This test re-runs
//! the lowering through the same engine and asserts on
//! `MacroContext::macros_applied`.

use schema_next::{MacroContext, SchemaEngine, SchemaIdentity};

#[test]
fn lojix_next_schema_lowering_reaches_nested_macros() {
    let source = include_str!("../schema/lojix.schema");
    let mut context = MacroContext::default();
    let _ = SchemaEngine::default()
        .lower_source_with_context(
            source,
            SchemaIdentity::new("lojix_next", "0.1.0"),
            &mut context,
        )
        .expect("lower lojix-next schema");
    let macros_applied: Vec<&str> = context
        .macros_applied()
        .iter()
        .map(String::as_str)
        .collect();
    let struct_macro_pair = macros_applied
        .windows(2)
        .any(|pair| pair == ["SchemaStructDefinition", "SchemaStructFields"]);
    let enum_macro_pair = macros_applied
        .windows(2)
        .any(|pair| pair == ["SchemaEnumDefinition", "SchemaEnumVariants"]);
    assert!(
        struct_macro_pair,
        "expected SchemaStructDefinition + SchemaStructFields pair in macros_applied, got: {macros_applied:?}"
    );
    assert!(
        enum_macro_pair,
        "expected SchemaEnumDefinition + SchemaEnumVariants pair in macros_applied, got: {macros_applied:?}"
    );
}
