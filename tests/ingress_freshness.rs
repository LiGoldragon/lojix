use datom_codec::{Actualizing as _, Potential as DatomPotential};
use ethos_zero::{Actualizing, File, Generating, Potential as EthosPotential};
use lojix::ingress::{InspectStore, InspectionRequest};

#[test]
fn committed_ingress_is_fresh_from_its_authored_ethos() {
    let source = include_str!("../ethos/ingress.ethos");
    let generated = EthosPotential::<File>::from(source)
        .actualize()
        .expect("ingress ethos reads")
        .generate()
        .expect("ingress ethos generates");
    assert_eq!(generated, include_str!("../src/ingress.rs"));
}

#[test]
fn string_ingress_preserves_bare_paths_and_colon_urls() {
    for expected in [
        ".state/cache",
        "https://example.invalid/a..b",
        "checks.fixture-a",
    ] {
        let source = format!("InspectStore.{{ {expected} }}");
        let request = DatomPotential::<InspectionRequest>::from(source)
            .actualize(&mut <lojix::Ingress as lojix::Budgeted>::budget())
            .expect("typed String ingress reads its canonical bare form");
        let InspectionRequest::InspectStore(InspectStore { string }) = request;
        assert_eq!(string, expected);
    }
}
