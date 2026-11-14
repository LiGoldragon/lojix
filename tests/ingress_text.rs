//! The offline tools read their requests as the generated ingress types, each
//! the struct itself; ethos/ingress.ethos is held to src/ingress.rs by build.rs.

use datom_codec::{Actualizing as _, Potential};
use lojix::ingress::{InspectStore, ResetStore};

#[test]
fn string_ingress_preserves_bare_paths_and_colon_urls() {
    for expected in [
        ".state/cache",
        "https://example.invalid/a..b",
        "checks.fixture-a",
    ] {
        let source = format!("{{ {expected} }}");
        let InspectStore { string } = Potential::<InspectStore>::from(source)
            .actualize(&mut <lojix::Ingress as lojix::Budgeted>::budget())
            .expect("typed String ingress reads its canonical bare form");
        assert_eq!(string, expected);
    }
}

#[test]
fn the_reset_request_is_the_empty_struct_and_nothing_else() {
    for (source, reads) in [("{}", true), ("ResetStore", false), ("{ extra }", false)] {
        let read = Potential::<ResetStore>::from(source.to_owned())
            .actualize(&mut <lojix::Ingress as lojix::Budgeted>::budget());
        assert_eq!(read.is_ok(), reads, "{source}");
    }
}
