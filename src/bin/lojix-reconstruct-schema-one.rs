//! Read-only schema-one to schema-three store reconstruction CLI.

use std::ffi::OsString;

use lojix::reconstruction::SchemaOneReconstructor;

fn main() {
    match arguments().and_then(|(source, destination)| {
        SchemaOneReconstructor::new(source, destination).reconstruct()
    }) {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("(SchemaOneReconstructionRejected [{error}])");
            std::process::exit(2);
        }
    }
}

fn arguments() -> lojix::Result<(OsString, OsString)> {
    let values: Vec<_> = std::env::args_os().skip(1).collect();
    match values.as_slice() {
        [source, destination]
            if !source.to_string_lossy().starts_with('-')
                && !destination.to_string_lossy().starts_with('-') =>
        {
            Ok((source.clone(), destination.clone()))
        }
        _ => Err(lojix::Error::ExpectedSourceAndDestination),
    }
}
