use datom_codec::Datomizable;
use lojix_client::Invocable as _;
use protos::{Compactable, Protosizable};
fn main() {
    match lojix_client::Client::run_from_environment() {
        Ok(output) => println!("{}", output.datomize(vec![]).protosize().compact()),
        Err(error) => {
            eprintln!("(CliRejected [{error}])");
            std::process::exit(2);
        }
    }
}
