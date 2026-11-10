use meta_lojix_client::{Invocable as _, ReplyLine as _};
fn main() {
    let mut standard_output = std::io::stdout().lock();
    match meta_lojix_client::Client::run_from_environment(&mut |reply| {
        reply.write_line(&mut standard_output)
    }) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("(CliRejected [{error}])");
            std::process::exit(2);
        }
    }
}
