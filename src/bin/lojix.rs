//! lojix — thin CLI client for `lojix-daemon`.
//!
//! Reads one typed `signal_lojix::LojixCliConfiguration` source from
//! argv position 0, then reads one Nota request from argv position 1+
//! (joined with spaces) or stdin. Sends a `signal-frame` frame carrying a
//! `signal_lojix::Request`, awaits the matching `signal_lojix::Reply`,
//! and prints the reply payload as Nota.

use std::io::Read;
use std::process::ExitCode;

use nota_config::ConfigurationSource;

#[tokio::main]
async fn main() -> ExitCode {
    let configuration = match ConfigurationSource::from_argv_nth(0)
        .and_then(|source| source.decode::<lojix::wire::LojixCliConfiguration>())
    {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("lojix: {error}");
            return ExitCode::FAILURE;
        }
    };

    let input = match InputText::read() {
        Ok(input) => input,
        Err(error) => {
            eprintln!("lojix: {error}");
            return ExitCode::FAILURE;
        }
    };

    let client = lojix::Client::from_configuration(&configuration);
    match client
        .send_text_with_rendering(input.as_str(), configuration.reply_rendering)
        .await
    {
        Ok(reply) => {
            println!("{reply}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("lojix: {error}");
            ExitCode::FAILURE
        }
    }
}

struct InputText {
    value: String,
}

impl InputText {
    fn read() -> std::io::Result<Self> {
        let arguments: Vec<String> = std::env::args().skip(2).collect();
        let value = if arguments.is_empty() {
            let mut buffer = String::new();
            std::io::stdin().read_to_string(&mut buffer)?;
            buffer
        } else {
            arguments.join(" ")
        };
        Ok(Self { value })
    }

    fn as_str(&self) -> &str {
        self.value.trim()
    }
}
