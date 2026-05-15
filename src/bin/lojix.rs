//! lojix — thin CLI client for `lojix-daemon`.
//!
//! Reads one Nota request from argv (joined with spaces) or stdin,
//! opens `/run/lojix/daemon.sock` unless `LOJIX_SOCKET_PATH` overrides
//! the launch boundary, sends a `signal-core` frame carrying a
//! `signal_lojix::Request`, awaits the matching `signal_lojix::Reply`,
//! and prints the reply payload as Nota.

use std::io::Read;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let input = match InputText::read() {
        Ok(input) => input,
        Err(error) => {
            eprintln!("lojix: {error}");
            return ExitCode::FAILURE;
        }
    };

    let client = lojix::Client::from_environment();
    match client.send_text(input.as_str()).await {
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
        let arguments: Vec<String> = std::env::args().skip(1).collect();
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
