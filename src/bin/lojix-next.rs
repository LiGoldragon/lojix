use std::{env, fs, path::Path};

use lojix_next::{Input, Output};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const LENGTH_PREFIX_BYTE_COUNT: usize = 4;

#[tokio::main]
async fn main() {
    let invocation = CliInvocation::from_environment();
    if let Err(error) = invocation.run().await {
        eprintln!("lojix-next: {error}");
        std::process::exit(1);
    }
}

struct CliInvocation {
    arguments: Vec<String>,
}

impl CliInvocation {
    fn from_environment() -> Self {
        Self {
            arguments: env::args().skip(1).collect(),
        }
    }

    async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        if self.arguments.len() != 1 {
            return Err("expected exactly one NOTA argument or path".into());
        }
        let argument = self.arguments[0].clone();
        let source = self.read_argument(&argument)?;
        let input: Input = source.parse()?;
        let socket_path =
            env::var("LOJIX_NEXT_SOCKET").unwrap_or_else(|_| String::from("/tmp/lojix-next.sock"));
        let exchanger = SocketExchange::new(socket_path);
        let output = exchanger.exchange(&input).await?;
        println!("{output}");
        Ok(())
    }

    fn read_argument(&self, argument: &str) -> Result<String, Box<dyn std::error::Error>> {
        if argument.trim_start().starts_with('(') {
            Ok(argument.to_owned())
        } else if Path::new(argument).exists() {
            Ok(fs::read_to_string(argument)?)
        } else {
            Err("inline operation must be a parenthesized NOTA value".into())
        }
    }
}

struct SocketExchange {
    socket_path: String,
}

impl SocketExchange {
    fn new(socket_path: String) -> Self {
        Self { socket_path }
    }

    async fn exchange(&self, input: &Input) -> Result<Output, Box<dyn std::error::Error>> {
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        let frame = input.encode_signal_frame()?;
        let length = u32::try_from(frame.len()).map_err(|_| "frame too large for u32 prefix")?;
        stream.write_all(&length.to_be_bytes()).await?;
        stream.write_all(&frame).await?;
        stream.flush().await?;

        let mut length_bytes = [0_u8; LENGTH_PREFIX_BYTE_COUNT];
        stream.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut reply = vec![0_u8; length];
        stream.read_exact(&mut reply).await?;
        let (_route, output) = Output::decode_signal_frame(&reply)?;
        Ok(output)
    }
}
