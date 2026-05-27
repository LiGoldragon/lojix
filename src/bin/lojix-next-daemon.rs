use std::env;

use lojix_next::RunDaemon;

#[tokio::main]
async fn main() {
    let invocation = DaemonInvocation::from_environment();
    if let Err(error) = invocation.run().await {
        eprintln!("lojix-next-daemon: {error}");
        std::process::exit(1);
    }
}

struct DaemonInvocation {
    arguments: Vec<String>,
}

impl DaemonInvocation {
    fn from_environment() -> Self {
        Self {
            arguments: env::args().skip(1).collect(),
        }
    }

    async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        if self.arguments.len() != 1 {
            return Err("expected exactly one NOTA configuration argument or path".into());
        }
        let argument = self.arguments.into_iter().next().expect("count checked");
        let mut daemon = RunDaemon::from_single_argument(&argument).await?;
        daemon.start().await?;
        daemon.serve_forever().await?;
        Ok(())
    }
}
