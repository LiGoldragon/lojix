//! lojix daemon entry — single NOTA argument decodes the daemon configuration,
//! then the daemon binds the ordinary + owner sockets and drives the Nexus
//! runner. NOTA is the only argument language; the configuration is decoded
//! through `nota_config::ConfigurationSource`.

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("(DaemonRejected [{error}])");
            std::process::exit(2);
        }
    }
}

fn run() -> lojix::Result<()> {
    let configuration = nota_config::ConfigurationSource::from_argv()?.decode()?;
    lojix::daemon::Daemon::new(configuration).run()
}
