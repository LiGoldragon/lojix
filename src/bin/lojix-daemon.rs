//! lojix daemon entry — one rkyv startup file decodes the daemon configuration,
//! then the daemon binds the ordinary + owner sockets and drives the Nexus
//! runner. The daemon rejects inline NOTA and `.nota` startup files through the
//! component argument parser's signal-file boundary.

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
    let command = triad_runtime::ComponentCommand::from_environment();
    let argument = command.signal_file_argument()?;
    let file = argument
        .into_signal_file()
        .ok_or(triad_runtime::ArgumentError::ExpectedSignalFile)?;
    let configuration = lojix::DaemonConfiguration::from_rkyv_file(file.as_path())?;
    lojix::daemon::Daemon::new(configuration).run()
}
