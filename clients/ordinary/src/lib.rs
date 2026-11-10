use lojix::InlineDatomArguments as _;
use std::ffi::OsString;
use std::io::Write;

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use lojix::client::{NexusSocket, SocketExchange};
use protos::{Compactable, Protosizable, ReaderBudget};
use signal::{ByteViewable, Restorable, Signal, Signalizable};

const SOCKET_ENV: &str = "LOJIX_ORDINARY_SOCKET";

#[derive(Debug)]
pub struct Client {
    input: signal_lojix::Query,
}

pub trait Invocable {
    /// The decode budget this client actualizes its one inline Datom argument
    /// under. It belongs to the client, which is the thing that has a budget.
    fn budget() -> Budget
    where
        Self: Sized;
    /// Read the one inline Datom argument, send it to the socket the
    /// environment names, and hand on each reply until the Nexus closes.
    fn run_from_environment(
        each: &mut dyn FnMut(signal_lojix::Response) -> lojix::Result<()>,
    ) -> lojix::Result<()>
    where
        Self: Sized;
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self>
    where
        Self: Sized;
    fn input(&self) -> &signal_lojix::Query;
    /// Send the input on `socket` and hand on each reply, in order, until the
    /// Nexus closes the connection. The Nexus decides how many replies an
    /// input has: one for an ordinary input, every matching event for a
    /// deployment watch.
    fn run(
        self,
        socket: &SocketExchange,
        each: &mut dyn FnMut(signal_lojix::Response) -> lojix::Result<()>,
    ) -> lojix::Result<()>
    where
        Self: Sized;
}

/// A reply as the client prints it: one compact Datom line.
pub trait ReplyLine {
    fn line(&self) -> String;

    /// Write the line and flush it, so a watcher reading the client's output
    /// sees each event as it arrives.
    fn write_line(&self, output: &mut dyn Write) -> lojix::Result<()> {
        writeln!(output, "{}", self.line())?;
        output.flush()?;
        Ok(())
    }
}

impl ReplyLine for signal_lojix::Response {
    fn line(&self) -> String {
        self.clone().datomize(vec![]).protosize().compact()
    }
}

impl Invocable for Client {
    fn budget() -> Budget {
        Budget {
            remaining: 16_384,
            reader: ReaderBudget { remaining: 16_384 },
            depth: 0,
            maximum_depth: 16_384,
        }
    }
    fn run_from_environment(
        each: &mut dyn FnMut(signal_lojix::Response) -> lojix::Result<()>,
    ) -> lojix::Result<()> {
        let client = Self::from_arguments(std::env::args_os().skip(1))?;
        client.run(&SocketExchange::for_environment(SOCKET_ENV)?, each)
    }
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self> {
        let source = (arguments).single_inline_datom()?;
        let input = Potential::<signal_lojix::Query>::from(source)
            .actualize(&mut <Self as Invocable>::budget())
            .map_err(|fault| lojix::Error::DatomRequestText(format!("{fault:?}")))?;
        Ok(Self { input })
    }
    fn input(&self) -> &signal_lojix::Query {
        &self.input
    }
    fn run(
        self,
        socket: &SocketExchange,
        each: &mut dyn FnMut(signal_lojix::Response) -> lojix::Result<()>,
    ) -> lojix::Result<()> {
        let request = self
            .input
            .signalize()
            .map_err(|fault| lojix::Error::Wire(format!("{fault:?}")))?;
        socket.exchange(request.bytes().to_vec(), &mut |reply| {
            each(
                Signal::<signal_lojix::Response>::from(reply)
                    .restore()
                    .map_err(|fault| lojix::Error::Wire(format!("{fault:?}")))?,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protos::Textualizable;
    #[test]
    fn decodes_canonical_inline_query() {
        let input = signal_lojix::Query::Unwatch(signal_lojix::SubscriptionClose {
            subscription_token: 7,
        });
        let text = input.datomize(vec![]).protosize().textualize();
        assert_eq!(
            Client::from_arguments([OsString::from(text)])
                .unwrap()
                .input(),
            &input
        );
    }
    #[test]
    fn rejects_non_single_inline_arguments() {
        assert!(Client::from_arguments(Vec::<OsString>::new()).is_err());
        assert!(Client::from_arguments([OsString::from("--pretty")]).is_err());
    }
}
