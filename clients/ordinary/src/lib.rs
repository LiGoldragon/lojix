use lojix::InlineDatomArguments as _;
use std::ffi::OsString;

use datom_codec::{Actualizing, Budget, Potential};
use lojix::client::{NexusSocket, SocketExchange};
use protos::ReaderBudget;
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
    fn run_from_environment() -> lojix::Result<signal_lojix::Response>
    where
        Self: Sized;
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self>
    where
        Self: Sized;
    fn input(&self) -> &signal_lojix::Query;
    fn run(self) -> lojix::Result<signal_lojix::Response>
    where
        Self: Sized;
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
    fn run_from_environment() -> lojix::Result<signal_lojix::Response> {
        Self::from_arguments(std::env::args_os().skip(1))?.run()
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
    fn run(self) -> lojix::Result<signal_lojix::Response> {
        let request = self
            .input
            .signalize()
            .map_err(|fault| lojix::Error::Wire(format!("{fault:?}")))?;
        let reply =
            SocketExchange::for_environment(SOCKET_ENV)?.exchange(request.bytes().to_vec())?;
        Signal::<signal_lojix::Response>::from(reply)
            .restore()
            .map_err(|fault| lojix::Error::Wire(format!("{fault:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datom_codec::Datomizable;
    use protos::{Protosizable, Textualizable};
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
