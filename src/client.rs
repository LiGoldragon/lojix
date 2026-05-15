use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_core::{Reply as CoreReply, RequestPayload, SubReply};
use tokio::net::UnixStream;

use crate::error::{Error, Result};
use crate::socket::{Connection, ExchangeIdentity, SocketAddress};
use crate::wire;

pub struct Client {
    address: SocketAddress,
}

impl Client {
    pub fn new(address: SocketAddress) -> Self {
        Self { address }
    }

    pub fn from_environment() -> Self {
        Self::new(SocketAddress::from_environment())
    }

    pub async fn send(&self, request: wire::Request) -> Result<wire::Reply> {
        let stream = UnixStream::connect(self.address.path()).await?;
        let mut connection = Connection::new(stream);
        let exchange = ExchangeIdentity::first_connector_exchange();
        let frame = wire::LojixFrame::new(wire::LojixFrameBody::Request {
            exchange: exchange.value(),
            request: request.into_request(),
        });
        connection.write_frame(&frame).await?;

        let frame = connection.read_frame().await?;
        match frame.into_body() {
            wire::LojixFrameBody::Reply {
                exchange: reply_exchange,
                reply,
            } => {
                if reply_exchange != exchange.value() {
                    return Err(Error::ReplyExchangeMismatch);
                }
                ReplyPayloadExtractor::new(reply).into_payload()
            }
            _ => Err(Error::ExpectedReplyFrame),
        }
    }

    pub async fn send_text(&self, text: &str) -> Result<String> {
        let mut decoder = Decoder::new(text);
        let request = wire::Request::decode(&mut decoder)?;
        let reply = self.send(request).await?;
        let mut encoder = Encoder::new();
        reply.encode(&mut encoder)?;
        Ok(encoder.into_string())
    }
}

struct ReplyPayloadExtractor {
    reply: wire::LojixChannelReply,
}

impl ReplyPayloadExtractor {
    fn new(reply: wire::LojixChannelReply) -> Self {
        Self { reply }
    }

    fn into_payload(self) -> Result<wire::Reply> {
        match self.reply {
            CoreReply::Accepted { per_operation, .. } => match per_operation.into_head_and_tail() {
                (SubReply::Ok { payload, .. }, tail) if tail.is_empty() => Ok(payload),
                _ => Err(Error::ExpectedSingleReplyPayload),
            },
            CoreReply::Rejected { reason } => Err(Error::RequestRejected(reason)),
        }
    }
}
