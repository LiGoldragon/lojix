use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_frame::{Reply as CoreReply, RequestPayload, StreamingFrameBody, SubReply};
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

    pub fn from_configuration(configuration: &wire::LojixCliConfiguration) -> Self {
        Self::new(SocketAddress::new(
            configuration.daemon_socket_path.as_str(),
        ))
    }

    pub async fn send(&self, request: wire::Operation) -> Result<wire::LojixReply> {
        let stream = UnixStream::connect(self.address.path()).await?;
        let mut connection = Connection::new(stream);
        let exchange = ExchangeIdentity::first_connector_exchange();
        let frame = wire::Frame::new(StreamingFrameBody::Request {
            exchange: exchange.value(),
            request: request.into_request(),
        });
        connection.write_frame(&frame).await?;

        let frame = connection.read_frame().await?;
        match frame.into_body() {
            StreamingFrameBody::Reply {
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
        let request = Self::decode_operation(text)?;
        let reply = self.send(request).await?;
        Self::render_reply(reply, wire::ReplyRendering::Compact)
    }

    pub async fn send_text_with_rendering(
        &self,
        text: &str,
        rendering: wire::ReplyRendering,
    ) -> Result<String> {
        let request = Self::decode_operation(text)?;
        let reply = self.send(request).await?;
        Self::render_reply(reply, rendering)
    }

    fn decode_operation(text: &str) -> Result<wire::Operation> {
        let mut decoder = Decoder::new(text);
        let head = decoder.peek_record_head()?;
        let operation = match head.as_str() {
            "Deploy" => wire::Operation::Deploy(Self::decode_payload(&mut decoder, "Deploy")?),
            "Pin" => wire::Operation::Pin(Self::decode_payload(&mut decoder, "Pin")?),
            "Unpin" => wire::Operation::Unpin(Self::decode_payload(&mut decoder, "Unpin")?),
            "Retire" => wire::Operation::Retire(Self::decode_payload(&mut decoder, "Retire")?),
            "Query" => wire::Operation::Query(Self::decode_payload(&mut decoder, "Query")?),
            "WatchDeployments" => wire::Operation::WatchDeployments(Self::decode_payload(
                &mut decoder,
                "WatchDeployments",
            )?),
            "UnwatchDeployments" => wire::Operation::UnwatchDeployments(Self::decode_payload(
                &mut decoder,
                "UnwatchDeployments",
            )?),
            "WatchCacheRetention" => wire::Operation::WatchCacheRetention(Self::decode_payload(
                &mut decoder,
                "WatchCacheRetention",
            )?),
            "UnwatchCacheRetention" => wire::Operation::UnwatchCacheRetention(
                Self::decode_payload(&mut decoder, "UnwatchCacheRetention")?,
            ),
            _ => {
                return Err(nota_codec::Error::UnknownVariant {
                    enum_name: "Operation",
                    got: head,
                }
                .into());
            }
        };
        Ok(operation)
    }

    fn decode_payload<Payload: NotaDecode>(
        decoder: &mut Decoder<'_>,
        variant: &'static str,
    ) -> Result<Payload> {
        decoder.expect_record_head(variant)?;
        let payload = Payload::decode(decoder)?;
        decoder.expect_record_end()?;
        Ok(payload)
    }

    fn render_reply(reply: wire::LojixReply, rendering: wire::ReplyRendering) -> Result<String> {
        match rendering {
            wire::ReplyRendering::Compact => Self::render_compact_reply(reply),
        }
    }

    fn render_compact_reply(reply: wire::LojixReply) -> Result<String> {
        let mut encoder = Encoder::new();
        Self::encode_reply(&reply, &mut encoder)?;
        Ok(encoder.into_string())
    }

    fn encode_reply(reply: &wire::LojixReply, encoder: &mut Encoder) -> Result<()> {
        match reply {
            wire::LojixReply::DeploymentAccepted(payload) => {
                Self::encode_payload(encoder, "DeploymentAccepted", payload)
            }
            wire::LojixReply::DeploymentRejected(payload) => {
                Self::encode_payload(encoder, "DeploymentRejected", payload)
            }
            wire::LojixReply::CacheRetentionAccepted(payload) => {
                Self::encode_payload(encoder, "CacheRetentionAccepted", payload)
            }
            wire::LojixReply::CacheRetentionRejected(payload) => {
                Self::encode_payload(encoder, "CacheRetentionRejected", payload)
            }
            wire::LojixReply::GenerationListing(payload) => {
                Self::encode_payload(encoder, "GenerationListing", payload)
            }
            wire::LojixReply::DeploymentObservationSubscriptionOpened(payload) => {
                Self::encode_payload(encoder, "DeploymentObservationSubscriptionOpened", payload)
            }
            wire::LojixReply::DeploymentObservationSubscriptionClosed(payload) => {
                Self::encode_payload(encoder, "DeploymentObservationSubscriptionClosed", payload)
            }
            wire::LojixReply::CacheRetentionObservationSubscriptionOpened(payload) => {
                Self::encode_payload(
                    encoder,
                    "CacheRetentionObservationSubscriptionOpened",
                    payload,
                )
            }
            wire::LojixReply::CacheRetentionObservationSubscriptionClosed(payload) => {
                Self::encode_payload(
                    encoder,
                    "CacheRetentionObservationSubscriptionClosed",
                    payload,
                )
            }
        }
    }

    fn encode_payload<Payload: NotaEncode>(
        encoder: &mut Encoder,
        variant: &'static str,
        payload: &Payload,
    ) -> Result<()> {
        encoder.start_record(variant)?;
        payload.encode(encoder)?;
        encoder.end_record()?;
        Ok(())
    }
}

struct ReplyPayloadExtractor {
    reply: signal_frame::Reply<wire::LojixReply>,
}

impl ReplyPayloadExtractor {
    fn new(reply: signal_frame::Reply<wire::LojixReply>) -> Self {
        Self { reply }
    }

    fn into_payload(self) -> Result<wire::LojixReply> {
        match self.reply {
            CoreReply::Accepted { per_operation, .. } => match per_operation.into_head_and_tail() {
                (SubReply::Ok(payload), tail) if tail.is_empty() => Ok(payload),
                _ => Err(Error::ExpectedSingleReplyPayload),
            },
            CoreReply::Rejected { reason } => Err(Error::RequestRejected(reason)),
        }
    }
}
