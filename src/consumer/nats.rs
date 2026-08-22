#[cfg(feature = "nats")]
#[allow(clippy::module_inception)]
pub mod nats {
    use anyhow::{Context, Result};
    use async_nats::jetstream::{
        consumer::pull::{Config as PullConfig, Stream as PullStream},
        consumer::PullConsumer,
        stream::Config as StreamConfig,
        AckKind, Message as JetStreamMessage,
    };
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::collections::HashMap;

    use super::super::r#trait::{BrokerMessage, Consumer, DeliveryTag, RecvOutcome};

    /// NATS JetStream pull consumer. A durable pull consumer reuses the
    /// server-side cursor across restarts, and explicit acks give the same
    /// at-least-once semantics the `Consumer` trait assumes.
    pub struct NatsConsumer {
        // Held so the client connection stays open for the consumer's lifetime.
        _client: async_nats::Client,
        stream: String,
        messages: tokio::sync::Mutex<PullStream>,
        // Messages awaiting settlement, keyed by stream sequence (the delivery
        // tag). Bounded by one in-flight message per session loop iteration.
        pending: std::sync::Mutex<HashMap<u64, JetStreamMessage>>,
    }

    impl NatsConsumer {
        pub async fn connect(
            url: &str,
            stream: &str,
            subject: Option<&str>,
            durable: &str,
            create_stream: bool,
        ) -> Result<Self> {
            let client = async_nats::connect(url)
                .await
                .with_context(|| format!("Failed to connect to NATS at {url}"))?;
            let jetstream = async_nats::jetstream::new(client.clone());

            let stream_handle = if create_stream {
                jetstream
                    .get_or_create_stream(StreamConfig {
                        name: stream.to_string(),
                        subjects: subject.map(|s| vec![s.to_string()]).unwrap_or_default(),
                        ..Default::default()
                    })
                    .await
                    .with_context(|| format!("Failed to get or create JetStream stream {stream}"))?
            } else {
                jetstream.get_stream(stream).await.with_context(|| {
                    format!(
                        "JetStream stream '{stream}' does not exist — create it with \
                         --nats-create-stream or out-of-band via the NATS CLI"
                    )
                })?
            };

            let config = PullConfig {
                durable_name: Some(durable.to_string()),
                filter_subject: subject.unwrap_or("").to_string(),
                ..Default::default()
            };
            let consumer: PullConsumer = if create_stream {
                stream_handle
                    .get_or_create_consumer(durable, config)
                    .await
                    .with_context(|| {
                        format!("Failed to get or create durable consumer {durable}")
                    })?
            } else {
                stream_handle
                    .get_consumer(durable)
                    .await
                    .map_err(Self::err)
                    .with_context(|| {
                        format!(
                            "Durable consumer '{durable}' does not exist on stream '{stream}' — \
                         create it with --nats-create-stream or out-of-band via the NATS CLI"
                        )
                    })?
            };

            // Long-lived pull stream: re-issues fetch requests internally and
            // blocks until a message arrives, matching recv()'s contract.
            let messages = consumer
                .stream()
                .messages()
                .await
                .context("Failed to start JetStream pull subscription")?;

            Ok(Self {
                _client: client,
                stream: stream.to_string(),
                messages: tokio::sync::Mutex::new(messages),
                pending: std::sync::Mutex::new(HashMap::new()),
            })
        }

        /// async-nats' own `Error` is a boxed trait object that does not implement
        /// `std::error::Error`, so it cannot take anyhow's `.context()` directly;
        /// convert via its Display instead.
        fn err(e: impl std::fmt::Display) -> anyhow::Error {
            anyhow::Error::msg(e.to_string())
        }

        fn nats_message_id(msg_id_header: Option<&str>, stream_sequence: u64) -> Option<String> {
            // Identity for dedupe: the producer-set `Nats-Msg-Id` header wins;
            // otherwise the stream sequence, which is stable across redeliveries.
            // Unlike AMQP there is always a native id, so this never returns None.
            match msg_id_header {
                Some(id) if !id.is_empty() => Some(id.to_string()),
                _ => Some(stream_sequence.to_string()),
            }
        }
    }

    #[async_trait]
    impl Consumer for NatsConsumer {
        fn name(&self) -> &str {
            "nats"
        }

        async fn recv(&self) -> Result<RecvOutcome> {
            let mut messages = self.messages.lock().await;
            match messages.next().await {
                Some(Ok(msg)) => {
                    let info = msg
                        .info()
                        .map_err(Self::err)
                        .context("JetStream message missing ack info")?;
                    let seq = info.stream_sequence;
                    let payload = String::from_utf8_lossy(&msg.message.payload).to_string();

                    let mut headers = HashMap::new();
                    headers.insert("x-stream-sequence".to_string(), seq.to_string());
                    headers.insert(
                        "x-consumer-sequence".to_string(),
                        info.consumer_sequence.to_string(),
                    );
                    if let Some(hdrs) = msg.message.headers.as_ref() {
                        for (key, vals) in hdrs.iter() {
                            if let Some(v) = vals.last() {
                                headers.insert(key.to_string(), v.to_string());
                            }
                        }
                    }
                    let message_id =
                        Self::nats_message_id(headers.get("Nats-Msg-Id").map(String::as_str), seq);

                    self.pending.lock().unwrap().insert(seq, msg);

                    Ok(RecvOutcome::Message(BrokerMessage {
                        topic: self.stream.clone(),
                        payload,
                        headers,
                        message_id,
                        delivery_tag: DeliveryTag::from_u64(seq),
                    }))
                }
                Some(Err(e)) => Err(e).context("NATS recv failed"),
                None => Ok(RecvOutcome::Closed),
            }
        }

        async fn ack(&self, tag: DeliveryTag) -> Result<()> {
            let msg = self
                .pending
                .lock()
                .unwrap()
                .remove(&tag.as_u64())
                .context("No pending NATS message for delivery tag")?;
            msg.ack_with(AckKind::Ack)
                .await
                .map_err(Self::err)
                .context("Failed to ack NATS message")
        }

        async fn nack(&self, tag: DeliveryTag, requeue: bool) -> Result<()> {
            let kind = if requeue {
                // Immediate redelivery; retry backoff is left to the
                // consumer-level ack_wait/max_deliver JetStream config.
                AckKind::Nak(None)
            } else {
                // Discard: terminate redelivery without acknowledging.
                AckKind::Term
            };
            let msg = self
                .pending
                .lock()
                .unwrap()
                .remove(&tag.as_u64())
                .context("No pending NATS message for delivery tag")?;
            msg.ack_with(kind)
                .await
                .map_err(Self::err)
                .context("Failed to nack NATS message")
        }
    }

    #[cfg(test)]
    mod tests {
        use super::NatsConsumer;

        #[test]
        fn message_id_prefers_nats_msg_id_header() {
            assert_eq!(
                NatsConsumer::nats_message_id(Some("order-42"), 17),
                Some("order-42".to_string())
            );
        }

        #[test]
        fn message_id_falls_back_to_stream_sequence() {
            assert_eq!(
                NatsConsumer::nats_message_id(None, 42),
                Some("42".to_string())
            );
        }

        #[test]
        fn message_id_ignores_empty_header() {
            assert_eq!(
                NatsConsumer::nats_message_id(Some(""), 7),
                Some("7".to_string())
            );
        }

        #[test]
        fn delivery_tag_round_trips_stream_sequence() {
            for seq in [0u64, 1, 42, u32::MAX as u64, u64::MAX] {
                assert_eq!(
                    super::super::super::r#trait::DeliveryTag::from_u64(seq).as_u64(),
                    seq
                );
            }
        }
    }
}
