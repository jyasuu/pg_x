#[cfg(feature = "nats")]
#[allow(clippy::module_inception)]
pub mod nats {
    use anyhow::{Context, Result};
    use async_trait::async_trait;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent, SimpleMessage},
        delivery::nats::Nats,
        sink::Downstream,
    };

    /// Publishes every NOTIFY payload as a `SimpleMessage` envelope to a
    /// fixed JetStream subject.
    pub struct SimpleNatsDownstream {
        nats: Nats,
        subject: String,
    }

    impl SimpleNatsDownstream {
        pub async fn connect(
            url: &str,
            subject: impl Into<String>,
            stream: &str,
            create_stream: bool,
        ) -> Result<Self> {
            let subject = subject.into();
            let nats = Nats::connect(url).await?;
            if create_stream {
                nats.ensure_stream(stream, &subject).await?;
            }
            Ok(Self { nats, subject })
        }
    }

    #[async_trait]
    impl Downstream for SimpleNatsDownstream {
        fn name(&self) -> &str {
            "nats-simple"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            let msg = SimpleMessage::from(event);
            let body = serde_json::to_vec(&msg).context("Serialise SimpleMessage")?;
            self.nats.publish(&self.subject, &[], &body).await
        }
    }

    /// Parses the NOTIFY payload as a [`ContractMessage`] and uses the
    /// embedded `nats_subject` routing hint for the destination subject.
    /// Falls back to the configured default when the hint is absent.
    pub struct ContractNatsDownstream {
        nats: Nats,
        default_subject: String,
    }

    impl ContractNatsDownstream {
        pub async fn connect(
            url: &str,
            default_subject: impl Into<String>,
            stream: &str,
            create_stream: bool,
        ) -> Result<Self> {
            let default_subject = default_subject.into();
            let nats = Nats::connect(url).await?;
            if create_stream {
                nats.ensure_stream(stream, &default_subject).await?;
            }
            Ok(Self {
                nats,
                default_subject,
            })
        }
    }

    #[async_trait]
    impl Downstream for ContractNatsDownstream {
        fn name(&self) -> &str {
            "nats-contract"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            if let Some(contract) = ContractMessage::try_parse(&event.payload) {
                let r = &contract.meta.routing;
                let subject = r
                    .nats_subject
                    .clone()
                    .unwrap_or_else(|| self.default_subject.clone());

                let mut headers: Vec<(String, String)> = Vec::new();
                if let Some(et) = &contract.meta.event_type {
                    headers.push(("x-event-type".to_string(), et.clone()));
                }
                headers.push(("x-pg-channel".to_string(), event.channel.clone()));
                headers.push((
                    "x-schema-version".to_string(),
                    contract.meta.schema_version.clone(),
                ));

                self.nats
                    .publish(&subject, &headers, event.payload.as_bytes())
                    .await
            } else {
                // Plain payload — envelope it so consumers get consistent shape.
                let msg = SimpleMessage::from(event);
                let body = serde_json::to_vec(&msg).context("Serialise SimpleMessage")?;
                self.nats.publish(&self.default_subject, &[], &body).await
            }
        }
    }
}
