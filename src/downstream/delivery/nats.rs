use anyhow::{Context, Result};

/// NATS JetStream publisher handle shared by the sink seams.
///
/// Hides client construction, stream provisioning, and the per-message
/// JetStream publish (subject, headers, body) behind small methods.
pub struct Nats {
    // Held so the client connection stays open for the handle's lifetime.
    _client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
}

impl Nats {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = async_nats::connect(url)
            .await
            .with_context(|| format!("Failed to connect to NATS at {url}"))?;
        let jetstream = async_nats::jetstream::new(client.clone());

        Ok(Self {
            _client: client,
            jetstream,
        })
    }

    /// Create the named JetStream stream (capturing `subject`) if missing.
    pub async fn ensure_stream(&self, stream: &str, subject: &str) -> Result<()> {
        self.jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: stream.to_string(),
                subjects: vec![subject.to_string()],
                ..Default::default()
            })
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to get or create JetStream stream {stream}: {e}")
            })?;
        Ok(())
    }

    /// Publish `body` to `subject` with the given headers and wait for the
    /// server's persistence ack.
    pub async fn publish(
        &self,
        subject: &str,
        headers: &[(String, String)],
        body: impl AsRef<[u8]>,
    ) -> Result<()> {
        let mut map = async_nats::HeaderMap::new();
        for (k, v) in headers {
            map.insert(k.as_str(), v.as_str());
        }

        let ack = if headers.is_empty() {
            self.jetstream
                .publish(subject.to_string(), body.as_ref().to_vec().into())
                .await
                .map_err(|e| anyhow::anyhow!("JetStream publish failed: {e}"))?
        } else {
            self.jetstream
                .publish_with_headers(subject.to_string(), map, body.as_ref().to_vec().into())
                .await
                .map_err(|e| anyhow::anyhow!("JetStream publish failed: {e}"))?
        };

        ack.await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("no responders") {
                anyhow::anyhow!(
                    "JetStream publish failed: no stream captures subject \
                     '{subject}' — create one with --nats-create-stream or via \
                     the NATS CLI"
                )
            } else {
                anyhow::anyhow!("JetStream publish not acknowledged: {msg}")
            }
        })?;

        Ok(())
    }
}
