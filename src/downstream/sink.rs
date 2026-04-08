use anyhow::Result;
use async_trait::async_trait;

use super::contract::NotifyEvent;

/// Every downstream sink implements this trait.
///
/// Implementations decide whether to treat the payload as a simple
/// pass-through or attempt to deserialize a [`ContractMessage`].
#[async_trait]
pub trait Downstream: Send + Sync {
    /// Human-readable name used in log output.
    fn name(&self) -> &str;

    /// Send a single notify event to the downstream.
    async fn send(&self, event: &NotifyEvent) -> Result<()>;
}
