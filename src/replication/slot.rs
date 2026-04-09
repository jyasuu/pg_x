//! Helpers for managing PostgreSQL logical replication slots.

use anyhow::{bail, Context, Result};
use tokio_postgres::Client;

/// Information about an existing replication slot.
#[derive(Debug)]
pub struct SlotInfo {
    pub name: String,
    pub plugin: String,
    pub confirmed_flush_lsn: Option<String>,
    pub active: bool,
}

/// Ensure a logical replication slot with the given name exists.
///
/// If the slot already exists this is a no-op (returns the existing slot info).
/// If it does not exist it is created with the `pgoutput` plugin.
///
/// `temporary` — create a temporary slot that is dropped when the session ends.
pub async fn ensure_slot(client: &Client, slot_name: &str, temporary: bool) -> Result<()> {
    // Check if the slot already exists.
    let rows = client
        .query(
            "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context("Failed to query pg_replication_slots")?;

    if let Some(row) = rows.first() {
        let active: bool = row.get("active");
        if active {
            bail!(
                "Replication slot '{}' already exists and is currently active (in use by another process). \
                 Stop the other consumer or choose a different slot name.",
                slot_name
            );
        }
        // Slot exists and is not active — we can reuse it.
        return Ok(());
    }

    // Create the slot.
    let sql = if temporary {
        format!("CREATE_REPLICATION_SLOT {slot_name} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT")
    } else {
        format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT")
    };

    client
        .simple_query(&sql)
        .await
        .with_context(|| format!("Failed to create replication slot '{slot_name}'"))?;

    Ok(())
}

/// Drop a replication slot by name. Does nothing if the slot doesn't exist.
pub async fn drop_slot(client: &Client, slot_name: &str) -> Result<()> {
    let rows = client
        .query(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context("Failed to query pg_replication_slots")?;

    if rows.is_empty() {
        return Ok(());
    }

    client
        .simple_query(&format!("DROP_REPLICATION_SLOT {slot_name}"))
        .await
        .with_context(|| format!("Failed to drop replication slot '{slot_name}'"))?;

    Ok(())
}

/// List all logical replication slots on the server.
pub async fn list_slots(client: &Client) -> Result<Vec<SlotInfo>> {
    let rows = client
        .query(
            "SELECT slot_name, plugin, confirmed_flush_lsn::text, active \
             FROM pg_replication_slots \
             WHERE slot_type = 'logical' \
             ORDER BY slot_name",
            &[],
        )
        .await
        .context("Failed to list replication slots")?;

    Ok(rows
        .into_iter()
        .map(|row| SlotInfo {
            name: row.get("slot_name"),
            plugin: row.get("plugin"),
            confirmed_flush_lsn: row.get("confirmed_flush_lsn"),
            active: row.get("active"),
        })
        .collect())
}

/// Parse a human-supplied LSN string (e.g. "0/1A2B3C") into a u64.
pub fn parse_lsn(s: &str) -> Result<u64> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        bail!("Invalid LSN '{}': expected format <hi>/<lo>", s);
    }
    let hi = u64::from_str_radix(parts[0], 16)
        .with_context(|| format!("Invalid LSN high part '{}'", parts[0]))?;
    let lo = u64::from_str_radix(parts[1], 16)
        .with_context(|| format!("Invalid LSN low part '{}'", parts[1]))?;
    Ok((hi << 32) | lo)
}
