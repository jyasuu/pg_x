use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A decoded WAL event from the pgoutput logical replication protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WalEvent {
    /// Marks the start of a transaction.
    Begin {
        /// The final LSN of the transaction.
        lsn: String,
        /// The commit timestamp (microseconds since 2000-01-01).
        commit_time: i64,
        /// Transaction ID.
        xid: u32,
    },

    /// Marks the successful commit of a transaction.
    Commit {
        /// LSN of the commit record.
        lsn: String,
        /// LSN of the end of the commit record.
        end_lsn: String,
        /// The commit timestamp (microseconds since 2000-01-01).
        commit_time: i64,
    },

    /// Describes a relation (table) that subsequent DML messages reference.
    Relation {
        /// Relation OID.
        rel_id: u32,
        /// Schema name (namespace).
        schema: String,
        /// Table name.
        table: String,
        /// Column definitions in ordinal order.
        columns: Vec<ColumnDef>,
    },

    /// A row was inserted.
    Insert {
        /// Relation OID (resolved by the caller using the Relation cache).
        rel_id: u32,
        /// Schema of the table.
        schema: String,
        /// Name of the table.
        table: String,
        /// New row as a column-name → value map.
        new: HashMap<String, Option<String>>,
    },

    /// A row was updated.
    Update {
        rel_id: u32,
        schema: String,
        table: String,
        /// Old row values (present only when REPLICA IDENTITY is FULL or when
        /// the update changes a replica-identity column).
        old: Option<HashMap<String, Option<String>>>,
        /// New (post-update) row values.
        new: HashMap<String, Option<String>>,
    },

    /// A row was deleted.
    Delete {
        rel_id: u32,
        schema: String,
        table: String,
        /// The deleted row (present when REPLICA IDENTITY is FULL or DEFAULT
        /// and the key columns are known).
        old: HashMap<String, Option<String>>,
    },

    /// One or more tables were truncated.
    Truncate {
        /// Relation OIDs of truncated tables.
        rel_ids: Vec<u32>,
        /// Names of truncated tables (schema.table).
        tables: Vec<String>,
        cascade: bool,
        restart_seqs: bool,
    },

    /// A keepalive/heartbeat from the server (not a data change).
    Keepalive {
        /// Server WAL end position.
        wal_end: String,
        /// Whether the server requests an immediate status update reply.
        reply_requested: bool,
    },
}

/// Column metadata from a Relation message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    /// Whether this column is part of the replica identity.
    pub is_key: bool,
    /// PostgreSQL type OID.
    pub type_id: u32,
    /// Type modifier (atttypmod).
    pub type_modifier: i32,
}

impl WalEvent {
    /// Return a short human-readable operation label for log output.
    pub fn op_label(&self) -> &'static str {
        match self {
            WalEvent::Begin { .. } => "BEGIN",
            WalEvent::Commit { .. } => "COMMIT",
            WalEvent::Relation { .. } => "RELATION",
            WalEvent::Insert { .. } => "INSERT",
            WalEvent::Update { .. } => "UPDATE",
            WalEvent::Delete { .. } => "DELETE",
            WalEvent::Truncate { .. } => "TRUNCATE",
            WalEvent::Keepalive { .. } => "KEEPALIVE",
        }
    }

    /// Return the table name if this is a DML event.
    pub fn table_name(&self) -> Option<(&str, &str)> {
        match self {
            WalEvent::Insert { schema, table, .. }
            | WalEvent::Update { schema, table, .. }
            | WalEvent::Delete { schema, table, .. } => Some((schema, table)),
            _ => None,
        }
    }

    /// Serialize to a compact JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}
