//! Decoder for the PostgreSQL `pgoutput` logical replication plugin wire format.
//!
//! Reference: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
//!
//! Every CopyData frame from `START_REPLICATION` begins with a single byte:
//!
//! | Byte | Meaning                        |
//! |------|--------------------------------|
//! | `w`  | WAL data (XLogData)            |
//! | `k`  | Primary keepalive message      |
//!
//! XLogData payload itself has the pgoutput message type as its first byte:
//!
//! | Byte | Message type |
//! |------|--------------|
//! | `B`  | Begin        |
//! | `C`  | Commit       |
//! | `R`  | Relation     |
//! | `I`  | Insert       |
//! | `U`  | Update       |
//! | `D`  | Delete       |
//! | `T`  | Truncate     |
//! | `O`  | Origin       |
//! | `Y`  | Type         |

use anyhow::{bail, Result};
use std::collections::HashMap;

use super::event::{ColumnDef, WalEvent};

// ─────────────────────────────────────────────────────────────────────────────
// Low-level byte reader
// ─────────────────────────────────────────────────────────────────────────────

pub struct Buf<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Buf<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.data.len() {
            bail!("unexpected end of buffer reading u8");
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        self.ensure(2)?;
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        self.ensure(4)?;
        let v = i32::from_be_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        self.ensure(4)?;
        let v = u32::from_be_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        self.ensure(8)?;
        let v = u64::from_be_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        self.ensure(8)?;
        let v = i64::from_be_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    /// Read a null-terminated string.
    pub fn read_cstring(&mut self) -> Result<String> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.data.len() {
            bail!("unterminated cstring in replication buffer");
        }
        let s = std::str::from_utf8(&self.data[start..self.pos])
            .map(|s| s.to_string())
            .unwrap_or_default();
        self.pos += 1; // consume the null byte
        Ok(s)
    }

    /// Read exactly `n` bytes.
    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.ensure(n)?;
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn ensure(&self, n: usize) -> Result<()> {
        if self.pos + n > self.data.len() {
            bail!(
                "buffer underrun: need {} bytes, have {}",
                n,
                self.data.len() - self.pos
            );
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LSN formatting helper
// ─────────────────────────────────────────────────────────────────────────────

pub fn fmt_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

// ─────────────────────────────────────────────────────────────────────────────
// Relation cache (OID → schema, table, columns)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

pub type RelationCache = HashMap<u32, RelationInfo>;

// ─────────────────────────────────────────────────────────────────────────────
// Top-level CopyData frame decoder
// ─────────────────────────────────────────────────────────────────────────────

/// Decode a raw CopyData frame from a logical replication stream.
///
/// Returns `Ok(Some(event))` for interesting events, `Ok(None)` for messages
/// we intentionally skip (Origin, Type, etc.), and `Err` for malformed data.
pub fn decode_copy_data(
    data: &[u8],
    rel_cache: &mut RelationCache,
) -> Result<Option<(WalEvent, u64)>> {
    let mut buf = Buf::new(data);
    let frame_type = buf.read_u8()?;

    match frame_type {
        b'w' => {
            // XLogData: wal_start(u64) + wal_end(u64) + send_time(i64) + data
            let _wal_start = buf.read_u64()?;
            let wal_end = buf.read_u64()?;
            let _send_time = buf.read_i64()?;
            let event = decode_pgoutput(&mut buf, rel_cache, wal_end)?;
            Ok(event.map(|e| (e, wal_end)))
        }
        b'k' => {
            // Primary keepalive: wal_end(u64) + send_time(i64) + reply_req(u8)
            let wal_end = buf.read_u64()?;
            let _send_time = buf.read_i64()?;
            let reply_requested = buf.read_u8()? != 0;
            Ok(Some((
                WalEvent::Keepalive {
                    wal_end: fmt_lsn(wal_end),
                    reply_requested,
                },
                wal_end,
            )))
        }
        other => {
            // Unknown frame type — skip gracefully
            bail!("unknown CopyData frame type: 0x{:02X}", other);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// pgoutput message decoder
// ─────────────────────────────────────────────────────────────────────────────

fn decode_pgoutput(
    buf: &mut Buf<'_>,
    rel_cache: &mut RelationCache,
    wal_end: u64,
) -> Result<Option<WalEvent>> {
    let msg_type = buf.read_u8()?;

    match msg_type {
        b'B' => decode_begin(buf),
        b'C' => decode_commit(buf),
        b'R' => decode_relation(buf, rel_cache),
        b'I' => decode_insert(buf, rel_cache, wal_end),
        b'U' => decode_update(buf, rel_cache, wal_end),
        b'D' => decode_delete(buf, rel_cache, wal_end),
        b'T' => decode_truncate(buf, rel_cache),
        b'O' => Ok(None), // Origin — skip
        b'Y' => Ok(None), // Type — skip
        b'M' => Ok(None), // Logical decoding message — skip
        other => {
            bail!(
                "unknown pgoutput message type: 0x{:02X} ('{}')",
                other,
                other as char
            );
        }
    }
}

// ── Begin ─────────────────────────────────────────────────────────────────────

fn decode_begin(buf: &mut Buf<'_>) -> Result<Option<WalEvent>> {
    let lsn = buf.read_u64()?;
    let commit_time = buf.read_i64()?;
    let xid = buf.read_u32()?;
    Ok(Some(WalEvent::Begin {
        lsn: fmt_lsn(lsn),
        commit_time,
        xid,
    }))
}

// ── Commit ────────────────────────────────────────────────────────────────────

fn decode_commit(buf: &mut Buf<'_>) -> Result<Option<WalEvent>> {
    let _flags = buf.read_u8()?;
    let lsn = buf.read_u64()?;
    let end_lsn = buf.read_u64()?;
    let commit_time = buf.read_i64()?;
    Ok(Some(WalEvent::Commit {
        lsn: fmt_lsn(lsn),
        end_lsn: fmt_lsn(end_lsn),
        commit_time,
    }))
}

// ── Relation ──────────────────────────────────────────────────────────────────

fn decode_relation(buf: &mut Buf<'_>, rel_cache: &mut RelationCache) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;
    let schema = buf.read_cstring()?;
    let table = buf.read_cstring()?;
    let _replica_identity = buf.read_u8()?;
    let ncols = buf.read_u16()?;

    let mut columns = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let flags = buf.read_u8()?;
        let is_key = (flags & 0x01) != 0;
        let name = buf.read_cstring()?;
        let type_id = buf.read_u32()?;
        let type_modifier = buf.read_i32()?;
        columns.push(ColumnDef {
            name,
            is_key,
            type_id,
            type_modifier,
        });
    }

    rel_cache.insert(
        rel_id,
        RelationInfo {
            schema: schema.clone(),
            table: table.clone(),
            columns: columns.clone(),
        },
    );

    Ok(Some(WalEvent::Relation {
        rel_id,
        schema,
        table,
        columns,
    }))
}

// ── Tuple decoding (shared by Insert / Update / Delete) ───────────────────────

fn decode_tuple_data(
    buf: &mut Buf<'_>,
    columns: &[ColumnDef],
) -> Result<HashMap<String, Option<String>>> {
    let ncols = buf.read_u16()? as usize;
    let mut row = HashMap::with_capacity(ncols);

    for i in 0..ncols {
        let col_name = columns
            .get(i)
            .map(|c| c.name.as_str())
            .unwrap_or("?")
            .to_string();

        let kind = buf.read_u8()?;
        match kind {
            b'n' => {
                // null
                row.insert(col_name, None);
            }
            b'u' => {
                // unchanged toast value
                row.insert(col_name, Some("<unchanged-toast>".to_string()));
            }
            b't' => {
                // text representation
                let len = buf.read_u32()? as usize;
                let bytes = buf.read_bytes(len)?;
                let val = String::from_utf8_lossy(bytes).to_string();
                row.insert(col_name, Some(val));
            }
            b'b' => {
                // binary representation (proto_version >= 2)
                let len = buf.read_u32()? as usize;
                let bytes = buf.read_bytes(len)?;
                // Represent as hex string — consumers can decode further
                let val = format!("\\x{}", hex::encode(bytes));
                row.insert(col_name, Some(val));
            }
            other => {
                bail!("unknown tuple column kind: 0x{:02X}", other);
            }
        }
    }
    Ok(row)
}

// ── Insert ────────────────────────────────────────────────────────────────────

fn decode_insert(
    buf: &mut Buf<'_>,
    rel_cache: &RelationCache,
    _wal_end: u64,
) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;
    let _new_tuple_flag = buf.read_u8()?; // always 'N'

    let info = rel_cache
        .get(&rel_id)
        .cloned()
        .unwrap_or_else(|| RelationInfo {
            schema: "unknown".into(),
            table: "unknown".into(),
            columns: vec![],
        });

    let new = decode_tuple_data(buf, &info.columns)?;

    Ok(Some(WalEvent::Insert {
        rel_id,
        schema: info.schema,
        table: info.table,
        new,
    }))
}

// ── Update ────────────────────────────────────────────────────────────────────

fn decode_update(
    buf: &mut Buf<'_>,
    rel_cache: &RelationCache,
    _wal_end: u64,
) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;

    let info = rel_cache
        .get(&rel_id)
        .cloned()
        .unwrap_or_else(|| RelationInfo {
            schema: "unknown".into(),
            table: "unknown".into(),
            columns: vec![],
        });

    // Optional old tuple ('O' = old key cols, 'K' = key cols only)
    let flag = buf.read_u8()?;
    let old = match flag {
        b'O' | b'K' => {
            let old_row = decode_tuple_data(buf, &info.columns)?;
            // After old tuple, there must be a 'N' flag for the new tuple
            let _new_flag = buf.read_u8()?;
            Some(old_row)
        }
        b'N' => None, // no old tuple, 'N' is the new tuple flag
        other => bail!("unexpected update flag: 0x{:02X}", other),
    };

    let new = decode_tuple_data(buf, &info.columns)?;

    Ok(Some(WalEvent::Update {
        rel_id,
        schema: info.schema,
        table: info.table,
        old,
        new,
    }))
}

// ── Delete ────────────────────────────────────────────────────────────────────

fn decode_delete(
    buf: &mut Buf<'_>,
    rel_cache: &RelationCache,
    _wal_end: u64,
) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;
    let _flag = buf.read_u8()?; // 'O' or 'K'

    let info = rel_cache
        .get(&rel_id)
        .cloned()
        .unwrap_or_else(|| RelationInfo {
            schema: "unknown".into(),
            table: "unknown".into(),
            columns: vec![],
        });

    let old = decode_tuple_data(buf, &info.columns)?;

    Ok(Some(WalEvent::Delete {
        rel_id,
        schema: info.schema,
        table: info.table,
        old,
    }))
}

// ── Truncate ──────────────────────────────────────────────────────────────────

fn decode_truncate(buf: &mut Buf<'_>, rel_cache: &RelationCache) -> Result<Option<WalEvent>> {
    let nrels = buf.read_u32()?;
    let options = buf.read_u8()?;
    let cascade = (options & 0x01) != 0;
    let restart_seqs = (options & 0x02) != 0;

    let mut rel_ids = Vec::with_capacity(nrels as usize);
    let mut tables = Vec::with_capacity(nrels as usize);

    for _ in 0..nrels {
        let rid = buf.read_u32()?;
        rel_ids.push(rid);
        if let Some(info) = rel_cache.get(&rid) {
            tables.push(format!("{}.{}", info.schema, info.table));
        } else {
            tables.push(format!("<rel:{rid}>"));
        }
    }

    Ok(Some(WalEvent::Truncate {
        rel_ids,
        tables,
        cascade,
        restart_seqs,
    }))
}
