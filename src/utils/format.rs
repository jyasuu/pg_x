use anyhow::Result;
use serde_json::{json, Value};
use tokio_postgres::Row;

/// A serializable, format-agnostic result set.
#[derive(Debug)]
pub struct RowSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl RowSet {
    /// Convert raw `tokio_postgres::Row` slice into a `RowSet`.
    /// `limit = 0` means no limit.
    pub fn from_pg_rows(rows: &[Row], limit: usize) -> Result<Self> {
        if rows.is_empty() {
            return Ok(RowSet {
                columns: vec![],
                rows: vec![],
            });
        }

        // Column names from the first row's columns()
        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_owned())
            .collect();

        let take = if limit == 0 {
            rows.len()
        } else {
            limit.min(rows.len())
        };

        let mut result_rows: Vec<Vec<String>> = Vec::with_capacity(take);

        for row in rows.iter().take(take) {
            let cells: Vec<String> = (0..columns.len())
                .map(|i| pg_cell_to_string(row, i))
                .collect();
            result_rows.push(cells);
        }

        Ok(RowSet {
            columns,
            rows: result_rows,
        })
    }

    /// Convert to a `serde_json::Value` array of objects.
    pub fn to_json_value(&self) -> Value {
        let objects: Vec<Value> = self
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, Value> = self
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(k, v)| (k.clone(), json!(v)))
                    .collect();
                Value::Object(obj)
            })
            .collect();
        Value::Array(objects)
    }
}

/// Try to extract a human-readable string for any supported Postgres column type.
fn pg_cell_to_string(row: &Row, idx: usize) -> String {
    let col_type = row.columns()[idx].type_().name();

    macro_rules! try_get {
        ($t:ty) => {
            if let Ok(Some(v)) = row.try_get::<_, Option<$t>>(idx) {
                return v.to_string();
            }
            if let Ok(None::<$t>) = row.try_get(idx) {
                return "NULL".to_owned();
            }
        };
    }

    match col_type {
        "bool" => {
            try_get!(bool);
        }
        "int2" => {
            try_get!(i16);
        }
        "int4" => {
            try_get!(i32);
        }
        "int8" | "oid" => {
            try_get!(i64);
        }
        "float4" => {
            try_get!(f32);
        }
        "float8" | "numeric" => {
            try_get!(f64);
        }
        "text" | "varchar" | "char" | "bpchar" | "name" | "citext" => {
            try_get!(String);
        }
        "json" | "jsonb" => {
            if let Ok(Some(v)) = row.try_get::<_, Option<serde_json::Value>>(idx) {
                return v.to_string();
            }
            return "NULL".to_owned();
        }
        "uuid" => {
            try_get!(uuid::Uuid);
        }
        "timestamp" | "timestamptz" => {
            if let Ok(Some(v)) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                return v.format("%Y-%m-%d %H:%M:%S UTC").to_string();
            }
            if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
                return v.format("%Y-%m-%d %H:%M:%S").to_string();
            }
            return "NULL".to_owned();
        }
        "date" => {
            if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
                return v.to_string();
            }
            return "NULL".to_owned();
        }
        _ => {}
    }

    // Generic fallback: try String first, then format unknown
    if let Ok(Some(v)) = row.try_get::<_, Option<String>>(idx) {
        return v;
    }
    if row.try_get::<_, Option<String>>(idx).is_ok() {
        return "NULL".to_owned();
    }

    format!("<{col_type}>")
}
