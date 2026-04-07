use anyhow::{Context, Result};
use std::path::Path;

use crate::utils::format::RowSet;

pub fn write_json(rowset: &RowSet, path: &Path) -> Result<()> {
    let value = rowset.to_json_value();
    let content = serde_json::to_string_pretty(&value).context("Serialize JSON")?;
    std::fs::write(path, content)
        .with_context(|| format!("Cannot write JSON file: {}", path.display()))?;
    Ok(())
}
