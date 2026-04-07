use anyhow::{Context, Result};
use std::path::Path;

use crate::utils::format::RowSet;

pub fn write_csv(rowset: &RowSet, path: &Path) -> Result<()> {
    let file = std::fs::File::create(path)
        .with_context(|| format!("Cannot create CSV file: {}", path.display()))?;
    let mut wtr = csv::Writer::from_writer(file);

    // Header
    wtr.write_record(&rowset.columns)
        .context("Write CSV header")?;

    // Rows
    for row in &rowset.rows {
        wtr.write_record(row).context("Write CSV row")?;
    }

    wtr.flush().context("Flush CSV")?;
    Ok(())
}
