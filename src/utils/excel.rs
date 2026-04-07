use anyhow::{Context, Result};
use rust_xlsxwriter::*;
use std::path::Path;

use crate::utils::format::RowSet;

/// Write a `RowSet` to an Excel file with rich formatting.
pub fn write_excel(
    rowset: &RowSet,
    path: &Path,
    sheet_name: &str,
    freeze_header: bool,
    autofit: bool,
    stripe: bool,
) -> Result<()> {
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();

    ws.set_name(sheet_name).context("Invalid sheet name")?;

    // ── Formats ──────────────────────────────────────────────────────────────
    let fmt_header = Format::new()
        .set_bold()
        .set_font_color(Color::White)
        .set_background_color(Color::RGB(0x2E_74_B5)) // Office blue
        .set_border_bottom(FormatBorder::Medium)
        .set_text_wrap();
    // .set_align(FormatVerticalAlignment::Bottom);

    let fmt_even = Format::new().set_background_color(Color::RGB(0xDD_EA_F1));

    let fmt_odd = Format::new(); // default (white)

    let fmt_null = Format::new()
        .set_font_color(Color::RGB(0xAA_AA_AA))
        .set_italic();

    // ── Header row ───────────────────────────────────────────────────────────
    for (col, name) in rowset.columns.iter().enumerate() {
        ws.write_with_format(0, col as u16, name.as_str(), &fmt_header)
            .context("Write header")?;
    }

    // Row height for header
    ws.set_row_height(0, 22.0).ok();

    // ── Data rows ────────────────────────────────────────────────────────────
    // Track max content width per column for autofit
    let mut col_widths: Vec<f64> = rowset.columns.iter().map(|c| c.len() as f64).collect();

    for (row_idx, row) in rowset.rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        let row_fmt = if stripe && row_idx % 2 == 1 {
            Some(&fmt_even)
        } else {
            Some(&fmt_odd)
        };

        for (col_idx, cell) in row.iter().enumerate() {
            let col = col_idx as u16;

            // Track column width
            let cell_len = cell.chars().count() as f64;
            if cell_len > col_widths[col_idx] {
                col_widths[col_idx] = cell_len;
            }

            if cell == "NULL" {
                ws.write_with_format(excel_row, col, "NULL", &fmt_null)
                    .context("Write null cell")?;
            } else {
                // Try numeric, then boolean, then string
                if let Ok(n) = cell.parse::<f64>() {
                    if let Some(fmt) = row_fmt {
                        ws.write_number_with_format(excel_row, col, n, fmt)
                    } else {
                        ws.write_number(excel_row, col, n)
                    }
                    .context("Write number cell")?;
                } else if cell == "true" || cell == "false" {
                    let b = cell == "true";
                    if let Some(fmt) = row_fmt {
                        ws.write_boolean_with_format(excel_row, col, b, fmt)
                    } else {
                        ws.write_boolean(excel_row, col, b)
                    }
                    .context("Write bool cell")?;
                } else {
                    if let Some(fmt) = row_fmt {
                        ws.write_with_format(excel_row, col, cell.as_str(), fmt)
                    } else {
                        ws.write(excel_row, col, cell.as_str())
                    }
                    .context("Write string cell")?;
                }
            }
        }
    }

    // ── Autofit ──────────────────────────────────────────────────────────────
    if autofit {
        for (col_idx, &width) in col_widths.iter().enumerate() {
            // Clamp: min 6, max 60 chars
            let w = width.clamp(6.0, 60.0) + 2.0; // +2 padding
            ws.set_column_width(col_idx as u16, w)
                .context("Set column width")?;
        }
    }

    // ── Freeze header ────────────────────────────────────────────────────────
    if freeze_header {
        ws.set_freeze_panes(1, 0).context("Freeze header")?;
    }

    // ── Auto filter ──────────────────────────────────────────────────────────
    if !rowset.columns.is_empty() {
        ws.autofilter(
            0,
            0,
            rowset.rows.len() as u32,
            (rowset.columns.len() - 1) as u16,
        )
        .context("Add autofilter")?;
    }

    // ── Save ─────────────────────────────────────────────────────────────────
    wb.save(path)
        .with_context(|| format!("Cannot save Excel file: {}", path.display()))?;

    Ok(())
}
