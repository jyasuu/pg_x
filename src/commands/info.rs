use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::utils::db::connect;

#[derive(Args, Debug)]
pub struct InfoArgs {
    /// Show active connections
    #[arg(long)]
    pub connections: bool,

    /// Show all databases
    #[arg(long)]
    pub databases: bool,

    /// Show tables in current database
    #[arg(long)]
    pub tables: bool,

    /// Show server version and settings
    #[arg(long, default_value_t = true)]
    pub version: bool,
}

pub async fn run(url: String, args: InfoArgs) -> Result<()> {
    let client = connect(&url).await?;

    if args.version {
        let row = client.query_one("SELECT version()", &[]).await?;
        let v: String = row.get(0);
        println!("{} {}", "PostgreSQL:".cyan().bold(), v);
    }

    if args.databases {
        println!("\n{}", "── Databases ──".cyan().bold());
        let rows = client
            .query(
                "SELECT datname, pg_size_pretty(pg_database_size(datname)) AS size \
                 FROM pg_database WHERE datistemplate = false ORDER BY datname",
                &[],
            )
            .await?;
        for r in &rows {
            let name: String = r.get(0);
            let size: String = r.get(1);
            println!("  {:<30} {}", name.yellow(), size.dimmed());
        }
    }

    if args.tables {
        println!("\n{}", "── Tables ──".cyan().bold());
        let rows = client
            .query(
                "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) \
                 FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') \
                 ORDER BY schemaname, tablename",
                &[],
            )
            .await?;
        for r in &rows {
            let schema: String = r.get(0);
            let table: String = r.get(1);
            let size: String = r.get(2);
            println!(
                "  {}.{:<40} {}",
                schema.dimmed(),
                table.yellow(),
                size.dimmed()
            );
        }
    }

    if args.connections {
        println!("\n{}", "── Active Connections ──".cyan().bold());
        let rows = client
            .query(
                "SELECT pid, usename, application_name, client_addr, state, query_start \
                 FROM pg_stat_activity WHERE state IS NOT NULL ORDER BY query_start DESC NULLS LAST",
                &[],
            )
            .await?;
        for r in &rows {
            let pid: i32 = r.get(0);
            let user: Option<String> = r.get(1);
            let app: Option<String> = r.get(2);
            let state: Option<String> = r.get(4);
            println!(
                "  pid={} user={} app={} state={}",
                pid.to_string().yellow(),
                user.unwrap_or_default().cyan(),
                app.unwrap_or_default().dimmed(),
                state.unwrap_or_default().green(),
            );
        }
    }

    Ok(())
}
