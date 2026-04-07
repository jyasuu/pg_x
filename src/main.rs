mod commands;
mod utils;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{export, info, query};
use utils::config::Config;

/// pgx — PostgreSQL power CLI (beyond psql & pg_*)
#[derive(Parser)]
#[command(
    name = "pgx",
    version,
    about = "A feature-rich PostgreSQL CLI tool",
    long_about = None,
    arg_required_else_help = true,
)]
struct Cli {
    /// PostgreSQL connection URL
    /// e.g. postgres://user:pass@localhost:5432/mydb
    #[arg(short = 'U', long = "url", env = "DATABASE_URL", global = true)]
    url: Option<String>,

    /// Named connection from ~/.pgx/config.toml
    #[arg(short = 'c', long = "conn", global = true)]
    connection: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export SQL query results to Excel (.xlsx), CSV, or JSON
    Export(export::ExportArgs),

    /// Run a SQL query and display results in the terminal
    Query(query::QueryArgs),

    /// Show database / server information
    Info(info::InfoArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Resolve connection URL: flag > named connection > config default
    let url = resolve_url(cli.url, cli.connection)?;

    match cli.command {
        Commands::Export(args) => export::run(url, args).await,
        Commands::Query(args) => query::run(url, args).await,
        Commands::Info(args) => info::run(url, args).await,
    }
}

fn resolve_url(flag: Option<String>, conn_name: Option<String>) -> Result<String> {
    if let Some(u) = flag {
        return Ok(u);
    }

    let cfg = Config::load()?;

    if let Some(name) = conn_name {
        return cfg
            .connection(&name)
            .ok_or_else(|| anyhow::anyhow!("No connection named '{}' in config", name));
    }

    cfg.default_url().ok_or_else(|| {
        anyhow::anyhow!(
            "No database URL supplied.\n\
             Use -U <url>, set DATABASE_URL, or add a default in ~/.pgx/config.toml"
        )
    })
}
