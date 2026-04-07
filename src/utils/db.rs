use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

/// Connect to Postgres using a URL.
/// Returns a connected `tokio_postgres::Client`.
/// The connection task is spawned on the Tokio runtime and runs in the background.
pub async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .with_context(|| format!("Failed to connect to: {url}"))?;

    // Spawn the connection driver; it will log errors to stderr
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });

    Ok(client)
}
