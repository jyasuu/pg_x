use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::downstream::{contract::NotifyEvent, sink::Downstream};

/// Wait for SIGINT (Ctrl-C) or SIGTERM (kill / container stop).
/// Returns when either signal is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[derive(Args)]
pub struct ListenArgs {
    /// NOTIFY channel(s) to subscribe to (repeatable: -C orders -C inventory)
    #[arg(short = 'C', long = "channel", required = true)]
    pub channels: Vec<String>,

    #[command(subcommand)]
    pub downstream: DownstreamCommand,
}

#[derive(Subcommand)]
pub enum DownstreamCommand {
    /// Forward events to RabbitMQ (AMQP)
    #[cfg(feature = "rabbitmq")]
    Rabbitmq(RabbitmqArgs),

    /// Forward events to Apache Kafka
    #[cfg(feature = "kafka")]
    Kafka(KafkaArgs),

    /// Forward events via HTTP webhook (POST)
    #[cfg(feature = "webhook")]
    Webhook(WebhookArgs),

    /// Forward events to a shell command
    Shell(ShellArgs),
}

#[cfg(feature = "rabbitmq")]
#[derive(Args)]
pub struct RabbitmqArgs {
    #[arg(
        long,
        env = "AMQP_URL",
        default_value = "amqp://guest:guest@localhost:5672/%2F"
    )]
    pub amqp_url: String,
    #[arg(long, default_value = "pgx")]
    pub exchange: String,
    #[arg(long, default_value = "pgx.notify")]
    pub routing_key: String,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[cfg(feature = "kafka")]
#[derive(Args)]
pub struct KafkaArgs {
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    pub brokers: String,
    #[arg(long, default_value = "pgx-notify")]
    pub topic: String,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[cfg(feature = "webhook")]
#[derive(Args)]
pub struct WebhookArgs {
    #[arg(long, env = "WEBHOOK_URL")]
    pub url: String,
    #[arg(long = "header", value_parser = parse_key_val)]
    pub headers: Vec<(String, String)>,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[derive(Args)]
pub struct ShellArgs {
    /// Command executed via `sh -c`. Env vars: PGX_CHANNEL, PGX_PID,
    /// PGX_PAYLOAD (+ PGX_EVENT_TYPE, PGX_SCHEMA_VERSION in contract mode)
    #[arg(long)]
    pub command: String,
    #[arg(long = "env", value_parser = parse_key_val)]
    pub envs: Vec<(String, String)>,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[derive(Clone, ValueEnum)]
pub enum ForwardMode {
    /// Pass the raw NOTIFY payload as the message body.
    Simple,
    /// Parse the payload as a ContractMessage and use embedded routing hints.
    Contract,
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("Expected KEY=VALUE, got '{s}'"))
}

pub async fn run(url: String, args: ListenArgs) -> Result<()> {
    let sink: Arc<dyn Downstream> = build_downstream(&args.downstream).await?;

    // Backoff parameters for reconnection.
    const BASE_DELAY_MS: u64 = 1_000;
    const MAX_DELAY_MS: u64 = 60_000;
    const MAX_ATTEMPTS: u32 = 10;

    // Pin the shutdown future outside the retry loop so a signal cancels
    // both the event loop and any in-progress backoff sleep.
    tokio::pin!(let shutdown = shutdown_signal(););

    let mut attempt: u32 = 0;

    loop {
        // ── Backoff sleep (skipped on first attempt) ──────────────────────────
        if attempt > 0 {
            // Exponential backoff: 1s, 2s, 4s, … capped at 60s, ±20% jitter.
            let base = (BASE_DELAY_MS * (1u64 << (attempt - 1).min(6))).min(MAX_DELAY_MS);
            let jitter = base / 5;
            let delay_ms = base - jitter + (rand::random::<u64>() % (jitter * 2 + 1));
            let delay = std::time::Duration::from_millis(delay_ms);

            warn!(
                attempt,
                max_attempts = MAX_ATTEMPTS,
                delay_secs = delay.as_secs_f32(),
                "Connection lost, reconnecting…"
            );

            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("Signal received, shutting down");
                    return Ok(());
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }

        if attempt >= MAX_ATTEMPTS {
            return Err(anyhow::anyhow!(
                "Giving up after {MAX_ATTEMPTS} consecutive connection failures"
            ));
        }

        // ── Connect ───────────────────────────────────────────────────────────
        info!("Connecting to PostgreSQL…");

        let (client, connection) = match tokio_postgres::connect(&url, NoTls).await {
            Ok(pair) => pair,
            Err(e) => {
                error!(error = %e, "Connection failed");
                attempt += 1;
                continue;
            }
        };

        // ── Spawn Drainer task ────────────────────────────────────────────────
        // The Drainer bridges the tokio-postgres connection future into an mpsc
        // channel we can select! on. A fresh channel is created each reconnect
        // so there is no risk of stale notifications from a previous session.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<tokio_postgres::Notification>();

        tokio::spawn({
            let tx = tx.clone();
            async move {
                use std::future::Future;
                use std::pin::Pin;
                use std::task::{Context as Cx, Poll};

                struct Drainer {
                    conn: tokio_postgres::Connection<
                        tokio_postgres::Socket,
                        tokio_postgres::tls::NoTlsStream,
                    >,
                    tx: tokio::sync::mpsc::UnboundedSender<tokio_postgres::Notification>,
                }

                impl Future for Drainer {
                    type Output = ();
                    fn poll(mut self: Pin<&mut Self>, cx: &mut Cx<'_>) -> Poll<()> {
                        loop {
                            match self.conn.poll_message(cx) {
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(None) => return Poll::Ready(()),
                                Poll::Ready(Some(Ok(
                                    tokio_postgres::AsyncMessage::Notification(n),
                                ))) => {
                                    let _ = self.tx.send(n);
                                }
                                Poll::Ready(Some(Ok(_))) => {}
                                Poll::Ready(Some(Err(e))) => {
                                    error!(error = %e, "PostgreSQL connection error");
                                    return Poll::Ready(());
                                }
                            }
                        }
                    }
                }

                Drainer {
                    conn: connection,
                    tx,
                }
                .await;
            }
        });

        // ── LISTEN on each channel ────────────────────────────────────────────
        let mut listen_ok = true;
        for ch in &args.channels {
            match client.execute(&format!("LISTEN \"{ch}\""), &[]).await {
                Ok(_) => info!(channel = %ch, "Listening on channel"),
                Err(e) => {
                    error!(channel = %ch, error = %e, "LISTEN failed");
                    listen_ok = false;
                    break;
                }
            }
        }
        if !listen_ok {
            attempt += 1;
            continue;
        }

        info!(sink = sink.name(), "Forwarding events — Ctrl-C to stop");

        // ── Event loop for this connection ────────────────────────────────────
        // Returns true if the disconnect was unplanned (should reconnect).
        let disconnected = loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => {
                    info!("Signal received, shutting down");
                    return Ok(());
                }

                maybe_n = rx.recv() => {
                    let Some(n) = maybe_n else {
                        // Drainer exited — connection dropped.
                        break true;
                    };

                    let event = NotifyEvent {
                        channel: n.channel().to_string(),
                        payload: n.payload().to_string(),
                        pid: n.process_id() as i32,
                    };

                    // Per-event record at debug level.
                    // - Text mode (default): set RUST_LOG=debug to see it.
                    // - JSON mode (--log-json): becomes a structured record
                    //   parseable by log aggregators.
                    debug!(
                        channel = %event.channel,
                        pid = event.pid,
                        payload = %event.payload,
                        "NOTIFY received"
                    );

                    if let Err(e) = sink.send(&event).await {
                        error!(sink = sink.name(), error = %e, "Downstream send failed");
                    }
                }
            }
        };

        if disconnected {
            warn!("Connection dropped unexpectedly");
            attempt += 1;
        } else {
            break;
        }
    }

    info!("Stopped");
    Ok(())
}

async fn build_downstream(cmd: &DownstreamCommand) -> Result<Arc<dyn Downstream>> {
    match cmd {
        #[cfg(feature = "rabbitmq")]
        DownstreamCommand::Rabbitmq(a) => {
            use crate::downstream::rabbitmq::rabbitmq::{
                ContractRabbitMqDownstream, SimpleRabbitMqDownstream,
            };
            match a.mode {
                ForwardMode::Simple => Ok(Arc::new(
                    SimpleRabbitMqDownstream::connect(&a.amqp_url, &a.exchange, &a.routing_key)
                        .await?,
                )),
                ForwardMode::Contract => Ok(Arc::new(
                    ContractRabbitMqDownstream::connect(&a.amqp_url, &a.exchange, &a.routing_key)
                        .await?,
                )),
            }
        }

        #[cfg(feature = "kafka")]
        DownstreamCommand::Kafka(a) => {
            use crate::downstream::kafka::kafka::{ContractKafkaDownstream, SimpleKafkaDownstream};
            match a.mode {
                ForwardMode::Simple => Ok(Arc::new(SimpleKafkaDownstream::connect(
                    &a.brokers, &a.topic,
                )?)),
                ForwardMode::Contract => Ok(Arc::new(ContractKafkaDownstream::connect(
                    &a.brokers, &a.topic,
                )?)),
            }
        }

        #[cfg(feature = "webhook")]
        DownstreamCommand::Webhook(a) => {
            use crate::downstream::webhook::webhook::{
                ContractWebhookDownstream, SimpleWebhookDownstream,
            };
            let headers: HashMap<String, String> = a.headers.iter().cloned().collect();
            match a.mode {
                ForwardMode::Simple => Ok(Arc::new(SimpleWebhookDownstream::new(&a.url))),
                ForwardMode::Contract => {
                    Ok(Arc::new(ContractWebhookDownstream::new(&a.url, headers)))
                }
            }
        }

        DownstreamCommand::Shell(a) => {
            use crate::downstream::shell::shell::ShellDownstream;
            let base_env: HashMap<String, String> = a.envs.iter().cloned().collect();
            let contract_mode = matches!(a.mode, ForwardMode::Contract);
            Ok(Arc::new(ShellDownstream::new(
                &a.command,
                base_env,
                contract_mode,
            )))
        }
    }
}
