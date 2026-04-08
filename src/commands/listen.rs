use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::NoTls;

use crate::downstream::{contract::NotifyEvent, sink::Downstream};

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

    println!("{} Connecting to PostgreSQL…", "pgx-listen".cyan().bold());

    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<tokio_postgres::Notification>();

    tokio::spawn(async move {
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
                        Poll::Ready(Some(Ok(tokio_postgres::AsyncMessage::Notification(n)))) => {
                            let _ = self.tx.send(n);
                        }
                        Poll::Ready(Some(Ok(_))) => {}
                        Poll::Ready(Some(Err(e))) => {
                            eprintln!("{} connection error: {e}", "✗".red());
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
    });

    for ch in &args.channels {
        client
            .execute(&format!("LISTEN \"{ch}\""), &[])
            .await
            .with_context(|| format!("LISTEN {ch} failed"))?;
        println!("{} Listening on channel '{}'", "▶".green(), ch.yellow());
    }

    println!(
        "{} Forwarding to '{}' — Ctrl-C to stop.",
        "▶".green(),
        sink.name().cyan()
    );

    while let Some(n) = rx.recv().await {
        let event = NotifyEvent {
            channel: n.channel().to_string(),
            payload: n.payload().to_string(),
            pid: n.process_id() as i32,
        };

        println!(
            "{} [{}] pid={} payload={}",
            "◀".blue(),
            event.channel.yellow(),
            event.pid,
            &event.payload,
        );

        if let Err(e) = sink.send(&event).await {
            eprintln!("{} downstream '{}' error: {e:#}", "✗".red(), sink.name());
        }
    }

    println!("{} Connection closed.", "pgx-listen".cyan().bold());
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
