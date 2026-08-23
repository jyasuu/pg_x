use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Top-level config file: ~/.pgx/config.toml
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    /// The name of the default connection profile
    pub default: Option<String>,

    /// Named connection profiles
    #[serde(default)]
    pub connections: HashMap<String, Connection>,

    /// Resolver mappings for GraphQL composition
    #[serde(default)]
    pub resolvers: HashMap<String, ResolverConfig>,

    /// Named SQL mutations applied to a second Postgres database by the
    /// `consume` `graphql-mutate` sink.
    #[serde(default)]
    pub mutations: HashMap<String, MutationConfig>,
}

/// Behavior when the notification channel is full.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelFullBehavior {
    /// Block the drainer until the downstream consumes a notification.
    Block,
    /// Drop the oldest notification in the channel (current default).
    #[default]
    DropOldest,
    /// Grow the channel capacity (unbounded — use with caution).
    Grow,
}

impl std::fmt::Display for ChannelFullBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Block => write!(f, "block"),
            Self::DropOldest => write!(f, "drop_oldest"),
            Self::Grow => write!(f, "grow"),
        }
    }
}

impl std::str::FromStr for ChannelFullBehavior {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "block" => Ok(Self::Block),
            "drop_oldest" => Ok(Self::DropOldest),
            "grow" => Ok(Self::Grow),
            other => Err(format!(
                "unknown channel_full_behavior '{other}'; expected block|drop_oldest|grow"
            )),
        }
    }
}

/// Configuration for a single resolver — maps a GraphQL field to SQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolverConfig {
    /// The SQL query string (or path to a .sql file)
    pub sql: String,
    /// Which variable/column to bind as $1
    pub param: Option<String>,
    /// Column name used for DataLoader batching (ANY($1))
    pub batch_by: Option<String>,
    /// Optional named connection override (e.g. read replica)
    pub connection: Option<String>,
}

/// Configuration for a single mutation — SQL applied to a second Postgres
/// database ("DB B") by the `consume` `graphql-mutate` sink. Fields of the
/// composed document are bound to the statement's positional parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationConfig {
    /// Target database URL; falls back to `--mutate-target-url`.
    pub target_url: Option<String>,
    /// Single SQL statement (INSERT/UPDATE) with positional $1…$n parameters,
    /// executed in autocommit. Exactly one of `sql` / `statements`.
    #[serde(default)]
    pub sql: Option<String>,
    /// Ordered statement list executed in ONE transaction (all-or-nothing).
    /// Each statement sees the same $1…$n parameters. Exactly one of
    /// `sql` / `statements`.
    #[serde(default)]
    pub statements: Option<Vec<String>>,
    /// Document fields bound to the parameters, in order ($1, $2, …).
    #[serde(default)]
    pub params: Vec<String>,
}

impl MutationConfig {
    /// The statements to execute, regardless of which config form was used.
    pub fn statement_list(&self) -> &[String] {
        match &self.statements {
            Some(stmts) => stmts,
            None => match &self.sql {
                Some(sql) => std::slice::from_ref(sql),
                None => &[],
            },
        }
    }

    /// Exactly one of `sql` / `statements` must be set.
    pub fn validate(&self) -> Result<()> {
        match (&self.sql, &self.statements) {
            (Some(_), Some(_)) => {
                anyhow::bail!("mutation config must set either 'sql' or 'statements', not both")
            }
            (None, None) => {
                anyhow::bail!("mutation config needs 'sql' or 'statements'")
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Connection {
    pub url: String,
    pub description: Option<String>,

    /// Defaults for the `listen` sub-command when using this connection.
    #[serde(default)]
    pub listen: Option<ListenSinkConfig>,

    /// Defaults for the `replicate` sub-command when using this connection.
    #[serde(default)]
    pub replicate: Option<ReplicateSinkConfig>,

    /// Defaults for the `consume` sub-command when using this connection.
    #[serde(default)]
    pub consume: Option<ConsumeConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListenSinkConfig {
    /// NOTIFY channels to subscribe to.
    #[serde(default)]
    pub channels: Vec<String>,

    /// Maximum reconnect attempts (0 = infinite).
    pub max_reconnect_attempts: Option<u32>,

    /// Base reconnect delay in milliseconds.
    pub reconnect_base_ms: Option<u64>,

    /// Maximum reconnect delay cap in milliseconds.
    pub reconnect_max_ms: Option<u64>,

    /// Behavior when the internal notification channel is full.
    /// Options: "block", "drop_oldest" (default), "grow".
    pub channel_full_behavior: Option<ChannelFullBehavior>,

    /// Downstream sink kind and its options.
    pub sink: Option<DownstreamSinkKind>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateSinkConfig {
    /// Replication slot name.
    pub slot: Option<String>,

    /// Publication name(s).
    #[serde(default)]
    pub publications: Vec<String>,

    /// Table filter(s).
    #[serde(default)]
    pub tables: Vec<String>,

    /// Operation filter(s).
    #[serde(default)]
    pub ops: Vec<String>,

    /// Row-level WHERE filters.
    #[serde(default)]
    pub filters: Vec<String>,

    /// Column drop rules.
    #[serde(default)]
    pub drop_cols: Vec<String>,

    /// Column rename rules.
    #[serde(default)]
    pub rename: Vec<String>,

    /// Use a temporary slot.
    pub temporary: Option<bool>,

    /// Emit BEGIN/COMMIT events.
    pub emit_txn_boundaries: Option<bool>,

    /// Emit RELATION events.
    pub emit_schema: Option<bool>,

    /// Maximum reconnect attempts (0 = infinite). Default: 10.
    pub max_reconnect_attempts: Option<u32>,

    /// Base reconnect delay in milliseconds. Default: 1000.
    pub reconnect_base_ms: Option<u64>,

    /// Maximum reconnect delay cap in milliseconds. Default: 60000.
    pub reconnect_max_ms: Option<u64>,

    /// Primary downstream sink kind and its options.
    pub sink: Option<DownstreamSinkKind>,

    /// Additional downstream sinks for fan-out (repeatable).
    #[serde(default)]
    pub additional_sinks: Vec<DownstreamSinkKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DownstreamSinkKind {
    /// Print events as JSON to stdout.
    Stdout {
        /// Pretty-print JSON output.
        pretty: Option<bool>,
    },
    /// Forward events to a shell command.
    Shell {
        /// Shell command to execute.
        command: String,
        /// Extra environment variables.
        envs: Option<Vec<String>>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Forward events via HTTP webhook.
    Webhook {
        /// Webhook URL.
        url: String,
        /// HTTP headers (key=value).
        headers: Option<Vec<String>>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Forward events to RabbitMQ.
    Rabbitmq {
        /// AMQP URL.
        amqp_url: Option<String>,
        /// Exchange name.
        exchange: Option<String>,
        /// Routing key.
        routing_key: Option<String>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Forward events to Apache Kafka.
    Kafka {
        /// Kafka brokers.
        brokers: Option<String>,
        /// Topic name.
        topic: Option<String>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Forward events to NATS JetStream.
    Nats {
        /// NATS server URL.
        url: Option<String>,
        /// Publish subject.
        subject: Option<String>,
        /// JetStream stream the subject is captured by.
        stream: Option<String>,
        /// Create the stream if missing.
        create_stream: Option<bool>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Index documents into Elasticsearch.
    Elasticsearch {
        /// Elasticsearch URL (e.g. http://localhost:9200).
        url: String,
        /// Elasticsearch index name.
        index: String,
        /// Optional field to use as document _id.
        id_field: Option<String>,
        /// Schema directory override.
        schema_dir: Option<String>,
    },

    /// Apply WAL changes directly to another PostgreSQL database.
    Postgres {
        /// Target database URL.
        target_url: String,
        /// Optional schema/table remapping (src_schema.src_table=tgt_schema.tgt_table).
        schema_map: Option<Vec<String>>,
        /// Maximum statements per transaction batch.
        batch_size: Option<u32>,
    },

    /// Write WAL events to Apache Parquet files.
    Parquet {
        /// Output directory for Parquet files.
        output_dir: Option<String>,
        /// Maximum rows per file before rotation.
        max_rows: Option<usize>,
        /// Flush interval in seconds.
        flush_interval: Option<u64>,
        /// Compression: snappy, zstd, or none.
        compression: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumeConfig {
    /// Message source configuration.
    pub source: ConsumeSourceKind,
    /// Sink for the composed GraphQL document.
    pub sink: ConsumeSinkKind,
    /// Query mode: "contract" (from message event_type) or "simple" (fixed).
    pub query_mode: Option<String>,
    /// Query name (required in simple mode).
    pub query: Option<String>,
    /// Maximum resolver recursion depth.
    pub max_depth: Option<u32>,
    /// Schema directory override.
    pub schema_dir: Option<String>,
    /// Error mode: "lenient" or "strict".
    pub on_error: Option<String>,
    /// Dedupe redelivered messages and derive stable sink keys from the
    /// message id.
    #[serde(default)]
    pub idempotent: Option<bool>,
    /// How long (seconds) to remember seen message ids for dedupe.
    #[serde(default)]
    pub dedup_ttl: Option<u64>,
    /// Embedding-stage options; when present the sink is wrapped in the
    /// embedding decorator.
    #[serde(default)]
    pub embed: Option<EmbedConfig>,
    /// Additional sinks for fan-out alongside `sink`.
    #[serde(default)]
    pub additional_sinks: Vec<ConsumeSinkKind>,
    /// Default table name for `postgres-vector` sinks.
    #[serde(default)]
    pub vector_table: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConsumeSourceKind {
    Rabbitmq {
        amqp_url: Option<String>,
        queue: Option<String>,
        exchange: Option<String>,
        routing_key: Option<String>,
        /// Per-consumer prefetch: max unacknowledged messages (0 = no limit).
        prefetch_count: Option<u16>,
    },
    Kafka {
        brokers: Option<String>,
        topic: Option<String>,
        group_id: Option<String>,
    },
    Nats {
        /// NATS server URL.
        url: Option<String>,
        /// JetStream stream name.
        stream: Option<String>,
        /// Subject filter within the stream (wildcards allowed).
        subject: Option<String>,
        /// Durable pull-consumer name; reuses the server-side cursor across
        /// restarts.
        consumer: Option<String>,
        /// Create the stream (and durable consumer) if missing.
        create_stream: Option<bool>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConsumeSinkKind {
    /// Print the composed document as JSON to stdout.
    Stdout,
    /// Index the document into Elasticsearch.
    Elasticsearch {
        url: String,
        index: String,
        id_field: Option<String>,
    },
    /// POST the document as JSON to a webhook URL.
    Webhook {
        url: String,
        headers: Option<Vec<String>>,
    },
    /// Store the document in a key-value store (Redis / Memcached).
    Kv {
        /// KV store URL (redis://... or memcached://...).
        url: String,
        /// Field in the document to use as the cache key.
        key_field: Option<String>,
        /// Prefix to prepend to the cache key.
        key_prefix: Option<String>,
        /// TTL in seconds (0 = no expiry).
        ttl: Option<u64>,
    },
    /// Upsert the document's `embedding` field into a Postgres pgvector table.
    PostgresVector {
        /// Table name (default `chunk_embeddings`).
        table: Option<String>,
    },
    /// Execute a named mutation's SQL against a second Postgres database,
    /// binding composed-document fields to positional parameters.
    GraphqlMutate {
        /// Name of the `[mutations.<name>]` entry in ~/.pgx/config.toml
        /// (falls back to `--mutation`).
        mutation: Option<String>,
        /// Target database URL override (e.g. `--mutate-target-url`); used
        /// only when the mutation has no target_url of its own.
        target_url: Option<String>,
    },
}

/// Embedding-stage options for the `consume` command.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmbedConfig {
    /// Embedding API base URL (e.g. http://localhost:11434).
    pub url: Option<String>,
    /// Wire format: "ollama" or "openai" (default "ollama").
    pub api: Option<String>,
    /// Model name passed to the embedding API.
    pub model: Option<String>,
    /// Field whose value is embedded when no template is set (default "content").
    pub field: Option<String>,
    /// Template interpolating {{field}} / {{a.b}} from the document.
    pub template: Option<String>,
    /// Document field receiving the vector (default "embedding").
    pub output_field: Option<String>,
    /// Expected vector dimension; mismatches fail the sink stage.
    pub dim: Option<usize>,
}

/// Merge a CLI Option field from a config Option (CLI wins).
pub fn merge_opt<T: Clone>(field: &mut Option<T>, config: &Option<T>) {
    if field.is_none() {
        if let Some(val) = config {
            *field = Some(val.clone());
        }
    }
}

/// Merge a CLI bool field from a config Option<bool> (CLI wins).
/// Only sets the field to `true` if config says so and CLI hasn't set it.
pub fn merge_bool(field: &mut bool, config: Option<bool>) {
    if !*field {
        if let Some(true) = config {
            *field = true;
        }
    }
}

/// Merge a CLI Vec field from a config Vec (CLI wins).
pub fn merge_vec<T: Clone>(field: &mut Vec<T>, config: &[T]) {
    if field.is_empty() && !config.is_empty() {
        *field = config.to_vec();
    }
}

impl Config {
    pub fn path() -> Result<PathBuf> {
        let home = dirs::home_dir().context("Cannot determine home directory")?;
        Ok(home.join(".pgx").join("config.toml"))
    }

    pub fn load() -> Result<Self> {
        let path = Self::path()?;
        if !path.exists() {
            tracing::debug!(path = %path.display(), "No config file found, using defaults");
            return Ok(Config::default());
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("Cannot read config: {}", path.display()))?;
        toml::from_str(&raw).context("Invalid config TOML")
    }

    /// Look up a named connection profile.
    pub fn get(&self, name: &str) -> Option<&Connection> {
        self.connections.get(name)
    }

    /// Save the config back to ~/.pgx/config.toml.
    /// Creates the parent directory if it doesn't exist.
    pub fn save(&self) -> Result<()> {
        let path = Self::path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Cannot create config directory: {}", parent.display()))?;
        }
        let raw = toml::to_string_pretty(self).context("Cannot serialize config")?;
        std::fs::write(&path, raw)
            .with_context(|| format!("Cannot write config: {}", path.display()))?;
        tracing::debug!(path = %path.display(), "Config saved");
        Ok(())
    }

    /// Look up a named connection URL.
    #[inline]
    pub fn connection(&self, name: &str) -> Option<String> {
        self.get(name).map(|c| c.url.clone())
    }

    /// Return the default connection name if configured.
    #[inline]
    pub fn default_name(&self) -> Option<&str> {
        self.default.as_deref()
    }

    /// Like [`resolve`] but uses an already-loaded config instead of loading
    /// from disk.  Used when the caller already has a `Config` handle.
    pub fn resolve_from(
        &self,
        url_flag: Option<String>,
        conn_flag: Option<String>,
    ) -> Result<(String, Option<String>)> {
        if let Some(u) = url_flag {
            return Ok((u, None));
        }

        if let Some(name) = conn_flag {
            let url = self
                .connection(&name)
                .ok_or_else(|| anyhow::anyhow!("No connection named '{}' in config", name))?;
            return Ok((url, Some(name)));
        }

        if let Some(name) = self.default_name() {
            if let Some(url) = self.connection(name) {
                return Ok((url, Some(name.to_string())));
            }
        }

        Err(anyhow::anyhow!(
            "No database URL supplied.\n\
             Use -U <url>, set DATABASE_URL, or add a default in ~/.pgx/config.toml"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_opt_cli_wins() {
        let mut field: Option<String> = Some("cli".to_string());
        let config = Some("config".to_string());
        merge_opt(&mut field, &config);
        assert_eq!(field, Some("cli".to_string()));
    }

    #[test]
    fn merge_opt_config_fills() {
        let mut field: Option<String> = None;
        let config = Some("config".to_string());
        merge_opt(&mut field, &config);
        assert_eq!(field, Some("config".to_string()));
    }

    #[test]
    fn merge_opt_both_none() {
        let mut field: Option<String> = None;
        let config: Option<String> = None;
        merge_opt(&mut field, &config);
        assert_eq!(field, None);
    }

    #[test]
    fn merge_bool_false_with_config_true() {
        let mut field = false;
        merge_bool(&mut field, Some(true));
        assert!(field);
    }

    #[test]
    fn merge_bool_already_true() {
        let mut field = true;
        merge_bool(&mut field, Some(false));
        assert!(field);
    }

    #[test]
    fn merge_vec_empty_fills() {
        let mut field: Vec<String> = vec![];
        let config = vec!["a".to_string(), "b".to_string()];
        merge_vec(&mut field, &config);
        assert_eq!(field, vec!["a", "b"]);
    }

    #[test]
    fn merge_vec_cli_wins() {
        let mut field = vec!["cli".to_string()];
        let config = vec!["config".to_string()];
        merge_vec(&mut field, &config);
        assert_eq!(field, vec!["cli"]);
    }

    #[test]
    fn config_default_is_empty() {
        let cfg = Config::default();
        assert!(cfg.connections.is_empty());
        assert!(cfg.default.is_none());
    }

    #[test]
    fn resolve_from_url_flag_wins() {
        let cfg = Config::default();
        let (url, name) = cfg
            .resolve_from(Some("postgres://cli".to_string()), None)
            .unwrap();
        assert_eq!(url, "postgres://cli");
        assert!(name.is_none());
    }

    #[test]
    fn with_connection_serde_roundtrip() {
        let conn = Connection {
            url: "postgres://user:pass@localhost:5432/db".to_string(),
            description: Some("test".to_string()),
            listen: None,
            replicate: None,
            consume: None,
        };
        let toml_str = toml::to_string_pretty(&conn).expect("serialize");
        let back: Connection = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(conn.url, back.url);
        assert_eq!(conn.description, back.description);
    }

    #[test]
    fn downstream_sink_kind_stdout_roundtrip() {
        let kind = DownstreamSinkKind::Stdout { pretty: Some(true) };
        let toml_str = toml::to_string(&kind).expect("serialize");
        let back: DownstreamSinkKind = toml::from_str(&toml_str).expect("deserialize");
        assert!(matches!(
            back,
            DownstreamSinkKind::Stdout { pretty: Some(true) }
        ));
    }

    #[test]
    fn mutation_config_toml_roundtrip_and_defaults() {
        let toml_str = r#"
        target_url = "postgres://user:pass@dbB:5432/warehouse"
        sql = "INSERT INTO t (a, b) VALUES ($1, $2) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b"
        params = ["mat_no", "name"]
        "#;
        let m: MutationConfig = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(
            m.target_url.as_deref(),
            Some("postgres://user:pass@dbB:5432/warehouse")
        );
        assert_eq!(m.params, vec!["mat_no".to_string(), "name".to_string()]);
        m.validate().expect("valid");
        assert_eq!(m.statement_list().len(), 1);

        // params is optional; target_url falls back at runtime.
        let minimal: MutationConfig = toml::from_str("sql = \"SELECT 1\"").expect("deserialize");
        assert_eq!(minimal.target_url, None);
        assert!(minimal.params.is_empty());
    }

    #[test]
    fn mutation_config_transactional_statements() {
        let toml_str = r#"
        statements = [
          "DELETE FROM sizes_mirror WHERE mat_no = $1",
          "INSERT INTO sizes_mirror SELECT $1, e->>'size_code' FROM jsonb_array_elements($2::text::jsonb) e"
        ]
        params = ["mat_no", "sizes"]
        "#;
        let m: MutationConfig = toml::from_str(toml_str).expect("deserialize");
        m.validate().expect("valid");
        assert_eq!(m.statement_list().len(), 2);
    }

    #[test]
    fn mutation_config_sql_xor_statements() {
        let both: MutationConfig =
            toml::from_str("sql = \"SELECT 1\"\nstatements = [\"SELECT 2\"]").expect("deserialize");
        assert!(both.validate().is_err());

        let neither = MutationConfig {
            target_url: None,
            sql: None,
            statements: None,
            params: vec![],
        };
        assert!(neither.validate().is_err());
    }

    // ── ChannelFullBehavior tests ───────────────────────────────────────────

    #[test]
    fn channel_full_behavior_from_str() {
        assert_eq!(
            "block".parse::<ChannelFullBehavior>().unwrap(),
            ChannelFullBehavior::Block
        );
        assert_eq!(
            "drop_oldest".parse::<ChannelFullBehavior>().unwrap(),
            ChannelFullBehavior::DropOldest
        );
        assert_eq!(
            "grow".parse::<ChannelFullBehavior>().unwrap(),
            ChannelFullBehavior::Grow
        );
        assert!("invalid".parse::<ChannelFullBehavior>().is_err());
    }

    #[test]
    fn channel_full_behavior_display() {
        assert_eq!(ChannelFullBehavior::Block.to_string(), "block");
        assert_eq!(ChannelFullBehavior::DropOldest.to_string(), "drop_oldest");
        assert_eq!(ChannelFullBehavior::Grow.to_string(), "grow");
    }

    #[test]
    fn channel_full_behavior_serde_roundtrip() {
        for behavior in &[
            ChannelFullBehavior::Block,
            ChannelFullBehavior::DropOldest,
            ChannelFullBehavior::Grow,
        ] {
            let json_str = serde_json::to_string(behavior).expect("serialize");
            let back: ChannelFullBehavior = serde_json::from_str(&json_str).expect("deserialize");
            assert_eq!(*behavior, back);
        }
    }

    #[test]
    fn channel_full_behavior_default() {
        assert_eq!(
            ChannelFullBehavior::default(),
            ChannelFullBehavior::DropOldest
        );
    }

    // ── ListenSinkConfig with channel_full_behavior ─────────────────────────

    #[test]
    fn listen_sink_config_with_channel_behavior() {
        let toml_str = r#"
        channels = ["orders"]
        channel_full_behavior = "block"
        [sink]
        type = "shell"
        command = "echo test"
        "#;
        let config: ListenSinkConfig = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(
            config.channel_full_behavior,
            Some(ChannelFullBehavior::Block)
        );
    }
}
