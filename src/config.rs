use anyhow::{Context, Result};
use std::{env, path::PathBuf};

#[derive(Clone)]
pub struct Config {
    pub discord_token: String,
    pub aternos_user: String,
    pub aternos_pass: String,
    pub minecraft_server_addr: String,
    pub server_id: Option<String>,
    pub headless: bool,
    pub start_wait_online_secs: u64,
    pub run_history_limit: usize,
    pub artifact_dir: PathBuf,
    pub persist_run_events: bool,
    pub redact_run_events: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let discord_token = required_var("DISCORD_TOKEN")?;
        let aternos_user = required_var("ATERNOS_USER")?;
        let aternos_pass = required_var("ATERNOS_PASS")?;
        let minecraft_server_addr =
            env::var("MINECRAFT_SERVER_ADDR").unwrap_or_else(|_| "localhost:25565".to_string());

        Ok(Self {
            discord_token,
            aternos_user,
            aternos_pass,
            minecraft_server_addr,
            server_id: optional_nonempty_var("SERVER_ID"),
            headless: bool_var("HEADLESS", true),
            start_wait_online_secs: u64_var("START_WAIT_ONLINE_SECS", 600),
            run_history_limit: usize_var("RUN_HISTORY_LIMIT", 20),
            artifact_dir: PathBuf::from(
                env::var("ARTIFACT_DIR").unwrap_or_else(|_| "artifacts/runs".to_string()),
            ),
            persist_run_events: bool_var("BUTLER_PERSIST_RUN_EVENTS", true),
            redact_run_events: bool_var("BUTLER_REDACT_RUN_EVENTS", true),
        })
    }
}

fn required_var(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set"))
}

fn optional_nonempty_var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn bool_var(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => Some(true),
            "0" | "false" | "no" | "n" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn u64_var(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn usize_var(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
