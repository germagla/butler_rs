use anyhow::{Context, Result, bail};
use std::{collections::HashSet, env, path::PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactCapture {
    Screenshots,
    Full,
    Failure,
    Off,
}

impl ArtifactCapture {
    pub fn capture_success_screenshot(self) -> bool {
        matches!(self, Self::Screenshots | Self::Full)
    }

    pub fn capture_success_html(self) -> bool {
        matches!(self, Self::Full)
    }

    pub fn capture_failure_screenshot(self) -> bool {
        matches!(self, Self::Screenshots | Self::Full | Self::Failure)
    }

    pub fn capture_failure_html(self) -> bool {
        matches!(self, Self::Screenshots | Self::Full | Self::Failure)
    }
}

impl std::fmt::Display for ArtifactCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Screenshots => "screenshots",
            Self::Full => "full",
            Self::Failure => "failure",
            Self::Off => "off",
        };
        write!(f, "{value}")
    }
}

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
    pub artifact_capture: ArtifactCapture,
    pub attach_screenshots: bool,
    pub persist_run_events: bool,
    pub redact_run_events: bool,
    pub owner_user_ids: HashSet<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let discord_token = required_var("DISCORD_TOKEN")?;
        let aternos_user = required_var("ATERNOS_USER")?;
        let aternos_pass = required_var("ATERNOS_PASS")?;
        let minecraft_server_addr =
            env::var("MINECRAFT_SERVER_ADDR").unwrap_or_else(|_| "localhost:25565".to_string());
        bool_var("BUTLER_COLOR", true)?;

        Ok(Self {
            discord_token,
            aternos_user,
            aternos_pass,
            minecraft_server_addr,
            server_id: optional_nonempty_var("SERVER_ID"),
            headless: bool_var("HEADLESS", true)?,
            start_wait_online_secs: u64_var("START_WAIT_ONLINE_SECS", 600)?,
            run_history_limit: usize_var("RUN_HISTORY_LIMIT", 20)?,
            artifact_dir: PathBuf::from(
                env::var("ARTIFACT_DIR").unwrap_or_else(|_| "artifacts/runs".to_string()),
            ),
            artifact_capture: artifact_capture_var(
                "ARTIFACT_CAPTURE",
                ArtifactCapture::Screenshots,
            )?,
            attach_screenshots: bool_var("BUTLER_ATTACH_SCREENSHOTS", true)?,
            persist_run_events: bool_var("BUTLER_PERSIST_RUN_EVENTS", true)?,
            redact_run_events: bool_var("BUTLER_REDACT_RUN_EVENTS", true)?,
            owner_user_ids: owner_user_ids_var("BUTLER_OWNER_IDS")?,
        })
    }
}

fn required_var(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set"))
}

fn optional_nonempty_var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn bool_var(name: &str, default: bool) -> Result<bool> {
    parse_bool_value(env::var(name).ok().as_deref(), default)
        .with_context(|| format!("{name} must be a boolean"))
}

fn parse_bool_value(value: Option<&str>, default: bool) -> Result<bool> {
    let Some(value) = value else {
        return Ok(default);
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        other => bail!("invalid boolean value `{other}`"),
    }
}

fn u64_var(name: &str, default: u64) -> Result<u64> {
    u64_var_value(env::var(name).ok().as_deref(), default)
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn usize_var(name: &str, default: usize) -> Result<usize> {
    usize_var_value(env::var(name).ok().as_deref(), default)
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn u64_var_value(value: Option<&str>, default: u64) -> Result<u64> {
    let Some(value) = value else {
        return Ok(default);
    };
    value.trim().parse().context("invalid unsigned integer")
}

fn usize_var_value(value: Option<&str>, default: usize) -> Result<usize> {
    let Some(value) = value else {
        return Ok(default);
    };
    value.trim().parse().context("invalid unsigned integer")
}

fn artifact_capture_var(name: &str, default: ArtifactCapture) -> Result<ArtifactCapture> {
    match env::var(name) {
        Ok(value) => parse_artifact_capture_value(&value)
            .with_context(|| format!("{name} must be one of screenshots, full, failure, off")),
        Err(_) => Ok(default),
    }
}

fn parse_artifact_capture_value(value: &str) -> Result<ArtifactCapture> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "screenshots" => Ok(ArtifactCapture::Screenshots),
        "full" => Ok(ArtifactCapture::Full),
        "failure" => Ok(ArtifactCapture::Failure),
        "off" => Ok(ArtifactCapture::Off),
        other => bail!("invalid artifact capture mode `{other}`"),
    }
}

fn owner_user_ids_var(name: &str) -> Result<HashSet<String>> {
    parse_owner_user_ids_value(&env::var(name).unwrap_or_default())
        .with_context(|| format!("{name} must be a comma-separated list of Discord user IDs"))
}

fn parse_owner_user_ids_value(value: &str) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for raw_id in value.split(',') {
        let id = raw_id.trim();
        if id.is_empty() {
            continue;
        }
        if !id.chars().all(|ch| ch.is_ascii_digit()) {
            bail!("invalid Discord user ID `{id}`");
        }
        ids.insert(id.to_string());
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_owner_user_ids() {
        let ids = parse_owner_user_ids_value("123, 456,,789").unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains("123"));
        assert!(ids.contains("456"));
        assert!(ids.contains("789"));
    }

    #[test]
    fn rejects_invalid_owner_user_ids() {
        let error = parse_owner_user_ids_value("123,abc").unwrap_err();
        assert!(error.to_string().contains("invalid Discord user ID"));
    }

    #[test]
    fn parses_artifact_capture_modes() {
        assert_eq!(
            parse_artifact_capture_value("").unwrap(),
            ArtifactCapture::Screenshots
        );
        assert_eq!(
            parse_artifact_capture_value("screenshots").unwrap(),
            ArtifactCapture::Screenshots
        );
        assert_eq!(
            parse_artifact_capture_value("full").unwrap(),
            ArtifactCapture::Full
        );
        assert_eq!(
            parse_artifact_capture_value("failure").unwrap(),
            ArtifactCapture::Failure
        );
        assert_eq!(
            parse_artifact_capture_value("off").unwrap(),
            ArtifactCapture::Off
        );
        assert!(parse_artifact_capture_value("html").is_err());
    }

    #[test]
    fn artifact_capture_default_is_screenshots() {
        assert!(ArtifactCapture::Screenshots.capture_success_screenshot());
        assert!(!ArtifactCapture::Screenshots.capture_success_html());
        assert!(ArtifactCapture::Screenshots.capture_failure_screenshot());
        assert!(ArtifactCapture::Screenshots.capture_failure_html());
    }

    #[test]
    fn screenshot_attachment_default_is_enabled() {
        assert!(parse_bool_value(None, true).unwrap());
        assert!(!parse_bool_value(Some("false"), true).unwrap());
    }

    #[test]
    fn rejects_invalid_boolean_values() {
        let error = parse_bool_value(Some("fales"), true).unwrap_err();
        assert!(error.to_string().contains("invalid boolean value"));
    }

    #[test]
    fn parses_strict_numeric_values() {
        assert_eq!(u64_var_value(Some("42"), 600).unwrap(), 42);
        assert_eq!(u64_var_value(None, 600).unwrap(), 600);
        assert!(u64_var_value(Some("soon"), 600).is_err());
        assert_eq!(usize_var_value(Some("3"), 20).unwrap(), 3);
        assert_eq!(usize_var_value(None, 20).unwrap(), 20);
        assert!(usize_var_value(Some("many"), 20).is_err());
    }
}
