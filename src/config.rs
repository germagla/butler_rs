use anyhow::{Context, Result, bail};
use std::{collections::HashSet, env, path::PathBuf};
use url::Url;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerProviderKind {
    Aternos,
    Pterodactyl,
}

impl std::fmt::Display for ServerProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aternos => write!(f, "aternos"),
            Self::Pterodactyl => write!(f, "pterodactyl"),
        }
    }
}

#[derive(Clone)]
pub struct AternosConfig {
    pub user: String,
    pub password: String,
    pub server_id: Option<String>,
    pub headless: bool,
    pub chrome_path: Option<PathBuf>,
}

#[derive(Clone)]
pub struct PterodactylConfig {
    pub panel_origin: Url,
    pub server_id: String,
    pub api_token: String,
    pub power_enabled: bool,
    pub allocation_wait_secs: u64,
    pub flaresolverr_url: Url,
    pub flaresolverr_container: String,
    pub orbctl_path: PathBuf,
    pub docker_path: PathBuf,
}

#[derive(Clone)]
pub enum ProviderConfig {
    Aternos(AternosConfig),
    Pterodactyl(Box<PterodactylConfig>),
}

#[derive(Clone)]
pub struct Config {
    pub discord_token: String,
    pub provider: ProviderConfig,
    pub minecraft_server_addr: String,
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
        let provider_kind = provider_kind_var("SERVER_PROVIDER")?;
        let provider = match provider_kind {
            ServerProviderKind::Aternos => ProviderConfig::Aternos(AternosConfig {
                user: required_var("ATERNOS_USER")?,
                password: required_var("ATERNOS_PASS")?,
                server_id: optional_nonempty_var("ATERNOS_SERVER_ID")
                    .or_else(|| optional_nonempty_var("SERVER_ID")),
                headless: bool_var("HEADLESS", true)?,
                chrome_path: optional_nonempty_var("CHROME").map(PathBuf::from),
            }),
            ServerProviderKind::Pterodactyl => {
                ProviderConfig::Pterodactyl(Box::new(PterodactylConfig {
                    panel_origin: panel_origin_var("PTERODACTYL_PANEL_URL")?,
                    server_id: pterodactyl_server_id_var("PTERODACTYL_SERVER_ID")?,
                    api_token: required_nonempty_var("PTERODACTYL_API_TOKEN")?,
                    power_enabled: bool_var("PTERODACTYL_POWER_ENABLED", false)?,
                    allocation_wait_secs: bounded_positive_u64_var(
                        "PTERODACTYL_ALLOCATION_WAIT_SECS",
                        1_200,
                        3_600,
                    )?,
                    flaresolverr_url: flaresolverr_url_var("FLARESOLVERR_URL")?,
                    flaresolverr_container: container_name_var("FLARESOLVERR_CONTAINER")?,
                    orbctl_path: PathBuf::from(
                        optional_nonempty_var("ORBSTACK_BIN")
                            .unwrap_or_else(|| "/opt/homebrew/bin/orbctl".to_string()),
                    ),
                    docker_path: PathBuf::from(
                        optional_nonempty_var("DOCKER_BIN")
                            .unwrap_or_else(|| "/usr/local/bin/docker".to_string()),
                    ),
                }))
            }
        };
        let minecraft_server_addr =
            env::var("MINECRAFT_SERVER_ADDR").unwrap_or_else(|_| "localhost:25565".to_string());
        bool_var("BUTLER_COLOR", true)?;

        Ok(Self {
            discord_token,
            provider,
            minecraft_server_addr,
            start_wait_online_secs: bounded_positive_u64_var("START_WAIT_ONLINE_SECS", 600, 3_600)?,
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

fn required_nonempty_var(name: &str) -> Result<String> {
    let value = required_var(name)?;
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value.trim().to_string())
}

fn bounded_positive_u64_var(name: &str, default: u64, maximum: u64) -> Result<u64> {
    bounded_positive_u64_var_value(env::var(name).ok().as_deref(), default, maximum).with_context(
        || format!("{name} must be a positive unsigned integer no greater than {maximum}"),
    )
}

fn bounded_positive_u64_var_value(value: Option<&str>, default: u64, maximum: u64) -> Result<u64> {
    let value = u64_var_value(value, default)?;
    if value == 0 {
        bail!("value must be greater than zero");
    }
    if value > maximum {
        bail!("value must not exceed {maximum}");
    }
    Ok(value)
}

fn optional_nonempty_var(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn provider_kind_var(name: &str) -> Result<ServerProviderKind> {
    let value = required_nonempty_var(name)?;
    parse_provider_kind(&value)
        .with_context(|| format!("{name} must be one of aternos, pterodactyl"))
}

fn parse_provider_kind(value: &str) -> Result<ServerProviderKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "aternos" => Ok(ServerProviderKind::Aternos),
        "pterodactyl" => Ok(ServerProviderKind::Pterodactyl),
        other => bail!("unsupported server provider `{other}`"),
    }
}

fn panel_origin_var(name: &str) -> Result<Url> {
    parse_panel_origin(&required_nonempty_var(name)?)
        .with_context(|| format!("{name} must be an HTTPS origin without credentials or a path"))
}

fn parse_panel_origin(value: &str) -> Result<Url> {
    let mut url = Url::parse(value).context("invalid URL")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        bail!("panel URL must be an HTTPS origin");
    }
    url.set_path("/");
    Ok(url)
}

fn flaresolverr_url_var(name: &str) -> Result<Url> {
    let value = optional_nonempty_var(name).unwrap_or_else(|| "http://127.0.0.1:8191/".to_string());
    parse_flaresolverr_url(&value)
        .with_context(|| format!("{name} must be an HTTP loopback origin"))
}

fn parse_flaresolverr_url(value: &str) -> Result<Url> {
    let mut url = Url::parse(value).context("invalid URL")?;
    let is_loopback = match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
        None => false,
    };
    if url.scheme() != "http"
        || !is_loopback
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        bail!("FlareSolverr URL must be an HTTP loopback origin");
    }
    url.set_path("/");
    Ok(url)
}

fn container_name_var(name: &str) -> Result<String> {
    let value = optional_nonempty_var(name).unwrap_or_else(|| "flaresolverr".to_string());
    if value.len() > 128
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'))
        || !value
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric())
    {
        bail!("{name} contains an invalid Docker container name");
    }
    Ok(value)
}

fn pterodactyl_server_id_var(name: &str) -> Result<String> {
    let value = required_nonempty_var(name)?;
    parse_pterodactyl_server_id(&value)
        .with_context(|| format!("{name} contains an invalid server identifier"))
}

fn parse_pterodactyl_server_id(value: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 64
        || !value.chars().all(|ch| ch.is_ascii_hexdigit() || ch == '-')
    {
        bail!("invalid server identifier");
    }
    Ok(value.to_string())
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
        assert_eq!(
            bounded_positive_u64_var_value(None, 1_200, 3_600).unwrap(),
            1_200
        );
        assert_eq!(
            bounded_positive_u64_var_value(Some("900"), 1_200, 3_600).unwrap(),
            900
        );
        assert!(bounded_positive_u64_var_value(Some("0"), 1_200, 3_600).is_err());
        assert!(bounded_positive_u64_var_value(Some("3601"), 1_200, 3_600).is_err());
        assert!(bounded_positive_u64_var_value(Some("never"), 1_200, 3_600).is_err());
    }

    #[test]
    fn parses_required_provider_kind() {
        assert_eq!(
            parse_provider_kind("aternos").unwrap(),
            ServerProviderKind::Aternos
        );
        assert_eq!(
            parse_provider_kind(" PTERODACTYL ").unwrap(),
            ServerProviderKind::Pterodactyl
        );
        assert!(parse_provider_kind("automatic").is_err());
    }

    #[test]
    fn validates_panel_origin() {
        assert_eq!(
            parse_panel_origin("https://panel.play.hosting")
                .unwrap()
                .as_str(),
            "https://panel.play.hosting/"
        );
        assert!(parse_panel_origin("http://panel.play.hosting").is_err());
        assert!(parse_panel_origin("https://user@example.com").is_err());
        assert!(parse_panel_origin("https://example.com/account").is_err());
    }

    #[test]
    fn validates_loopback_flaresolverr_url() {
        assert_eq!(
            parse_flaresolverr_url("http://127.0.0.1:8191")
                .unwrap()
                .as_str(),
            "http://127.0.0.1:8191/"
        );
        assert!(parse_flaresolverr_url("http://localhost:8191").is_ok());
        assert!(parse_flaresolverr_url("http://192.168.1.5:8191").is_err());
        assert!(parse_flaresolverr_url("https://127.0.0.1:8191").is_err());
    }

    #[test]
    fn validates_pterodactyl_server_identifier() {
        assert_eq!(parse_pterodactyl_server_id("34634dd7").unwrap(), "34634dd7");
        assert!(parse_pterodactyl_server_id("../server").is_err());
        assert!(parse_pterodactyl_server_id("server name").is_err());
        assert!(parse_pterodactyl_server_id("").is_err());
    }
}
