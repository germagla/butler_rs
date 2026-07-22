use crate::{
    config::PterodactylConfig,
    run_history::{ensure_owner_only_dir, ensure_owner_only_file},
    terminal,
};
use anyhow::{Context, Result, bail};
use reqwest::{Client, redirect::Policy};
use serde::Deserialize;
use std::{collections::HashMap, path::Path, process::ExitStatus, time::Duration};
use tokio::{
    process::Command,
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, interval, sleep, timeout},
};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const ORBSTACK_READY_TIMEOUT: Duration = Duration::from_secs(60);
const FLARESOLVERR_READY_TIMEOUT: Duration = Duration::from_secs(45);
const MONITOR_INTERVAL: Duration = Duration::from_secs(20);
const HEALTH_FAILURE_THRESHOLD: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrbStackStatus {
    Running,
    Starting,
    Stopped,
}

pub struct FlareSolverrSupervisor {
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct FlareSolverrRuntimeConfig {
    flaresolverr_url: url::Url,
    flaresolverr_container: String,
    orbctl_path: std::path::PathBuf,
    docker_path: std::path::PathBuf,
    ownership_marker: std::path::PathBuf,
}

impl FlareSolverrRuntimeConfig {
    pub fn new(config: &PterodactylConfig, artifact_dir: &Path) -> Self {
        Self {
            flaresolverr_url: config.flaresolverr_url.clone(),
            flaresolverr_container: config.flaresolverr_container.clone(),
            orbctl_path: config.orbctl_path.clone(),
            docker_path: config.docker_path.clone(),
            ownership_marker: artifact_dir.join(".flaresolverr-owned"),
        }
    }
}

impl FlareSolverrSupervisor {
    pub async fn start(config: FlareSolverrRuntimeConfig) -> Result<Self> {
        let client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .context("could not build FlareSolverr health client")?;

        ensure_orbstack_running(&config).await?;
        let initial_container = inspect_container(&config).await?;
        let marker_exists = config.ownership_marker.is_file();
        let marker_container_id = read_ownership_marker(&config)?;
        let inherited_ownership = marker_container_id.as_deref() == Some(&initial_container.id);
        if marker_exists && !inherited_ownership {
            remove_ownership_marker(&config);
        } else if inherited_ownership {
            ensure_owner_only_file(&config.ownership_marker)?;
        }
        let started_container = !initial_container.running;
        if started_container {
            start_container(&config, &initial_container.id).await?;
        }
        let current_container = if started_container {
            match inspect_container_reference(&config, &initial_container.id).await {
                Ok(container) => container,
                Err(error) => {
                    let _ = stop_container(&config, &initial_container.id).await;
                    return Err(error);
                }
            }
        } else {
            initial_container
        };
        let owned_container_id =
            (inherited_ownership || started_container).then(|| current_container.id.clone());
        if started_container
            && let Err(error) = write_ownership_marker(&config, &current_container.id)
        {
            let _ = stop_container(&config, &current_container.id).await;
            remove_ownership_marker(&config);
            return Err(error);
        }
        if let Err(first_error) = wait_until_ready(&client, &config).await {
            let recovery = match owned_container_id.as_deref() {
                Some(container_id) => match restart_container(&config, container_id).await {
                    Ok(()) => wait_until_ready(&client, &config).await,
                    Err(error) => Err(error),
                },
                None => Err(anyhow::anyhow!(
                    "FlareSolverrUnavailable: refusing to restart an unowned container"
                )),
            };
            if recovery.is_ok() {
                let (shutdown_tx, shutdown_rx) = oneshot::channel();
                let task = tokio::spawn(supervise(config, client, owned_container_id, shutdown_rx));
                return Ok(Self {
                    shutdown_tx: Some(shutdown_tx),
                    task: Some(task),
                });
            }
            if let Some(container_id) = owned_container_id.as_deref()
                && stop_owned_container(&config, container_id).await.is_ok()
            {
                remove_ownership_marker(&config);
            }
            return Err(first_error);
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(supervise(config, client, owned_container_id, shutdown_rx));

        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(task) = self.task.take()
            && let Err(error) = task.await
        {
            emit_warning("supervisor join failed", &error.to_string());
        }
    }
}

async fn supervise(
    config: FlareSolverrRuntimeConfig,
    client: Client,
    mut owned_container_id: Option<String>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut ticker = interval(MONITOR_INTERVAL);
    let mut consecutive_health_failures = 0;
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            _ = ticker.tick() => {
                if is_ready(&client, &config).await {
                    consecutive_health_failures = 0;
                    continue;
                }
                consecutive_health_failures += 1;
                if consecutive_health_failures < HEALTH_FAILURE_THRESHOLD {
                    continue;
                }
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    result = ensure_runtime_healthy(&client, &config, owned_container_id.as_deref()) => {
                        match result {
                            Ok(adopted_container_id) => {
                                if owned_container_id.is_none() {
                                    owned_container_id = adopted_container_id;
                                }
                                consecutive_health_failures = 0;
                            }
                            Err(error) => emit_warning("health recovery failed", &error.to_string()),
                        }
                    }
                }
            }
        }
    }

    if let Some(container_id) = owned_container_id {
        match stop_owned_container(&config, &container_id).await {
            Ok(()) => remove_ownership_marker(&config),
            Err(error) => emit_warning("shutdown could not stop container", &error.to_string()),
        }
    }
}

fn write_ownership_marker(config: &FlareSolverrRuntimeConfig, container_id: &str) -> Result<()> {
    let parent = config
        .ownership_marker
        .parent()
        .context("FlareSolverrConfiguration: ownership marker had no parent")?;
    ensure_owner_only_dir(parent)?;
    std::fs::write(&config.ownership_marker, format!("{container_id}\n"))?;
    ensure_owner_only_file(&config.ownership_marker)
}

fn read_ownership_marker(config: &FlareSolverrRuntimeConfig) -> Result<Option<String>> {
    match std::fs::read_to_string(&config.ownership_marker) {
        Ok(value) => {
            let value = value.trim();
            if value.is_empty() || !value.chars().all(|character| character.is_ascii_hexdigit()) {
                return Ok(None);
            }
            Ok(Some(value.to_string()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn remove_ownership_marker(config: &FlareSolverrRuntimeConfig) {
    match std::fs::remove_file(&config.ownership_marker) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => emit_warning("could not remove ownership marker", &error.to_string()),
    }
}

async fn ensure_runtime_healthy(
    client: &Client,
    config: &FlareSolverrRuntimeConfig,
    owned_container_id: Option<&str>,
) -> Result<Option<String>> {
    if is_ready(client, config).await {
        return Ok(owned_container_id.map(str::to_string));
    }

    ensure_orbstack_running(config).await?;
    let container = inspect_container(config).await?;
    match container_recovery_action(&container, owned_container_id) {
        ContainerRecoveryAction::RestartOwned => {
            let expected_id = owned_container_id.expect("owned recovery requires an ID");
            restart_container(config, expected_id).await?;
            wait_until_ready(client, config).await?;
            Ok(Some(expected_id.to_string()))
        }
        ContainerRecoveryAction::StartOwned => {
            let expected_id = owned_container_id.expect("owned recovery requires an ID");
            start_container(config, expected_id).await?;
            wait_until_ready(client, config).await?;
            Ok(Some(expected_id.to_string()))
        }
        ContainerRecoveryAction::StartAndAdopt => {
            start_container(config, &container.id).await?;
            if let Err(error) = write_ownership_marker(config, &container.id) {
                let _ = stop_container(config, &container.id).await;
                remove_ownership_marker(config);
                return Err(error);
            }
            if let Err(error) = wait_until_ready(client, config).await {
                let _ = stop_owned_container(config, &container.id).await;
                remove_ownership_marker(config);
                return Err(error);
            }
            Ok(Some(container.id))
        }
        ContainerRecoveryAction::RefuseUnownedRunning => {
            bail!("FlareSolverrUnavailable: refusing to restart an unowned container")
        }
        ContainerRecoveryAction::IdentityChanged => {
            bail!("FlareSolverrUnavailable: owned container identity changed")
        }
    }
}

async fn ensure_orbstack_running(config: &FlareSolverrRuntimeConfig) -> Result<()> {
    match orb_status(config).await? {
        OrbStackStatus::Running => return Ok(()),
        OrbStackStatus::Starting => {}
        OrbStackStatus::Stopped => {
            // OrbStack may return a timeout even when the VM continues starting.
            let _ = run_status(&config.orbctl_path, &["start"]).await;
        }
    }

    let deadline = Instant::now() + ORBSTACK_READY_TIMEOUT;
    while Instant::now() < deadline {
        if matches!(orb_status(config).await, Ok(OrbStackStatus::Running)) {
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }
    bail!("OrbStackUnavailable: OrbStack did not become ready before timeout")
}

async fn orb_status(config: &FlareSolverrRuntimeConfig) -> Result<OrbStackStatus> {
    let status = run_status(&config.orbctl_path, &["status"]).await?;
    parse_orb_status(status)
}

fn parse_orb_status(status: ExitStatus) -> Result<OrbStackStatus> {
    match status.code() {
        Some(0) => Ok(OrbStackStatus::Running),
        Some(1) => Ok(OrbStackStatus::Stopped),
        Some(2) => Ok(OrbStackStatus::Starting),
        _ => bail!("OrbStackUnavailable: could not determine OrbStack status"),
    }
}

async fn inspect_container(config: &FlareSolverrRuntimeConfig) -> Result<ContainerSnapshot> {
    inspect_container_reference(config, &config.flaresolverr_container).await
}

async fn inspect_container_reference(
    config: &FlareSolverrRuntimeConfig,
    reference: &str,
) -> Result<ContainerSnapshot> {
    let output = run_output(&config.docker_path, &["inspect", reference]).await?;
    if !output.status.success() {
        bail!("FlareSolverrUnavailable: configured container could not be inspected");
    }
    parse_container_inspection(&output.stdout, config.flaresolverr_url.port().unwrap_or(80))
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerInspection {
    id: String,
    state: DockerContainerState,
    config: DockerContainerConfig,
    host_config: DockerHostConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerState {
    running: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerConfig {
    image: String,
    #[serde(default)]
    labels: HashMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerHostConfig {
    restart_policy: DockerRestartPolicy,
    #[serde(default)]
    port_bindings: HashMap<String, Option<Vec<DockerPortBinding>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerRestartPolicy {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerPortBinding {
    host_ip: String,
    host_port: String,
}

#[derive(Debug, PartialEq, Eq)]
struct ContainerSnapshot {
    id: String,
    running: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ContainerRecoveryAction {
    StartOwned,
    RestartOwned,
    StartAndAdopt,
    RefuseUnownedRunning,
    IdentityChanged,
}

fn container_recovery_action(
    container: &ContainerSnapshot,
    owned_container_id: Option<&str>,
) -> ContainerRecoveryAction {
    match owned_container_id {
        Some(expected_id) if container.id != expected_id => {
            ContainerRecoveryAction::IdentityChanged
        }
        Some(_) if container.running => ContainerRecoveryAction::RestartOwned,
        Some(_) => ContainerRecoveryAction::StartOwned,
        None if container.running => ContainerRecoveryAction::RefuseUnownedRunning,
        None => ContainerRecoveryAction::StartAndAdopt,
    }
}

fn parse_container_inspection(output: &[u8], expected_host_port: u16) -> Result<ContainerSnapshot> {
    let inspections: Vec<DockerContainerInspection> = serde_json::from_slice(output)
        .context("FlareSolverrUnavailable: container inspection was not valid JSON")?;
    let inspection = inspections
        .first()
        .context("FlareSolverrUnavailable: container inspection was empty")?;
    let bindings = inspection
        .host_config
        .port_bindings
        .get("8191/tcp")
        .and_then(Option::as_deref)
        .unwrap_or_default();
    let expected_binding = bindings.len() == 1
        && bindings[0].host_ip == "127.0.0.1"
        && bindings[0].host_port == expected_host_port.to_string();
    let pinned_image = inspection
        .config
        .image
        .starts_with("ghcr.io/flaresolverr/flaresolverr@sha256:");
    let compose_service = inspection
        .config
        .labels
        .get("com.docker.compose.service")
        .is_some_and(|value| value == "flaresolverr");
    if !expected_binding
        || !pinned_image
        || !compose_service
        || inspection.host_config.restart_policy.name != "no"
    {
        bail!("FlareSolverrConfiguration: container does not match the secured Butler profile");
    }
    if inspection.id.is_empty()
        || !inspection
            .id
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        bail!("FlareSolverrUnavailable: container ID was invalid");
    }
    Ok(ContainerSnapshot {
        id: inspection.id.clone(),
        running: inspection.state.running,
    })
}

async fn start_container(config: &FlareSolverrRuntimeConfig, container_id: &str) -> Result<()> {
    let status = run_status(&config.docker_path, &["start", container_id]).await?;
    if !status.success() {
        bail!("FlareSolverrUnavailable: container could not be started");
    }
    Ok(())
}

async fn stop_container(config: &FlareSolverrRuntimeConfig, container_id: &str) -> Result<()> {
    let status = run_status(&config.docker_path, &["stop", "--time", "10", container_id]).await?;
    if !status.success() {
        bail!("FlareSolverrCleanupFailed: container could not be stopped");
    }
    Ok(())
}

async fn stop_owned_container(config: &FlareSolverrRuntimeConfig, expected_id: &str) -> Result<()> {
    let current = inspect_container_reference(config, expected_id).await?;
    if current.id != expected_id {
        bail!("FlareSolverrCleanupFailed: configured container identity changed");
    }
    stop_container(config, expected_id).await
}

async fn restart_container(config: &FlareSolverrRuntimeConfig, container_id: &str) -> Result<()> {
    let status = run_status(
        &config.docker_path,
        &["restart", "--time", "10", container_id],
    )
    .await?;
    if !status.success() {
        bail!("FlareSolverrUnavailable: unhealthy container could not be restarted");
    }
    Ok(())
}

async fn wait_until_ready(client: &Client, config: &FlareSolverrRuntimeConfig) -> Result<()> {
    let deadline = Instant::now() + FLARESOLVERR_READY_TIMEOUT;
    while Instant::now() < deadline {
        if is_ready(client, config).await {
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }
    bail!("FlareSolverrUnavailable: readiness endpoint did not become healthy before timeout")
}

async fn is_ready(client: &Client, config: &FlareSolverrRuntimeConfig) -> bool {
    let Ok(mut response) = client.get(config.flaresolverr_url.clone()).send().await else {
        return false;
    };
    if !response.status().is_success() {
        return false;
    }
    let mut body = Vec::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(_) => return false,
        };
        if body.len().saturating_add(chunk.len()) > 4096 {
            return false;
        }
        body.extend_from_slice(&chunk);
    }
    is_ready_body(&body)
}

fn is_ready_body(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("msg")?.as_str().map(str::to_owned))
        .is_some_and(|message| message == "FlareSolverr is ready!")
}

async fn run_status(path: &Path, args: &[&str]) -> Result<ExitStatus> {
    Ok(run_output(path, args).await?.status)
}

async fn run_output(path: &Path, args: &[&str]) -> Result<std::process::Output> {
    let mut command = Command::new(path);
    command
        .args(args)
        .env_clear()
        .env(
            "PATH",
            "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        )
        .kill_on_drop(true);
    for name in ["HOME", "TMPDIR"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    timeout(COMMAND_TIMEOUT, command.output())
        .await
        .context("local runtime command timed out")?
        .with_context(|| format!("could not execute {}", path.display()))
}

fn emit_warning(action: &str, error: &str) {
    terminal::emit(terminal::line(
        "WARN",
        "flaresolverr",
        "",
        "",
        None,
        format!("{}; error {}", action, terminal::clean(error)),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn exit_status(code: i32) -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(code << 8)
    }

    #[cfg(unix)]
    #[test]
    fn parses_orbstack_exit_codes() {
        assert_eq!(
            parse_orb_status(exit_status(0)).unwrap(),
            OrbStackStatus::Running
        );
        assert_eq!(
            parse_orb_status(exit_status(1)).unwrap(),
            OrbStackStatus::Stopped
        );
        assert_eq!(
            parse_orb_status(exit_status(2)).unwrap(),
            OrbStackStatus::Starting
        );
        assert!(parse_orb_status(exit_status(3)).is_err());
    }

    #[test]
    fn validates_secured_container_inspection() {
        let valid = br#"[{"Id":"abcdef123456","State":{"Running":true},"Config":{"Image":"ghcr.io/flaresolverr/flaresolverr@sha256:abc","Labels":{"com.docker.compose.service":"flaresolverr"}},"HostConfig":{"RestartPolicy":{"Name":"no"},"PortBindings":{"8191/tcp":[{"HostIp":"127.0.0.1","HostPort":"8191"}]}}}]"#;
        assert_eq!(
            parse_container_inspection(valid, 8191).unwrap(),
            ContainerSnapshot {
                id: "abcdef123456".to_string(),
                running: true,
            }
        );

        let public_binding = String::from_utf8(valid.to_vec())
            .unwrap()
            .replace("127.0.0.1", "0.0.0.0");
        assert!(parse_container_inspection(public_binding.as_bytes(), 8191).is_err());
        assert!(parse_container_inspection(valid, 9999).is_err());
        assert!(parse_container_inspection(b"not json", 8191).is_err());
    }

    #[test]
    fn validates_readiness_body() {
        assert!(is_ready_body(br#"{"msg":"FlareSolverr is ready!"}"#));
        assert!(!is_ready_body(br#"{"msg":"starting"}"#));
        assert!(!is_ready_body(b"not json"));
    }

    #[test]
    fn selects_identity_safe_container_recovery_actions() {
        let running = ContainerSnapshot {
            id: "container-a".to_string(),
            running: true,
        };
        let stopped = ContainerSnapshot {
            id: "container-a".to_string(),
            running: false,
        };

        assert_eq!(
            container_recovery_action(&running, Some("container-a")),
            ContainerRecoveryAction::RestartOwned
        );
        assert_eq!(
            container_recovery_action(&stopped, Some("container-a")),
            ContainerRecoveryAction::StartOwned
        );
        assert_eq!(
            container_recovery_action(&stopped, None),
            ContainerRecoveryAction::StartAndAdopt
        );
        assert_eq!(
            container_recovery_action(&running, None),
            ContainerRecoveryAction::RefuseUnownedRunning
        );
        assert_eq!(
            container_recovery_action(&running, Some("container-b")),
            ContainerRecoveryAction::IdentityChanged
        );
    }
}
