use crate::{
    config::PterodactylConfig,
    run_history::{ensure_owner_only_dir, ensure_owner_only_file},
    runtime_lock::{RuntimeFileLock, acquire_runtime_lock, runtime_lock_has_live_owner},
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
const BORROWER_SHUTDOWN_WAIT: Duration = Duration::from_secs(120);

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

pub enum FlareSolverrOperationRuntime {
    Borrowed(RuntimeFileLock),
    Owned(FlareSolverrSupervisor),
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
    pub async fn ensure_for_operation(
        config: FlareSolverrRuntimeConfig,
    ) -> Result<FlareSolverrOperationRuntime> {
        let client = health_client()?;
        if is_ready(&client, &config).await {
            let container = inspect_container(&config).await?;
            if !container.running {
                bail!("FlareSolverrUnavailable: ready endpoint has no running container");
            }
            let usage_lock = acquire_runtime_lock(
                &flaresolverr_usage_lock_path(&config)?,
                "FlareSolverr usage",
            )?;
            if is_ready(&client, &config).await {
                return Ok(FlareSolverrOperationRuntime::Borrowed(usage_lock));
            }
            drop(usage_lock);
        }
        Self::start(config)
            .await
            .map(FlareSolverrOperationRuntime::Owned)
    }

    pub async fn start(config: FlareSolverrRuntimeConfig) -> Result<Self> {
        let supervisor_lock_path = config
            .ownership_marker
            .parent()
            .context("FlareSolverrConfiguration: ownership marker had no parent")?
            .join(".flaresolverr-supervisor.lock");
        let supervisor_lock =
            acquire_runtime_lock(&supervisor_lock_path, "FlareSolverr supervisor")?;
        let client = health_client()?;

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
                let task = tokio::spawn(supervise(
                    config,
                    client,
                    owned_container_id,
                    shutdown_rx,
                    supervisor_lock,
                ));
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
        let task = tokio::spawn(supervise(
            config,
            client,
            owned_container_id,
            shutdown_rx,
            supervisor_lock,
        ));

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

impl FlareSolverrOperationRuntime {
    pub async fn shutdown(self) {
        match self {
            Self::Borrowed(usage_lock) => drop(usage_lock),
            Self::Owned(supervisor) => supervisor.shutdown().await,
        }
    }
}

fn health_client() -> Result<Client> {
    Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .context("could not build FlareSolverr health client")
}

async fn supervise(
    config: FlareSolverrRuntimeConfig,
    client: Client,
    mut owned_container_id: Option<String>,
    mut shutdown_rx: oneshot::Receiver<()>,
    _supervisor_lock: RuntimeFileLock,
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
        let Some(_usage_guard) = wait_for_flaresolverr_borrowers(&config).await else {
            emit_warning(
                "shutdown left owned container running",
                "could not safely exclude borrowed operations",
            );
            return;
        };
        match stop_owned_container(&config, &container_id).await {
            Ok(()) => remove_ownership_marker(&config),
            Err(error) => emit_warning("shutdown could not stop container", &error.to_string()),
        }
    }
}

fn flaresolverr_usage_lock_path(config: &FlareSolverrRuntimeConfig) -> Result<std::path::PathBuf> {
    Ok(config
        .ownership_marker
        .parent()
        .context("FlareSolverrConfiguration: ownership marker had no parent")?
        .join(".flaresolverr-use.lock"))
}

async fn wait_for_flaresolverr_borrowers(
    config: &FlareSolverrRuntimeConfig,
) -> Option<RuntimeFileLock> {
    let path = match flaresolverr_usage_lock_path(config) {
        Ok(path) => path,
        Err(error) => {
            emit_warning("could not construct usage lock", &error.to_string());
            return None;
        }
    };
    let deadline = Instant::now() + BORROWER_SHUTDOWN_WAIT;
    loop {
        if Instant::now() >= deadline {
            emit_warning(
                "timed out waiting for borrowed operations",
                "leaving the container running",
            );
            return None;
        }
        match acquire_runtime_lock(&path, "FlareSolverr usage") {
            Ok(lock) => return Some(lock),
            Err(error) => match runtime_lock_has_live_owner(&path, "FlareSolverr usage") {
                Ok(true) => sleep(Duration::from_millis(250)).await,
                Ok(false) => sleep(Duration::from_millis(50)).await,
                Err(inspect_error) => {
                    emit_warning(
                        "could not inspect usage lock",
                        &format!("{error}; {inspect_error}"),
                    );
                    return None;
                }
            },
        }
    }
}

fn write_ownership_marker(config: &FlareSolverrRuntimeConfig, container_id: &str) -> Result<()> {
    let parent = config
        .ownership_marker
        .parent()
        .context("FlareSolverrConfiguration: ownership marker had no parent")?;
    ensure_owner_only_dir(parent)?;
    match std::fs::symlink_metadata(&config.ownership_marker) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            std::fs::remove_file(&config.ownership_marker)
                .context("could not replace stale FlareSolverr ownership marker")?;
        }
        Ok(_) => bail!("FlareSolverrConfiguration: ownership marker is not a safe regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("could not inspect FlareSolverr ownership marker"),
    }
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options
            .open(&config.ownership_marker)
            .context("could not create FlareSolverr ownership marker")?;
        use std::io::Write as _;
        file.write_all(format!("{container_id}\n").as_bytes())?;
        file.sync_all()?;
        ensure_owner_only_file(&config.ownership_marker)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&config.ownership_marker);
    }
    result
}

fn read_ownership_marker(config: &FlareSolverrRuntimeConfig) -> Result<Option<String>> {
    match std::fs::symlink_metadata(&config.ownership_marker) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("could not inspect FlareSolverr ownership marker"),
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => bail!("FlareSolverrConfiguration: ownership marker is not a safe regular file"),
    }
    match std::fs::read_to_string(&config.ownership_marker) {
        Ok(value) => {
            let value = value.trim();
            if value.is_empty() || !value.chars().all(|character| character.is_ascii_hexdigit()) {
                return Ok(None);
            }
            Ok(Some(value.to_string()))
        }
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
    let action = container_recovery_action(&container, owned_container_id);
    let _usage_guard = if matches!(
        action,
        ContainerRecoveryAction::RestartOwned
            | ContainerRecoveryAction::StartOwned
            | ContainerRecoveryAction::StartAndAdopt
    ) {
        Some(
            acquire_runtime_lock(&flaresolverr_usage_lock_path(config)?, "FlareSolverr usage")
                .context("FlareSolverrUnavailable: a borrowed operation is active")?,
        )
    } else {
        None
    };
    match action {
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    #[cfg(unix)]
    #[tokio::test]
    async fn operation_borrows_ready_runtime_without_supervisor_ownership() {
        use std::os::unix::fs::PermissionsExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1024];
                let _ = socket.read(&mut request).await.unwrap();
                let body = r#"{"msg":"FlareSolverr is ready!"}"#;
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-flaresolverr-borrow-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let docker_path = artifact_dir.join("docker");
        std::fs::write(
            &docker_path,
            format!(
                "#!/bin/sh\nprintf '%s' '[{{\"Id\":\"abcdef123456\",\"State\":{{\"Running\":true}},\"Config\":{{\"Image\":\"ghcr.io/flaresolverr/flaresolverr@sha256:abc\",\"Labels\":{{\"com.docker.compose.service\":\"flaresolverr\"}}}},\"HostConfig\":{{\"RestartPolicy\":{{\"Name\":\"no\"}},\"PortBindings\":{{\"8191/tcp\":[{{\"HostIp\":\"127.0.0.1\",\"HostPort\":\"{}\"}}]}}}}}}]'\n",
                address.port()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&docker_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = FlareSolverrRuntimeConfig {
            flaresolverr_url: format!("http://{address}/").parse().unwrap(),
            flaresolverr_container: "unused".to_string(),
            orbctl_path: "/does/not/exist".into(),
            docker_path,
            ownership_marker: artifact_dir.join(".flaresolverr-owned"),
        };

        let runtime = FlareSolverrSupervisor::ensure_for_operation(config)
            .await
            .unwrap();

        assert!(matches!(
            &runtime,
            FlareSolverrOperationRuntime::Borrowed(_)
        ));
        assert!(artifact_dir.join(".flaresolverr-use.lock").is_file());
        assert!(!artifact_dir.join(".flaresolverr-supervisor.lock").exists());
        runtime.shutdown().await;
        assert!(!artifact_dir.join(".flaresolverr-use.lock").exists());
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn shutdown_waits_for_borrowed_runtime_usage() {
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-flaresolverr-usage-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let config = FlareSolverrRuntimeConfig {
            flaresolverr_url: "http://127.0.0.1:8191/".parse().unwrap(),
            flaresolverr_container: "unused".to_string(),
            orbctl_path: "/does/not/exist".into(),
            docker_path: "/does/not/exist".into(),
            ownership_marker: artifact_dir.join(".flaresolverr-owned"),
        };
        let usage_lock = acquire_runtime_lock(
            &artifact_dir.join(".flaresolverr-use.lock"),
            "FlareSolverr usage",
        )
        .unwrap();
        let wait_config = config.clone();
        let mut waiter =
            tokio::spawn(async move { wait_for_flaresolverr_borrowers(&wait_config).await });

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiter)
                .await
                .is_err()
        );
        drop(usage_lock);
        let acquired = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        drop(acquired);
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn ownership_marker_rejects_symlinks_without_touching_target() {
        use std::os::unix::fs::symlink;

        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-flaresolverr-marker-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let target = artifact_dir.join("target");
        std::fs::write(&target, "preserve\n").unwrap();
        let marker = artifact_dir.join(".flaresolverr-owned");
        symlink(&target, &marker).unwrap();
        let config = FlareSolverrRuntimeConfig {
            flaresolverr_url: "http://127.0.0.1:8191/".parse().unwrap(),
            flaresolverr_container: "unused".to_string(),
            orbctl_path: "/does/not/exist".into(),
            docker_path: "/does/not/exist".into(),
            ownership_marker: marker,
        };

        assert!(read_ownership_marker(&config).is_err());
        assert!(write_ownership_marker(&config, "abcdef").is_err());
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "preserve\n");
        std::fs::remove_dir_all(artifact_dir).unwrap();
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
