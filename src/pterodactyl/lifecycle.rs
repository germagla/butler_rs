use crate::{
    config::PterodactylConfig,
    run_history::{ensure_owner_only_dir, ensure_owner_only_file, write_owner_only_atomic},
    runtime_lock::{RuntimeFileLock, acquire_runtime_lock, runtime_lock_has_live_owner},
    terminal,
};
use anyhow::{Context, Result, bail};
use reqwest::{Client, redirect::Policy};
use serde::{Deserialize, Serialize};
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
const POST_MUTATION_INSPECT_ATTEMPTS: usize = 3;
const POST_MUTATION_INSPECT_INTERVAL: Duration = Duration::from_millis(250);

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
    Owned {
        supervisor: FlareSolverrSupervisor,
        usage_lock: RuntimeFileLock,
    },
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
    pub async fn reconcile_stale_ownership(config: FlareSolverrRuntimeConfig) -> Result<()> {
        tokio::spawn(Self::reconcile_stale_ownership_inner(config))
            .await
            .context("FlareSolverr reconciliation task failed")?
    }

    async fn reconcile_stale_ownership_inner(config: FlareSolverrRuntimeConfig) -> Result<()> {
        let Some(marker_file) = read_ownership_marker(&config)? else {
            return Ok(());
        };
        let marker = match marker_file {
            OwnershipMarkerFile::Current(marker) => OwnershipMarkerFile::Current(marker),
            OwnershipMarkerFile::Pending(_) => {
                emit_warning(
                    "pending ownership intent needs manual reconciliation",
                    "leaving the marker and container unchanged because the resulting start generation was never recorded",
                );
                return Ok(());
            }
            OwnershipMarkerFile::Legacy => {
                emit_warning(
                    "legacy ownership marker needs manual reconciliation",
                    "leaving the marker and container unchanged because its start generation is unknown",
                );
                return Ok(());
            }
        };
        let supervisor_lock_path = flaresolverr_supervisor_lock_path(&config)?;
        if runtime_lock_has_live_owner(&supervisor_lock_path, "FlareSolverr supervisor")? {
            return Ok(());
        }
        if !matches!(orb_status(&config).await?, OrbStackStatus::Running) {
            return Ok(());
        }

        let _supervisor_lock =
            acquire_runtime_lock(&supervisor_lock_path, "FlareSolverr supervisor")?;
        let _usage_lock = wait_for_usage_lock(&config).await?;
        let Some(current_marker) = read_ownership_marker(&config)? else {
            return Ok(());
        };
        if current_marker != marker {
            return Ok(());
        }
        let container = inspect_container(&config).await?;
        if !marker.matches(&container) {
            remove_ownership_marker(&config);
            return Ok(());
        }
        if container.running {
            stop_owned_and_remove_marker(&config, &container).await?;
        } else {
            remove_ownership_marker(&config);
        }
        Ok(())
    }

    pub async fn ensure_for_operation(
        config: FlareSolverrRuntimeConfig,
    ) -> Result<FlareSolverrOperationRuntime> {
        tokio::spawn(Self::ensure_for_operation_inner(config))
            .await
            .context("FlareSolverr startup task failed")?
    }

    async fn ensure_for_operation_inner(
        config: FlareSolverrRuntimeConfig,
    ) -> Result<FlareSolverrOperationRuntime> {
        let client = health_client()?;
        if is_ready(&client, &config).await {
            let container = inspect_container(&config).await?;
            if !container.running {
                bail!("FlareSolverrUnavailable: ready endpoint has no running container");
            }
            match read_ownership_marker(&config)? {
                Some(marker)
                    if marker.matches(&container)
                        && !runtime_lock_has_live_owner(
                            &flaresolverr_supervisor_lock_path(&config)?,
                            "FlareSolverr supervisor",
                        )? =>
                {
                    return Self::start_owned_operation(config).await;
                }
                Some(OwnershipMarkerFile::Current(marker)) if !marker.matches(&container) => {
                    remove_ownership_marker(&config);
                }
                Some(OwnershipMarkerFile::Pending(_)) => emit_warning(
                    "pending ownership intent needs manual reconciliation",
                    "borrowing the container without claiming or stopping its unknown generation",
                ),
                Some(OwnershipMarkerFile::Legacy) => emit_warning(
                    "generation-unknown ownership marker needs manual reconciliation",
                    "borrowing the running container without claiming or stopping it",
                ),
                _ => {}
            }
            let usage_lock = wait_for_usage_lock(&config).await?;
            if is_ready(&client, &config).await {
                return Ok(FlareSolverrOperationRuntime::Borrowed(usage_lock));
            }
            drop(usage_lock);
        }
        if runtime_lock_has_live_owner(
            &flaresolverr_supervisor_lock_path(&config)?,
            "FlareSolverr supervisor",
        )? {
            return wait_for_supervised_runtime(&client, &config).await;
        }
        match Self::start_owned_operation(config.clone()).await {
            Ok(runtime) => Ok(runtime),
            Err(_error)
                if runtime_lock_has_live_owner(
                    &flaresolverr_supervisor_lock_path(&config)?,
                    "FlareSolverr supervisor",
                )? =>
            {
                wait_for_supervised_runtime(&client, &config).await
            }
            Err(error) => Err(error),
        }
    }

    async fn start_owned_operation(
        config: FlareSolverrRuntimeConfig,
    ) -> Result<FlareSolverrOperationRuntime> {
        let supervisor = Self::start(config.clone()).await?;
        let usage_lock = wait_for_usage_lock(&config).await?;
        Ok(FlareSolverrOperationRuntime::Owned {
            supervisor,
            usage_lock,
        })
    }

    pub async fn start(config: FlareSolverrRuntimeConfig) -> Result<Self> {
        let supervisor_lock_path = flaresolverr_supervisor_lock_path(&config)?;
        let supervisor_lock =
            acquire_runtime_lock(&supervisor_lock_path, "FlareSolverr supervisor")?;
        let client = health_client()?;

        ensure_orbstack_running(&config).await?;
        let initial_container = inspect_container(&config).await?;
        let marker = read_ownership_marker(&config)?;
        let inherited_ownership = marker
            .as_ref()
            .is_some_and(|marker| marker.matches(&initial_container));
        match marker.as_ref() {
            Some(OwnershipMarkerFile::Current(_)) if !inherited_ownership => {
                remove_ownership_marker(&config);
            }
            Some(OwnershipMarkerFile::Current(_)) => {
                ensure_owner_only_file(&config.ownership_marker)?;
            }
            Some(OwnershipMarkerFile::Pending(_)) => emit_warning(
                "pending ownership intent needs manual reconciliation",
                "leaving the marker unchanged because the resulting start generation was never recorded",
            ),
            Some(OwnershipMarkerFile::Legacy) => emit_warning(
                "generation-unknown ownership marker needs manual reconciliation",
                "leaving the marker unchanged because its start generation is unknown",
            ),
            None => {}
        }
        let started_container = !initial_container.running;
        if started_container {
            if matches!(marker, Some(OwnershipMarkerFile::Legacy)) {
                bail!(
                    "FlareSolverrUnavailable: legacy ownership marker must be reconciled before starting the container"
                );
            }
            start_container_with_intent(&config, &initial_container).await?;
        }
        let current_container = if started_container {
            match inspect_container_reference_after_mutation(&config, &initial_container.id).await {
                Ok(container) => container,
                Err(error) => return Err(error),
            }
        } else {
            initial_container
        };
        let mut owned_container =
            (inherited_ownership || started_container).then(|| current_container.clone());
        if started_container && let Err(error) = write_ownership_marker(&config, &current_container)
        {
            if let Err(cleanup_error) =
                stop_owned_and_remove_marker(&config, &current_container).await
            {
                emit_warning(
                    "could not clean up after ownership marker failure",
                    &cleanup_error.to_string(),
                );
            }
            return Err(error);
        }
        if let Err(first_error) = wait_until_ready(&client, &config).await {
            let recovery = match owned_container.as_ref() {
                Some(container) => match restart_container_with_intent(&config, container).await {
                    Ok(()) => {
                        match inspect_container_reference_after_mutation(&config, &container.id)
                            .await
                        {
                            Ok(restarted) => match write_ownership_marker(&config, &restarted) {
                                Ok(()) => {
                                    owned_container = Some(restarted);
                                    wait_until_ready(&client, &config).await
                                }
                                Err(error) => {
                                    if let Err(cleanup_error) =
                                        stop_owned_and_remove_marker(&config, &restarted).await
                                    {
                                        emit_warning(
                                            "could not clean up after ownership marker failure",
                                            &cleanup_error.to_string(),
                                        );
                                    }
                                    Err(error)
                                }
                            },
                            Err(error) => Err(error),
                        }
                    }
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
                    owned_container,
                    shutdown_rx,
                    supervisor_lock,
                ));
                return Ok(Self {
                    shutdown_tx: Some(shutdown_tx),
                    task: Some(task),
                });
            }
            if let Some(container) = owned_container.as_ref()
                && stop_owned_container(&config, container).await.is_ok()
            {
                remove_ownership_marker(&config);
            }
            return Err(first_error);
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(supervise(
            config,
            client,
            owned_container,
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

impl Drop for FlareSolverrSupervisor {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

impl FlareSolverrOperationRuntime {
    pub async fn shutdown(self) {
        match self {
            Self::Borrowed(usage_lock) => drop(usage_lock),
            Self::Owned {
                supervisor,
                usage_lock,
            } => {
                drop(usage_lock);
                supervisor.shutdown().await;
            }
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
    mut owned_container: Option<ContainerSnapshot>,
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
                    result = ensure_runtime_healthy(&client, &config, owned_container.as_ref()) => {
                        match result {
                            Ok(updated_container) => {
                                if updated_container.is_some() {
                                    owned_container = updated_container;
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

    if let Some(container) = owned_container {
        let Some(_usage_guard) = wait_for_flaresolverr_borrowers(&config).await else {
            emit_warning(
                "shutdown left owned container running",
                "could not safely exclude borrowed operations",
            );
            return;
        };
        match stop_owned_container(&config, &container).await {
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

fn flaresolverr_supervisor_lock_path(
    config: &FlareSolverrRuntimeConfig,
) -> Result<std::path::PathBuf> {
    Ok(config
        .ownership_marker
        .parent()
        .context("FlareSolverrConfiguration: ownership marker had no parent")?
        .join(".flaresolverr-supervisor.lock"))
}

async fn wait_for_usage_lock(config: &FlareSolverrRuntimeConfig) -> Result<RuntimeFileLock> {
    let path = flaresolverr_usage_lock_path(config)?;
    let deadline = Instant::now() + BORROWER_SHUTDOWN_WAIT;
    loop {
        match acquire_runtime_lock(&path, "FlareSolverr usage") {
            Ok(lock) => return Ok(lock),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => sleep(Duration::from_millis(250)).await,
        }
    }
}

async fn wait_for_supervised_runtime(
    client: &Client,
    config: &FlareSolverrRuntimeConfig,
) -> Result<FlareSolverrOperationRuntime> {
    let deadline = Instant::now() + FLARESOLVERR_READY_TIMEOUT;
    while Instant::now() < deadline {
        if is_ready(client, config).await {
            let container = inspect_container(config).await?;
            if !container.running {
                bail!("FlareSolverrUnavailable: ready endpoint has no running container");
            }
            let usage_lock = wait_for_usage_lock(config).await?;
            if is_ready(client, config).await {
                return Ok(FlareSolverrOperationRuntime::Borrowed(usage_lock));
            }
            drop(usage_lock);
        }
        sleep(Duration::from_millis(250)).await;
    }
    bail!("FlareSolverrUnavailable: active supervisor did not become ready before timeout")
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct OwnershipMarker {
    version: u8,
    container_id: String,
    started_at: String,
}

impl OwnershipMarker {
    fn from_container(container: &ContainerSnapshot) -> Self {
        Self {
            version: 1,
            container_id: container.id.clone(),
            started_at: container.started_at.clone(),
        }
    }

    fn matches(&self, container: &ContainerSnapshot) -> bool {
        self.version == 1
            && self.container_id == container.id
            && self.started_at == container.started_at
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct PendingOwnershipMarker {
    version: u8,
    pending: bool,
    container_id: String,
    previous_started_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OwnershipMarkerFile {
    Current(OwnershipMarker),
    Pending(PendingOwnershipMarker),
    Legacy,
}

impl OwnershipMarkerFile {
    fn matches(&self, container: &ContainerSnapshot) -> bool {
        match self {
            Self::Current(marker) => marker.matches(container),
            Self::Pending(_) => false,
            Self::Legacy => false,
        }
    }
}

fn write_ownership_marker(
    config: &FlareSolverrRuntimeConfig,
    container: &ContainerSnapshot,
) -> Result<()> {
    let parent = config
        .ownership_marker
        .parent()
        .context("FlareSolverrConfiguration: ownership marker had no parent")?;
    ensure_owner_only_dir(parent)?;
    match std::fs::symlink_metadata(&config.ownership_marker) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => bail!("FlareSolverrConfiguration: ownership marker is not a safe regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("could not inspect FlareSolverr ownership marker"),
    }
    let bytes = serde_json::to_vec(&OwnershipMarker::from_container(container))
        .context("could not serialize FlareSolverr ownership marker")?;
    write_owner_only_atomic(&config.ownership_marker, &bytes)
}

fn write_pending_ownership_marker(
    config: &FlareSolverrRuntimeConfig,
    previous: &ContainerSnapshot,
) -> Result<()> {
    let bytes = serde_json::to_vec(&PendingOwnershipMarker {
        version: 1,
        pending: true,
        container_id: previous.id.clone(),
        previous_started_at: previous.started_at.clone(),
    })
    .context("could not serialize pending FlareSolverr ownership marker")?;
    write_owner_only_atomic(&config.ownership_marker, &bytes)
}

async fn start_container_with_intent(
    config: &FlareSolverrRuntimeConfig,
    previous: &ContainerSnapshot,
) -> Result<()> {
    write_pending_ownership_marker(config, previous)?;
    start_container(config, &previous.id).await
}

async fn restart_container_with_intent(
    config: &FlareSolverrRuntimeConfig,
    previous: &ContainerSnapshot,
) -> Result<()> {
    write_pending_ownership_marker(config, previous)?;
    restart_container(config, &previous.id).await
}

fn read_ownership_marker(
    config: &FlareSolverrRuntimeConfig,
) -> Result<Option<OwnershipMarkerFile>> {
    match std::fs::symlink_metadata(&config.ownership_marker) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("could not inspect FlareSolverr ownership marker"),
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => bail!("FlareSolverrConfiguration: ownership marker is not a safe regular file"),
    }
    match std::fs::read_to_string(&config.ownership_marker) {
        Ok(value) => {
            let value = value.trim();
            if !value.is_empty() && value.chars().all(|character| character.is_ascii_hexdigit()) {
                return Ok(Some(OwnershipMarkerFile::Legacy));
            }
            if let Ok(marker) = serde_json::from_str::<OwnershipMarker>(value) {
                if marker.version != 1
                    || marker.container_id.is_empty()
                    || !marker
                        .container_id
                        .chars()
                        .all(|character| character.is_ascii_hexdigit())
                    || marker.started_at.trim().is_empty()
                {
                    bail!("FlareSolverr ownership marker was invalid");
                }
                return Ok(Some(OwnershipMarkerFile::Current(marker)));
            }
            let marker: PendingOwnershipMarker = serde_json::from_str(value)
                .context("FlareSolverr ownership marker was not valid JSON")?;
            if marker.version != 1
                || !marker.pending
                || marker.container_id.is_empty()
                || !marker
                    .container_id
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
                || marker.previous_started_at.trim().is_empty()
            {
                bail!("FlareSolverr pending ownership marker was invalid");
            }
            Ok(Some(OwnershipMarkerFile::Pending(marker)))
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
    owned_container: Option<&ContainerSnapshot>,
) -> Result<Option<ContainerSnapshot>> {
    if is_ready(client, config).await {
        return Ok(owned_container.cloned());
    }

    ensure_orbstack_running(config).await?;
    let container = inspect_container(config).await?;
    let action = container_recovery_action(&container, owned_container);
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
            let expected = owned_container.expect("owned recovery requires an ID");
            restart_container_with_intent(config, expected).await?;
            let restarted =
                inspect_container_reference_after_mutation(config, &expected.id).await?;
            if let Err(error) = write_ownership_marker(config, &restarted) {
                if let Err(cleanup_error) = stop_owned_and_remove_marker(config, &restarted).await {
                    emit_warning(
                        "could not clean up after ownership marker failure",
                        &cleanup_error.to_string(),
                    );
                }
                return Err(error);
            }
            if let Err(error) = wait_until_ready(client, config).await {
                if let Err(cleanup_error) = stop_owned_and_remove_marker(config, &restarted).await {
                    emit_warning(
                        "could not clean up after readiness failure",
                        &cleanup_error.to_string(),
                    );
                }
                return Err(error);
            }
            Ok(Some(restarted))
        }
        ContainerRecoveryAction::StartOwned => {
            let expected = owned_container.expect("owned recovery requires an ID");
            start_container_with_intent(config, expected).await?;
            let started = inspect_container_reference_after_mutation(config, &expected.id).await?;
            if let Err(error) = write_ownership_marker(config, &started) {
                if let Err(cleanup_error) = stop_owned_and_remove_marker(config, &started).await {
                    emit_warning(
                        "could not clean up after ownership marker failure",
                        &cleanup_error.to_string(),
                    );
                }
                return Err(error);
            }
            if let Err(error) = wait_until_ready(client, config).await {
                if let Err(cleanup_error) = stop_owned_and_remove_marker(config, &started).await {
                    emit_warning(
                        "could not clean up after readiness failure",
                        &cleanup_error.to_string(),
                    );
                }
                return Err(error);
            }
            Ok(Some(started))
        }
        ContainerRecoveryAction::StartAndAdopt => {
            start_container_with_intent(config, &container).await?;
            let started = inspect_container_reference_after_mutation(config, &container.id).await?;
            if let Err(error) = write_ownership_marker(config, &started) {
                if let Err(cleanup_error) = stop_owned_and_remove_marker(config, &started).await {
                    emit_warning(
                        "could not clean up after ownership marker failure",
                        &cleanup_error.to_string(),
                    );
                }
                return Err(error);
            }
            if let Err(error) = wait_until_ready(client, config).await {
                if let Err(cleanup_error) = stop_owned_and_remove_marker(config, &started).await {
                    emit_warning(
                        "could not clean up after readiness failure",
                        &cleanup_error.to_string(),
                    );
                }
                return Err(error);
            }
            Ok(Some(started))
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

async fn inspect_container_reference_after_mutation(
    config: &FlareSolverrRuntimeConfig,
    reference: &str,
) -> Result<ContainerSnapshot> {
    for attempt in 1..=POST_MUTATION_INSPECT_ATTEMPTS {
        match inspect_container_reference(config, reference).await {
            Ok(container) => return Ok(container),
            Err(error) if attempt < POST_MUTATION_INSPECT_ATTEMPTS => {
                emit_warning(
                    "post-mutation container inspection failed",
                    &error.to_string(),
                );
                sleep(POST_MUTATION_INSPECT_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("inspection attempts must return a result")
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
    started_at: String,
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ContainerSnapshot {
    id: String,
    running: bool,
    started_at: String,
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
    owned_container: Option<&ContainerSnapshot>,
) -> ContainerRecoveryAction {
    match owned_container {
        Some(expected)
            if container.id != expected.id || container.started_at != expected.started_at =>
        {
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
    if inspection.state.started_at.trim().is_empty()
        || inspection.state.started_at.chars().any(char::is_control)
    {
        bail!("FlareSolverrUnavailable: container start timestamp was invalid");
    }
    Ok(ContainerSnapshot {
        id: inspection.id.clone(),
        running: inspection.state.running,
        started_at: inspection.state.started_at.clone(),
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

async fn stop_owned_container(
    config: &FlareSolverrRuntimeConfig,
    expected: &ContainerSnapshot,
) -> Result<()> {
    let current = inspect_container_reference(config, &expected.id).await?;
    if current.id != expected.id || current.started_at != expected.started_at {
        bail!("FlareSolverrCleanupFailed: configured container identity changed");
    }
    stop_container(config, &expected.id).await
}

async fn stop_owned_and_remove_marker(
    config: &FlareSolverrRuntimeConfig,
    expected: &ContainerSnapshot,
) -> Result<()> {
    stop_owned_container(config, expected).await?;
    remove_ownership_marker(config);
    Ok(())
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
        let valid = br#"[{"Id":"abcdef123456","State":{"Running":true,"StartedAt":"2026-07-28T12:00:00Z"},"Config":{"Image":"ghcr.io/flaresolverr/flaresolverr@sha256:abc","Labels":{"com.docker.compose.service":"flaresolverr"}},"HostConfig":{"RestartPolicy":{"Name":"no"},"PortBindings":{"8191/tcp":[{"HostIp":"127.0.0.1","HostPort":"8191"}]}}}]"#;
        assert_eq!(
            parse_container_inspection(valid, 8191).unwrap(),
            ContainerSnapshot {
                id: "abcdef123456".to_string(),
                running: true,
                started_at: "2026-07-28T12:00:00Z".to_string(),
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
                "#!/bin/sh\nprintf '%s' '[{{\"Id\":\"abcdef123456\",\"State\":{{\"Running\":true,\"StartedAt\":\"2026-07-28T12:00:00Z\"}},\"Config\":{{\"Image\":\"ghcr.io/flaresolverr/flaresolverr@sha256:abc\",\"Labels\":{{\"com.docker.compose.service\":\"flaresolverr\"}}}},\"HostConfig\":{{\"RestartPolicy\":{{\"Name\":\"no\"}},\"PortBindings\":{{\"8191/tcp\":[{{\"HostIp\":\"127.0.0.1\",\"HostPort\":\"{}\"}}]}}}}}}]'\n",
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

    #[cfg(unix)]
    #[tokio::test]
    async fn operation_starts_and_stops_owned_runtime() {
        use std::os::unix::fs::PermissionsExt;

        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-flaresolverr-owned-operation-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let running_path = artifact_dir.join("running");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_running_path = running_path.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 1024];
                let _ = socket.read(&mut request).await;
                let (status, body) = if server_running_path.is_file() {
                    ("200 OK", r#"{"msg":"FlareSolverr is ready!"}"#)
                } else {
                    ("503 Service Unavailable", r#"{"msg":"stopped"}"#)
                };
                let _ = socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await;
            }
        });
        let docker_path = artifact_dir.join("docker");
        std::fs::write(
            &docker_path,
            format!(
                "#!/bin/sh\nrunning='{running}'\ncase \"$1\" in\ninspect)\n  if [ -f \"$running\" ]; then state=true; started='2026-07-28T12:00:00Z'; else state=false; started='0001-01-01T00:00:00Z'; fi\n  printf '[{{\"Id\":\"abcdef123456\",\"State\":{{\"Running\":%s,\"StartedAt\":\"%s\"}},\"Config\":{{\"Image\":\"ghcr.io/flaresolverr/flaresolverr@sha256:abc\",\"Labels\":{{\"com.docker.compose.service\":\"flaresolverr\"}}}},\"HostConfig\":{{\"RestartPolicy\":{{\"Name\":\"no\"}},\"PortBindings\":{{\"8191/tcp\":[{{\"HostIp\":\"127.0.0.1\",\"HostPort\":\"{port}\"}}]}}}}}}]' \"$state\" \"$started\"\n  ;;\nstart) : > \"$running\" ;;\nstop) rm -f \"$running\" ;;\nrestart) : > \"$running\" ;;\nesac\n",
                running = running_path.display(),
                port = address.port(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&docker_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let orbctl_path = artifact_dir.join("orbctl");
        std::fs::write(&orbctl_path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&orbctl_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = FlareSolverrRuntimeConfig {
            flaresolverr_url: format!("http://{address}/").parse().unwrap(),
            flaresolverr_container: "flaresolverr".to_string(),
            orbctl_path,
            docker_path,
            ownership_marker: artifact_dir.join(".flaresolverr-owned"),
        };

        let runtime = FlareSolverrSupervisor::ensure_for_operation(config)
            .await
            .unwrap();
        assert!(matches!(
            &runtime,
            FlareSolverrOperationRuntime::Owned { .. }
        ));
        assert!(running_path.is_file());
        assert!(artifact_dir.join(".flaresolverr-owned").is_file());

        runtime.shutdown().await;

        assert!(!running_path.exists());
        assert!(!artifact_dir.join(".flaresolverr-owned").exists());
        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_post_start_inspection_retains_pending_ownership() {
        use std::os::unix::fs::PermissionsExt;

        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-flaresolverr-pending-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let inspected_path = artifact_dir.join("inspected");
        let docker_path = artifact_dir.join("docker");
        std::fs::write(
            &docker_path,
            format!(
                "#!/bin/sh\ninspected='{inspected}'\ncase \"$1\" in\ninspect)\n  if [ ! -f \"$inspected\" ]; then\n    touch \"$inspected\"\n    printf '[{{\"Id\":\"abcdef123456\",\"State\":{{\"Running\":false,\"StartedAt\":\"2026-07-28T12:00:00Z\"}},\"Config\":{{\"Image\":\"ghcr.io/flaresolverr/flaresolverr@sha256:abc\",\"Labels\":{{\"com.docker.compose.service\":\"flaresolverr\"}}}},\"HostConfig\":{{\"RestartPolicy\":{{\"Name\":\"no\"}},\"PortBindings\":{{\"8191/tcp\":[{{\"HostIp\":\"127.0.0.1\",\"HostPort\":\"8191\"}}]}}}}}}]'\n  else\n    exit 1\n  fi ;;\nstart) exit 0 ;;\nesac\n",
                inspected = inspected_path.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&docker_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let orbctl_path = artifact_dir.join("orbctl");
        std::fs::write(&orbctl_path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&orbctl_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = FlareSolverrRuntimeConfig {
            flaresolverr_url: "http://127.0.0.1:8191/".parse().unwrap(),
            flaresolverr_container: "flaresolverr".to_string(),
            orbctl_path,
            docker_path,
            ownership_marker: artifact_dir.join(".flaresolverr-owned"),
        };

        assert!(FlareSolverrSupervisor::start(config.clone()).await.is_err());
        let marker = read_ownership_marker(&config).unwrap().unwrap();

        assert!(matches!(
            marker,
            OwnershipMarkerFile::Pending(PendingOwnershipMarker {
                container_id,
                ..
            }) if container_id == "abcdef123456"
        ));
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn startup_reconciliation_stops_exact_stale_owned_generation() {
        use std::os::unix::fs::PermissionsExt;

        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-flaresolverr-stale-reconcile-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let running_path = artifact_dir.join("running");
        std::fs::write(&running_path, "").unwrap();
        let docker_path = artifact_dir.join("docker");
        std::fs::write(
            &docker_path,
            format!(
                "#!/bin/sh\nrunning='{running}'\ncase \"$1\" in\ninspect) printf '[{{\"Id\":\"abcdef123456\",\"State\":{{\"Running\":true,\"StartedAt\":\"2026-07-28T12:00:00Z\"}},\"Config\":{{\"Image\":\"ghcr.io/flaresolverr/flaresolverr@sha256:abc\",\"Labels\":{{\"com.docker.compose.service\":\"flaresolverr\"}}}},\"HostConfig\":{{\"RestartPolicy\":{{\"Name\":\"no\"}},\"PortBindings\":{{\"8191/tcp\":[{{\"HostIp\":\"127.0.0.1\",\"HostPort\":\"8191\"}}]}}}}}}]' ;;\nstop) rm -f \"$running\" ;;\nesac\n",
                running = running_path.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&docker_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let orbctl_path = artifact_dir.join("orbctl");
        std::fs::write(&orbctl_path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&orbctl_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = FlareSolverrRuntimeConfig {
            flaresolverr_url: "http://127.0.0.1:8191/".parse().unwrap(),
            flaresolverr_container: "flaresolverr".to_string(),
            orbctl_path,
            docker_path,
            ownership_marker: artifact_dir.join(".flaresolverr-owned"),
        };
        write_ownership_marker(
            &config,
            &ContainerSnapshot {
                id: "abcdef123456".to_string(),
                running: true,
                started_at: "2026-07-28T12:00:00Z".to_string(),
            },
        )
        .unwrap();

        let usage_lock = acquire_runtime_lock(
            &artifact_dir.join(".flaresolverr-use.lock"),
            "FlareSolverr usage",
        )
        .unwrap();
        let mut reconciliation =
            tokio::spawn(FlareSolverrSupervisor::reconcile_stale_ownership(config));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut reconciliation)
                .await
                .is_err()
        );
        assert!(running_path.exists());
        drop(usage_lock);
        tokio::time::timeout(Duration::from_secs(30), reconciliation)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(!running_path.exists());
        assert!(!artifact_dir.join(".flaresolverr-owned").exists());
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
        assert!(
            write_ownership_marker(
                &config,
                &ContainerSnapshot {
                    id: "abcdef".to_string(),
                    running: true,
                    started_at: "2026-07-28T12:00:00Z".to_string(),
                }
            )
            .is_err()
        );
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "preserve\n");
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[test]
    fn selects_identity_safe_container_recovery_actions() {
        let running = ContainerSnapshot {
            id: "container-a".to_string(),
            running: true,
            started_at: "start-a".to_string(),
        };
        let stopped = ContainerSnapshot {
            id: "container-a".to_string(),
            running: false,
            started_at: "start-a".to_string(),
        };

        assert_eq!(
            container_recovery_action(&running, Some(&running)),
            ContainerRecoveryAction::RestartOwned
        );
        assert_eq!(
            container_recovery_action(&stopped, Some(&stopped)),
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
            container_recovery_action(
                &running,
                Some(&ContainerSnapshot {
                    id: "container-b".to_string(),
                    running: true,
                    started_at: "start-b".to_string(),
                })
            ),
            ContainerRecoveryAction::IdentityChanged
        );
        assert_eq!(
            container_recovery_action(
                &running,
                Some(&ContainerSnapshot {
                    id: "container-a".to_string(),
                    running: true,
                    started_at: "later-start".to_string(),
                })
            ),
            ContainerRecoveryAction::IdentityChanged
        );
    }

    #[test]
    fn ownership_marker_matches_container_generation_and_legacy_does_not() {
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-flaresolverr-marker-generation-{}",
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
        let container = ContainerSnapshot {
            id: "abcdef123456".to_string(),
            running: true,
            started_at: "2026-07-28T12:00:00Z".to_string(),
        };

        write_ownership_marker(&config, &container).unwrap();
        let marker = read_ownership_marker(&config).unwrap().unwrap();
        assert!(marker.matches(&container));
        assert!(!marker.matches(&ContainerSnapshot {
            started_at: "2026-07-28T13:00:00Z".to_string(),
            ..container.clone()
        }));

        std::fs::write(&config.ownership_marker, "abcdef123456\n").unwrap();
        let legacy = read_ownership_marker(&config).unwrap().unwrap();
        assert!(!legacy.matches(&container));

        let pending = OwnershipMarkerFile::Pending(PendingOwnershipMarker {
            version: 1,
            pending: true,
            container_id: container.id.clone(),
            previous_started_at: container.started_at.clone(),
        });
        assert!(!pending.matches(&container));
        assert!(!pending.matches(&ContainerSnapshot {
            started_at: "2026-12-31T12:00:00Z".to_string(),
            ..container
        }));
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }
}
