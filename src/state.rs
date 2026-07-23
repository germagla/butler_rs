use crate::{
    config::{ArtifactCapture, Config},
    provider::ServerStartProvider,
    run_history::{
        EventFileMaintenance, RunContext, RunStore, ensure_owner_only_file,
        verify_artifact_dir_writable,
    },
    terminal,
};
use anyhow::{Context, Result};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify, RwLock};

#[derive(Clone, Debug)]
pub struct ActiveStartRun {
    pub run_id: String,
    pub guild_id: Option<String>,
}

pub enum StartAdmissionError {
    Busy(ActiveStartRun),
    ShuttingDown,
    Unavailable,
}

pub struct ActiveStartLease {
    inner: Option<Arc<ActiveStartLeaseInner>>,
}

struct ActiveStartLeaseInner {
    active_start_run: Arc<Mutex<Option<ActiveStartRun>>>,
    active_start_marker: PathBuf,
    run_id: String,
    _admission_lock: RuntimeFileLock,
    _provider_operation_guard: ProviderOperationGuard,
}

pub struct ActiveStartOperationGuard {
    _inner: Arc<ActiveStartLeaseInner>,
}

pub struct ProviderOperationGuard {
    tracker: Arc<ProviderOperationTracker>,
}

#[derive(Default)]
struct ProviderOperationTracker {
    active: AtomicUsize,
    idle: Notify,
}

struct RuntimeFileLock {
    path: PathBuf,
    owner: String,
}

impl ActiveStartLease {
    pub async fn finish(mut self) {
        if let Some(inner) = self.inner.take() {
            clear_active_start(&inner.active_start_run, &inner.run_id).await;
            clear_active_start_marker(&inner.active_start_marker, &inner.run_id);
        }
    }

    pub fn operation_guard(&self) -> ActiveStartOperationGuard {
        ActiveStartOperationGuard {
            _inner: self
                .inner
                .as_ref()
                .expect("active lease must exist")
                .clone(),
        }
    }
}

impl Drop for ActiveStartLeaseInner {
    fn drop(&mut self) {
        let active_start_run = self.active_start_run.clone();
        let active_start_marker = self.active_start_marker.clone();
        let run_id = self.run_id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                clear_active_start(&active_start_run, &run_id).await;
                clear_active_start_marker(&active_start_marker, &run_id);
            });
        }
    }
}

#[derive(Clone)]
pub struct BotState {
    pub config: Arc<Config>,
    pub provider: Arc<dyn ServerStartProvider>,
    pub run_store: RunStore,
    active_start_run: Arc<Mutex<Option<ActiveStartRun>>>,
    active_start_marker: PathBuf,
    start_admission_lock: PathBuf,
    _instance_lock: Arc<RuntimeFileLock>,
    minecraft_address: Arc<RwLock<String>>,
    provider_operations: Arc<ProviderOperationTracker>,
    accepting_starts: Arc<AtomicBool>,
}

impl BotState {
    pub fn new(config: Config, provider: Arc<dyn ServerStartProvider>) -> Result<Self> {
        match RunStore::prepare_artifact_dir(
            &config.artifact_dir,
            config.run_history_limit,
            config.redact_run_events,
        ) {
            Ok(Some(maintenance)) => {
                let detail = match maintenance {
                    EventFileMaintenance::RotatedUnredacted(path) => {
                        format!("rotated unredacted events to {}", path.display())
                    }
                    EventFileMaintenance::QuarantinedCorrupt(path) => {
                        format!("quarantined corrupt events to {}", path.display())
                    }
                };
                terminal::emit(terminal::line("WARN", "artifacts", "", "", None, detail));
            }
            Ok(None) => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "artifact maintenance failed for {}",
                        config.artifact_dir.display()
                    )
                });
            }
        }

        if config.artifact_capture != ArtifactCapture::Off
            || config.persist_run_events
            || matches!(
                &config.provider,
                crate::config::ProviderConfig::Pterodactyl(_)
            )
        {
            verify_artifact_dir_writable(&config.artifact_dir).with_context(|| {
                format!(
                    "artifact directory is not writable: {}",
                    config.artifact_dir.display()
                )
            })?;
        }

        let active_start_marker = config.artifact_dir.join(".active-start");
        let instance_lock = Arc::new(acquire_runtime_lock(
            &config.artifact_dir.join(".instance-lock"),
            "Butler instance",
        )?);
        remove_stale_active_marker(&active_start_marker)?;
        let run_store = RunStore::new(
            config.run_history_limit,
            &config.artifact_dir,
            config.persist_run_events,
            config.redact_run_events,
        );
        let start_admission_lock = config.artifact_dir.join(".start-admission.lock");
        let minecraft_address = config.minecraft_server_addr.clone();
        Ok(Self {
            config: Arc::new(config),
            provider,
            run_store,
            active_start_run: Arc::new(Mutex::new(None)),
            active_start_marker,
            start_admission_lock,
            _instance_lock: instance_lock,
            minecraft_address: Arc::new(RwLock::new(minecraft_address)),
            provider_operations: Arc::new(ProviderOperationTracker::default()),
            accepting_starts: Arc::new(AtomicBool::new(true)),
        })
    }

    pub async fn active_start_run(&self) -> Option<ActiveStartRun> {
        self.active_start_run.lock().await.clone()
    }

    pub async fn minecraft_address(&self) -> String {
        self.minecraft_address.read().await.clone()
    }

    pub async fn set_minecraft_address(&self, address: String) {
        *self.minecraft_address.write().await = address;
    }

    pub async fn begin_shutdown(&self) {
        let _active = self.active_start_run.lock().await;
        self.accepting_starts.store(false, Ordering::Release);
    }

    pub async fn wait_for_provider_operations(&self, duration: Duration) -> bool {
        self.provider_operations.wait_for_idle(duration).await
    }

    pub async fn try_begin_start_run(
        &self,
        context: &RunContext,
    ) -> std::result::Result<ActiveStartLease, StartAdmissionError> {
        if !self.accepting_starts.load(Ordering::Acquire) {
            return Err(StartAdmissionError::ShuttingDown);
        }
        let mut active = self.active_start_run.lock().await;
        if !self.accepting_starts.load(Ordering::Acquire) {
            return Err(StartAdmissionError::ShuttingDown);
        }
        if let Some(active) = active.as_ref() {
            return Err(StartAdmissionError::Busy(active.clone()));
        }
        let admission_lock =
            match acquire_runtime_lock(&self.start_admission_lock, "start admission") {
                Ok(lock) => lock,
                Err(error) => {
                    terminal::emit(terminal::line(
                        "WARN",
                        "start.admission",
                        "",
                        "",
                        None,
                        format!(
                            "could not acquire start-admission lock; error {}",
                            terminal::clean(&error.to_string())
                        ),
                    ));
                    return Err(StartAdmissionError::Unavailable);
                }
            };
        if let Err(error) = write_active_start_marker(&self.active_start_marker, &context.run_id) {
            terminal::emit(terminal::line(
                "WARN",
                "start.marker",
                "",
                "",
                None,
                format!(
                    "could not write active-start marker; error {}",
                    terminal::clean(&error.to_string())
                ),
            ));
            return Err(StartAdmissionError::Unavailable);
        }
        *active = Some(ActiveStartRun {
            run_id: context.run_id.clone(),
            guild_id: context.guild_id.clone(),
        });
        Ok(ActiveStartLease {
            inner: Some(Arc::new(ActiveStartLeaseInner {
                active_start_run: self.active_start_run.clone(),
                active_start_marker: self.active_start_marker.clone(),
                run_id: context.run_id.clone(),
                _admission_lock: admission_lock,
                _provider_operation_guard: self.provider_operations.begin(),
            })),
        })
    }
}

impl ProviderOperationTracker {
    fn begin(self: &Arc<Self>) -> ProviderOperationGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        ProviderOperationGuard {
            tracker: self.clone(),
        }
    }

    async fn wait_for_idle(&self, duration: Duration) -> bool {
        tokio::time::timeout(duration, async {
            loop {
                let notified = self.idle.notified();
                if self.active.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }
}

impl Drop for ProviderOperationGuard {
    fn drop(&mut self) {
        if self.tracker.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.idle.notify_waiters();
        }
    }
}

impl Drop for RuntimeFileLock {
    fn drop(&mut self) {
        if std::fs::read_to_string(&self.path).is_ok_and(|contents| contents == self.owner) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

async fn clear_active_start(active_start_run: &Mutex<Option<ActiveStartRun>>, run_id: &str) {
    let mut active = active_start_run.lock().await;
    if active.as_ref().map(|active_run| active_run.run_id.as_str()) == Some(run_id) {
        *active = None;
    }
}

fn write_active_start_marker(path: &Path, run_id: &str) -> Result<()> {
    std::fs::write(path, format!("{run_id}\n"))?;
    ensure_owner_only_file(path)?;
    Ok(())
}

fn remove_stale_active_marker(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("could not remove stale active-start marker"),
    }
}

fn acquire_runtime_lock(path: &Path, purpose: &str) -> Result<RuntimeFileLock> {
    let owner = runtime_lock_owner();
    match try_publish_runtime_lock(path, &owner) {
        Ok(()) => {
            return Ok(RuntimeFileLock {
                path: path.to_path_buf(),
                owner,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error).with_context(|| format!("could not create {purpose} lock"));
        }
    }

    let reclaim_path = path.with_file_name(format!(
        "{}.reclaim",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("butler-lock")
    ));
    let reclaim_owner = runtime_lock_owner();
    try_publish_runtime_lock(&reclaim_path, &reclaim_owner)
        .with_context(|| format!("could not serialize stale {purpose} lock inspection"))?;
    let _reclaim_guard = RuntimeFileLock {
        path: reclaim_path,
        owner: reclaim_owner,
    };

    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not inspect existing {purpose} lock"));
        }
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                anyhow::bail!("existing {purpose} lock is not a safe regular file");
            }
            let existing = std::fs::read_to_string(path)
                .with_context(|| format!("could not read existing {purpose} lock"))?;
            let pid = existing
                .lines()
                .next()
                .and_then(|pid| pid.parse::<libc::pid_t>().ok())
                .filter(|pid| *pid > 0)
                .ok_or_else(|| anyhow::anyhow!("existing {purpose} lock has invalid ownership"))?;
            if process_is_alive(pid) {
                anyhow::bail!("{purpose} lock is held by another process");
            }
            std::fs::remove_file(path)
                .with_context(|| format!("could not remove stale {purpose} lock"))?;
        }
    }

    try_publish_runtime_lock(path, &owner)
        .with_context(|| format!("could not acquire {purpose} lock after recovery"))?;
    Ok(RuntimeFileLock {
        path: path.to_path_buf(),
        owner,
    })
}

fn runtime_lock_owner() -> String {
    format!("{}\n{:016x}\n", std::process::id(), rand::random::<u64>())
}

fn try_publish_runtime_lock(path: &Path, owner: &str) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("butler-lock");
    let temp_path = path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        rand::random::<u64>()
    ));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(&temp_path)?;
        use std::io::Write as _;
        file.write_all(owner.as_bytes())?;
        file.sync_all()?;
        ensure_owner_only_file(&temp_path).map_err(std::io::Error::other)?;
        std::fs::hard_link(&temp_path, path)
    })();
    let _ = std::fs::remove_file(&temp_path);
    result
}

fn process_is_alive(pid: libc::pid_t) -> bool {
    let result = unsafe { libc::kill(pid, 0) };
    if result == 0 {
        return true;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => false,
        Some(libc::EPERM) => true,
        _ => true,
    }
}

fn clear_active_start_marker(path: &Path, run_id: &str) {
    let marker_matches = std::fs::read_to_string(path)
        .ok()
        .is_some_and(|contents| contents.trim() == run_id);
    if marker_matches {
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn operation_guard_keeps_run_active_after_command_cancellation() {
        let active_start_marker =
            std::env::temp_dir().join(format!("butler-active-start-{}", rand::random::<u64>()));
        let admission_lock_path =
            std::env::temp_dir().join(format!("butler-start-lock-{}", rand::random::<u64>()));
        write_active_start_marker(&active_start_marker, "run-1").unwrap();
        let active_start_run = Arc::new(Mutex::new(Some(ActiveStartRun {
            run_id: "run-1".to_string(),
            guild_id: None,
        })));
        let lease = ActiveStartLease {
            inner: Some(Arc::new(ActiveStartLeaseInner {
                active_start_run: active_start_run.clone(),
                active_start_marker: active_start_marker.clone(),
                run_id: "run-1".to_string(),
                _admission_lock: acquire_runtime_lock(&admission_lock_path, "test").unwrap(),
                _provider_operation_guard: Arc::new(ProviderOperationTracker::default()).begin(),
            })),
        };
        let guard = lease.operation_guard();

        drop(lease);
        tokio::task::yield_now().await;
        assert!(active_start_run.lock().await.is_some());

        drop(guard);
        tokio::task::yield_now().await;
        assert!(active_start_run.lock().await.is_none());
        assert!(!active_start_marker.exists());
        assert!(!admission_lock_path.exists());
    }

    #[tokio::test]
    async fn provider_operation_guard_blocks_shutdown_drain_until_drop() {
        let tracker = Arc::new(ProviderOperationTracker::default());
        let guard = tracker.begin();

        assert!(!tracker.wait_for_idle(Duration::from_millis(1)).await);
        drop(guard);
        assert!(tracker.wait_for_idle(Duration::from_millis(10)).await);
    }

    #[test]
    fn runtime_lock_rejects_live_owner_and_reclaims_stale_owner() {
        let lock_path =
            std::env::temp_dir().join(format!("butler-runtime-lock-{}", rand::random::<u64>()));
        let lock = acquire_runtime_lock(&lock_path, "test").unwrap();
        assert!(acquire_runtime_lock(&lock_path, "test").is_err());
        drop(lock);

        std::fs::write(&lock_path, "999999\nstale\n").unwrap();
        let reclaimed = acquire_runtime_lock(&lock_path, "test").unwrap();
        drop(reclaimed);
        assert!(!lock_path.exists());
    }

    #[test]
    fn runtime_lock_does_not_reclaim_invalid_or_replacement_ownership() {
        let invalid_path =
            std::env::temp_dir().join(format!("butler-invalid-lock-{}", rand::random::<u64>()));
        std::fs::write(&invalid_path, "invalid\n").unwrap();
        assert!(acquire_runtime_lock(&invalid_path, "test").is_err());
        assert!(invalid_path.exists());
        std::fs::remove_file(invalid_path).unwrap();

        let replacement_path =
            std::env::temp_dir().join(format!("butler-replaced-lock-{}", rand::random::<u64>()));
        let lock = acquire_runtime_lock(&replacement_path, "test").unwrap();
        std::fs::write(
            &replacement_path,
            format!("{}\nreplacement\n", std::process::id()),
        )
        .unwrap();
        drop(lock);
        assert!(replacement_path.exists());
        std::fs::remove_file(replacement_path).unwrap();
    }

    #[test]
    fn concurrent_stale_reclaimers_allow_only_one_owner() {
        let lock_path = Arc::new(std::env::temp_dir().join(format!(
            "butler-concurrent-stale-lock-{}",
            rand::random::<u64>()
        )));
        std::fs::write(lock_path.as_ref(), "999999\nstale\n").unwrap();
        let start = Arc::new(std::sync::Barrier::new(3));
        let acquired = Arc::new(std::sync::Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let lock_path = lock_path.clone();
            let start = start.clone();
            let acquired = acquired.clone();
            threads.push(std::thread::spawn(move || {
                start.wait();
                let result = acquire_runtime_lock(&lock_path, "test");
                acquired.wait();
                result
            }));
        }

        start.wait();
        acquired.wait();
        let owners = threads
            .into_iter()
            .map(|thread| thread.join().unwrap().is_ok())
            .filter(|owned| *owned)
            .count();
        assert_eq!(owners, 1);
        assert!(!lock_path.exists());
    }
}
