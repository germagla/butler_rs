use crate::{
    config::{ArtifactCapture, Config},
    provider::ServerStartProvider,
    run_history::{EventFileMaintenance, RunContext, RunStore, verify_artifact_dir_writable},
    runtime_lock::{
        RuntimeFileLock, StartAdmissionFileLease, acquire_runtime_lock, remove_stale_active_marker,
        runtime_lock_has_live_owner,
    },
    terminal,
};
use anyhow::{Context, Result};
use std::{
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
    run_id: String,
    _admission_lease: StartAdmissionFileLease,
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

impl ActiveStartLease {
    pub async fn finish(mut self) {
        if let Some(inner) = self.inner.take() {
            clear_active_start(&inner.active_start_run, &inner.run_id).await;
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

    #[cfg(test)]
    pub(crate) fn for_test(
        artifact_dir: &std::path::Path,
        run_id: &str,
    ) -> (Self, Arc<Mutex<Option<ActiveStartRun>>>) {
        std::fs::create_dir_all(artifact_dir).unwrap();
        let active_start_run = Arc::new(Mutex::new(Some(ActiveStartRun {
            run_id: run_id.to_string(),
            guild_id: None,
        })));
        let lease = Self {
            inner: Some(Arc::new(ActiveStartLeaseInner {
                active_start_run: active_start_run.clone(),
                run_id: run_id.to_string(),
                _admission_lease: StartAdmissionFileLease::acquire(artifact_dir, run_id).unwrap(),
                _provider_operation_guard: Arc::new(ProviderOperationTracker::default()).begin(),
            })),
        };
        (lease, active_start_run)
    }
}

impl Drop for ActiveStartLeaseInner {
    fn drop(&mut self) {
        let active_start_run = self.active_start_run.clone();
        let run_id = self.run_id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                clear_active_start(&active_start_run, &run_id).await;
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
    _instance_lock: Arc<RuntimeFileLock>,
    minecraft_address: Arc<RwLock<String>>,
    provider_operations: Arc<ProviderOperationTracker>,
    accepting_starts: Arc<AtomicBool>,
}

impl BotState {
    pub fn new(config: Config, provider: Arc<dyn ServerStartProvider>) -> Result<Self> {
        let start_admission_path = config.artifact_dir.join(".start-admission.lock");
        let external_start_active =
            runtime_lock_has_live_owner(&start_admission_path, "start admission")?;
        match RunStore::prepare_artifact_dir(
            &config.artifact_dir,
            config.run_history_limit,
            config.redact_run_events,
            !external_start_active,
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
        if external_start_active {
            terminal::emit(terminal::line(
                "WARN",
                "artifacts",
                "",
                "",
                None,
                "skipping startup artifact pruning while an external start is active",
            ));
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
        if !external_start_active {
            remove_stale_active_marker(&active_start_marker)?;
        }
        let run_store = RunStore::new(
            config.run_history_limit,
            &config.artifact_dir,
            config.persist_run_events,
            config.redact_run_events,
        );
        let minecraft_address = config.minecraft_server_addr.clone();
        Ok(Self {
            config: Arc::new(config),
            provider,
            run_store,
            active_start_run: Arc::new(Mutex::new(None)),
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
        let admission_lease =
            match StartAdmissionFileLease::acquire(&self.config.artifact_dir, &context.run_id) {
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
        *active = Some(ActiveStartRun {
            run_id: context.run_id.clone(),
            guild_id: context.guild_id.clone(),
        });
        Ok(ActiveStartLease {
            inner: Some(Arc::new(ActiveStartLeaseInner {
                active_start_run: self.active_start_run.clone(),
                run_id: context.run_id.clone(),
                _admission_lease: admission_lease,
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

async fn clear_active_start(active_start_run: &Mutex<Option<ActiveStartRun>>, run_id: &str) {
    let mut active = active_start_run.lock().await;
    if active.as_ref().map(|active_run| active_run.run_id.as_str()) == Some(run_id) {
        *active = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn operation_guard_keeps_run_active_after_command_cancellation() {
        let artifact_dir =
            std::env::temp_dir().join(format!("butler-state-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let active_start_marker = artifact_dir.join(".active-start");
        let admission_lock_path = artifact_dir.join(".start-admission.lock");
        let active_start_run = Arc::new(Mutex::new(Some(ActiveStartRun {
            run_id: "run-1".to_string(),
            guild_id: None,
        })));
        let lease = ActiveStartLease {
            inner: Some(Arc::new(ActiveStartLeaseInner {
                active_start_run: active_start_run.clone(),
                run_id: "run-1".to_string(),
                _admission_lease: StartAdmissionFileLease::acquire(&artifact_dir, "run-1").unwrap(),
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
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn provider_operation_guard_blocks_shutdown_drain_until_drop() {
        let tracker = Arc::new(ProviderOperationTracker::default());
        let guard = tracker.begin();

        assert!(!tracker.wait_for_idle(Duration::from_millis(1)).await);
        drop(guard);
        assert!(tracker.wait_for_idle(Duration::from_millis(10)).await);
    }
}
