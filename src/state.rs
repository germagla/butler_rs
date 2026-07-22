use crate::{
    config::{ArtifactCapture, Config},
    provider::ServerStartProvider,
    run_history::{EventFileMaintenance, RunContext, RunStore, verify_artifact_dir_writable},
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
}

pub struct ActiveStartLease {
    inner: Option<Arc<ActiveStartLeaseInner>>,
}

struct ActiveStartLeaseInner {
    active_start_run: Arc<Mutex<Option<ActiveStartRun>>>,
    run_id: String,
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
        *active = Some(ActiveStartRun {
            run_id: context.run_id.clone(),
            guild_id: context.guild_id.clone(),
        });
        Ok(ActiveStartLease {
            inner: Some(Arc::new(ActiveStartLeaseInner {
                active_start_run: self.active_start_run.clone(),
                run_id: context.run_id.clone(),
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
        let active_start_run = Arc::new(Mutex::new(Some(ActiveStartRun {
            run_id: "run-1".to_string(),
            guild_id: None,
        })));
        let lease = ActiveStartLease {
            inner: Some(Arc::new(ActiveStartLeaseInner {
                active_start_run: active_start_run.clone(),
                run_id: "run-1".to_string(),
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
