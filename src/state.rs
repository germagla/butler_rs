use crate::{
    config::{ArtifactCapture, Config},
    run_history::{EventFileMaintenance, RunContext, RunStore, verify_artifact_dir_writable},
    terminal,
};
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct ActiveStartRun {
    pub run_id: String,
    pub guild_id: Option<String>,
}

pub struct BotState {
    pub config: Arc<Config>,
    pub run_store: RunStore,
    active_start_run: Arc<Mutex<Option<ActiveStartRun>>>,
}

impl BotState {
    pub fn new(config: Config) -> Result<Self> {
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

        if config.artifact_capture != ArtifactCapture::Off || config.persist_run_events {
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
        Ok(Self {
            config: Arc::new(config),
            run_store,
            active_start_run: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn active_start_run(&self) -> Option<ActiveStartRun> {
        self.active_start_run.lock().await.clone()
    }

    pub async fn begin_start_run(&self, context: &RunContext) -> bool {
        let mut active = self.active_start_run.lock().await;
        if active.is_some() {
            return false;
        }
        *active = Some(ActiveStartRun {
            run_id: context.run_id.clone(),
            guild_id: context.guild_id.clone(),
        });
        true
    }

    pub async fn finish_start_run(&self, run_id: &str) {
        let mut active = self.active_start_run.lock().await;
        if active.as_ref().map(|active_run| active_run.run_id.as_str()) == Some(run_id) {
            *active = None;
        }
    }
}
