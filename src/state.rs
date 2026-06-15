use crate::{config::Config, run_history::RunStore};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct BotState {
    pub config: Arc<Config>,
    pub run_store: RunStore,
    active_start_run: Arc<Mutex<Option<String>>>,
}

impl BotState {
    pub fn new(config: Config) -> Self {
        let run_store = RunStore::new(
            config.run_history_limit,
            &config.artifact_dir,
            config.persist_run_events,
            config.redact_run_events,
        );
        Self {
            config: Arc::new(config),
            run_store,
            active_start_run: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn active_start_run_id(&self) -> Option<String> {
        self.active_start_run.lock().await.clone()
    }

    pub async fn begin_start_run(&self, run_id: &str) -> bool {
        let mut active = self.active_start_run.lock().await;
        if active.is_some() {
            return false;
        }
        *active = Some(run_id.to_string());
        true
    }

    pub async fn finish_start_run(&self, run_id: &str) {
        let mut active = self.active_start_run.lock().await;
        if active.as_deref() == Some(run_id) {
            *active = None;
        }
    }
}
