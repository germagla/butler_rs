use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunContext {
    pub run_id: String,
    pub command: String,
    pub guild_id: Option<String>,
    pub guild_name: String,
    pub channel_id: String,
    #[serde(default)]
    pub channel_name: Option<String>,
    pub user_id: String,
    pub user_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunStep {
    pub at_ms: u128,
    pub step: String,
    pub status: String,
    pub detail: Option<String>,
    pub screenshot_path: Option<String>,
    pub error_class: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunSummary {
    pub context: RunContext,
    pub started_at_ms: u128,
    pub finished_at_ms: u128,
    pub duration_ms: u128,
    pub outcome: String,
    pub final_aternos_status: Option<String>,
    pub final_minecraft_status: Option<String>,
    pub screenshot_path: Option<String>,
    pub error_class: Option<String>,
    pub steps: Vec<RunStep>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunEvent {
    pub context: RunContext,
    pub step: RunStep,
}

#[derive(Clone)]
pub struct RunStore {
    inner: Arc<Mutex<VecDeque<RunSummary>>>,
    limit: usize,
    event_file: Arc<PathBuf>,
    persist_events: bool,
    redact_events: bool,
}

impl RunStore {
    pub fn new(
        limit: usize,
        artifact_dir: &Path,
        persist_events: bool,
        redact_events: bool,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(limit))),
            limit,
            event_file: Arc::new(artifact_dir.join("events.jsonl")),
            persist_events,
            redact_events,
        }
    }

    pub async fn push(&self, summary: RunSummary) {
        let mut runs = self.inner.lock().await;
        runs.push_front(summary);
        while runs.len() > self.limit {
            runs.pop_back();
        }
    }

    pub async fn recent(&self, limit: usize) -> Vec<RunSummary> {
        let runs = self.inner.lock().await;
        runs.iter().take(limit).cloned().collect()
    }

    pub async fn find(&self, run_id: &str) -> Option<RunSummary> {
        let runs = self.inner.lock().await;
        runs.iter()
            .find(|run| run.context.run_id == run_id)
            .cloned()
    }

    pub async fn last_error(&self) -> Option<RunSummary> {
        let runs = self.inner.lock().await;
        runs.iter()
            .find(|run| run.error_class.is_some() || run.outcome.eq_ignore_ascii_case("failed"))
            .cloned()
    }

    pub fn append_event(&self, event: &RunEvent) -> Result<()> {
        if !self.persist_events {
            return Ok(());
        }

        if let Some(parent) = self.event_file.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.event_file.as_ref())?;
        let event = if self.redact_events {
            event.redacted()
        } else {
            event.clone()
        };
        writeln!(file, "{}", serde_json::to_string(&event)?)?;
        Ok(())
    }
}

impl RunEvent {
    fn redacted(&self) -> Self {
        let mut context = self.context.clone();
        context.guild_id = None;
        context.guild_name = "redacted".to_string();
        context.channel_id = "redacted".to_string();
        context.channel_name = None;
        context.user_id = "redacted".to_string();
        context.user_name = "redacted".to_string();

        Self {
            context,
            step: self.step.clone(),
        }
    }
}

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
