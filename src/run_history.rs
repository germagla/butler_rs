use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

const RUN_DIR_MARKER: &str = ".butler-run";
const LEGACY_RUN_ARTIFACT_FILES: &[&str] = &[
    "dashboard_after_start.png",
    "dashboard_after_start.html",
    "failure.png",
    "failure.html",
];
const ARTIFACT_WRITE_PROBE_PREFIX: &str = ".butler-write-probe";

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunQueryScope {
    All,
    Guild(String),
}

impl RunQueryScope {
    pub fn allows_context(&self, context: &RunContext) -> bool {
        self.allows_guild_id(context.guild_id.as_deref())
    }

    pub fn allows_guild_id(&self, guild_id: Option<&str>) -> bool {
        match self {
            Self::All => true,
            Self::Guild(allowed_guild_id) => guild_id == Some(allowed_guild_id.as_str()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventFileMaintenance {
    RotatedUnredacted(PathBuf),
    QuarantinedCorrupt(PathBuf),
}

#[derive(Clone)]
pub struct RunStore {
    inner: Arc<Mutex<VecDeque<RunSummary>>>,
    limit: usize,
    artifact_dir: Arc<PathBuf>,
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
            artifact_dir: Arc::new(artifact_dir.to_path_buf()),
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

    pub async fn recent_scoped(&self, limit: usize, scope: &RunQueryScope) -> Vec<RunSummary> {
        let runs = self.inner.lock().await;
        runs.iter()
            .filter(|run| scope.allows_context(&run.context))
            .take(limit)
            .cloned()
            .collect()
    }

    pub async fn find_scoped(&self, run_id: &str, scope: &RunQueryScope) -> Option<RunSummary> {
        let runs = self.inner.lock().await;
        runs.iter()
            .find(|run| run.context.run_id == run_id && scope.allows_context(&run.context))
            .cloned()
    }

    pub async fn last_error_scoped(&self, scope: &RunQueryScope) -> Option<RunSummary> {
        let runs = self.inner.lock().await;
        runs.iter()
            .find(|run| {
                scope.allows_context(&run.context)
                    && (run.error_class.is_some() || run.outcome.eq_ignore_ascii_case("failed"))
            })
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

    pub fn prepare_artifact_dir(
        artifact_dir: &Path,
        limit: usize,
        rotate_unredacted_events: bool,
    ) -> Result<Option<EventFileMaintenance>> {
        let rotated = if rotate_unredacted_events {
            maintain_event_file_if_needed(artifact_dir)?
        } else {
            None
        };
        prune_run_artifact_dirs(artifact_dir, limit)?;
        Ok(rotated)
    }

    pub async fn prune_artifacts(&self) -> Result<()> {
        let artifact_dir = self.artifact_dir.as_ref().clone();
        let limit = self.limit;
        tokio::task::spawn_blocking(move || prune_run_artifact_dirs(&artifact_dir, limit))
            .await
            .context("artifact pruning task failed")?
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

pub fn mark_run_artifact_dir(run_dir: &Path) -> Result<()> {
    fs::write(run_dir.join(RUN_DIR_MARKER), "butler-run\n").with_context(|| {
        format!(
            "ArtifactWrite: could not write marker in {}",
            run_dir.display()
        )
    })?;
    Ok(())
}

pub fn verify_artifact_dir_writable(artifact_dir: &Path) -> Result<()> {
    fs::create_dir_all(artifact_dir).with_context(|| {
        format!(
            "ArtifactWrite: could not create artifact dir {}",
            artifact_dir.display()
        )
    })?;
    for attempt in 0..16 {
        let probe = artifact_dir.join(format!(
            "{ARTIFACT_WRITE_PROBE_PREFIX}-{}-{}-{attempt}",
            std::process::id(),
            now_ms()
        ));
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&probe) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "ArtifactWrite: could not create artifact probe in {}",
                        artifact_dir.display()
                    )
                });
            }
        };
        file.write_all(b"butler artifact write probe\n")
            .with_context(|| {
                format!(
                    "ArtifactWrite: could not write artifact probe in {}",
                    artifact_dir.display()
                )
            })?;
        drop(file);
        fs::remove_file(&probe).with_context(|| {
            format!(
                "ArtifactWrite: could not remove artifact probe {}",
                probe.display()
            )
        })?;
        return Ok(());
    }
    anyhow::bail!(
        "ArtifactWrite: could not create unique artifact probe in {}",
        artifact_dir.display()
    );
}

pub fn maintain_event_file_if_needed(artifact_dir: &Path) -> Result<Option<EventFileMaintenance>> {
    let event_file = artifact_dir.join("events.jsonl");
    if !event_file.exists() {
        return Ok(None);
    }

    let inspection = inspect_event_file(&event_file);
    let maintenance = match inspection {
        EventFileInspection::Redacted => return Ok(None),
        EventFileInspection::Unredacted => {
            let backup = backup_event_path(artifact_dir, "unredacted");
            fs::rename(&event_file, &backup)?;
            EventFileMaintenance::RotatedUnredacted(backup)
        }
        EventFileInspection::Corrupt => {
            let backup = backup_event_path(artifact_dir, "corrupt");
            fs::rename(&event_file, &backup)?;
            EventFileMaintenance::QuarantinedCorrupt(backup)
        }
    };
    Ok(Some(maintenance))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventFileInspection {
    Redacted,
    Unredacted,
    Corrupt,
}

fn inspect_event_file(event_file: &Path) -> EventFileInspection {
    let Ok(file) = fs::File::open(event_file) else {
        return EventFileInspection::Corrupt;
    };
    let reader = BufReader::new(file);
    let mut saw_event = false;
    let mut saw_corrupt = false;

    for line in reader.lines() {
        let Ok(line) = line else {
            return EventFileInspection::Corrupt;
        };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            saw_corrupt = true;
            continue;
        };
        match event_json_appears_unredacted(&value) {
            Some(true) => return EventFileInspection::Unredacted,
            Some(false) => saw_event = true,
            None => saw_corrupt = true,
        }
    }

    if saw_corrupt {
        EventFileInspection::Corrupt
    } else {
        let _ = saw_event;
        EventFileInspection::Redacted
    }
}

fn event_json_appears_unredacted(value: &serde_json::Value) -> Option<bool> {
    let context = value.get("context")?;
    let guild_id = context.get("guild_id");
    let guild_name = context.get("guild_name")?.as_str()?;
    let channel_id = context.get("channel_id")?.as_str()?;
    let channel_name = context.get("channel_name");
    let user_id = context.get("user_id")?.as_str()?;
    let user_name = context.get("user_name")?.as_str()?;

    Some(
        guild_id.is_some_and(|value| !value.is_null())
            || guild_name != "redacted"
            || channel_id != "redacted"
            || channel_name.is_some_and(|value| !value.is_null())
            || user_id != "redacted"
            || user_name != "redacted",
    )
}

fn backup_event_path(artifact_dir: &Path, kind: &str) -> PathBuf {
    let primary = artifact_dir.join(format!("events.{kind}.backup.jsonl"));
    if !primary.exists() {
        return primary;
    }
    artifact_dir.join(format!("events.{kind}.backup.{}.jsonl", now_ms()))
}

pub fn prune_run_artifact_dirs(artifact_dir: &Path, keep_newest: usize) -> Result<()> {
    let dirs = collect_run_artifact_dirs(artifact_dir)?;
    prune_run_artifact_dir_paths(dirs, keep_newest)
}

fn collect_run_artifact_dirs(artifact_dir: &Path) -> Result<Vec<(SystemTime, PathBuf)>> {
    let entries = match fs::read_dir(artifact_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if !file_type.is_dir() || file_type.is_symlink() || !is_butler_run_artifact_dir(&path) {
            continue;
        }
        let modified = entry
            .metadata()?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH);
        dirs.push((modified, path));
    }

    Ok(dirs)
}

fn prune_run_artifact_dir_paths(
    mut dirs: Vec<(SystemTime, PathBuf)>,
    keep_newest: usize,
) -> Result<()> {
    dirs.sort_by(|(left_time, left_path), (right_time, right_path)| {
        right_time
            .cmp(left_time)
            .then_with(|| right_path.cmp(left_path))
    });

    for (_, path) in dirs.into_iter().skip(keep_newest) {
        fs::remove_dir_all(path)?;
    }

    Ok(())
}

fn is_butler_run_artifact_dir(path: &Path) -> bool {
    if path.join(RUN_DIR_MARKER).is_file() {
        return true;
    }
    is_legacy_run_artifact_dir(path)
}

fn is_legacy_run_artifact_dir(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if name.len() != 6
        || !name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
    {
        return false;
    }
    LEGACY_RUN_ARTIFACT_FILES
        .iter()
        .any(|file_name| path.join(file_name).is_file())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn temp_artifact_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "butler_rs_{name}_{}_{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn sample_context() -> RunContext {
        RunContext {
            run_id: "abc123".to_string(),
            command: "server.start".to_string(),
            guild_id: Some("guild-1".to_string()),
            guild_name: "Guild".to_string(),
            channel_id: "channel-1".to_string(),
            channel_name: Some("ops".to_string()),
            user_id: "user-1".to_string(),
            user_name: "alice".to_string(),
        }
    }

    fn sample_event() -> RunEvent {
        RunEvent {
            context: sample_context(),
            step: RunStep {
                at_ms: 1,
                step: "aternos_dashboard".to_string(),
                status: "failed".to_string(),
                detail: Some("detail".to_string()),
                screenshot_path: Some("artifacts/runs/abc123/failure.png".to_string()),
                error_class: Some("StartNotAccepted".to_string()),
            },
        }
    }

    fn sample_summary(run_id: &str, guild_id: Option<&str>, failed: bool) -> RunSummary {
        let mut context = sample_context();
        context.run_id = run_id.to_string();
        context.guild_id = guild_id.map(str::to_string);
        context.guild_name = guild_id.unwrap_or("DM").to_string();
        RunSummary {
            context,
            started_at_ms: 1,
            finished_at_ms: 2,
            duration_ms: 1,
            outcome: if failed { "Failed" } else { "StartClicked" }.to_string(),
            final_aternos_status: None,
            final_minecraft_status: None,
            screenshot_path: None,
            error_class: failed.then(|| "StartNotAccepted".to_string()),
            steps: Vec::new(),
        }
    }

    fn create_marked_run_dir(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        fs::create_dir(&path).unwrap();
        mark_run_artifact_dir(&path).unwrap();
        path
    }

    #[test]
    fn artifact_writable_probe_creates_missing_dir_and_cleans_up() {
        let dir = std::env::temp_dir().join(format!(
            "butler_rs_probe_{}_{}",
            std::process::id(),
            now_ms()
        ));

        verify_artifact_dir_writable(&dir).unwrap();

        assert!(dir.is_dir());
        let leftovers = fs::read_dir(&dir).unwrap().count();
        assert_eq!(leftovers, 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn writes_redacted_jsonl_events_by_default() {
        let dir = temp_artifact_dir("redacted");
        let store = RunStore::new(10, &dir, true, true);

        store.append_event(&sample_event()).unwrap();

        let line = fs::read_to_string(dir.join("events.jsonl")).unwrap();
        let event: RunEvent = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(event.context.run_id, "abc123");
        assert_eq!(event.context.guild_id, None);
        assert_eq!(event.context.guild_name, "redacted");
        assert_eq!(event.context.channel_id, "redacted");
        assert_eq!(event.context.channel_name, None);
        assert_eq!(event.context.user_id, "redacted");
        assert_eq!(event.context.user_name, "redacted");

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn writes_full_jsonl_when_redaction_is_disabled() {
        let dir = temp_artifact_dir("full");
        let store = RunStore::new(10, &dir, true, false);

        store.append_event(&sample_event()).unwrap();

        let line = fs::read_to_string(dir.join("events.jsonl")).unwrap();
        let event: RunEvent = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(event.context.guild_id.as_deref(), Some("guild-1"));
        assert_eq!(event.context.guild_name, "Guild");
        assert_eq!(event.context.channel_id, "channel-1");
        assert_eq!(event.context.channel_name.as_deref(), Some("ops"));
        assert_eq!(event.context.user_id, "user-1");
        assert_eq!(event.context.user_name, "alice");

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn persistence_disabled_does_not_create_event_file() {
        let dir = temp_artifact_dir("disabled");
        let store = RunStore::new(10, &dir, false, true);

        store.append_event(&sample_event()).unwrap();

        assert!(!dir.join("events.jsonl").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rotates_unredacted_event_file_once() {
        let dir = temp_artifact_dir("rotate");
        let unredacted = serde_json::to_string(&sample_event()).unwrap();
        fs::write(dir.join("events.jsonl"), format!("{unredacted}\n")).unwrap();

        let maintenance = maintain_event_file_if_needed(&dir).unwrap().unwrap();
        let EventFileMaintenance::RotatedUnredacted(backup) = maintenance else {
            panic!("expected unredacted rotation");
        };

        assert_eq!(
            backup.file_name().and_then(|name| name.to_str()),
            Some("events.unredacted.backup.jsonl")
        );
        assert!(backup.exists());
        assert!(!dir.join("events.jsonl").exists());

        fs::write(
            dir.join("events.jsonl"),
            serde_json::to_string(&sample_event().redacted()).unwrap(),
        )
        .unwrap();
        assert!(maintain_event_file_if_needed(&dir).unwrap().is_none());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rotates_when_unredacted_event_appears_after_many_redacted_events() {
        let dir = temp_artifact_dir("rotate_late");
        let redacted = serde_json::to_string(&sample_event().redacted()).unwrap();
        let unredacted = serde_json::to_string(&sample_event()).unwrap();
        let mut lines = vec![redacted; 25];
        lines.push(unredacted);
        fs::write(dir.join("events.jsonl"), format!("{}\n", lines.join("\n"))).unwrap();

        let maintenance = maintain_event_file_if_needed(&dir).unwrap().unwrap();

        assert!(matches!(
            maintenance,
            EventFileMaintenance::RotatedUnredacted(_)
        ));
        assert!(!dir.join("events.jsonl").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn quarantines_corrupt_event_file() {
        let dir = temp_artifact_dir("corrupt");
        fs::write(
            dir.join("events.jsonl"),
            format!(
                "{}\nnot-json\n",
                serde_json::to_string(&sample_event().redacted()).unwrap()
            ),
        )
        .unwrap();

        let maintenance = maintain_event_file_if_needed(&dir).unwrap().unwrap();
        let EventFileMaintenance::QuarantinedCorrupt(backup) = maintenance else {
            panic!("expected corrupt quarantine");
        };

        assert_eq!(
            backup.file_name().and_then(|name| name.to_str()),
            Some("events.corrupt.backup.jsonl")
        );
        assert!(backup.exists());
        assert!(!dir.join("events.jsonl").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn event_backup_name_collision_uses_timestamped_name() {
        let dir = temp_artifact_dir("collision");
        fs::write(dir.join("events.unredacted.backup.jsonl"), "").unwrap();
        fs::write(
            dir.join("events.jsonl"),
            serde_json::to_string(&sample_event()).unwrap(),
        )
        .unwrap();

        let maintenance = maintain_event_file_if_needed(&dir).unwrap().unwrap();
        let EventFileMaintenance::RotatedUnredacted(backup) = maintenance else {
            panic!("expected unredacted rotation");
        };

        let backup_name = backup.file_name().and_then(|name| name.to_str()).unwrap();
        assert!(backup_name.starts_with("events.unredacted.backup."));
        assert!(backup_name.ends_with(".jsonl"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn run_query_scope_limits_history_by_guild() {
        let dir = temp_artifact_dir("scope");
        let store = RunStore::new(10, &dir, true, true);
        store
            .push(sample_summary("guild2", Some("guild-2"), true))
            .await;
        store.push(sample_summary("dmrun1", None, true)).await;
        store
            .push(sample_summary("guild1", Some("guild-1"), true))
            .await;

        let owner_runs = store.recent_scoped(10, &RunQueryScope::All).await;
        assert_eq!(owner_runs.len(), 3);

        let guild_scope = RunQueryScope::Guild("guild-1".to_string());
        let guild_runs = store.recent_scoped(10, &guild_scope).await;
        assert_eq!(guild_runs.len(), 1);
        assert_eq!(guild_runs[0].context.run_id, "guild1");
        assert!(store.find_scoped("guild2", &guild_scope).await.is_none());
        assert!(store.find_scoped("dmrun1", &guild_scope).await.is_none());
        assert_eq!(
            store
                .last_error_scoped(&guild_scope)
                .await
                .unwrap()
                .context
                .run_id,
            "guild1"
        );

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn prunes_marked_run_directories_deterministically() {
        let dir = temp_artifact_dir("prune_deterministic");
        let old = create_marked_run_dir(&dir, "oldrun");
        let middle = create_marked_run_dir(&dir, "midrun");
        let new = create_marked_run_dir(&dir, "newrun");

        prune_run_artifact_dir_paths(
            vec![
                (SystemTime::UNIX_EPOCH + Duration::from_secs(1), old.clone()),
                (
                    SystemTime::UNIX_EPOCH + Duration::from_secs(2),
                    middle.clone(),
                ),
                (SystemTime::UNIX_EPOCH + Duration::from_secs(3), new.clone()),
            ],
            2,
        )
        .unwrap();

        assert!(!old.exists());
        assert!(middle.exists());
        assert!(new.exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn pruning_skips_unknown_dirs_and_keeps_event_files() {
        let dir = temp_artifact_dir("prune_skip");
        fs::write(dir.join("events.jsonl"), "").unwrap();
        fs::write(dir.join("events.unredacted.backup.jsonl"), "").unwrap();
        fs::create_dir(dir.join("manual")).unwrap();
        let marked = create_marked_run_dir(&dir, "marked");
        let legacy = dir.join("abc123");
        fs::create_dir(&legacy).unwrap();
        fs::write(legacy.join("failure.png"), "").unwrap();

        prune_run_artifact_dirs(&dir, 0).unwrap();

        assert!(!marked.exists());
        assert!(!legacy.exists());
        assert!(dir.join("manual").exists());
        assert!(dir.join("events.jsonl").exists());
        assert!(dir.join("events.unredacted.backup.jsonl").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn pruning_skips_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = temp_artifact_dir("prune_symlink");
        let target = create_marked_run_dir(&dir, "target");
        let link = dir.join("link01");
        symlink(&target, &link).unwrap();

        prune_run_artifact_dirs(&dir, 0).unwrap();

        assert!(!target.exists());
        assert!(fs::symlink_metadata(&link).is_ok());
        fs::remove_file(link).unwrap();
        fs::remove_dir_all(dir).unwrap();
    }
}
