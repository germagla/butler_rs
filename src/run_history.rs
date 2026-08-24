use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
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
    #[serde(default = "default_provider_name")]
    pub provider: String,
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
    #[serde(alias = "final_aternos_status")]
    pub final_provider_status: Option<String>,
    pub final_minecraft_status: Option<String>,
    pub screenshot_path: Option<String>,
    pub error_class: Option<String>,
    pub steps: Vec<RunStep>,
}

impl RunSummary {
    pub fn display_outcome(&self) -> String {
        if self.background_verification_pending() {
            format!("{} (verification pending)", self.outcome)
        } else {
            self.outcome.clone()
        }
    }

    fn background_verification_pending(&self) -> bool {
        self.steps
            .iter()
            .rev()
            .find(|step| step.step == "minecraft_background_verify")
            .is_some_and(|step| step.status == "scheduled")
    }
}

fn default_provider_name() -> String {
    "aternos".to_string()
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
    event_write_lock: Arc<StdMutex<()>>,
    follow_up_lock: Arc<Mutex<()>>,
    persist_events: bool,
    redact_events: bool,
}

pub struct FollowUpApplyResult {
    pub summary_updated: bool,
    pub event_persisted: bool,
    pub event_error: Option<String>,
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
            event_write_lock: Arc::new(StdMutex::new(())),
            follow_up_lock: Arc::new(Mutex::new(())),
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

    pub async fn apply_follow_up(
        &self,
        context: RunContext,
        step: RunStep,
        outcome: &str,
        final_provider_status: Option<String>,
        final_minecraft_status: Option<String>,
        error_class: Option<String>,
    ) -> FollowUpApplyResult {
        let _follow_up_guard = self.follow_up_lock.lock().await;
        let summary_exists = {
            let runs = self.inner.lock().await;
            match runs.iter().find(|run| run.context.run_id == context.run_id) {
                Some(run) => {
                    if run.steps.iter().any(|existing| {
                        existing.step == step.step && existing.status != "scheduled"
                    }) {
                        return FollowUpApplyResult {
                            summary_updated: false,
                            event_persisted: false,
                            event_error: None,
                        };
                    }
                    if !run.background_verification_pending() {
                        return FollowUpApplyResult {
                            summary_updated: false,
                            event_persisted: false,
                            event_error: None,
                        };
                    }
                    true
                }
                None => false,
            }
        };
        let event = RunEvent {
            context: context.clone(),
            step: step.clone(),
        };
        let store = self.clone();
        let event_result = tokio::task::spawn_blocking(move || store.append_event(&event))
            .await
            .context("run follow-up event writer task failed")
            .and_then(|result| result);
        if !summary_exists {
            return FollowUpApplyResult {
                summary_updated: false,
                event_persisted: event_result.is_ok(),
                event_error: event_result.err().map(|error| error.to_string()),
            };
        }

        let mut runs = self.inner.lock().await;
        let Some(run) = runs
            .iter_mut()
            .find(|run| run.context.run_id == context.run_id)
        else {
            return FollowUpApplyResult {
                summary_updated: false,
                event_persisted: event_result.is_ok(),
                event_error: event_result.err().map(|error| error.to_string()),
            };
        };
        run.outcome = outcome.to_string();
        if final_provider_status.is_some() {
            run.final_provider_status = final_provider_status;
        }
        if final_minecraft_status.is_some() {
            run.final_minecraft_status = final_minecraft_status;
        }
        run.error_class = error_class;
        run.finished_at_ms = step.at_ms;
        run.duration_ms = step.at_ms.saturating_sub(run.started_at_ms);
        run.steps.push(step);
        FollowUpApplyResult {
            summary_updated: true,
            event_persisted: event_result.is_ok(),
            event_error: event_result.err().map(|error| error.to_string()),
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
        let _write_guard = self
            .event_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("run event writer lock is poisoned"))?;

        if let Some(parent) = self.event_file.parent() {
            ensure_owner_only_dir(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.event_file.as_ref())?;
        ensure_owner_only_file(self.event_file.as_ref())?;
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
        prune_artifacts: bool,
    ) -> Result<Option<EventFileMaintenance>> {
        harden_existing_artifact_permissions(artifact_dir)?;
        let rotated = if rotate_unredacted_events {
            maintain_event_file_if_needed(artifact_dir)?
        } else {
            None
        };
        if prune_artifacts {
            prune_run_artifact_dirs(artifact_dir, limit)?;
        }
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
    match fs::symlink_metadata(run_dir) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => bail!("ArtifactWrite: run artifact path is not a safe directory"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("ArtifactWrite: could not inspect run directory"),
    }
    ensure_owner_only_dir(run_dir)?;
    let marker = run_dir.join(RUN_DIR_MARKER);
    match fs::symlink_metadata(&marker) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            if fs::read_to_string(&marker).ok().as_deref() != Some("butler-run\n") {
                bail!("ArtifactWrite: existing run marker was invalid");
            }
            ensure_owner_only_file(&marker)?;
            return Ok(());
        }
        Ok(_) => bail!("ArtifactWrite: run marker is not a safe regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("ArtifactWrite: could not inspect run marker"),
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let result = (|| {
        let mut file = options.open(&marker).with_context(|| {
            format!(
                "ArtifactWrite: could not write marker in {}",
                run_dir.display()
            )
        })?;
        file.write_all(b"butler-run\n")?;
        file.sync_all()?;
        ensure_owner_only_file(&marker)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&marker);
    }
    result
}

pub fn verify_artifact_dir_writable(artifact_dir: &Path) -> Result<()> {
    ensure_owner_only_dir(artifact_dir).with_context(|| {
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
            ensure_owner_only_file(&backup)?;
            EventFileMaintenance::RotatedUnredacted(backup)
        }
        EventFileInspection::Corrupt => {
            let backup = backup_event_path(artifact_dir, "corrupt");
            fs::rename(&event_file, &backup)?;
            ensure_owner_only_file(&backup)?;
            EventFileMaintenance::QuarantinedCorrupt(backup)
        }
    };
    Ok(Some(maintenance))
}

pub fn ensure_owner_only_dir(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => bail!(
            "ArtifactWrite: artifact directory is not a safe regular directory: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path)?;
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                bail!(
                    "ArtifactWrite: artifact directory is not a safe regular directory: {}",
                    path.display()
                );
            }
        }
        Err(error) => return Err(error.into()),
    }
    set_owner_only_mode(path, true)
}

pub fn ensure_owner_only_file(path: &Path) -> Result<()> {
    set_owner_only_mode(path, false)
}

pub fn write_owner_only_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("ArtifactWrite: artifact path has no parent directory")?;
    ensure_owner_only_dir(parent)?;

    for attempt in 0..16 {
        let temp_path = parent.join(format!(
            ".butler-write-{}-{}-{}-{attempt}",
            std::process::id(),
            now_ms(),
            rand::random::<u64>()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = match options.open(&temp_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "ArtifactWrite: could not create temporary file in {}",
                        parent.display()
                    )
                });
            }
        };

        let result = (|| -> Result<()> {
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp_path, path)?;
            ensure_owner_only_file(path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        return result.with_context(|| {
            format!(
                "ArtifactWrite: could not atomically write {}",
                path.display()
            )
        });
    }

    bail!(
        "ArtifactWrite: could not create a unique temporary file in {}",
        parent.display()
    )
}

fn harden_existing_artifact_permissions(artifact_dir: &Path) -> Result<()> {
    ensure_owner_only_dir(artifact_dir)?;
    for entry in fs::read_dir(artifact_dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("events") && name.ends_with(".jsonl") {
                ensure_owner_only_file(&path)?;
            }
            continue;
        }
        if !file_type.is_dir() || !is_butler_run_artifact_dir(&path) {
            continue;
        }
        set_owner_only_mode(&path, true)?;
        for artifact in fs::read_dir(&path)? {
            let artifact = artifact?;
            if artifact.file_type()?.is_file() {
                ensure_owner_only_file(&artifact.path())?;
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_mode(path: &Path, directory: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = if directory { 0o700 } else { 0o600 };
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_mode(_path: &Path, _directory: bool) -> Result<()> {
    Ok(())
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
    let marker = path.join(RUN_DIR_MARKER);
    if fs::symlink_metadata(&marker).is_ok_and(|metadata| {
        metadata.file_type().is_file()
            && !metadata.file_type().is_symlink()
            && fs::read_to_string(&marker).ok().as_deref() == Some("butler-run\n")
    }) {
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
    LEGACY_RUN_ARTIFACT_FILES.iter().any(|file_name| {
        fs::symlink_metadata(path.join(file_name)).is_ok_and(|metadata| {
            metadata.file_type().is_file() && !metadata.file_type().is_symlink()
        })
    })
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

    #[cfg(unix)]
    #[test]
    fn artifact_permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = temp_artifact_dir("private_permissions");
        ensure_owner_only_dir(&dir).unwrap();
        let run_dir = dir.join("abc123");
        mark_run_artifact_dir(&run_dir).unwrap();
        let artifact = run_dir.join("failure.html");
        fs::write(&artifact, "secret").unwrap();
        ensure_owner_only_file(&artifact).unwrap();

        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&run_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&artifact).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn owner_only_directory_rejects_symlink_without_touching_target() {
        use std::os::unix::fs::symlink;

        let parent = temp_artifact_dir("directory_symlink");
        let target = parent.join("target");
        let link = parent.join("artifact-link");
        fs::create_dir(&target).unwrap();
        symlink(&target, &link).unwrap();

        let error = ensure_owner_only_dir(&link).unwrap_err();

        assert!(error.to_string().contains("not a safe regular directory"));
        assert!(target.is_dir());
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        fs::remove_file(link).unwrap();
        fs::remove_dir_all(parent).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn atomic_artifact_write_replaces_symlink_without_touching_target() {
        use std::os::unix::fs::symlink;

        let dir = temp_artifact_dir("atomic_symlink");
        let target = dir.join("outside-target");
        fs::write(&target, b"leave me alone").unwrap();
        let artifact = dir.join("provider_snapshot.png");
        symlink(&target, &artifact).unwrap();

        write_owner_only_atomic(&artifact, b"new artifact").unwrap();

        assert_eq!(fs::read(&target).unwrap(), b"leave me alone");
        assert_eq!(fs::read(&artifact).unwrap(), b"new artifact");
        assert!(
            !fs::symlink_metadata(&artifact)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    fn sample_context() -> RunContext {
        RunContext {
            run_id: "abc123".to_string(),
            command: "server.start".to_string(),
            provider: "aternos".to_string(),
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
            final_provider_status: None,
            final_minecraft_status: None,
            screenshot_path: None,
            error_class: failed.then(|| "StartNotAccepted".to_string()),
            steps: Vec::new(),
        }
    }

    fn sample_pending_summary(run_id: &str, guild_id: Option<&str>) -> RunSummary {
        let mut summary = sample_summary(run_id, guild_id, false);
        summary.outcome = "StartRequested".to_string();
        summary.steps.push(RunStep {
            at_ms: 3,
            step: "minecraft_background_verify".to_string(),
            status: "scheduled".to_string(),
            detail: None,
            screenshot_path: None,
            error_class: None,
        });
        summary
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

    #[tokio::test]
    async fn follow_up_updates_completed_run_and_persists_terminal_event() {
        let dir = temp_artifact_dir("follow_up");
        let store = RunStore::new(10, &dir, true, true);
        store
            .push(sample_pending_summary("abc123", Some("guild-1")))
            .await;
        let step = RunStep {
            at_ms: 10,
            step: "minecraft_background_verify".to_string(),
            status: "online".to_string(),
            detail: Some("Online after 8s".to_string()),
            screenshot_path: None,
            error_class: None,
        };

        let result = store
            .apply_follow_up(
                sample_context(),
                step.clone(),
                "MinecraftOnline",
                Some("Running".to_string()),
                Some("Online (1/20 players)".to_string()),
                None,
            )
            .await;

        assert!(result.summary_updated);
        assert!(result.event_persisted);
        assert!(result.event_error.is_none());
        let run = store
            .find_scoped("abc123", &RunQueryScope::All)
            .await
            .unwrap();
        assert_eq!(run.outcome, "MinecraftOnline");
        assert_eq!(run.display_outcome(), "MinecraftOnline");
        assert_eq!(run.final_provider_status.as_deref(), Some("Running"));
        assert_eq!(
            run.final_minecraft_status.as_deref(),
            Some("Online (1/20 players)")
        );
        assert_eq!(run.finished_at_ms, 10);
        assert_eq!(run.duration_ms, 9);
        assert_eq!(run.steps.len(), 2);
        assert_eq!(run.steps[1].step, step.step);
        assert_eq!(run.steps[1].status, step.status);
        assert_eq!(run.steps[1].detail, step.detail);

        let events = fs::read_to_string(dir.join("events.jsonl")).unwrap();
        let event: RunEvent = serde_json::from_str(events.trim()).unwrap();
        assert_eq!(event.context.run_id, "abc123");
        assert_eq!(event.step.status, "online");
        assert_eq!(event.context.user_name, "redacted");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn provisional_summary_displays_verification_pending() {
        let mut run = sample_summary("abc123", Some("guild-1"), false);
        run.outcome = "StartRequested".to_string();
        run.steps.push(RunStep {
            at_ms: 3,
            step: "minecraft_background_verify".to_string(),
            status: "scheduled".to_string(),
            detail: None,
            screenshot_path: None,
            error_class: None,
        });

        assert_eq!(
            run.display_outcome(),
            "StartRequested (verification pending)"
        );
    }

    #[tokio::test]
    async fn follow_up_is_persisted_when_summary_has_left_memory() {
        let dir = temp_artifact_dir("follow_up_missing");
        let store = RunStore::new(1, &dir, true, true);
        let step = RunStep {
            at_ms: 10,
            step: "minecraft_background_verify".to_string(),
            status: "timeout".to_string(),
            detail: Some("Timed out".to_string()),
            screenshot_path: None,
            error_class: Some("StartVerificationTimeout".to_string()),
        };

        let result = store
            .apply_follow_up(
                sample_context(),
                step,
                "StartVerificationTimeout",
                None,
                Some("Offline".to_string()),
                Some("StartVerificationTimeout".to_string()),
            )
            .await;

        assert!(!result.summary_updated);
        assert!(result.event_persisted);
        assert!(result.event_error.is_none());
        assert!(dir.join("events.jsonl").is_file());
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn duplicate_terminal_follow_up_is_ignored() {
        let dir = temp_artifact_dir("follow_up_duplicate");
        let store = RunStore::new(10, &dir, true, true);
        store
            .push(sample_pending_summary("abc123", Some("guild-1")))
            .await;
        let step = RunStep {
            at_ms: 10,
            step: "minecraft_background_verify".to_string(),
            status: "online".to_string(),
            detail: Some("Online".to_string()),
            screenshot_path: None,
            error_class: None,
        };

        let first = store
            .apply_follow_up(
                sample_context(),
                step.clone(),
                "MinecraftOnline",
                None,
                Some("Online".to_string()),
                None,
            )
            .await;
        assert!(first.summary_updated);
        assert!(first.event_persisted);
        let duplicate = store
            .apply_follow_up(
                sample_context(),
                step,
                "MinecraftOnline",
                None,
                Some("Online".to_string()),
                None,
            )
            .await;
        assert!(!duplicate.summary_updated);
        assert!(!duplicate.event_persisted);
        assert!(duplicate.event_error.is_none());

        let run = store
            .find_scoped("abc123", &RunQueryScope::All)
            .await
            .unwrap();
        assert_eq!(run.steps.len(), 2);
        assert_eq!(
            fs::read_to_string(dir.join("events.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn follow_up_does_not_overwrite_non_pending_failure() {
        let dir = temp_artifact_dir("follow_up_failed");
        let store = RunStore::new(10, &dir, true, true);
        store
            .push(sample_summary("abc123", Some("guild-1"), true))
            .await;

        let result = store
            .apply_follow_up(
                sample_context(),
                RunStep {
                    at_ms: 10,
                    step: "minecraft_background_verify".to_string(),
                    status: "online".to_string(),
                    detail: Some("Online".to_string()),
                    screenshot_path: None,
                    error_class: None,
                },
                "MinecraftOnline",
                None,
                Some("Online".to_string()),
                None,
            )
            .await;

        assert!(!result.summary_updated);
        assert!(!result.event_persisted);
        let run = store
            .find_scoped("abc123", &RunQueryScope::All)
            .await
            .unwrap();
        assert_eq!(run.outcome, "Failed");
        assert_eq!(run.error_class.as_deref(), Some("StartNotAccepted"));
        assert!(!dir.join("events.jsonl").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn follow_up_updates_memory_when_persistence_is_disabled() {
        let dir = temp_artifact_dir("follow_up_no_persistence");
        let store = RunStore::new(10, &dir, false, true);
        store
            .push(sample_pending_summary("abc123", Some("guild-1")))
            .await;

        let result = store
            .apply_follow_up(
                sample_context(),
                RunStep {
                    at_ms: 10,
                    step: "minecraft_background_verify".to_string(),
                    status: "timeout".to_string(),
                    detail: Some("Timed out".to_string()),
                    screenshot_path: None,
                    error_class: Some("StartVerificationTimeout".to_string()),
                },
                "StartVerificationTimeout",
                None,
                Some("Offline".to_string()),
                Some("StartVerificationTimeout".to_string()),
            )
            .await;

        assert!(result.summary_updated);
        assert!(result.event_persisted);
        assert!(result.event_error.is_none());
        assert_eq!(
            store
                .find_scoped("abc123", &RunQueryScope::All)
                .await
                .unwrap()
                .outcome,
            "StartVerificationTimeout"
        );
        assert!(!dir.join("events.jsonl").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn interrupted_follow_up_is_not_treated_as_last_error() {
        let dir = temp_artifact_dir("follow_up_interrupted");
        let store = RunStore::new(10, &dir, false, true);
        store
            .push(sample_pending_summary("abc123", Some("guild-1")))
            .await;

        store
            .apply_follow_up(
                sample_context(),
                RunStep {
                    at_ms: 10,
                    step: "minecraft_background_verify".to_string(),
                    status: "interrupted".to_string(),
                    detail: Some("Butler is restarting".to_string()),
                    screenshot_path: None,
                    error_class: None,
                },
                "StartVerificationInterrupted",
                None,
                Some("Offline".to_string()),
                None,
            )
            .await;

        assert!(store.last_error_scoped(&RunQueryScope::All).await.is_none());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_follow_up_append_still_finalizes_memory() {
        use std::os::unix::fs::symlink;

        let parent = temp_artifact_dir("follow_up_retry");
        let target = parent.join("target");
        let artifact_dir = parent.join("artifact-link");
        fs::create_dir(&target).unwrap();
        symlink(&target, &artifact_dir).unwrap();
        let store = RunStore::new(10, &artifact_dir, true, true);
        store
            .push(sample_pending_summary("abc123", Some("guild-1")))
            .await;
        let step = RunStep {
            at_ms: 10,
            step: "minecraft_background_verify".to_string(),
            status: "online".to_string(),
            detail: Some("Online".to_string()),
            screenshot_path: None,
            error_class: None,
        };

        let result = store
            .apply_follow_up(
                sample_context(),
                step,
                "MinecraftOnline",
                None,
                Some("Online".to_string()),
                None,
            )
            .await;

        assert!(result.summary_updated);
        assert!(!result.event_persisted);
        assert!(result.event_error.is_some());
        let completed = store
            .find_scoped("abc123", &RunQueryScope::All)
            .await
            .unwrap();
        assert_eq!(completed.outcome, "MinecraftOnline");
        assert_eq!(completed.steps.len(), 2);
        assert!(!target.join("events.jsonl").exists());
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn concurrent_event_appends_remain_separate_json_lines() {
        let dir = temp_artifact_dir("concurrent_events");
        let store = RunStore::new(10, &dir, true, true);
        let writers = (0..4)
            .map(|writer| {
                let store = store.clone();
                std::thread::spawn(move || {
                    for event_index in 0..25 {
                        let mut event = sample_event();
                        event.context.run_id = format!("{writer}-{event_index}");
                        store.append_event(&event).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();

        for writer in writers {
            writer.join().unwrap();
        }

        let events = fs::read_to_string(dir.join("events.jsonl")).unwrap();
        let parsed = events
            .lines()
            .map(serde_json::from_str::<RunEvent>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(parsed.len(), 100);
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

    #[test]
    fn pruning_requires_valid_regular_run_marker() {
        let dir = temp_artifact_dir("prune_marker_validation");
        let invalid = dir.join("invalid");
        fs::create_dir(&invalid).unwrap();
        fs::write(invalid.join(RUN_DIR_MARKER), "not-butler\n").unwrap();

        prune_run_artifact_dirs(&dir, 0).unwrap();

        assert!(invalid.exists());
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
