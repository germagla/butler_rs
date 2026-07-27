use crate::run_history::ensure_owner_only_file;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub(crate) struct RuntimeFileLock {
    path: PathBuf,
    owner: String,
}

pub(crate) struct StartAdmissionFileLease {
    _lock: RuntimeFileLock,
    marker_path: PathBuf,
    run_id: String,
}

impl StartAdmissionFileLease {
    pub(crate) fn acquire(artifact_dir: &Path, run_id: &str) -> Result<Self> {
        let lock = acquire_runtime_lock(
            &artifact_dir.join(".start-admission.lock"),
            "start admission",
        )?;
        let marker_path = artifact_dir.join(".active-start");
        if let Err(error) = write_active_start_marker(&marker_path, run_id) {
            drop(lock);
            return Err(error);
        }
        Ok(Self {
            _lock: lock,
            marker_path,
            run_id: run_id.to_string(),
        })
    }
}

impl Drop for StartAdmissionFileLease {
    fn drop(&mut self) {
        clear_active_start_marker(&self.marker_path, &self.run_id);
    }
}

impl Drop for RuntimeFileLock {
    fn drop(&mut self) {
        if std::fs::read_to_string(&self.path).is_ok_and(|contents| contents == self.owner) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn acquire_runtime_lock(path: &Path, purpose: &str) -> Result<RuntimeFileLock> {
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

pub(crate) fn runtime_lock_has_live_owner(path: &Path, purpose: &str) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("could not inspect existing {purpose} lock"))
        }
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                anyhow::bail!("existing {purpose} lock is not a safe regular file");
            }
            let pid = lock_owner_pid(path, purpose)?;
            Ok(process_is_alive(pid))
        }
    }
}

pub(crate) fn remove_stale_active_marker(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("could not remove stale active-start marker"),
    }
}

fn runtime_lock_owner() -> String {
    format!("{}\n{:016x}\n", std::process::id(), rand::random::<u64>())
}

fn lock_owner_pid(path: &Path, purpose: &str) -> Result<libc::pid_t> {
    std::fs::read_to_string(path)
        .with_context(|| format!("could not read existing {purpose} lock"))?
        .lines()
        .next()
        .and_then(|pid| pid.parse::<libc::pid_t>().ok())
        .filter(|pid| *pid > 0)
        .ok_or_else(|| anyhow::anyhow!("existing {purpose} lock has invalid ownership"))
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

fn write_active_start_marker(path: &Path, run_id: &str) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            std::fs::remove_file(path).context("could not remove stale active-start marker")?;
        }
        Ok(_) => anyhow::bail!("existing active-start marker is not a safe regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).context("could not inspect existing active-start marker");
        }
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let result = (|| {
        let mut file = options
            .open(path)
            .context("could not create active-start marker")?;
        use std::io::Write as _;
        file.write_all(format!("{run_id}\n").as_bytes())?;
        file.sync_all()?;
        ensure_owner_only_file(path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result
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
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "butler-{label}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ))
    }

    #[test]
    fn runtime_lock_rejects_live_owner_and_reclaims_stale_owner() {
        let lock_path = temp_path("runtime-lock");
        let lock = acquire_runtime_lock(&lock_path, "test").unwrap();
        assert!(acquire_runtime_lock(&lock_path, "test").is_err());
        drop(lock);

        std::fs::write(&lock_path, "2147483647\nstale\n").unwrap();
        let reclaimed = acquire_runtime_lock(&lock_path, "test").unwrap();
        drop(reclaimed);
        assert!(!lock_path.exists());
    }

    #[test]
    fn runtime_lock_drop_preserves_replacement_owner() {
        let lock_path = temp_path("replacement-lock");
        let lock = acquire_runtime_lock(&lock_path, "test").unwrap();
        std::fs::remove_file(&lock_path).unwrap();
        std::fs::write(&lock_path, "999999\nreplacement\n").unwrap();

        drop(lock);

        assert_eq!(
            std::fs::read_to_string(&lock_path).unwrap(),
            "999999\nreplacement\n"
        );
        std::fs::remove_file(lock_path).unwrap();
    }

    #[test]
    fn concurrent_stale_reclaimers_have_one_winner() {
        let lock_path = temp_path("concurrent-lock");
        std::fs::write(&lock_path, "2147483647\nstale\n").unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let lock_path = lock_path.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    acquire_runtime_lock(&lock_path, "test")
                })
            })
            .collect();
        barrier.wait();

        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        drop(results);
        let _ = std::fs::remove_file(&lock_path);
        let _ = std::fs::remove_file(lock_path.with_file_name(format!(
            "{}.reclaim",
            lock_path.file_name().unwrap().to_string_lossy()
        )));
    }

    #[test]
    fn admission_lease_writes_and_cleans_marker_and_lock() {
        let artifact_dir = temp_path("admission");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let lease = StartAdmissionFileLease::acquire(&artifact_dir, "abc123").unwrap();

        assert_eq!(
            std::fs::read_to_string(artifact_dir.join(".active-start")).unwrap(),
            "abc123\n"
        );
        assert!(artifact_dir.join(".start-admission.lock").is_file());
        assert!(
            runtime_lock_has_live_owner(
                &artifact_dir.join(".start-admission.lock"),
                "start admission"
            )
            .unwrap()
        );

        drop(lease);
        assert!(!artifact_dir.join(".active-start").exists());
        assert!(!artifact_dir.join(".start-admission.lock").exists());
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn admission_lease_rejects_symlink_marker_without_touching_target() {
        use std::os::unix::fs::symlink;

        let artifact_dir = temp_path("admission-symlink");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let target = artifact_dir.join("target");
        std::fs::write(&target, "preserve\n").unwrap();
        symlink(&target, artifact_dir.join(".active-start")).unwrap();

        let result = StartAdmissionFileLease::acquire(&artifact_dir, "abc123");

        assert!(result.is_err());
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "preserve\n");
        assert!(!artifact_dir.join(".start-admission.lock").exists());
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }
}
