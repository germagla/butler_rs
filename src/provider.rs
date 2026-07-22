use crate::config::Config;
use std::{future::Future, path::PathBuf, pin::Pin};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    StartClicked,
    DashboardChanged,
}

impl std::fmt::Display for StartOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartClicked => write!(f, "StartClicked"),
            Self::DashboardChanged => write!(f, "DashboardChanged"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProviderStartResult {
    pub outcome: StartOutcome,
    pub dashboard_status: String,
    pub screenshot_path: Option<PathBuf>,
    pub html_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct ProviderStartFailure {
    pub error_class: String,
    pub message: String,
    pub screenshot_path: Option<PathBuf>,
    pub html_path: Option<PathBuf>,
    pub start_may_have_been_submitted: bool,
}

impl std::fmt::Display for ProviderStartFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_class, self.message)
    }
}

impl std::error::Error for ProviderStartFailure {}

pub type ProviderStartFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ProviderStartResult, ProviderStartFailure>> + Send + 'a>>;

pub trait ServerStartProvider {
    fn start<'a>(&'a self, config: &'a Config, run_id: &'a str) -> ProviderStartFuture<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ArtifactCapture;
    use std::{collections::HashSet, path::PathBuf};

    struct FakeProvider {
        result: Result<ProviderStartResult, ProviderStartFailure>,
    }

    impl ServerStartProvider for FakeProvider {
        fn start<'a>(&'a self, _config: &'a Config, _run_id: &'a str) -> ProviderStartFuture<'a> {
            Box::pin(async move { self.result.clone() })
        }
    }

    fn test_config() -> Config {
        Config {
            discord_token: "discord".to_string(),
            aternos_user: "user".to_string(),
            aternos_pass: "pass".to_string(),
            minecraft_server_addr: "localhost:25565".to_string(),
            server_id: None,
            headless: true,
            start_wait_online_secs: 1,
            run_history_limit: 2,
            artifact_dir: PathBuf::from("artifacts/runs"),
            artifact_capture: ArtifactCapture::Screenshots,
            attach_screenshots: true,
            persist_run_events: true,
            redact_run_events: true,
            owner_user_ids: HashSet::new(),
        }
    }

    #[tokio::test]
    async fn fake_provider_returns_neutral_success_result() {
        let provider = FakeProvider {
            result: Ok(ProviderStartResult {
                outcome: StartOutcome::StartClicked,
                dashboard_status: "Online".to_string(),
                screenshot_path: Some(PathBuf::from("shot.png")),
                html_path: None,
            }),
        };

        let config = test_config();
        let result = provider.start(&config, "abc123").await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartClicked);
        assert_eq!(result.dashboard_status, "Online");
        assert_eq!(result.screenshot_path, Some(PathBuf::from("shot.png")));
    }

    #[tokio::test]
    async fn fake_provider_returns_neutral_failure_result() {
        let provider = FakeProvider {
            result: Err(ProviderStartFailure {
                error_class: "StartNotAccepted".to_string(),
                message: "still offline".to_string(),
                screenshot_path: None,
                html_path: None,
                start_may_have_been_submitted: false,
            }),
        };

        let config = test_config();
        let failure = provider.start(&config, "abc123").await.unwrap_err();

        assert_eq!(failure.error_class, "StartNotAccepted");
        assert_eq!(failure.to_string(), "StartNotAccepted: still offline");
    }
}
