use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};
use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    StartClicked,
    DashboardChanged,
    StartRequested,
    AlreadyActive,
}

impl std::fmt::Display for StartOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartClicked => write!(f, "StartClicked"),
            Self::DashboardChanged => write!(f, "DashboardChanged"),
            Self::StartRequested => write!(f, "StartRequested"),
            Self::AlreadyActive => write!(f, "AlreadyActive"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProviderStartResult {
    pub outcome: StartOutcome,
    pub provider_status: String,
    pub minecraft_address: Option<Arc<str>>,
    pub screenshot_path: Option<PathBuf>,
    pub detail_artifact_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderCheckResult {
    pub provider_status: String,
    pub minecraft_address: Option<Arc<str>>,
    pub screenshot_path: Option<PathBuf>,
    pub detail_artifact_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct ProviderStartFailure {
    pub error_class: String,
    pub message: String,
    pub screenshot_path: Option<PathBuf>,
    pub detail_artifact_path: Option<PathBuf>,
    pub minecraft_address: Option<Arc<str>>,
    pub uncertain_mutation: ProviderMutation,
    pub retryable: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProviderMutation {
    #[default]
    None,
    WakeAllocation,
    PowerStart,
    BrowserStart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionVerificationSource {
    Provider,
    Minecraft,
}

impl ProviderMutation {
    pub fn may_have_started_server(self) -> bool {
        matches!(self, Self::PowerStart | Self::BrowserStart)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderProgress {
    pub stage: ProviderProgressStage,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderProgressStage {
    SolvingChallenge,
    RequestingAllocation,
    WaitingForAllocation,
    RequestingPower,
    VerifyingPower,
}

impl std::fmt::Display for ProviderProgressStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::SolvingChallenge => "SolvingChallenge",
            Self::RequestingAllocation => "RequestingAllocation",
            Self::WaitingForAllocation => "WaitingForAllocation",
            Self::RequestingPower => "RequestingPower",
            Self::VerifyingPower => "VerifyingPower",
        };
        write!(f, "{value}")
    }
}

pub type ProviderProgressSender = mpsc::UnboundedSender<ProviderProgress>;

impl std::fmt::Display for ProviderStartFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_class, self.message)
    }
}

impl std::error::Error for ProviderStartFailure {}

pub type ProviderStartFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ProviderStartResult, ProviderStartFailure>> + Send + 'a>>;
pub type ProviderCheckFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ProviderCheckResult, ProviderStartFailure>> + Send + 'a>>;
pub type ProviderArtifactFuture<'a> = Pin<Box<dyn Future<Output = Option<PathBuf>> + Send + 'a>>;

pub trait ServerStartProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn check<'a>(&'a self, run_id: &'a str) -> ProviderCheckFuture<'a>;
    fn start<'a>(&'a self, run_id: &'a str) -> ProviderStartFuture<'a>;

    fn start_with_progress<'a>(
        &'a self,
        run_id: &'a str,
        _progress: ProviderProgressSender,
    ) -> ProviderStartFuture<'a> {
        self.start(run_id)
    }

    fn submission_verified_artifact<'a>(
        &'a self,
        _run_id: &'a str,
        _source: SubmissionVerificationSource,
        _observed_state: &'a str,
    ) -> ProviderArtifactFuture<'a> {
        Box::pin(async { None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct FakeProvider {
        result: Result<ProviderStartResult, ProviderStartFailure>,
    }

    impl ServerStartProvider for FakeProvider {
        fn name(&self) -> &'static str {
            "fake"
        }

        fn check<'a>(&'a self, _run_id: &'a str) -> ProviderCheckFuture<'a> {
            Box::pin(async {
                Ok(ProviderCheckResult {
                    provider_status: "Offline".to_string(),
                    minecraft_address: None,
                    screenshot_path: None,
                    detail_artifact_path: None,
                })
            })
        }

        fn start<'a>(&'a self, _run_id: &'a str) -> ProviderStartFuture<'a> {
            Box::pin(async move { self.result.clone() })
        }
    }

    #[tokio::test]
    async fn fake_provider_returns_neutral_success_result() {
        let provider = FakeProvider {
            result: Ok(ProviderStartResult {
                outcome: StartOutcome::StartClicked,
                provider_status: "Online".to_string(),
                minecraft_address: None,
                screenshot_path: Some(PathBuf::from("shot.png")),
                detail_artifact_path: None,
            }),
        };

        let result = provider.start("abc123").await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartClicked);
        assert_eq!(result.provider_status, "Online");
        assert_eq!(result.screenshot_path, Some(PathBuf::from("shot.png")));
    }

    #[tokio::test]
    async fn fake_provider_returns_neutral_failure_result() {
        let provider = FakeProvider {
            result: Err(ProviderStartFailure {
                error_class: "StartNotAccepted".to_string(),
                message: "still offline".to_string(),
                screenshot_path: None,
                detail_artifact_path: None,
                minecraft_address: None,
                uncertain_mutation: ProviderMutation::None,
                retryable: false,
            }),
        };

        let failure = provider.start("abc123").await.unwrap_err();

        assert_eq!(failure.error_class, "StartNotAccepted");
        assert_eq!(failure.to_string(), "StartNotAccepted: still offline");
    }

    #[tokio::test]
    async fn fake_provider_returns_neutral_check_result() {
        let provider = FakeProvider {
            result: Err(ProviderStartFailure {
                error_class: "unused".to_string(),
                message: "unused".to_string(),
                screenshot_path: None,
                detail_artifact_path: None,
                minecraft_address: None,
                uncertain_mutation: ProviderMutation::None,
                retryable: false,
            }),
        };

        let result = provider.check("check1").await.unwrap();

        assert_eq!(result.provider_status, "Offline");
        assert_eq!(result.minecraft_address, None);
        assert_eq!(result.screenshot_path, None);
    }

    #[test]
    fn only_start_mutations_trigger_minecraft_verification() {
        assert!(!ProviderMutation::None.may_have_started_server());
        assert!(!ProviderMutation::WakeAllocation.may_have_started_server());
        assert!(ProviderMutation::PowerStart.may_have_started_server());
        assert!(ProviderMutation::BrowserStart.may_have_started_server());
    }
}
