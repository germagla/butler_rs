use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

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

#[derive(Clone, Debug)]
pub struct ProviderStartFailure {
    pub error_class: String,
    pub message: String,
    pub screenshot_path: Option<PathBuf>,
    pub detail_artifact_path: Option<PathBuf>,
    pub minecraft_address: Option<Arc<str>>,
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

pub trait ServerStartProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn start<'a>(&'a self, run_id: &'a str) -> ProviderStartFuture<'a>;
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
                start_may_have_been_submitted: false,
            }),
        };

        let failure = provider.start("abc123").await.unwrap_err();

        assert_eq!(failure.error_class, "StartNotAccepted");
        assert_eq!(failure.to_string(), "StartNotAccepted: still offline");
    }
}
