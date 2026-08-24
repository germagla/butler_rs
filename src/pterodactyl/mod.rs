mod lifecycle;
pub use lifecycle::{FlareSolverrRuntimeConfig, FlareSolverrSupervisor};

use crate::{
    config::{ArtifactCapture, PterodactylConfig},
    provider::{
        ProviderArtifactFuture, ProviderCheckFuture, ProviderCheckResult, ProviderMutation,
        ProviderProgress, ProviderProgressSender, ProviderProgressStage, ProviderStartFailure,
        ProviderStartFuture, ProviderStartResult, ServerStartProvider, StartOutcome,
        SubmissionVerificationSource,
    },
    run_history::{mark_run_artifact_dir, write_owner_only_atomic},
    terminal,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, COOKIE, HeaderMap, HeaderValue, USER_AGENT},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Mutex,
    time::{Instant, sleep, timeout},
};
use url::Url;

const API_TIMEOUT: Duration = Duration::from_secs(15);
const POWER_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const FLARESOLVERR_REQUEST_TIMEOUT_MS: u64 = 180_000;
const FLARESOLVERR_TIMEOUT: Duration = Duration::from_secs(195);
const FLARESOLVERR_RUNTIME_ATTEMPTS: usize = 2;
const FLARESOLVERR_RUNTIME_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const FLARESOLVERR_CLEARANCE_ATTEMPTS: usize = 2;
const FLARESOLVERR_CLEARANCE_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const FLARESOLVERR_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const STOPPING_WAIT_ATTEMPTS: usize = 6;
const STOPPING_WAIT_INTERVAL: Duration = Duration::from_secs(5);
const ALLOCATION_WAIT_INTERVAL: Duration = Duration::from_secs(10);
const ALLOCATION_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const ALLOCATION_EXTENSION_ERROR_LIMIT: usize = 3;
const ALLOCATION_EXTENSION_STALE_TIMEOUT: Duration = Duration::from_secs(120);
const POST_ALLOCATION_RESOURCE_ATTEMPTS: usize = 3;
const POST_ALLOCATION_RESOURCE_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const SUBMISSION_VERIFY_ATTEMPTS: usize = 12;
const SUBMISSION_VERIFY_INTERVAL: Duration = Duration::from_secs(5);
const SUBMISSION_VERIFY_TIMEOUT: Duration = Duration::from_secs(60);
const SUBMISSION_PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;
const STATE_ARTIFACT: &str = "provider_state.json";

pub struct PterodactylProvider {
    config: PterodactylConfig,
    artifact_dir: PathBuf,
    artifact_capture: ArtifactCapture,
    api_client: Client,
    flaresolverr_client: Client,
    clearance: Mutex<Option<Clearance>>,
    clearance_refresh: Mutex<()>,
    flaresolverr_runtime: FlareSolverrRuntimeConfig,
    allocation_wait_timeout: Duration,
    allocation_max_wait_timeout: Duration,
    allocation_wait_interval: Duration,
    post_allocation_resource_retry_interval: Duration,
    submission_probe_timeout: Duration,
}

impl PterodactylProvider {
    pub fn new(
        config: PterodactylConfig,
        artifact_dir: PathBuf,
        artifact_capture: ArtifactCapture,
    ) -> Result<Self> {
        let api_client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(API_TIMEOUT)
            .build()
            .context("could not build Pterodactyl API client")?;
        let flaresolverr_client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(FLARESOLVERR_TIMEOUT)
            .build()
            .context("could not build FlareSolverr client")?;
        let allocation_wait_timeout = Duration::from_secs(config.allocation_wait_secs);
        let allocation_max_wait_timeout = Duration::from_secs(config.allocation_max_wait_secs);
        let flaresolverr_runtime = FlareSolverrRuntimeConfig::new(&config, &artifact_dir);
        Ok(Self {
            config,
            artifact_dir,
            artifact_capture,
            api_client,
            flaresolverr_client,
            clearance: Mutex::new(None),
            clearance_refresh: Mutex::new(()),
            flaresolverr_runtime,
            allocation_wait_timeout,
            allocation_max_wait_timeout,
            allocation_wait_interval: ALLOCATION_WAIT_INTERVAL,
            post_allocation_resource_retry_interval: POST_ALLOCATION_RESOURCE_RETRY_INTERVAL,
            submission_probe_timeout: SUBMISSION_PROBE_TIMEOUT,
        })
    }

    async fn start_inner(
        &self,
        run_id: &str,
        progress: Option<&ProviderProgressSender>,
    ) -> Result<ProviderStartResult, ProviderStartFailure> {
        emit_progress(
            progress,
            ProviderProgressStage::SolvingChallenge,
            "Connecting to Play Hosting",
        );
        let details = self.details_with_refresh().await?;
        let mut is_limbo = details.is_limbo;
        let mut minecraft_address = None;
        let mut allocation = None;
        let mut artifact_path = self
            .write_state_artifact(
                run_id,
                details.status.as_deref().unwrap_or("unknown"),
                details.is_suspended,
                details.is_limbo,
            )
            .await;

        match action_for_server_details(&details) {
            ServerDetailsAction::Wake => {
                let allocation_result = self
                    .ensure_limbo_awake(run_id, &details.uuid, progress)
                    .await
                    .map_err(|mut failure| {
                        backfill_failure_artifacts(&mut failure, artifact_path.clone());
                        failure
                    })?;
                minecraft_address = allocation_result.minecraft_address.clone();
                allocation = Some(allocation_result);
            }
            ServerDetailsAction::FailSuspended => {
                return Err(provider_failure(
                    "ProviderSuspended",
                    "The configured server is suspended",
                    artifact_path,
                    ProviderMutation::None,
                ));
            }
            ServerDetailsAction::FetchResources => {}
        }

        let initial_resources = if let Some(allocation) = allocation.as_ref() {
            self.resources_after_allocation_with_retry(run_id, allocation, progress)
                .await
        } else {
            self.resources_with_refresh().await
        };
        let mut resources = match initial_resources {
            Ok(resources) => resources,
            Err(mut failure) if failure.error_class == "ProviderStateConflict" => {
                if allocation.is_some() {
                    backfill_failure_artifacts(&mut failure, artifact_path);
                    return Err(attach_minecraft_address(failure, &minecraft_address));
                }
                let refreshed = self
                    .details_with_refresh()
                    .await
                    .map_err(|mut inspection| {
                        backfill_failure_artifacts(
                            &mut inspection,
                            failure
                                .detail_artifact_path
                                .clone()
                                .or(artifact_path.clone()),
                        );
                        attach_minecraft_address(inspection, &minecraft_address)
                    })?;
                if refreshed.is_limbo {
                    is_limbo = true;
                    let allocation_result = self
                        .ensure_limbo_awake(run_id, &refreshed.uuid, progress)
                        .await
                        .map_err(|mut failure| {
                            backfill_failure_artifacts(&mut failure, artifact_path.clone());
                            failure
                        })?;
                    minecraft_address = allocation_result.minecraft_address.clone();
                    allocation = Some(allocation_result);
                    self.resources_after_allocation_with_retry(
                        run_id,
                        allocation.as_ref().expect("allocation was just stored"),
                        progress,
                    )
                    .await
                    .map_err(|mut failure| {
                        backfill_failure_artifacts(&mut failure, artifact_path.clone());
                        attach_minecraft_address(failure, &minecraft_address)
                    })?
                } else {
                    let mut failure = failure;
                    backfill_failure_artifacts(&mut failure, artifact_path);
                    return Err(failure);
                }
            }
            Err(mut failure) => {
                backfill_failure_artifacts(&mut failure, artifact_path);
                return Err(attach_minecraft_address(failure, &minecraft_address));
            }
        };
        artifact_path = self
            .write_state_artifact(
                run_id,
                &resources.current_state,
                resources.is_suspended,
                is_limbo,
            )
            .await;

        if resources.is_suspended {
            return Err(attach_minecraft_address(
                provider_failure(
                    "ProviderSuspended",
                    "The configured server is suspended",
                    artifact_path,
                    ProviderMutation::None,
                ),
                &minecraft_address,
            ));
        }

        let mut action = action_for_state(&resources.current_state);
        if action == StateAction::WaitForStopping {
            for _ in 0..STOPPING_WAIT_ATTEMPTS {
                sleep(STOPPING_WAIT_INTERVAL).await;
                resources = self.resources_with_refresh().await.map_err(|mut failure| {
                    backfill_failure_artifacts(&mut failure, artifact_path.clone());
                    attach_minecraft_address(failure, &minecraft_address)
                })?;
                artifact_path = self
                    .write_state_artifact(
                        run_id,
                        &resources.current_state,
                        resources.is_suspended,
                        is_limbo,
                    )
                    .await;
                action = action_for_state(&resources.current_state);
                if action != StateAction::WaitForStopping {
                    break;
                }
            }
        }

        if resources.is_suspended {
            return Err(attach_minecraft_address(
                provider_failure(
                    "ProviderSuspended",
                    "The configured server became suspended",
                    artifact_path,
                    ProviderMutation::None,
                ),
                &minecraft_address,
            ));
        }

        match action {
            StateAction::AlreadyActive => Ok(ProviderStartResult {
                outcome: StartOutcome::AlreadyActive,
                provider_status: normalized_state(&resources.current_state),
                minecraft_address,
                screenshot_path: None,
                detail_artifact_path: artifact_path,
            }),
            StateAction::RequestStart => {
                if !self.config.power_enabled {
                    return Err(attach_minecraft_address(
                        provider_failure(
                            "ProviderPowerDisabled",
                            "Pterodactyl power actions are disabled by configuration",
                            artifact_path,
                            ProviderMutation::None,
                        ),
                        &minecraft_address,
                    ));
                }
                emit_progress(
                    progress,
                    ProviderProgressStage::RequestingPower,
                    "A host is ready. Sending the Minecraft start command to Play Hosting.",
                );
                let activity_baseline =
                    timeout(self.submission_probe_timeout, self.activity_with_refresh())
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .and_then(|activity| activity.first().map(|item| item.id.clone()));
                artifact_path = self
                    .write_power_state_artifact(
                        run_id,
                        PowerArtifactState {
                            current_state: &resources.current_state,
                            is_suspended: resources.is_suspended,
                            is_limbo,
                            submission_state: PowerSubmissionState::Dispatching,
                            allocation: allocation.as_ref(),
                            last_error: None,
                            confirmation_source: None,
                        },
                    )
                    .await
                    .or(artifact_path);
                let attempted_at = Utc::now();
                let confirmation = match self.send_start_power().await {
                    Ok(()) => PowerConfirmation::accepted(),
                    Err(failure) if failure.uncertain_mutation.may_have_started_server() => {
                        let original_error =
                            format!("{}: {}", failure.error_class, failure.message);
                        artifact_path = self
                            .write_power_state_artifact(
                                run_id,
                                PowerArtifactState {
                                    current_state: &resources.current_state,
                                    is_suspended: resources.is_suspended,
                                    is_limbo,
                                    submission_state: PowerSubmissionState::Ambiguous,
                                    allocation: allocation.as_ref(),
                                    last_error: Some(&original_error),
                                    confirmation_source: None,
                                },
                            )
                            .await
                            .or(artifact_path);
                        emit_progress(
                            progress,
                            ProviderProgressStage::VerifyingPower,
                            "The start command may have gone through. Checking Play Hosting before sending anything again.",
                        );
                        match self
                            .verify_ambiguous_power(
                                failure,
                                attempted_at,
                                activity_baseline.as_deref(),
                            )
                            .await
                        {
                            Ok(mut confirmation) => {
                                confirmation.original_error = Some(original_error);
                                confirmation
                            }
                            Err(mut attempt) => {
                                let current_state = attempt
                                    .last_provider_state
                                    .as_deref()
                                    .unwrap_or(&resources.current_state);
                                let last_error = format!(
                                    "{}: {}",
                                    attempt.failure.error_class, attempt.failure.message
                                );
                                let failure_artifact_path = self
                                    .write_power_state_artifact(
                                        run_id,
                                        PowerArtifactState {
                                            current_state,
                                            is_suspended: resources.is_suspended,
                                            is_limbo,
                                            submission_state: PowerSubmissionState::Ambiguous,
                                            allocation: allocation.as_ref(),
                                            last_error: Some(&last_error),
                                            confirmation_source: None,
                                        },
                                    )
                                    .await;
                                artifact_path = failure_artifact_path.clone().or(artifact_path);
                                set_failure_artifacts(
                                    &mut attempt.failure,
                                    artifact_path,
                                    failure_artifact_path.as_ref(),
                                );
                                attempt.failure.minecraft_address = minecraft_address;
                                return Err(attempt.failure);
                            }
                        }
                    }
                    Err(failure) => {
                        let mut attempt = PowerAttemptFailure::new(failure);
                        let current_state = attempt
                            .last_provider_state
                            .as_deref()
                            .unwrap_or(&resources.current_state);
                        let last_error = format!(
                            "{}: {}",
                            attempt.failure.error_class, attempt.failure.message
                        );
                        let failure_artifact_path = self
                            .write_power_state_artifact(
                                run_id,
                                PowerArtifactState {
                                    current_state,
                                    is_suspended: resources.is_suspended,
                                    is_limbo,
                                    submission_state: PowerSubmissionState::Rejected,
                                    allocation: allocation.as_ref(),
                                    last_error: Some(&last_error),
                                    confirmation_source: None,
                                },
                            )
                            .await;
                        artifact_path = failure_artifact_path.clone().or(artifact_path);
                        set_failure_artifacts(
                            &mut attempt.failure,
                            artifact_path,
                            failure_artifact_path.as_ref(),
                        );
                        attempt.failure.minecraft_address = minecraft_address;
                        return Err(attempt.failure);
                    }
                };
                let confirmed_state = confirmation
                    .provider_state
                    .as_deref()
                    .unwrap_or(&resources.current_state);
                artifact_path = self
                    .write_power_state_artifact(
                        run_id,
                        PowerArtifactState {
                            current_state: confirmed_state,
                            is_suspended: resources.is_suspended,
                            is_limbo,
                            submission_state: PowerSubmissionState::Confirmed,
                            allocation: allocation.as_ref(),
                            last_error: confirmation.original_error.as_deref(),
                            confirmation_source: Some(confirmation.source),
                        },
                    )
                    .await
                    .or(artifact_path);
                Ok(ProviderStartResult {
                    outcome: StartOutcome::StartRequested,
                    provider_status: confirmation
                        .provider_state
                        .unwrap_or_else(|| "Start requested".to_string()),
                    minecraft_address,
                    screenshot_path: None,
                    detail_artifact_path: artifact_path,
                })
            }
            StateAction::WaitForStopping => Err(attach_minecraft_address(
                provider_failure(
                    "ProviderStopping",
                    "The server remained in stopping state",
                    artifact_path,
                    ProviderMutation::None,
                ),
                &minecraft_address,
            )),
            StateAction::FailUnknown => Err(attach_minecraft_address(
                provider_failure(
                    "ProviderStateUnknown",
                    "The provider returned an unsupported server state",
                    artifact_path,
                    ProviderMutation::None,
                ),
                &minecraft_address,
            )),
        }
    }

    async fn start_with_terminal_artifacts(
        &self,
        run_id: &str,
        progress: Option<&ProviderProgressSender>,
    ) -> Result<ProviderStartResult, ProviderStartFailure> {
        match self.start_inner(run_id, progress).await {
            Ok(result) => Ok(result),
            Err(mut failure) => {
                if failure.detail_artifact_path.is_none() {
                    failure.detail_artifact_path = self
                        .write_terminal_failure_artifact(
                            run_id,
                            &failure.error_class,
                            failure.uncertain_mutation,
                        )
                        .await;
                }
                Err(failure)
            }
        }
    }

    async fn check_inner(&self) -> Result<ProviderCheckResult, ProviderStartFailure> {
        let details = self.details_with_refresh().await?;
        if details.is_suspended && !details.is_limbo {
            return Ok(provider_check_result("Suspended", None));
        }
        if details.is_limbo {
            let allocation = self.sleep_status_with_refresh(&details.uuid).await?;
            let minecraft_address = minecraft_connection(&allocation);
            if sleep_status_allocation_ready(&allocation) {
                match self.resources_with_refresh().await {
                    Ok(resources) => {
                        let status = if !resources.is_suspended
                            && normalized_state(&resources.current_state) == "Offline"
                        {
                            "A host is allocated, but the Minecraft server process is offline."
                                .to_string()
                        } else if resources.is_suspended {
                            "Suspended".to_string()
                        } else {
                            normalized_state(&resources.current_state)
                        };
                        return Ok(provider_check_result(status, minecraft_address));
                    }
                    Err(failure) if failure.error_class == "ProviderStateConflict" => {}
                    Err(failure) => return Err(failure),
                }
            }
            return Ok(provider_check_result(
                allocation_status_detail(&allocation),
                minecraft_address,
            ));
        }

        let resources = self.resources_with_refresh().await?;
        Ok(provider_check_result(
            if resources.is_suspended {
                "Suspended".to_string()
            } else {
                normalized_state(&resources.current_state)
            },
            None,
        ))
    }

    async fn resources_with_refresh(&self) -> Result<PterodactylResources, ProviderStartFailure> {
        let cached = self.cached_clearance().await;

        match self.fetch_resources(cached.as_ref()).await {
            Ok(resources) => Ok(resources),
            Err(error) if error.challenge => {
                let refreshed = self.refresh_clearance(cached.as_ref()).await?;
                self.fetch_resources(Some(&refreshed))
                    .await
                    .map_err(ApiError::into_provider_failure)
            }
            Err(error) => Err(error.into_provider_failure()),
        }
    }

    async fn resources_after_allocation_with_retry(
        &self,
        run_id: &str,
        allocation: &AllocationResult,
        progress: Option<&ProviderProgressSender>,
    ) -> Result<PterodactylResources, ProviderStartFailure> {
        let started_at = Instant::now();
        for attempt in 1..=POST_ALLOCATION_RESOURCE_ATTEMPTS {
            match self.resources_with_refresh().await {
                Ok(resources) => return Ok(resources),
                Err(failure) => {
                    let category = post_allocation_resource_retry_category(&failure);
                    let should_retry =
                        category.is_some() && attempt < POST_ALLOCATION_RESOURCE_ATTEMPTS;
                    if should_retry {
                        emit_progress(
                            progress,
                            ProviderProgressStage::WaitingForAllocation,
                            format!(
                                "A host is ready, but the server controls are temporarily unavailable. Retrying the read-only check ({}/{}) without sending wake or power again.",
                                attempt + 1,
                                POST_ALLOCATION_RESOURCE_ATTEMPTS
                            ),
                        );
                        sleep(self.post_allocation_resource_retry_interval * attempt as u32).await;
                        continue;
                    }

                    let diagnostics = FailureArtifactDiagnostics {
                        failed_stage: "post_allocation_resources",
                        transport_category: category,
                        attempt_count: attempt,
                        timeout_ms: API_TIMEOUT.as_millis() as u64,
                        elapsed_ms: started_at.elapsed().as_millis(),
                    };
                    let artifact_path = self
                        .write_post_allocation_failure_artifact(
                            run_id,
                            allocation,
                            &failure.error_class,
                            &diagnostics,
                        )
                        .await;

                    if let Some(category) = category {
                        let error_class = if category == "timeout" {
                            "ProviderControlsTimeout"
                        } else {
                            "ProviderControlsUnavailable"
                        };
                        let mut exhausted = provider_failure(
                            error_class,
                            "Play Hosting allocated a host, but Butler could not read the server controls after three attempts; no power command was sent",
                            artifact_path,
                            ProviderMutation::None,
                        );
                        exhausted.retryable = true;
                        return Err(exhausted);
                    }

                    let mut failure = failure;
                    backfill_failure_artifacts(&mut failure, artifact_path);
                    return Err(failure);
                }
            }
        }
        unreachable!("post-allocation resource attempts are nonzero")
    }

    async fn details_with_refresh(&self) -> Result<PterodactylServer, ProviderStartFailure> {
        let cached = self.cached_clearance().await;

        match self.fetch_details(cached.as_ref()).await {
            Ok(details) => Ok(details),
            Err(error) if error.challenge => {
                let refreshed = self.refresh_clearance(cached.as_ref()).await?;
                self.fetch_details(Some(&refreshed))
                    .await
                    .map_err(ApiError::into_provider_failure)
            }
            Err(error) => Err(error.into_provider_failure()),
        }
    }

    async fn activity_with_refresh(&self) -> Result<Vec<ServerActivity>, ProviderStartFailure> {
        let cached = self.cached_clearance().await;

        match self.fetch_activity(cached.as_ref()).await {
            Ok(activity) => Ok(activity),
            Err(error) if error.challenge => {
                let refreshed = self.refresh_clearance(cached.as_ref()).await?;
                self.fetch_activity(Some(&refreshed))
                    .await
                    .map_err(ApiError::into_provider_failure)
            }
            Err(error) => Err(error.into_provider_failure()),
        }
    }

    async fn cached_clearance(&self) -> Option<Clearance> {
        let mut clearance = self.clearance.lock().await;
        if clearance.as_ref().is_some_and(Clearance::is_expired) {
            *clearance = None;
        }
        clearance.clone()
    }

    async fn fetch_details(
        &self,
        clearance: Option<&Clearance>,
    ) -> Result<PterodactylServer, ApiError> {
        let url = self.api_url(&self.config.server_id, None)?;
        let request = self.api_request(self.api_client.get(url), clearance)?;
        let response = request.send().await.map_err(classify_read_request_error)?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response_body(response).await?;

        if status == StatusCode::OK {
            let envelope: ServerEnvelope = serde_json::from_slice(&body).map_err(|_| {
                ApiError::definitive(
                    "ProviderProtocol",
                    "Provider server response was not valid JSON",
                )
            })?;
            if !valid_server_reference(&envelope.attributes.uuid) {
                return Err(ApiError::definitive(
                    "ProviderProtocol",
                    "Provider returned an invalid server UUID",
                ));
            }
            return Ok(envelope.attributes);
        }
        Err(classify_api_response(
            status,
            &headers,
            &body,
            ProviderMutation::None,
        ))
    }

    async fn fetch_resources(
        &self,
        clearance: Option<&Clearance>,
    ) -> Result<PterodactylResources, ApiError> {
        let url = self.api_url(&self.config.server_id, Some("resources"))?;
        let request = self.api_request(self.api_client.get(url), clearance)?;
        let response = request.send().await.map_err(classify_read_request_error)?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response_body(response).await?;

        if status == StatusCode::OK {
            let envelope: ResourceEnvelope = serde_json::from_slice(&body).map_err(|_| {
                ApiError::definitive(
                    "ProviderProtocol",
                    "Provider resources response was not valid JSON",
                )
            })?;
            return Ok(envelope.attributes);
        }
        Err(classify_api_response(
            status,
            &headers,
            &body,
            ProviderMutation::None,
        ))
    }

    async fn fetch_activity(
        &self,
        clearance: Option<&Clearance>,
    ) -> Result<Vec<ServerActivity>, ApiError> {
        let mut url = self.api_url(&self.config.server_id, Some("activity"))?;
        url.query_pairs_mut()
            .append_pair("sort", "-timestamp")
            .append_pair("filter[event]", "server:power.start");
        let request = self.api_request(self.api_client.get(url), clearance)?;
        let response = request.send().await.map_err(|_| {
            ApiError::transient(
                "ProviderUnavailable",
                "Provider activity request could not be completed",
            )
        })?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response_body(response).await?;

        if status == StatusCode::OK {
            let envelope: ActivityEnvelope = serde_json::from_slice(&body).map_err(|_| {
                ApiError::definitive(
                    "ProviderProtocol",
                    "Provider activity response was not valid JSON",
                )
            })?;
            return Ok(envelope
                .data
                .into_iter()
                .map(|item| item.attributes)
                .collect());
        }
        Err(classify_api_response(
            status,
            &headers,
            &body,
            ProviderMutation::None,
        ))
    }

    async fn send_start_power(&self) -> Result<(), ProviderStartFailure> {
        let clearance = self.clearance.lock().await.clone();
        let url = self
            .api_url(&self.config.server_id, Some("power"))
            .map_err(ApiError::into_provider_failure)?;
        let request = self
            .api_request(self.api_client.post(url), clearance.as_ref())
            .map_err(ApiError::into_provider_failure)?
            .timeout(POWER_REQUEST_TIMEOUT)
            .json(&serde_json::json!({"signal": "start"}));

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => return Err(classify_power_request_error(&error)),
        };
        let status = response.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(());
        }
        let headers = response.headers().clone();
        let body = response_body(response).await.unwrap_or_default();
        let mut failure =
            classify_api_response(status, &headers, &body, ProviderMutation::PowerStart)
                .into_provider_failure();
        failure.message = power_response_failure_message(status, &headers, &failure.message);
        Err(failure)
    }

    async fn send_wake(&self, server_uuid: &str) -> Result<(), ProviderStartFailure> {
        let clearance = self.clearance.lock().await.clone();
        let url = self
            .api_url(server_uuid, Some("wake"))
            .map_err(ApiError::into_provider_failure)?;
        let request = self
            .api_request(self.api_client.post(url), clearance.as_ref())
            .map_err(ApiError::into_provider_failure)?;

        let response = match request.send().await {
            Ok(response) => response,
            Err(_) => {
                let failure = provider_failure(
                    "ProviderWakeAmbiguous",
                    "The wake request connection ended before confirmation",
                    None,
                    ProviderMutation::WakeAllocation,
                );
                return self.verify_ambiguous_wake(server_uuid, failure).await;
            }
        };
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let headers = response.headers().clone();
        let body = response_body(response).await.unwrap_or_default();
        let failure =
            classify_api_response(status, &headers, &body, ProviderMutation::WakeAllocation)
                .into_provider_failure();
        if failure.uncertain_mutation == ProviderMutation::WakeAllocation {
            self.verify_ambiguous_wake(server_uuid, failure).await
        } else {
            Err(failure)
        }
    }

    async fn ensure_limbo_awake(
        &self,
        run_id: &str,
        server_uuid: &str,
        progress: Option<&ProviderProgressSender>,
    ) -> Result<AllocationResult, ProviderStartFailure> {
        let status = self.sleep_status_with_refresh(server_uuid).await?;
        if let Some((error_class, message)) = allocation_status_failure(&status) {
            let artifact_path = self
                .write_allocation_state_artifact(run_id, &status, false, None)
                .await;
            return Err(provider_failure(
                error_class,
                message,
                artifact_path,
                ProviderMutation::None,
            ));
        }
        if sleep_status_allocation_ready(&status) {
            return Ok(AllocationResult {
                minecraft_address: minecraft_connection(&status),
                status,
                wake_requested: false,
            });
        }

        if sleep_status_confirms_wake(&status) {
            return self
                .wait_for_allocation_ready(run_id, server_uuid, status, false, progress)
                .await
                .map(|status| AllocationResult {
                    minecraft_address: minecraft_connection(&status),
                    status,
                    wake_requested: false,
                });
        }

        if !self.config.power_enabled {
            return Err(provider_failure(
                "ProviderPowerDisabled",
                "Pterodactyl power actions are disabled by configuration",
                None,
                ProviderMutation::None,
            ));
        }

        emit_progress(
            progress,
            ProviderProgressStage::RequestingAllocation,
            "Play Hosting is asleep. Requesting a host for the server.",
        );
        self.send_wake(server_uuid).await?;
        self.wait_for_allocation_ready(run_id, server_uuid, status, true, progress)
            .await
            .map(|status| AllocationResult {
                minecraft_address: minecraft_connection(&status),
                status,
                wake_requested: true,
            })
    }

    async fn wait_for_allocation_ready(
        &self,
        run_id: &str,
        server_uuid: &str,
        mut last_status: PlaySleepStatus,
        wake_requested: bool,
        progress: Option<&ProviderProgressSender>,
    ) -> Result<PlaySleepStatus, ProviderStartFailure> {
        let allocation_started_at = Instant::now();
        let initial_deadline = allocation_started_at
            .checked_add(self.allocation_wait_timeout)
            .ok_or_else(|| {
                provider_failure(
                    "ProviderConfiguration",
                    "The allocation timeout exceeded the supported duration",
                    None,
                    ProviderMutation::None,
                )
            })?;
        let hard_deadline = allocation_started_at
            .checked_add(self.allocation_max_wait_timeout)
            .ok_or_else(|| {
                provider_failure(
                    "ProviderConfiguration",
                    "The maximum allocation timeout exceeded the supported duration",
                    None,
                    ProviderMutation::None,
                )
            })?;
        let mut deadline = initial_deadline;
        let mut extension_active = false;
        let mut last_error = None;
        let mut last_progress_at = None;
        let mut last_progress_key = None;
        let mut poll_attempt_count = 1usize;
        let mut consecutive_poll_errors = 0usize;
        let mut last_healthy_observation_at = allocation_started_at;
        let mut last_transport_category = None;

        loop {
            let artifact_path = self
                .write_allocation_state_artifact(
                    run_id,
                    &last_status,
                    wake_requested,
                    last_error.as_deref(),
                )
                .await;
            if let Some((error_class, message)) = allocation_status_failure(&last_status) {
                return Err(provider_failure(
                    error_class,
                    message,
                    artifact_path,
                    ProviderMutation::None,
                ));
            }
            emit_allocation_progress(
                progress,
                &last_status,
                &mut last_progress_at,
                &mut last_progress_key,
                allocation_started_at,
            );
            if sleep_status_allocation_ready(&last_status) {
                return Ok(last_status);
            }
            if extension_active
                && (consecutive_poll_errors >= ALLOCATION_EXTENSION_ERROR_LIMIT
                    || last_healthy_observation_at.elapsed() >= ALLOCATION_EXTENSION_STALE_TIMEOUT)
            {
                break;
            }

            let now = Instant::now();
            if now >= deadline {
                if !extension_active
                    && now < hard_deadline
                    && last_error.is_none()
                    && allocation_status_supports_extension(&last_status)
                {
                    extension_active = true;
                    deadline = hard_deadline;
                    emit_progress(
                        progress,
                        ProviderProgressStage::WaitingForAllocation,
                        format!(
                            "Play Hosting is still making healthy allocation progress. Extending the wait up to {} total without sending wake or power again.",
                            terminal::format_duration(self.allocation_max_wait_timeout.as_millis())
                        ),
                    );
                    continue;
                }
                break;
            }
            sleep(std::cmp::min(
                self.allocation_wait_interval,
                deadline.saturating_duration_since(now),
            ))
            .await;

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                continue;
            }
            poll_attempt_count += 1;
            match timeout(remaining, self.sleep_status_with_refresh(server_uuid)).await {
                Ok(Ok(status)) => {
                    if allocation_status_supports_extension(&status) {
                        last_healthy_observation_at = Instant::now();
                    }
                    last_status = status;
                    last_error = None;
                    last_transport_category = None;
                    consecutive_poll_errors = 0;
                }
                Ok(Err(failure)) if failure.retryable => {
                    last_transport_category =
                        post_allocation_resource_retry_category(&failure).or(Some("transport"));
                    last_error = Some(failure.message);
                    consecutive_poll_errors += 1;
                }
                Ok(Err(failure)) => return Err(failure),
                Err(_) => {
                    last_transport_category = Some("timeout");
                    last_error = Some("Provider allocation status poll timed out".to_string());
                    consecutive_poll_errors += 1;
                }
            }
        }

        let detail = allocation_timeout_detail(&last_status, last_error.as_deref());
        let diagnostics = FailureArtifactDiagnostics {
            failed_stage: "allocation",
            transport_category: last_transport_category,
            attempt_count: poll_attempt_count,
            timeout_ms: deadline
                .saturating_duration_since(allocation_started_at)
                .as_millis() as u64,
            elapsed_ms: allocation_started_at.elapsed().as_millis(),
        };
        let artifact_path = self
            .write_allocation_failure_artifact(
                run_id,
                &last_status,
                wake_requested,
                last_error.as_deref(),
                &diagnostics,
            )
            .await;
        Err(provider_failure(
            "ProviderAllocationTimeout",
            detail,
            artifact_path,
            ProviderMutation::None,
        ))
    }

    async fn wait_for_wake_confirmation(
        &self,
        server_uuid: &str,
    ) -> Result<(), ProviderStartFailure> {
        let deadline = Instant::now() + SUBMISSION_VERIFY_TIMEOUT;
        for attempt in 0..SUBMISSION_VERIFY_ATTEMPTS {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, self.sleep_status_with_refresh(server_uuid)).await {
                Ok(Ok(status)) if sleep_status_confirms_wake(&status) => return Ok(()),
                Ok(Ok(_)) => {}
                Ok(Err(failure)) if failure.retryable => {}
                Ok(Err(failure)) => return Err(failure),
                Err(_) => break,
            }
            if attempt + 1 < SUBMISSION_VERIFY_ATTEMPTS {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                sleep(std::cmp::min(SUBMISSION_VERIFY_INTERVAL, remaining)).await;
            }
        }
        Err(provider_failure(
            "ProviderWakeUnconfirmed",
            "The provider accepted the wake request but did not confirm allocation",
            None,
            ProviderMutation::WakeAllocation,
        ))
    }

    async fn verify_ambiguous_power(
        &self,
        mut failure: ProviderStartFailure,
        attempted_at: DateTime<Utc>,
        activity_baseline: Option<&str>,
    ) -> Result<PowerConfirmation, Box<PowerAttemptFailure>> {
        let deadline = Instant::now() + SUBMISSION_VERIFY_TIMEOUT;
        let mut last_provider_state = None;
        let mut last_verification_error = None;
        for attempt in 0..SUBMISSION_VERIFY_ATTEMPTS {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(
                std::cmp::min(remaining, SUBMISSION_PROBE_TIMEOUT),
                self.resources_with_refresh(),
            )
            .await
            {
                Ok(Ok(resources)) => {
                    last_provider_state = Some(resources.current_state.clone());
                    if action_for_state(&resources.current_state) == StateAction::AlreadyActive {
                        return Ok(PowerConfirmation {
                            source: PowerConfirmationSource::Resources,
                            provider_state: Some(resources.current_state),
                            original_error: None,
                        });
                    }
                }
                Ok(Err(verification_failure)) => {
                    last_verification_error = Some(format!(
                        "{}: {}",
                        verification_failure.error_class, verification_failure.message
                    ));
                }
                Err(_) => {
                    last_verification_error =
                        Some("Provider resources verification probe timed out".to_string());
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(
                std::cmp::min(remaining, SUBMISSION_PROBE_TIMEOUT),
                self.activity_with_refresh(),
            )
            .await
            {
                Ok(Ok(activity))
                    if activity_confirms_power_start(
                        &activity,
                        activity_baseline,
                        attempted_at,
                    ) =>
                {
                    return Ok(PowerConfirmation {
                        source: PowerConfirmationSource::Activity,
                        provider_state: last_provider_state,
                        original_error: None,
                    });
                }
                Ok(Ok(_)) => {}
                Ok(Err(verification_failure)) => {
                    last_verification_error = Some(format!(
                        "{}: {}",
                        verification_failure.error_class, verification_failure.message
                    ));
                }
                Err(_) => {
                    last_verification_error =
                        Some("Provider activity verification probe timed out".to_string());
                }
            }

            if attempt + 1 < SUBMISSION_VERIFY_ATTEMPTS {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                sleep(std::cmp::min(SUBMISSION_VERIFY_INTERVAL, remaining)).await;
            }
        }
        append_power_verification_detail(
            &mut failure.message,
            last_provider_state.as_deref(),
            last_verification_error.as_deref(),
        );
        Err(Box::new(PowerAttemptFailure {
            failure,
            last_provider_state,
        }))
    }

    async fn verify_ambiguous_wake(
        &self,
        server_uuid: &str,
        failure: ProviderStartFailure,
    ) -> Result<(), ProviderStartFailure> {
        match self.wait_for_wake_confirmation(server_uuid).await {
            Ok(()) => Ok(()),
            Err(verification_failure)
                if verification_failure.error_class == "ProviderWakeUnconfirmed" =>
            {
                Err(failure)
            }
            Err(mut verification_failure) => {
                verification_failure.uncertain_mutation = ProviderMutation::WakeAllocation;
                Err(verification_failure)
            }
        }
    }

    async fn sleep_status_with_refresh(
        &self,
        server_uuid: &str,
    ) -> Result<PlaySleepStatus, ProviderStartFailure> {
        let cached = self.cached_clearance().await;
        match self.fetch_sleep_status(server_uuid, cached.as_ref()).await {
            Ok(status) => Ok(status),
            Err(error) if error.challenge => {
                let refreshed = self.refresh_clearance(cached.as_ref()).await?;
                self.fetch_sleep_status(server_uuid, Some(&refreshed))
                    .await
                    .map_err(ApiError::into_provider_failure)
            }
            Err(error) => Err(error.into_provider_failure()),
        }
    }

    async fn fetch_sleep_status(
        &self,
        server_uuid: &str,
        clearance: Option<&Clearance>,
    ) -> Result<PlaySleepStatus, ApiError> {
        let url = self.api_url(server_uuid, Some("sleep-status"))?;
        let request = self.api_request(self.api_client.get(url), clearance)?;
        let response = request.send().await.map_err(classify_read_request_error)?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response_body(response).await?;
        if status == StatusCode::OK {
            return serde_json::from_slice(&body).map_err(|_| {
                ApiError::definitive(
                    "ProviderProtocol",
                    "Provider sleep-status response was not valid JSON",
                )
            });
        }
        Err(classify_api_response(
            status,
            &headers,
            &body,
            ProviderMutation::None,
        ))
    }

    fn api_request(
        &self,
        mut request: reqwest::RequestBuilder,
        clearance: Option<&Clearance>,
    ) -> Result<reqwest::RequestBuilder, ApiError> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", self.config.api_token))
            .map_err(|_| {
                ApiError::definitive("ProviderConfiguration", "API token was not a valid header")
            })?;
        authorization.set_sensitive(true);
        request = request
            .header(AUTHORIZATION, authorization)
            .header(ACCEPT, "Application/vnd.pterodactyl.v1+json");

        if let Some(clearance) = clearance {
            request = request
                .header(USER_AGENT, clearance.user_agent.clone())
                .header(COOKIE, clearance.cookie_header.clone());
        }
        Ok(request)
    }

    fn api_url(&self, server_reference: &str, endpoint: Option<&str>) -> Result<Url, ApiError> {
        if !valid_server_reference(server_reference) {
            return Err(ApiError::definitive(
                "ProviderConfiguration",
                "Server reference contained invalid characters",
            ));
        }
        let path = match endpoint {
            Some(endpoint) => format!("api/client/servers/{server_reference}/{endpoint}"),
            None => format!("api/client/servers/{server_reference}"),
        };
        self.config.panel_origin.join(&path).map_err(|_| {
            ApiError::definitive("ProviderConfiguration", "Could not construct provider URL")
        })
    }

    async fn refresh_clearance(
        &self,
        rejected: Option<&Clearance>,
    ) -> Result<Clearance, ProviderStartFailure> {
        let _refresh_guard = self.clearance_refresh.lock().await;
        if let Some(current) = self.cached_clearance().await
            && rejected != Some(&current)
        {
            return Ok(current);
        }

        let runtime = self.acquire_flaresolverr_runtime().await?;

        let result = async {
            let mut last_failure = None;
            for attempt in 1..=FLARESOLVERR_CLEARANCE_ATTEMPTS {
                match self.refresh_clearance_once().await {
                    Ok(clearance) => return Ok(clearance),
                    Err(failure)
                        if clearance_failure_is_retryable(&failure)
                            && attempt < FLARESOLVERR_CLEARANCE_ATTEMPTS =>
                    {
                        last_failure = Some(failure);
                        sleep(FLARESOLVERR_CLEARANCE_RETRY_INTERVAL).await;
                    }
                    Err(failure) => return Err(failure),
                }
            }
            Err(last_failure.expect("clearance attempts must record a failure"))
        }
        .await;

        if let Ok(clearance) = &result {
            *self.clearance.lock().await = Some(clearance.clone());
        }
        sleep(FLARESOLVERR_SHUTDOWN_GRACE).await;
        runtime.shutdown().await;
        result
    }

    async fn acquire_flaresolverr_runtime(
        &self,
    ) -> Result<lifecycle::FlareSolverrOperationRuntime, ProviderStartFailure> {
        for attempt in 1..=FLARESOLVERR_RUNTIME_ATTEMPTS {
            match FlareSolverrSupervisor::ensure_for_operation(self.flaresolverr_runtime.clone())
                .await
            {
                Ok(runtime) => return Ok(runtime),
                Err(_) if attempt < FLARESOLVERR_RUNTIME_ATTEMPTS => {
                    sleep(FLARESOLVERR_RUNTIME_RETRY_INTERVAL).await
                }
                Err(error) => return Err(flaresolverr_runtime_failure(&error)),
            }
        }
        unreachable!("runtime attempts must return a result")
    }

    async fn refresh_clearance_once(&self) -> Result<Clearance, ProviderStartFailure> {
        let endpoint = self.config.flaresolverr_url.join("v1").map_err(|_| {
            provider_failure(
                "FlareSolverrConfiguration",
                "Could not construct FlareSolverr URL",
                None,
                ProviderMutation::None,
            )
        })?;
        let request = FlareSolverrRequest {
            command: "request.get",
            url: self.config.panel_origin.as_str(),
            max_timeout: FLARESOLVERR_REQUEST_TIMEOUT_MS,
            return_only_cookies: true,
        };
        let response = self
            .flaresolverr_client
            .post(endpoint)
            .json(&request)
            .send()
            .await
            .map_err(|_| {
                retryable_provider_failure(
                    "FlareSolverrUnavailable",
                    "FlareSolverr request could not be completed",
                )
            })?;
        if !response.status().is_success() {
            return Err(retryable_provider_failure(
                "FlareSolverrUnavailable",
                "FlareSolverr returned an unsuccessful status",
            ));
        }
        let body = limited_response_body(response, MAX_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                LimitedBodyError::Timeout | LimitedBodyError::Connect => {
                    retryable_provider_failure(
                        "FlareSolverrUnavailable",
                        "FlareSolverr response connection ended before completion",
                    )
                }
                LimitedBodyError::Read => provider_failure(
                    "FlareSolverrProtocol",
                    "FlareSolverr response could not be read",
                    None,
                    ProviderMutation::None,
                ),
                LimitedBodyError::TooLarge => provider_failure(
                    "FlareSolverrProtocol",
                    "FlareSolverr response exceeded the allowed size",
                    None,
                    ProviderMutation::None,
                ),
            })?;
        let response: FlareSolverrResponse = serde_json::from_slice(&body).map_err(|_| {
            provider_failure(
                "FlareSolverrProtocol",
                "FlareSolverr response was not valid JSON",
                None,
                ProviderMutation::None,
            )
        })?;
        parse_clearance(
            response,
            self.config.panel_origin.host_str().unwrap_or_default(),
        )
    }

    async fn write_state_artifact(
        &self,
        run_id: &str,
        current_state: &str,
        is_suspended: bool,
        is_limbo: bool,
    ) -> Option<PathBuf> {
        self.write_state_artifact_value(
            run_id,
            &StateArtifact {
                provider: "pterodactyl",
                workflow_stage: "provider_state",
                current_state: Some(current_state),
                is_suspended: Some(is_suspended),
                is_limbo: Some(is_limbo),
                phase: None,
                connection: None,
                position: None,
                total: None,
                estimated_minutes: None,
                blocked: None,
                enabled: None,
                wake_requested: false,
                last_error: None,
                failed_stage: None,
                transport_category: None,
                attempt_count: None,
                timeout_ms: None,
                elapsed_ms: None,
                power_request_attempted: None,
                power_submission_state: None,
                power_confirmation_source: None,
            },
        )
        .await
    }

    async fn write_verified_submission_artifact(
        &self,
        run_id: &str,
        source: SubmissionVerificationSource,
        observed_state: &str,
    ) -> Option<PathBuf> {
        self.write_state_artifact_value(
            run_id,
            &StateArtifact {
                provider: "pterodactyl",
                workflow_stage: "submission_verified",
                current_state: Some(observed_state),
                is_suspended: None,
                is_limbo: None,
                phase: None,
                connection: None,
                position: None,
                total: None,
                estimated_minutes: None,
                blocked: None,
                enabled: None,
                wake_requested: false,
                last_error: None,
                failed_stage: None,
                transport_category: None,
                attempt_count: None,
                timeout_ms: None,
                elapsed_ms: None,
                power_request_attempted: Some(true),
                power_submission_state: Some(PowerSubmissionState::Confirmed),
                power_confirmation_source: Some(match source {
                    SubmissionVerificationSource::Provider => {
                        PowerConfirmationSource::ProviderVerification
                    }
                    SubmissionVerificationSource::Minecraft => PowerConfirmationSource::Minecraft,
                }),
            },
        )
        .await;
        None
    }

    async fn write_allocation_state_artifact(
        &self,
        run_id: &str,
        status: &PlaySleepStatus,
        wake_requested: bool,
        last_error: Option<&str>,
    ) -> Option<PathBuf> {
        self.write_allocation_artifact(
            run_id,
            "allocation",
            status,
            wake_requested,
            last_error,
            None,
        )
        .await
    }

    async fn write_allocation_failure_artifact(
        &self,
        run_id: &str,
        status: &PlaySleepStatus,
        wake_requested: bool,
        last_error: Option<&str>,
        diagnostics: &FailureArtifactDiagnostics<'_>,
    ) -> Option<PathBuf> {
        self.write_allocation_artifact(
            run_id,
            "allocation",
            status,
            wake_requested,
            last_error,
            Some(diagnostics),
        )
        .await
    }

    async fn write_post_allocation_failure_artifact(
        &self,
        run_id: &str,
        allocation: &AllocationResult,
        last_error: &str,
        diagnostics: &FailureArtifactDiagnostics<'_>,
    ) -> Option<PathBuf> {
        self.write_allocation_artifact(
            run_id,
            "provider_controls",
            &allocation.status,
            allocation.wake_requested,
            Some(last_error),
            Some(diagnostics),
        )
        .await
    }

    async fn write_allocation_artifact(
        &self,
        run_id: &str,
        workflow_stage: &str,
        status: &PlaySleepStatus,
        wake_requested: bool,
        last_error: Option<&str>,
        diagnostics: Option<&FailureArtifactDiagnostics<'_>>,
    ) -> Option<PathBuf> {
        self.write_state_artifact_value(
            run_id,
            &StateArtifact {
                provider: "pterodactyl",
                workflow_stage,
                current_state: None,
                is_suspended: None,
                is_limbo: Some(true),
                phase: Some(&status.phase),
                connection: status.connection.as_deref(),
                position: status.position,
                total: status.total,
                estimated_minutes: status.estimated_minutes,
                blocked: status.blocked,
                enabled: status.enabled,
                wake_requested,
                last_error,
                failed_stage: diagnostics.map(|value| value.failed_stage),
                transport_category: diagnostics.and_then(|value| value.transport_category),
                attempt_count: diagnostics.map(|value| value.attempt_count),
                timeout_ms: diagnostics.map(|value| value.timeout_ms),
                elapsed_ms: diagnostics.map(|value| value.elapsed_ms),
                power_request_attempted: Some(false),
                power_submission_state: None,
                power_confirmation_source: None,
            },
        )
        .await
    }

    async fn write_power_state_artifact(
        &self,
        run_id: &str,
        state: PowerArtifactState<'_>,
    ) -> Option<PathBuf> {
        let allocation_status = state.allocation.map(|allocation| &allocation.status);
        self.write_state_artifact_value(
            run_id,
            &StateArtifact {
                provider: "pterodactyl",
                workflow_stage: "power_start",
                current_state: Some(state.current_state),
                is_suspended: Some(state.is_suspended),
                is_limbo: Some(state.is_limbo),
                phase: allocation_status.map(|status| status.phase.as_str()),
                connection: allocation_status.and_then(|status| status.connection.as_deref()),
                position: allocation_status.and_then(|status| status.position),
                total: allocation_status.and_then(|status| status.total),
                estimated_minutes: allocation_status.and_then(|status| status.estimated_minutes),
                blocked: allocation_status.and_then(|status| status.blocked),
                enabled: allocation_status.and_then(|status| status.enabled),
                wake_requested: state
                    .allocation
                    .is_some_and(|allocation| allocation.wake_requested),
                last_error: state.last_error,
                failed_stage: None,
                transport_category: None,
                attempt_count: None,
                timeout_ms: None,
                elapsed_ms: None,
                power_request_attempted: match state.submission_state {
                    PowerSubmissionState::Dispatching => None,
                    PowerSubmissionState::Ambiguous
                    | PowerSubmissionState::Confirmed
                    | PowerSubmissionState::Rejected => Some(true),
                },
                power_submission_state: Some(state.submission_state),
                power_confirmation_source: state.confirmation_source,
            },
        )
        .await
    }

    async fn write_state_artifact_value(
        &self,
        run_id: &str,
        artifact: &StateArtifact<'_>,
    ) -> Option<PathBuf> {
        let run_dir = self.artifact_dir.join(run_id);
        if self.artifact_capture == ArtifactCapture::Off {
            return None;
        }
        let path = run_dir.join(STATE_ARTIFACT);
        let bytes = match serde_json::to_vec_pretty(artifact) {
            Ok(bytes) => bytes,
            Err(error) => {
                emit_state_artifact_warning(run_id, &error.into());
                return None;
            }
        };
        let write_run_dir = run_dir.clone();
        let write_path = path.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<()> {
            mark_run_artifact_dir(&write_run_dir)?;
            write_owner_only_atomic(&write_path, &bytes)
        })
        .await
        .context("provider state artifact task failed")
        .and_then(|result| result);
        match result {
            Ok(()) => Some(path),
            Err(error) => {
                emit_state_artifact_warning(run_id, &error);
                None
            }
        }
    }

    async fn write_terminal_failure_artifact(
        &self,
        run_id: &str,
        error_class: &str,
        uncertain_mutation: ProviderMutation,
    ) -> Option<PathBuf> {
        let submission_attempted = uncertain_mutation.may_have_started_server();
        self.write_state_artifact_value(
            run_id,
            &StateArtifact {
                provider: "pterodactyl",
                workflow_stage: if submission_attempted {
                    "provider_submission_attempted"
                } else {
                    "provider_failure"
                },
                current_state: None,
                is_suspended: None,
                is_limbo: None,
                phase: None,
                connection: None,
                position: None,
                total: None,
                estimated_minutes: None,
                blocked: None,
                enabled: None,
                wake_requested: false,
                last_error: Some(error_class),
                failed_stage: Some(if submission_attempted {
                    "power_start"
                } else {
                    "provider"
                }),
                transport_category: None,
                attempt_count: None,
                timeout_ms: None,
                elapsed_ms: None,
                power_request_attempted: Some(submission_attempted),
                power_submission_state: None,
                power_confirmation_source: None,
            },
        )
        .await
    }
}

fn clearance_failure_is_retryable(failure: &ProviderStartFailure) -> bool {
    failure.error_class == "FlareSolverrUnavailable"
}

fn flaresolverr_runtime_failure(error: &anyhow::Error) -> ProviderStartFailure {
    let detail = error.to_string();
    let error_class = if detail.starts_with("FlareSolverrConfiguration") {
        "FlareSolverrConfiguration"
    } else {
        "FlareSolverrUnavailable"
    };
    let mut failure = provider_failure(
        error_class,
        if error_class == "FlareSolverrConfiguration" {
            "FlareSolverr is not configured safely"
        } else {
            "FlareSolverr could not be started for provider access"
        },
        None,
        ProviderMutation::None,
    );
    failure.retryable = error_class == "FlareSolverrUnavailable";
    failure
}

impl ServerStartProvider for PterodactylProvider {
    fn name(&self) -> &'static str {
        "pterodactyl"
    }

    fn check<'a>(&'a self, _run_id: &'a str) -> ProviderCheckFuture<'a> {
        Box::pin(async move { self.check_inner().await })
    }

    fn start<'a>(&'a self, run_id: &'a str) -> ProviderStartFuture<'a> {
        Box::pin(async move { self.start_with_terminal_artifacts(run_id, None).await })
    }

    fn start_with_progress<'a>(
        &'a self,
        run_id: &'a str,
        progress: ProviderProgressSender,
    ) -> ProviderStartFuture<'a> {
        Box::pin(async move {
            self.start_with_terminal_artifacts(run_id, Some(&progress))
                .await
        })
    }

    fn submission_verified_artifact<'a>(
        &'a self,
        run_id: &'a str,
        source: SubmissionVerificationSource,
        observed_state: &'a str,
    ) -> ProviderArtifactFuture<'a> {
        Box::pin(async move {
            self.write_verified_submission_artifact(run_id, source, observed_state)
                .await
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
struct PterodactylResources {
    current_state: String,
    is_suspended: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct PterodactylServer {
    uuid: String,
    #[serde(default)]
    is_suspended: bool,
    #[serde(default)]
    is_limbo: bool,
    status: Option<String>,
}

#[derive(Deserialize)]
struct ResourceEnvelope {
    attributes: PterodactylResources,
}

#[derive(Deserialize)]
struct ServerEnvelope {
    attributes: PterodactylServer,
}

#[derive(Deserialize)]
struct ActivityEnvelope {
    data: Vec<ActivityItem>,
}

#[derive(Deserialize)]
struct ActivityItem {
    attributes: ServerActivity,
}

#[derive(Deserialize)]
struct ServerActivity {
    id: String,
    event: String,
    timestamp: String,
    #[serde(default)]
    is_api: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct PlaySleepStatus {
    #[serde(alias = "status")]
    phase: String,
    #[serde(default)]
    connection: Option<String>,
    #[serde(default)]
    position: Option<u64>,
    #[serde(default)]
    total: Option<u64>,
    #[serde(default)]
    estimated_minutes: Option<u64>,
    #[serde(default)]
    blocked: Option<bool>,
    #[serde(default)]
    enabled: Option<bool>,
}

struct AllocationResult {
    minecraft_address: Option<Arc<str>>,
    status: PlaySleepStatus,
    wake_requested: bool,
}

struct PowerConfirmation {
    source: PowerConfirmationSource,
    provider_state: Option<String>,
    original_error: Option<String>,
}

impl PowerConfirmation {
    fn accepted() -> Self {
        Self {
            source: PowerConfirmationSource::Response,
            provider_state: None,
            original_error: None,
        }
    }
}

struct PowerAttemptFailure {
    failure: ProviderStartFailure,
    last_provider_state: Option<String>,
}

impl PowerAttemptFailure {
    fn new(failure: ProviderStartFailure) -> Self {
        Self {
            failure,
            last_provider_state: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Clearance {
    user_agent: HeaderValue,
    cookie_header: HeaderValue,
    expires_at: Option<u64>,
}

impl Clearance {
    fn is_expired(&self) -> bool {
        let Some(expires_at) = self.expires_at else {
            return false;
        };
        unix_timestamp().saturating_add(30) >= expires_at
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FlareSolverrRequest<'a> {
    #[serde(rename = "cmd")]
    command: &'a str,
    url: &'a str,
    max_timeout: u64,
    return_only_cookies: bool,
}

#[derive(Deserialize)]
struct FlareSolverrResponse {
    status: String,
    solution: Option<FlareSolverrSolution>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FlareSolverrSolution {
    status: u16,
    user_agent: String,
    cookies: Vec<FlareSolverrCookie>,
}

#[derive(Deserialize)]
struct FlareSolverrCookie {
    name: String,
    value: String,
    domain: Option<String>,
    expires: Option<f64>,
}

#[derive(Serialize)]
struct StateArtifact<'a> {
    provider: &'a str,
    workflow_stage: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_state: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_suspended: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_limbo: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connection: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_minutes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blocked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    wake_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_stage: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transport_category: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    power_request_attempted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    power_submission_state: Option<PowerSubmissionState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    power_confirmation_source: Option<PowerConfirmationSource>,
}

struct FailureArtifactDiagnostics<'a> {
    failed_stage: &'a str,
    transport_category: Option<&'a str>,
    attempt_count: usize,
    timeout_ms: u64,
    elapsed_ms: u128,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum PowerSubmissionState {
    Dispatching,
    Ambiguous,
    Confirmed,
    Rejected,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum PowerConfirmationSource {
    Response,
    Resources,
    Activity,
    ProviderVerification,
    Minecraft,
}

struct PowerArtifactState<'a> {
    current_state: &'a str,
    is_suspended: bool,
    is_limbo: bool,
    submission_state: PowerSubmissionState,
    allocation: Option<&'a AllocationResult>,
    last_error: Option<&'a str>,
    confirmation_source: Option<PowerConfirmationSource>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateAction {
    AlreadyActive,
    RequestStart,
    WaitForStopping,
    FailUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServerDetailsAction {
    Wake,
    FailSuspended,
    FetchResources,
}

fn action_for_server_details(details: &PterodactylServer) -> ServerDetailsAction {
    if details.is_limbo {
        ServerDetailsAction::Wake
    } else if details.is_suspended {
        ServerDetailsAction::FailSuspended
    } else {
        ServerDetailsAction::FetchResources
    }
}

fn action_for_state(state: &str) -> StateAction {
    match state.trim().to_ascii_lowercase().as_str() {
        "running" | "starting" => StateAction::AlreadyActive,
        "offline" | "stopped" => StateAction::RequestStart,
        "stopping" => StateAction::WaitForStopping,
        _ => StateAction::FailUnknown,
    }
}

fn normalized_state(state: &str) -> String {
    let normalized = state.trim().to_ascii_lowercase();
    let mut chars = normalized.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => "Unknown".to_string(),
    }
}

fn provider_check_result(
    provider_status: impl Into<String>,
    minecraft_address: Option<Arc<str>>,
) -> ProviderCheckResult {
    ProviderCheckResult {
        provider_status: provider_status.into(),
        minecraft_address,
        screenshot_path: None,
        detail_artifact_path: None,
    }
}

fn sleep_status_confirms_wake(status: &PlaySleepStatus) -> bool {
    sleep_status_allocation_ready(status)
        || matches!(
            status.phase.trim().to_ascii_lowercase().as_str(),
            "queued" | "waking" | "attention"
        )
}

fn sleep_status_allocation_ready(status: &PlaySleepStatus) -> bool {
    status
        .connection
        .as_deref()
        .is_some_and(|connection| !connection.trim().is_empty())
        || status.phase.trim().eq_ignore_ascii_case("active")
}

fn allocation_status_supports_extension(status: &PlaySleepStatus) -> bool {
    status.blocked != Some(true)
        && status.enabled != Some(false)
        && matches!(
            status.phase.trim().to_ascii_lowercase().as_str(),
            "queued" | "waking"
        )
}

fn allocation_status_failure(status: &PlaySleepStatus) -> Option<(&'static str, &'static str)> {
    if status.enabled == Some(false) {
        return Some((
            "ProviderAllocationDisabled",
            "Play Hosting allocation is disabled for the configured server",
        ));
    }
    if status.blocked == Some(true) || status.phase.trim().eq_ignore_ascii_case("attention") {
        return Some((
            "ProviderAllocationBlocked",
            "Play Hosting allocation requires attention before it can continue",
        ));
    }
    match status.phase.trim().to_ascii_lowercase().as_str() {
        "active" | "queued" | "waking" | "suspended" | "sleeping" | "asleep" => None,
        _ => Some((
            "ProviderAllocationStateUnknown",
            "Play Hosting returned an unsupported allocation phase",
        )),
    }
}

fn minecraft_connection(status: &PlaySleepStatus) -> Option<Arc<str>> {
    status
        .connection
        .as_deref()
        .map(str::trim)
        .filter(|connection| !connection.is_empty())
        .map(Arc::from)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AllocationProgressKey {
    phase: String,
    position: Option<u64>,
    total: Option<u64>,
    estimated_minutes: Option<u64>,
    blocked: Option<bool>,
}

fn emit_progress(
    progress: Option<&ProviderProgressSender>,
    stage: ProviderProgressStage,
    detail: impl Into<String>,
) {
    if let Some(progress) = progress {
        let _ = progress.send(ProviderProgress {
            stage,
            detail: detail.into(),
        });
    }
}

fn emit_allocation_progress(
    progress: Option<&ProviderProgressSender>,
    status: &PlaySleepStatus,
    last_progress_at: &mut Option<Instant>,
    last_progress_key: &mut Option<AllocationProgressKey>,
    allocation_started_at: Instant,
) {
    let key = AllocationProgressKey {
        phase: status.phase.clone(),
        position: status.position,
        total: status.total,
        estimated_minutes: status.estimated_minutes,
        blocked: status.blocked,
    };
    let phase_changed = last_progress_key
        .as_ref()
        .is_some_and(|previous| previous.phase != key.phase);
    let interval_elapsed =
        last_progress_at.is_none_or(|last| last.elapsed() >= ALLOCATION_PROGRESS_INTERVAL);
    if last_progress_key.is_none() || phase_changed || interval_elapsed {
        emit_progress(
            progress,
            ProviderProgressStage::WaitingForAllocation,
            format!(
                "{} Waited {} so far.",
                allocation_status_detail(status),
                terminal::format_duration(allocation_started_at.elapsed().as_millis())
            ),
        );
        *last_progress_at = Some(Instant::now());
        *last_progress_key = Some(key);
    }
}

fn allocation_status_detail(status: &PlaySleepStatus) -> String {
    let phase = status.phase.trim().to_ascii_lowercase();
    let mut detail = match phase.as_str() {
        "asleep" | "sleeping" | "suspended" => {
            "Play Hosting is asleep. Waiting for it to assign a host.".to_string()
        }
        "queued" => match (status.position, status.total) {
            (Some(position), Some(total)) => {
                format!("In Play Hosting's queue: position {position} of {total}.")
            }
            (Some(position), None) => {
                format!("In Play Hosting's queue at position {position}.")
            }
            _ => "In Play Hosting's allocation queue.".to_string(),
        },
        "waking" => "A host is waking up for the server.".to_string(),
        "active" => "A host is ready for the server.".to_string(),
        "attention" => "Play Hosting needs attention before allocation can continue.".to_string(),
        _ => format!("Play Hosting reports allocation state '{phase}'."),
    };
    if phase == "waking"
        && let Some(position) = status.position
    {
        match status.total {
            Some(total) => {
                detail.push_str(&format!(" Queue position: {position} of {total}."));
            }
            None => detail.push_str(&format!(" Queue position: {position}.")),
        }
    }
    if let Some(minutes) = status.estimated_minutes {
        detail.push_str(&format!(" Estimated wait: about {minutes} minutes."));
    }
    if status.blocked == Some(true) {
        detail.push_str(" Allocation is blocked and needs attention.");
    }
    detail
}

fn allocation_timeout_detail(status: &PlaySleepStatus, last_error: Option<&str>) -> String {
    let mut detail = format!(
        "Provider allocation did not become active before the configured deadline ({})",
        allocation_status_detail(status)
    );
    if let Some(error) = last_error {
        detail.push_str("; last transient error: ");
        detail.push_str(error);
    }
    detail
}

fn valid_server_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_hexdigit() || character == '-')
}

fn parse_clearance(
    response: FlareSolverrResponse,
    panel_host: &str,
) -> Result<Clearance, ProviderStartFailure> {
    if response.status != "ok" {
        return Err(provider_failure(
            "FlareSolverrChallenge",
            "FlareSolverr did not solve the browser challenge",
            None,
            ProviderMutation::None,
        ));
    }
    let solution = response.solution.ok_or_else(|| {
        provider_failure(
            "FlareSolverrProtocol",
            "FlareSolverr response did not contain a solution",
            None,
            ProviderMutation::None,
        )
    })?;
    if solution.status != 200 {
        return Err(provider_failure(
            "FlareSolverrChallenge",
            "FlareSolverr solution did not reach the panel",
            None,
            ProviderMutation::None,
        ));
    }
    let cookie = solution
        .cookies
        .into_iter()
        .find(|cookie| cookie.name == "cf_clearance")
        .ok_or_else(|| {
            provider_failure(
                "FlareSolverrChallenge",
                "FlareSolverr solution did not include Cloudflare clearance",
                None,
                ProviderMutation::None,
            )
        })?;
    if !cookie_domain_matches(cookie.domain.as_deref(), panel_host) {
        return Err(provider_failure(
            "FlareSolverrProtocol",
            "Cloudflare clearance was scoped to an unexpected domain",
            None,
            ProviderMutation::None,
        ));
    }
    if !valid_cookie_value(&cookie.value) {
        return Err(provider_failure(
            "FlareSolverrProtocol",
            "FlareSolverr returned an invalid clearance cookie",
            None,
            ProviderMutation::None,
        ));
    }
    let user_agent = HeaderValue::from_str(&solution.user_agent).map_err(|_| {
        provider_failure(
            "FlareSolverrProtocol",
            "FlareSolverr returned an invalid user agent",
            None,
            ProviderMutation::None,
        )
    })?;
    let mut cookie_header = HeaderValue::from_str(&format!("cf_clearance={}", cookie.value))
        .map_err(|_| {
            provider_failure(
                "FlareSolverrProtocol",
                "FlareSolverr returned an invalid clearance cookie",
                None,
                ProviderMutation::None,
            )
        })?;
    cookie_header.set_sensitive(true);
    Ok(Clearance {
        user_agent,
        cookie_header,
        expires_at: cookie
            .expires
            .filter(|value| value.is_finite() && *value > 0.0)
            .map(|value| value as u64),
    })
}

fn cookie_domain_matches(domain: Option<&str>, panel_host: &str) -> bool {
    let Some(domain) = domain else {
        return false;
    };
    let domain = domain.trim_start_matches('.').to_ascii_lowercase();
    let panel_host = panel_host.to_ascii_lowercase();
    domain.split('.').count() >= 2
        && (panel_host == domain || panel_host.ends_with(&format!(".{domain}")))
}

fn valid_cookie_value(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            (0x21..=0x7e).contains(&byte) && !matches!(byte, b'"' | b',' | b';' | b'\\')
        })
}

struct ApiError {
    error_class: &'static str,
    message: &'static str,
    challenge: bool,
    uncertain_mutation: ProviderMutation,
    retryable: bool,
}

impl ApiError {
    fn definitive(error_class: &'static str, message: &'static str) -> Self {
        Self {
            error_class,
            message,
            challenge: false,
            uncertain_mutation: ProviderMutation::None,
            retryable: false,
        }
    }

    fn transient(error_class: &'static str, message: &'static str) -> Self {
        Self {
            error_class,
            message,
            challenge: false,
            uncertain_mutation: ProviderMutation::None,
            retryable: true,
        }
    }

    fn into_provider_failure(self) -> ProviderStartFailure {
        let mut failure = provider_failure(
            self.error_class,
            self.message,
            None,
            self.uncertain_mutation,
        );
        failure.retryable = self.retryable;
        failure
    }
}

fn classify_read_request_error(error: reqwest::Error) -> ApiError {
    if error.is_timeout() {
        return ApiError::transient("ProviderRequestTimeout", "Provider read request timed out");
    }
    if error.is_connect() {
        return ApiError::transient(
            "ProviderConnection",
            "Provider read endpoint could not be reached",
        );
    }
    ApiError::transient(
        "ProviderTransport",
        "Provider read request could not be completed",
    )
}

fn post_allocation_resource_retry_category(failure: &ProviderStartFailure) -> Option<&'static str> {
    if failure.uncertain_mutation != ProviderMutation::None || !failure.retryable {
        return None;
    }
    match failure.error_class.as_str() {
        "ProviderRequestTimeout" => Some("timeout"),
        "ProviderConnection" => Some("connect"),
        "ProviderRateLimited" => Some("rate_limited"),
        "ProviderUnavailable" => Some("server_error"),
        _ => None,
    }
}

fn classify_api_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    mutation: ProviderMutation,
) -> ApiError {
    if status == StatusCode::FORBIDDEN && is_cloudflare_challenge(headers, body) {
        return ApiError {
            error_class: "ProviderChallenge",
            message: "Cloudflare clearance is required",
            challenge: mutation == ProviderMutation::None,
            uncertain_mutation: ProviderMutation::None,
            retryable: false,
        };
    }
    let (error_class, message) = match status {
        StatusCode::UNAUTHORIZED => ("ProviderAuthentication", "Provider rejected the API token"),
        StatusCode::FORBIDDEN => (
            "ProviderAuthorization",
            "Provider denied access to the configured server",
        ),
        StatusCode::NOT_FOUND => (
            "ProviderServerUnavailable",
            "Configured provider server was not found",
        ),
        StatusCode::UNPROCESSABLE_ENTITY => (
            "ProviderProtocol",
            "Provider rejected the requested power action",
        ),
        StatusCode::CONFLICT => (
            "ProviderStateConflict",
            "Provider state changed while the request was in progress",
        ),
        StatusCode::TOO_MANY_REQUESTS => ("ProviderRateLimited", "Provider rate limit was reached"),
        status if status.is_server_error() => {
            ("ProviderUnavailable", "Provider returned a server error")
        }
        _ => ("ProviderProtocol", "Provider returned an unexpected status"),
    };
    ApiError {
        error_class,
        message,
        challenge: false,
        uncertain_mutation: if mutation != ProviderMutation::None && status.is_server_error() {
            mutation
        } else {
            ProviderMutation::None
        },
        retryable: status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error(),
    }
}

fn classify_power_request_error(error: &reqwest::Error) -> ProviderStartFailure {
    if error.is_connect() {
        let mut failure = provider_failure(
            "ProviderPowerConnection",
            "The power endpoint could not be reached before the request was submitted",
            None,
            ProviderMutation::None,
        );
        failure.retryable = true;
        return failure;
    }
    if error.is_timeout() {
        let mut failure = provider_failure(
            "ProviderPowerTimeout",
            "The power request timed out before the provider confirmed it",
            None,
            ProviderMutation::PowerStart,
        );
        failure.retryable = true;
        return failure;
    }

    let mut failure = provider_failure(
        "ProviderPowerAmbiguous",
        "The power request connection ended before confirmation",
        None,
        ProviderMutation::PowerStart,
    );
    failure.retryable = true;
    failure
}

fn power_response_failure_message(
    status: StatusCode,
    headers: &HeaderMap,
    fallback: &str,
) -> String {
    let mut diagnostics = vec![format!("HTTP {}", status.as_u16())];
    for name in ["cf-ray", "cf-mitigated", "server", "retry-after"] {
        if let Some(value) = safe_diagnostic_header(headers, name) {
            diagnostics.push(format!("{name}={value}"));
        }
    }
    format!("{fallback} ({})", diagnostics.join(", "))
}

fn safe_diagnostic_header(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?;
    let value = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric()
                || matches!(
                    character,
                    ' ' | '.' | ',' | ':' | '/' | ';' | '=' | '(' | ')' | '+' | '-' | '_'
                )
            {
                character
            } else {
                '?'
            }
        })
        .take(160)
        .collect::<String>();
    (!value.is_empty()).then_some(value)
}

fn activity_confirms_power_start(
    activity: &[ServerActivity],
    baseline_id: Option<&str>,
    attempted_at: DateTime<Utc>,
) -> bool {
    if let Some(baseline_id) = baseline_id
        && let Some(position) = activity.iter().position(|item| item.id == baseline_id)
    {
        return activity[..position].iter().any(is_api_power_start_activity);
    }

    let latest_expected = Utc::now() + chrono::Duration::seconds(5);
    activity.iter().any(|item| {
        is_api_power_start_activity(item)
            && DateTime::parse_from_rfc3339(&item.timestamp)
                .map(|timestamp| {
                    let timestamp = timestamp.with_timezone(&Utc);
                    timestamp >= attempted_at && timestamp <= latest_expected
                })
                .unwrap_or(false)
    })
}

fn is_api_power_start_activity(item: &ServerActivity) -> bool {
    item.is_api && item.event == "server:power.start"
}

fn append_power_verification_detail(
    message: &mut String,
    last_provider_state: Option<&str>,
    last_verification_error: Option<&str>,
) {
    if let Some(state) = last_provider_state {
        message.push_str(&format!(
            "; last provider state: {}",
            terminal::clean(state)
        ));
    }
    if let Some(error) = last_verification_error {
        message.push_str(&format!(
            "; last verification error: {}",
            terminal::clean(error)
        ));
    }
}

fn is_cloudflare_challenge(headers: &HeaderMap, body: &[u8]) -> bool {
    if headers
        .get("cf-mitigated")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("challenge"))
    {
        return true;
    }
    let text = String::from_utf8_lossy(body).to_ascii_lowercase();
    text.contains("cloudflare") && text.contains("just a moment")
}

enum LimitedBodyError {
    Timeout,
    Connect,
    Read,
    TooLarge,
}

async fn limited_response_body(
    mut response: Response,
    max_bytes: u64,
) -> Result<Vec<u8>, LimitedBodyError> {
    if response
        .content_length()
        .is_some_and(|size| size > max_bytes)
    {
        return Err(LimitedBodyError::TooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        if error.is_timeout() {
            LimitedBodyError::Timeout
        } else if error.is_connect() {
            LimitedBodyError::Connect
        } else {
            LimitedBodyError::Read
        }
    })? {
        if body.len().saturating_add(chunk.len()) as u64 > max_bytes {
            return Err(LimitedBodyError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn response_body(response: Response) -> Result<Vec<u8>, ApiError> {
    limited_response_body(response, MAX_RESPONSE_BYTES)
        .await
        .map_err(|error| match error {
            LimitedBodyError::Timeout => {
                ApiError::transient("ProviderRequestTimeout", "Provider response body timed out")
            }
            LimitedBodyError::Connect => ApiError::transient(
                "ProviderConnection",
                "Provider connection closed while reading the response",
            ),
            LimitedBodyError::Read => ApiError::transient(
                "ProviderResponseRead",
                "Provider response could not be read",
            ),
            LimitedBodyError::TooLarge => ApiError::definitive(
                "ProviderProtocol",
                "Provider response exceeded the allowed size",
            ),
        })
}

fn provider_failure(
    error_class: impl Into<String>,
    message: impl Into<String>,
    detail_artifact_path: Option<PathBuf>,
    uncertain_mutation: ProviderMutation,
) -> ProviderStartFailure {
    ProviderStartFailure {
        error_class: error_class.into(),
        message: message.into(),
        screenshot_path: None,
        detail_artifact_path,
        minecraft_address: None,
        uncertain_mutation,
        retryable: false,
    }
}

fn set_failure_artifacts(
    failure: &mut ProviderStartFailure,
    detail_artifact_path: Option<PathBuf>,
    _fresh_detail_artifact_path: Option<&PathBuf>,
) {
    failure.screenshot_path = None;
    failure.detail_artifact_path = detail_artifact_path;
}

fn backfill_failure_artifacts(
    failure: &mut ProviderStartFailure,
    fallback_detail_artifact_path: Option<PathBuf>,
) {
    if failure.detail_artifact_path.is_none() {
        failure.detail_artifact_path = fallback_detail_artifact_path;
    }
}

fn emit_state_artifact_warning(run_id: &str, error: &anyhow::Error) {
    terminal::emit(terminal::line(
        "WARN",
        "artifacts",
        "",
        "",
        None,
        format!(
            "could not write sanitized provider state for run {}; error {}",
            run_id,
            terminal::clean(&error.to_string())
        ),
    ));
}

fn retryable_provider_failure(
    error_class: impl Into<String>,
    message: impl Into<String>,
) -> ProviderStartFailure {
    let mut failure = provider_failure(error_class, message, None, ProviderMutation::None);
    failure.retryable = true;
    failure
}

fn attach_minecraft_address(
    mut failure: ProviderStartFailure,
    minecraft_address: &Option<Arc<str>>,
) -> ProviderStartFailure {
    failure.minecraft_address = minecraft_address.clone();
    failure
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn flaresolverr_response(domain: &str, cookie: &str) -> FlareSolverrResponse {
        FlareSolverrResponse {
            status: "ok".to_string(),
            solution: Some(FlareSolverrSolution {
                status: 200,
                user_agent: "Mozilla/5.0 test".to_string(),
                cookies: vec![FlareSolverrCookie {
                    name: "cf_clearance".to_string(),
                    value: cookie.to_string(),
                    domain: Some(domain.to_string()),
                    expires: Some((unix_timestamp() + 3600) as f64),
                }],
            }),
        }
    }

    fn server_details(is_limbo: bool, is_suspended: bool) -> PterodactylServer {
        PterodactylServer {
            uuid: "34634dd7-e564-480e-a3a7-84baf53c9328".to_string(),
            is_suspended,
            is_limbo,
            status: None,
        }
    }

    async fn mock_api(
        responses: Vec<(&'static str, StatusCode, &'static str)>,
    ) -> (Url, tokio::task::JoinHandle<()>) {
        mock_api_with_delays(
            responses
                .into_iter()
                .map(|(request, status, body)| (request, status, body, Duration::ZERO))
                .collect(),
        )
        .await
    }

    async fn mock_api_with_delays(
        responses: Vec<(&'static str, StatusCode, &'static str, Duration)>,
    ) -> (Url, tokio::task::JoinHandle<()>) {
        let (url, task, _) = mock_api_recording_with_delays(responses).await;
        (url, task)
    }

    async fn mock_api_recording(
        responses: Vec<(&'static str, StatusCode, &'static str)>,
    ) -> (Url, tokio::task::JoinHandle<()>, Arc<StdMutex<Vec<String>>>) {
        mock_api_recording_with_delays(
            responses
                .into_iter()
                .map(|(request, status, body)| (request, status, body, Duration::ZERO))
                .collect(),
        )
        .await
    }

    async fn mock_api_recording_with_delays(
        responses: Vec<(&'static str, StatusCode, &'static str, Duration)>,
    ) -> (Url, tokio::task::JoinHandle<()>, Arc<StdMutex<Vec<String>>>) {
        mock_api_recording_with_delay_mode(responses, false).await
    }

    async fn mock_api_recording_with_body_delays(
        responses: Vec<(&'static str, StatusCode, &'static str, Duration)>,
    ) -> (Url, tokio::task::JoinHandle<()>, Arc<StdMutex<Vec<String>>>) {
        mock_api_recording_with_delay_mode(responses, true).await
    }

    async fn mock_api_recording_with_delay_mode(
        responses: Vec<(&'static str, StatusCode, &'static str, Duration)>,
        delay_body: bool,
    ) -> (Url, tokio::task::JoinHandle<()>, Arc<StdMutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let task_requests = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut responses = responses.into_iter().peekable();
            while let Some((expected_request_line, _, _, _)) = responses.peek() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 8192];
                let size = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..size]);
                let request_line = request.lines().next().unwrap_or_default();
                task_requests.lock().unwrap().push(request_line.to_string());
                if request_line.contains("/activity?") && request_line != *expected_request_line {
                    let body = r#"{"data":[]}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    continue;
                }
                let (expected_request_line, status, body, delay) = responses.next().unwrap();
                assert_eq!(request_line, expected_request_line);
                let headers = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Unknown"),
                    body.len()
                );
                if delay_body {
                    let _ = stream.write_all(headers.as_bytes()).await;
                    sleep(delay).await;
                    let _ = stream.write_all(body.as_bytes()).await;
                } else {
                    sleep(delay).await;
                    let response = format!("{headers}{body}");
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
        });
        (
            Url::parse(&format!("http://{address}/")).unwrap(),
            task,
            requests,
        )
    }

    fn test_config(panel_origin: Url, power_enabled: bool) -> PterodactylConfig {
        PterodactylConfig {
            panel_origin,
            server_id: "34634dd7".to_string(),
            api_token: "test-token".to_string(),
            power_enabled,
            allocation_wait_secs: 720,
            allocation_max_wait_secs: 1_800,
            flaresolverr_url: Url::parse("http://127.0.0.1:8191/").unwrap(),
            flaresolverr_container: "flaresolverr".to_string(),
            orbctl_path: PathBuf::from("/unused/orbctl"),
            docker_path: PathBuf::from("/unused/docker"),
        }
    }

    fn test_provider(
        config: PterodactylConfig,
        artifact_dir: PathBuf,
        artifact_capture: ArtifactCapture,
    ) -> PterodactylProvider {
        let mut provider =
            PterodactylProvider::new(config, artifact_dir, artifact_capture).unwrap();
        provider.allocation_wait_interval = Duration::from_millis(1);
        provider.post_allocation_resource_retry_interval = Duration::from_millis(1);
        provider
    }

    #[tokio::test]
    async fn limbo_server_uses_full_uuid_wake_endpoint_once() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"asleep","connection":null}"#,
            ),
            (
                "POST /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/wake HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example"}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        assert_eq!(result.provider_status, "Start requested");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn active_limbo_allocation_skips_wake_and_starts_server() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example"}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ])
        .await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-pterodactyl-active-allocation-{}",
            rand::random::<u64>()
        ));
        let provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        assert_eq!(result.minecraft_address.as_deref(), Some("server.example"));
        assert_eq!(result.screenshot_path, None);
        let artifact =
            std::fs::read_to_string(result.detail_artifact_path.expect("state artifact")).unwrap();
        assert!(artifact.contains(r#""workflow_stage": "power_start""#));
        assert!(artifact.contains(r#""phase": "active""#));
        assert!(artifact.contains(r#""connection": "server.example""#));
        assert!(artifact.contains(r#""current_state": "offline""#));
        assert!(artifact.contains(r#""power_submission_state": "confirmed""#));
        assert!(artifact.contains(r#""power_confirmation_source": "response""#));
        assert!(!artifact.contains("test-token"));
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn power_artifact_records_ambiguous_state_and_original_error() {
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-pterodactyl-ambiguous-power-{}",
            rand::random::<u64>()
        ));
        let provider = test_provider(
            test_config(Url::parse("http://127.0.0.1:1/").unwrap(), true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );

        let path = provider
            .write_power_state_artifact(
                "run-id",
                PowerArtifactState {
                    current_state: "offline",
                    is_suspended: false,
                    is_limbo: true,
                    submission_state: PowerSubmissionState::Ambiguous,
                    allocation: None,
                    last_error: Some("ProviderPowerTimeout: request timed out"),
                    confirmation_source: None,
                },
            )
            .await
            .unwrap();
        let artifact = std::fs::read_to_string(path).unwrap();

        assert!(artifact.contains(r#""power_submission_state": "ambiguous""#));
        assert!(artifact.contains(r#""power_request_attempted": true"#));
        assert!(artifact.contains("ProviderPowerTimeout"));
        assert!(!artifact.contains("test-token"));
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn dispatching_power_artifact_does_not_claim_request_was_attempted() {
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-pterodactyl-dispatching-power-{}",
            rand::random::<u64>()
        ));
        let provider = test_provider(
            test_config(Url::parse("http://127.0.0.1:1/").unwrap(), true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );

        let path = provider
            .write_power_state_artifact(
                "run-id",
                PowerArtifactState {
                    current_state: "offline",
                    is_suspended: false,
                    is_limbo: false,
                    submission_state: PowerSubmissionState::Dispatching,
                    allocation: None,
                    last_error: None,
                    confirmation_source: None,
                },
            )
            .await
            .unwrap();
        let artifact = std::fs::read_to_string(path).unwrap();

        assert!(artifact.contains(r#""power_submission_state": "dispatching""#));
        assert!(!artifact.contains("power_request_attempted"));
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn queued_limbo_allocation_waits_without_replaying_wake() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null}"#,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example"}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn delayed_allocation_with_many_polls_completes_before_one_power_request() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let mut responses = vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"asleep","connection":null,"enabled":true}"#,
            ),
            (
                "POST /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/wake HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ];
        for _ in 0..64 {
            responses.push((
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"waking","connection":null,"position":1,"total":8,"estimated_minutes":6,"blocked":false,"enabled":true}"#,
            ));
        }
        responses.extend([
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example","enabled":true}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ]);
        let (panel_origin, server) = mock_api(responses).await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let result = provider
            .start_inner("run-id", Some(&progress_tx))
            .await
            .unwrap();
        drop(progress_tx);
        let mut progress = Vec::new();
        while let Ok(item) = progress_rx.try_recv() {
            progress.push(item);
        }

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        assert!(progress.iter().any(|item| {
            item.stage == ProviderProgressStage::WaitingForAllocation
                && item.detail.contains("Queue position: 1 of 8")
        }));
        assert!(
            progress
                .iter()
                .any(|item| item.stage == ProviderProgressStage::RequestingPower)
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn post_allocation_resource_retries_do_not_replay_mutations() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server, requests) = mock_api_recording(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"asleep","connection":null,"enabled":true}"#,
            ),
            (
                "POST /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/wake HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example","enabled":true}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::SERVICE_UNAVAILABLE,
                "{}",
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::TOO_MANY_REQUESTS,
                "{}",
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        server.await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/wake HTTP/1.1"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/resources HTTP/1.1"))
                .count(),
            3
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/power HTTP/1.1"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn post_allocation_state_conflict_does_not_replay_wake() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let (panel_origin, server, requests) = mock_api_recording(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"asleep","connection":null,"enabled":true}"#,
            ),
            (
                "POST /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/wake HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example","enabled":true}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::CONFLICT,
                "{}",
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let failure = provider.start_inner("run-id", None).await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderStateConflict");
        assert_eq!(failure.uncertain_mutation, ProviderMutation::None);
        server.await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/wake HTTP/1.1"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/power HTTP/1.1"))
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn exhausted_post_allocation_resource_retries_do_not_send_power() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let mut responses = vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"asleep","connection":null,"enabled":true}"#,
            ),
            (
                "POST /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/wake HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example","enabled":true}"#,
            ),
        ];
        for _ in 0..POST_ALLOCATION_RESOURCE_ATTEMPTS {
            responses.push((
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::SERVICE_UNAVAILABLE,
                "{}",
            ));
        }
        let (panel_origin, server, requests) = mock_api_recording(responses).await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-provider-controls-failure-{}",
            rand::random::<u64>()
        ));
        let provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let failure = provider
            .start_inner("run-id", Some(&progress_tx))
            .await
            .unwrap_err();
        drop(progress_tx);

        assert_eq!(failure.error_class, "ProviderControlsUnavailable");
        assert_eq!(failure.uncertain_mutation, ProviderMutation::None);
        assert!(failure.retryable);
        let mut progress = Vec::new();
        while let Ok(item) = progress_rx.try_recv() {
            progress.push(item);
        }
        assert!(
            !progress
                .iter()
                .any(|item| item.stage == ProviderProgressStage::RequestingPower)
        );
        let artifact = std::fs::read_to_string(
            failure
                .detail_artifact_path
                .as_ref()
                .expect("failure artifact"),
        )
        .unwrap();
        assert!(artifact.contains(r#""workflow_stage": "provider_controls""#));
        assert!(artifact.contains(r#""failed_stage": "post_allocation_resources""#));
        assert!(artifact.contains(r#""transport_category": "server_error""#));
        assert!(artifact.contains(r#""attempt_count": 3"#));
        assert!(artifact.contains(r#""timeout_ms": 15000"#));
        assert!(artifact.contains(r#""power_request_attempted": false"#));
        assert!(!artifact.contains("test-token"));
        server.await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/wake HTTP/1.1"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/resources HTTP/1.1"))
                .count(),
            3
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/power HTTP/1.1"))
                .count(),
            0
        );
        drop(requests);
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn post_allocation_resource_timeouts_are_classified_and_bounded() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let mut responses = vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
                Duration::ZERO,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example","enabled":true}"#,
                Duration::ZERO,
            ),
        ];
        for _ in 0..POST_ALLOCATION_RESOURCE_ATTEMPTS {
            responses.push((
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
                Duration::from_millis(30),
            ));
        }
        let (panel_origin, server, requests) = mock_api_recording_with_delays(responses).await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-provider-controls-timeout-{}",
            rand::random::<u64>()
        ));
        let mut provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );
        provider.api_client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(Duration::from_millis(5))
            .build()
            .unwrap();

        let failure = provider.start_inner("run-id", None).await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderControlsTimeout");
        let artifact = std::fs::read_to_string(
            failure
                .detail_artifact_path
                .as_ref()
                .expect("failure artifact"),
        )
        .unwrap();
        assert!(artifact.contains(r#""transport_category": "timeout""#));
        assert!(artifact.contains(r#""attempt_count": 3"#));
        assert!(artifact.contains(r#""power_request_attempted": false"#));
        server.await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/resources HTTP/1.1"))
                .count(),
            3
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/power HTTP/1.1"))
                .count(),
            0
        );
        drop(requests);
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn resource_body_timeout_uses_the_retryable_timeout_category() {
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server, _) = mock_api_recording_with_body_delays(vec![(
            "GET /api/client/servers/34634dd7/resources HTTP/1.1",
            StatusCode::OK,
            resources,
            Duration::from_millis(30),
        )])
        .await;
        let mut provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );
        provider.api_client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(Duration::from_millis(5))
            .build()
            .unwrap();

        let failure = provider.resources_with_refresh().await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderRequestTimeout");
        assert_eq!(
            post_allocation_resource_retry_category(&failure),
            Some("timeout")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn healthy_queued_allocation_extends_once_to_hard_deadline() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null,"blocked":false,"enabled":true}"#,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example","enabled":true}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ])
        .await;
        let mut provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );
        provider.allocation_wait_timeout = Duration::ZERO;
        provider.allocation_max_wait_timeout = Duration::from_millis(50);
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let result = provider
            .start_inner("run-id", Some(&progress_tx))
            .await
            .unwrap();
        drop(progress_tx);

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        let mut progress = Vec::new();
        while let Ok(item) = progress_rx.try_recv() {
            progress.push(item);
        }
        assert!(
            progress
                .iter()
                .any(|item| item.detail.contains("Extending the wait"))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn allocation_poll_timeout_suppresses_extension() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let (panel_origin, server) = mock_api_with_delays(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
                Duration::ZERO,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null,"blocked":false,"enabled":true}"#,
                Duration::ZERO,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null,"blocked":false,"enabled":true}"#,
                Duration::from_millis(200),
            ),
        ])
        .await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-allocation-poll-timeout-{}",
            rand::random::<u64>()
        ));
        let mut provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );
        provider.allocation_wait_timeout = Duration::from_millis(100);
        provider.allocation_max_wait_timeout = Duration::from_millis(300);
        provider.allocation_wait_interval = Duration::ZERO;
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let failure = provider
            .start_inner("run-id", Some(&progress_tx))
            .await
            .unwrap_err();
        drop(progress_tx);

        assert_eq!(failure.error_class, "ProviderAllocationTimeout");
        let mut progress = Vec::new();
        while let Ok(item) = progress_rx.try_recv() {
            progress.push(item);
        }
        assert!(
            !progress
                .iter()
                .any(|item| item.detail.contains("Extending the wait"))
        );
        let artifact = std::fs::read_to_string(
            failure
                .detail_artifact_path
                .as_ref()
                .expect("failure artifact"),
        )
        .unwrap();
        assert!(artifact.contains(r#""transport_category": "timeout""#));
        assert!(artifact.contains(r#""attempt_count": 2"#));
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn extended_allocation_never_moves_past_hard_deadline() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let mut responses = vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null,"blocked":false,"enabled":true}"#,
            ),
        ];
        for _ in 0..64 {
            responses.push((
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null,"blocked":false,"enabled":true}"#,
            ));
        }
        let (panel_origin, server, requests) = mock_api_recording(responses).await;
        let mut provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );
        provider.allocation_wait_timeout = Duration::ZERO;
        provider.allocation_max_wait_timeout = Duration::from_millis(10);
        provider.allocation_wait_interval = Duration::from_millis(1);
        let started_at = Instant::now();

        let failure = provider.start_inner("run-id", None).await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderAllocationTimeout");
        assert!(started_at.elapsed() < Duration::from_millis(100));
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/wake HTTP/1.1"))
                .count(),
            0
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains("/power HTTP/1.1"))
                .count(),
            0
        );
        drop(requests);
        server.abort();
    }

    #[tokio::test]
    async fn extended_allocation_stops_after_bounded_poll_errors() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let mut responses = vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null,"blocked":false,"enabled":true}"#,
            ),
        ];
        for _ in 0..ALLOCATION_EXTENSION_ERROR_LIMIT {
            responses.push((
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::SERVICE_UNAVAILABLE,
                "{}",
            ));
        }
        let (panel_origin, server) = mock_api(responses).await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-allocation-extension-errors-{}",
            rand::random::<u64>()
        ));
        let mut provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );
        provider.allocation_wait_timeout = Duration::ZERO;
        provider.allocation_max_wait_timeout = Duration::from_millis(500);
        let started_at = Instant::now();

        let failure = provider.start_inner("run-id", None).await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderAllocationTimeout");
        assert!(started_at.elapsed() < Duration::from_millis(200));
        let artifact = std::fs::read_to_string(
            failure
                .detail_artifact_path
                .as_ref()
                .expect("failure artifact"),
        )
        .unwrap();
        assert!(artifact.contains(r#""transport_category": "server_error""#));
        assert!(artifact.contains(r#""attempt_count": 4"#));
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn allocation_timeout_is_not_treated_as_power_submission() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null,"position":1,"total":4,"estimated_minutes":6,"enabled":true}"#,
            ),
        ])
        .await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-pterodactyl-timeout-{}",
            rand::random::<u64>()
        ));
        let mut provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );
        provider.allocation_wait_timeout = Duration::from_millis(1);
        provider.allocation_max_wait_timeout = Duration::from_millis(1);

        let failure = provider.start_inner("run-id", None).await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderAllocationTimeout");
        assert_eq!(failure.uncertain_mutation, ProviderMutation::None);
        assert!(!failure.uncertain_mutation.may_have_started_server());
        assert!(failure.message.contains("Play Hosting's queue"));
        assert!(failure.message.contains("position 1 of 4"));
        let artifact = std::fs::read_to_string(
            failure
                .detail_artifact_path
                .as_ref()
                .expect("timeout artifact"),
        )
        .unwrap();
        assert!(artifact.contains(r#""workflow_stage": "allocation""#));
        assert!(artifact.contains(r#""failed_stage": "allocation""#));
        assert!(artifact.contains(r#""phase": "queued""#));
        assert!(artifact.contains(r#""position": 1"#));
        assert!(artifact.contains(r#""power_request_attempted": false"#));
        assert!(!artifact.contains("test-token"));
        assert!(!artifact.contains("cf_clearance"));
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn read_only_mode_allows_active_limbo_server_status() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let resources = r#"{"attributes":{"current_state":"running","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example"}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, false),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::AlreadyActive);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn provider_check_reads_offline_state_without_power_request() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":false,"status":"offline"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.check("check1").await.unwrap();

        assert_eq!(result.provider_status, "Offline");
        assert_eq!(result.minecraft_address, None);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn provider_check_reads_limbo_queue_without_wake_request() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","position":3,"total":8}"#,
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.check("check1").await.unwrap();

        assert_eq!(
            result.provider_status,
            "In Play Hosting's queue: position 3 of 8."
        );
        assert_eq!(result.minecraft_address, None);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn provider_check_preserves_active_allocation_context_when_process_is_offline() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":true,"status":null}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example"}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.check("check1").await.unwrap();

        assert_eq!(
            result.provider_status,
            "A host is allocated, but the Minecraft server process is offline."
        );
        assert_eq!(result.minecraft_address.as_deref(), Some("server.example"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn provider_check_reports_non_limbo_suspension_without_resource_request() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":false,"status":"suspended"}}"#;
        let (panel_origin, server) = mock_api(vec![(
            "GET /api/client/servers/34634dd7 HTTP/1.1",
            StatusCode::OK,
            details,
        )])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.check("check1").await.unwrap();

        assert_eq!(result.provider_status, "Suspended");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn provider_check_reports_suspended_resource_state() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":false,"status":"offline"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":true}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.check("check1").await.unwrap();

        assert_eq!(result.provider_status, "Suspended");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ambiguous_wake_uses_sleep_status_without_replaying_post() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"asleep","connection":null}"#,
            ),
            (
                "POST /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/wake HTTP/1.1",
                StatusCode::BAD_GATEWAY,
                "{}",
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"queued","connection":null}"#,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"active","connection":"server.example"}"#,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ambiguous_wake_preserves_definitive_verification_failure() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":true,"status":"suspended"}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::OK,
                r#"{"phase":"asleep","connection":null}"#,
            ),
            (
                "POST /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/wake HTTP/1.1",
                StatusCode::BAD_GATEWAY,
                "{}",
            ),
            (
                "GET /api/client/servers/34634dd7-e564-480e-a3a7-84baf53c9328/sleep-status HTTP/1.1",
                StatusCode::UNAUTHORIZED,
                "{}",
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let failure = provider.start_inner("run-id", None).await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderAuthentication");
        assert_eq!(failure.uncertain_mutation, ProviderMutation::WakeAllocation);
        assert!(!failure.uncertain_mutation.may_have_started_server());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn offline_server_uses_standard_power_endpoint_once() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":false,"status":"offline"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ambiguous_power_uses_resources_without_replaying_post() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":false,"status":"offline"}}"#;
        let offline = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let starting = r#"{"attributes":{"current_state":"starting","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                offline,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::BAD_GATEWAY,
                "{}",
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                starting,
            ),
        ])
        .await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-pterodactyl-resource-confirmed-{}",
            rand::random::<u64>()
        ));
        let provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        let artifact =
            std::fs::read_to_string(result.detail_artifact_path.expect("state artifact")).unwrap();
        assert!(artifact.contains(r#""current_state": "starting""#));
        assert!(artifact.contains(r#""power_submission_state": "confirmed""#));
        assert!(artifact.contains(r#""power_confirmation_source": "resources""#));
        assert!(artifact.contains("HTTP 502"));
        assert!(!artifact.contains("test-token"));
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn activity_baseline_timeout_does_not_block_single_power_request() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":false,"status":"offline"}}"#;
        let offline = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api_with_delays(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
                Duration::ZERO,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                offline,
                Duration::ZERO,
            ),
            (
                "GET /api/client/servers/34634dd7/activity?sort=-timestamp&filter%5Bevent%5D=server%3Apower.start HTTP/1.1",
                StatusCode::OK,
                r#"{"data":[]}"#,
                Duration::from_millis(50),
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::NO_CONTENT,
                "",
                Duration::ZERO,
            ),
        ])
        .await;
        let mut provider = test_provider(
            test_config(panel_origin, true),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );
        provider.submission_probe_timeout = Duration::from_millis(10);

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ambiguous_power_uses_recent_api_activity_without_replaying_post() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":false,"status":"offline"}}"#;
        let offline = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let baseline = r#"{"data":[{"attributes":{"id":"baseline","event":"server:power.start","timestamp":"2000-01-01T00:00:00Z","is_api":true}}]}"#;
        let activity = r#"{"data":[{"attributes":{"id":"new-start","event":"server:power.start","timestamp":"2000-01-01T00:00:01Z","is_api":true}},{"attributes":{"id":"baseline","event":"server:power.start","timestamp":"2000-01-01T00:00:00Z","is_api":true}}]}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                offline,
            ),
            (
                "GET /api/client/servers/34634dd7/activity?sort=-timestamp&filter%5Bevent%5D=server%3Apower.start HTTP/1.1",
                StatusCode::OK,
                baseline,
            ),
            (
                "POST /api/client/servers/34634dd7/power HTTP/1.1",
                StatusCode::BAD_GATEWAY,
                "{}",
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                offline,
            ),
            (
                "GET /api/client/servers/34634dd7/activity?sort=-timestamp&filter%5Bevent%5D=server%3Apower.start HTTP/1.1",
                StatusCode::OK,
                activity,
            ),
        ])
        .await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-pterodactyl-activity-confirmed-{}",
            rand::random::<u64>()
        ));
        let provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Screenshots,
        );

        let result = provider.start_inner("run-id", None).await.unwrap();

        assert_eq!(result.outcome, StartOutcome::StartRequested);
        let artifact =
            std::fs::read_to_string(result.detail_artifact_path.expect("state artifact")).unwrap();
        assert!(artifact.contains(r#""current_state": "offline""#));
        assert!(artifact.contains(r#""power_submission_state": "confirmed""#));
        assert!(artifact.contains(r#""power_confirmation_source": "activity""#));
        assert!(artifact.contains("HTTP 502"));
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn power_disabled_fails_before_mutating_request() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":false,"is_limbo":false,"status":"offline"}}"#;
        let resources = r#"{"attributes":{"current_state":"offline","is_suspended":false}}"#;
        let (panel_origin, server) = mock_api(vec![
            (
                "GET /api/client/servers/34634dd7 HTTP/1.1",
                StatusCode::OK,
                details,
            ),
            (
                "GET /api/client/servers/34634dd7/resources HTTP/1.1",
                StatusCode::OK,
                resources,
            ),
        ])
        .await;
        let provider = test_provider(
            test_config(panel_origin, false),
            PathBuf::from("unused"),
            ArtifactCapture::Off,
        );

        let failure = provider.start_inner("run-id", None).await.unwrap_err();

        assert_eq!(failure.error_class, "ProviderPowerDisabled");
        server.await.unwrap();
    }

    #[test]
    fn limbo_takes_precedence_over_suspension() {
        assert_eq!(
            action_for_server_details(&server_details(true, true)),
            ServerDetailsAction::Wake
        );
        assert_eq!(
            action_for_server_details(&server_details(false, true)),
            ServerDetailsAction::FailSuspended
        );
        assert_eq!(
            action_for_server_details(&server_details(false, false)),
            ServerDetailsAction::FetchResources
        );
    }

    #[test]
    fn selects_actions_for_provider_states() {
        assert_eq!(action_for_state("running"), StateAction::AlreadyActive);
        assert_eq!(action_for_state("starting"), StateAction::AlreadyActive);
        assert_eq!(action_for_state("offline"), StateAction::RequestStart);
        assert_eq!(action_for_state("stopped"), StateAction::RequestStart);
        assert_eq!(action_for_state("stopping"), StateAction::WaitForStopping);
        assert_eq!(action_for_state("mystery"), StateAction::FailUnknown);
    }

    #[test]
    fn selects_wake_confirmation_sleep_states() {
        let status = |json| serde_json::from_str::<PlaySleepStatus>(json).unwrap();

        let active = status(r#"{"phase":"active","connection":"minecrafteruni.play.hosting"}"#);
        let queued = status(r#"{"phase":"queued","connection":null}"#);
        assert!(sleep_status_confirms_wake(&active));
        assert!(sleep_status_allocation_ready(&active));
        assert!(sleep_status_confirms_wake(&queued));
        assert!(!sleep_status_allocation_ready(&queued));
        assert!(sleep_status_confirms_wake(&status(
            r#"{"status":"waking"}"#
        )));
        assert!(!sleep_status_confirms_wake(&status(
            r#"{"phase":"asleep","connection":null}"#
        )));
        assert!(!sleep_status_confirms_wake(&status(
            r#"{"phase":"pausing","connection":""}"#
        )));
    }

    #[test]
    fn rejects_blocked_disabled_and_unknown_allocation_states() {
        let status = |json| serde_json::from_str::<PlaySleepStatus>(json).unwrap();

        assert_eq!(
            allocation_status_failure(&status(r#"{"phase":"waking","blocked":true}"#))
                .map(|failure| failure.0),
            Some("ProviderAllocationBlocked")
        );
        assert_eq!(
            allocation_status_failure(&status(r#"{"phase":"suspended","enabled":false}"#))
                .map(|failure| failure.0),
            Some("ProviderAllocationDisabled")
        );
        assert_eq!(
            allocation_status_failure(&status(r#"{"phase":"mystery"}"#)).map(|failure| failure.0),
            Some("ProviderAllocationStateUnknown")
        );
        assert!(allocation_status_failure(&status(r#"{"phase":"queued"}"#)).is_none());
    }

    #[test]
    fn detects_cloudflare_challenge() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-mitigated", HeaderValue::from_static("challenge"));
        assert!(is_cloudflare_challenge(&headers, b""));
        assert!(is_cloudflare_challenge(
            &HeaderMap::new(),
            b"Cloudflare - Just a moment"
        ));
        assert!(!is_cloudflare_challenge(&HeaderMap::new(), b"forbidden"));
    }

    #[test]
    fn accepts_only_panel_scoped_clearance() {
        assert!(
            parse_clearance(
                flaresolverr_response(".play.hosting", "safe-cookie"),
                "panel.play.hosting"
            )
            .is_ok()
        );
        assert!(
            parse_clearance(
                flaresolverr_response("attacker.example", "safe-cookie"),
                "panel.play.hosting"
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_header_injection_in_clearance() {
        let failure = match parse_clearance(
            flaresolverr_response(".play.hosting", "value\r\nInjected: yes"),
            "panel.play.hosting",
        ) {
            Ok(_) => panic!("header injection must be rejected"),
            Err(failure) => failure,
        };
        assert_eq!(failure.error_class, "FlareSolverrProtocol");
        assert!(!failure.message.contains("Injected"));
    }

    #[test]
    fn rejects_cookie_delimiters_in_clearance() {
        let failure = match parse_clearance(
            flaresolverr_response(".play.hosting", "safe; session=injected"),
            "panel.play.hosting",
        ) {
            Ok(_) => panic!("cookie delimiters must be rejected"),
            Err(failure) => failure,
        };
        assert_eq!(failure.error_class, "FlareSolverrProtocol");
    }

    #[test]
    fn retries_only_transient_clearance_failures() {
        assert!(clearance_failure_is_retryable(&provider_failure(
            "FlareSolverrUnavailable",
            "challenge timed out",
            None,
            ProviderMutation::None,
        )));
        assert!(!clearance_failure_is_retryable(&provider_failure(
            "FlareSolverrProtocol",
            "invalid response",
            None,
            ProviderMutation::None,
        )));
        assert!(!clearance_failure_is_retryable(&provider_failure(
            "FlareSolverrConfiguration",
            "invalid endpoint",
            None,
            ProviderMutation::None,
        )));
    }

    #[test]
    fn power_server_errors_are_ambiguous() {
        let error = classify_api_response(
            StatusCode::BAD_GATEWAY,
            &HeaderMap::new(),
            b"",
            ProviderMutation::PowerStart,
        );
        assert_eq!(error.uncertain_mutation, ProviderMutation::PowerStart);
        let wake_error = classify_api_response(
            StatusCode::BAD_GATEWAY,
            &HeaderMap::new(),
            b"",
            ProviderMutation::WakeAllocation,
        );
        assert_eq!(
            wake_error.uncertain_mutation,
            ProviderMutation::WakeAllocation
        );
        assert!(!wake_error.uncertain_mutation.may_have_started_server());
        let definitive = classify_api_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            &HeaderMap::new(),
            b"",
            ProviderMutation::PowerStart,
        );
        assert_eq!(definitive.uncertain_mutation, ProviderMutation::None);
        assert!(!definitive.retryable);

        let read_retry = classify_api_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &HeaderMap::new(),
            b"",
            ProviderMutation::None,
        );
        assert!(read_retry.retryable);
        assert_eq!(read_retry.uncertain_mutation, ProviderMutation::None);
    }

    #[test]
    fn post_allocation_retry_predicate_is_read_only_and_allowlisted() {
        for (class, expected) in [
            ("ProviderRequestTimeout", Some("timeout")),
            ("ProviderConnection", Some("connect")),
            ("ProviderRateLimited", Some("rate_limited")),
            ("ProviderUnavailable", Some("server_error")),
            ("ProviderAuthentication", None),
            ("ProviderAuthorization", None),
            ("ProviderProtocol", None),
            ("ProviderStateConflict", None),
            ("FlareSolverrUnavailable", None),
        ] {
            let mut failure = provider_failure(class, "test", None, ProviderMutation::None);
            failure.retryable = true;
            assert_eq!(post_allocation_resource_retry_category(&failure), expected);
        }

        let mut mutation = provider_failure(
            "ProviderUnavailable",
            "test",
            None,
            ProviderMutation::WakeAllocation,
        );
        mutation.retryable = true;
        assert_eq!(post_allocation_resource_retry_category(&mutation), None);
    }

    #[test]
    fn allocation_extension_requires_healthy_queue_progress() {
        let mut status = PlaySleepStatus {
            phase: "queued".to_string(),
            connection: None,
            position: Some(2),
            total: Some(8),
            estimated_minutes: Some(5),
            blocked: Some(false),
            enabled: Some(true),
        };
        assert!(allocation_status_supports_extension(&status));
        status.phase = "waking".to_string();
        assert!(allocation_status_supports_extension(&status));
        status.phase = "asleep".to_string();
        assert!(!allocation_status_supports_extension(&status));
        status.phase = "queued".to_string();
        status.blocked = Some(true);
        assert!(!allocation_status_supports_extension(&status));
        status.blocked = Some(false);
        status.enabled = Some(false);
        assert!(!allocation_status_supports_extension(&status));
    }

    #[tokio::test]
    async fn connection_failures_are_definitively_pre_submission() {
        let error = Client::new()
            .post("http://127.0.0.1:0/power")
            .send()
            .await
            .unwrap_err();

        let failure = classify_power_request_error(&error);

        assert_eq!(failure.error_class, "ProviderPowerConnection");
        assert_eq!(failure.uncertain_mutation, ProviderMutation::None);
    }

    #[tokio::test]
    async fn response_timeouts_remain_ambiguous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            sleep(Duration::from_millis(100)).await;
        });
        let client = Client::builder()
            .timeout(Duration::from_millis(10))
            .build()
            .unwrap();
        let error = client
            .post(format!("http://{address}/power"))
            .send()
            .await
            .unwrap_err();

        let failure = classify_power_request_error(&error);

        assert_eq!(failure.error_class, "ProviderPowerTimeout");
        assert_eq!(failure.uncertain_mutation, ProviderMutation::PowerStart);
        server.await.unwrap();
    }

    #[test]
    fn activity_confirmation_requires_recent_api_power_start() {
        let attempted_at = Utc::now() - chrono::Duration::seconds(2);
        let recent = (attempted_at + chrono::Duration::seconds(1)).to_rfc3339();
        let old = (attempted_at - chrono::Duration::seconds(1)).to_rfc3339();
        let activity = |id: &str, event: &str, timestamp: &str, is_api: bool| ServerActivity {
            id: id.to_string(),
            event: event.to_string(),
            timestamp: timestamp.to_string(),
            is_api,
        };

        assert!(activity_confirms_power_start(
            &[activity("new", "server:power.start", &recent, true,)],
            None,
            attempted_at,
        ));
        assert!(!activity_confirms_power_start(
            &[activity("old", "server:power.start", &old, true,)],
            None,
            attempted_at,
        ));
        assert!(!activity_confirms_power_start(
            &[activity("new", "server:power.start", &recent, false,)],
            None,
            attempted_at,
        ));
        assert!(!activity_confirms_power_start(
            &[activity(
                "future",
                "server:power.start",
                "9999-01-01T00:00:00+00:00",
                true,
            )],
            None,
            attempted_at,
        ));
        assert!(activity_confirms_power_start(
            &[
                activity(
                    "new",
                    "server:power.start",
                    "2000-01-01T00:00:00+00:00",
                    true,
                ),
                activity(
                    "baseline",
                    "server:power.start",
                    "1999-01-01T00:00:00+00:00",
                    true,
                ),
            ],
            Some("baseline"),
            attempted_at,
        ));
    }

    #[test]
    fn power_response_diagnostics_include_only_safe_selected_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-ray", HeaderValue::from_static("abc123"));
        headers.insert("server", HeaderValue::from_static("@everyone `unsafe`"));
        headers.insert("set-cookie", HeaderValue::from_static("secret=value"));

        let message =
            power_response_failure_message(StatusCode::BAD_GATEWAY, &headers, "server error");

        assert!(message.contains("HTTP 502"));
        assert!(message.contains("cf-ray=abc123"));
        assert!(!message.contains('@'));
        assert!(!message.contains('`'));
        assert!(!message.contains("secret"));
    }

    #[tokio::test]
    async fn terminal_provider_failure_writes_state_without_screenshot() {
        let details = r#"{"attributes":{"uuid":"34634dd7-e564-480e-a3a7-84baf53c9328","is_suspended":true,"is_limbo":false,"status":"suspended"}}"#;
        let (panel_origin, server) = mock_api(vec![(
            "GET /api/client/servers/34634dd7 HTTP/1.1",
            StatusCode::OK,
            details,
        )])
        .await;
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-provider-failure-state-{}",
            rand::random::<u64>()
        ));
        let provider = test_provider(
            test_config(panel_origin, true),
            artifact_dir.clone(),
            ArtifactCapture::Failure,
        );

        let failure = provider
            .start_with_terminal_artifacts("run-id", None)
            .await
            .unwrap_err();

        assert_eq!(failure.error_class, "ProviderSuspended");
        assert_eq!(failure.screenshot_path, None);
        let detail = std::fs::read_to_string(
            failure
                .detail_artifact_path
                .expect("provider failure state artifact"),
        )
        .unwrap();
        assert!(detail.contains(r#""workflow_stage": "provider_state""#));
        assert!(detail.contains(r#""is_suspended": true"#));
        server.await.unwrap();
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn pterodactyl_capture_modes_write_only_state_json() {
        let artifact = StateArtifact {
            provider: "pterodactyl",
            workflow_stage: "provider_state",
            current_state: Some("running"),
            is_suspended: Some(false),
            is_limbo: Some(false),
            phase: None,
            connection: None,
            position: None,
            total: None,
            estimated_minutes: None,
            blocked: None,
            enabled: None,
            wake_requested: false,
            last_error: None,
            failed_stage: None,
            transport_category: None,
            attempt_count: None,
            timeout_ms: None,
            elapsed_ms: None,
            power_request_attempted: None,
            power_submission_state: None,
            power_confirmation_source: None,
        };

        for (capture, expected_state) in [
            (ArtifactCapture::Screenshots, true),
            (ArtifactCapture::Full, true),
            (ArtifactCapture::Failure, true),
            (ArtifactCapture::Off, false),
        ] {
            let artifact_dir = std::env::temp_dir().join(format!(
                "butler-provider-capture-mode-{}",
                rand::random::<u64>()
            ));
            let provider = test_provider(
                test_config(Url::parse("http://127.0.0.1:1/").unwrap(), true),
                artifact_dir.clone(),
                capture,
            );
            let run_dir = artifact_dir.join("run-id");

            let detail = provider
                .write_state_artifact_value("run-id", &artifact)
                .await;

            assert_eq!(detail.is_some(), expected_state);
            assert_eq!(run_dir.join(STATE_ARTIFACT).is_file(), expected_state);
            assert!(!run_dir.join("provider_snapshot.png").exists());
            if artifact_dir.exists() {
                std::fs::remove_dir_all(artifact_dir).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn service_verification_replaces_attempted_state_without_screenshot() {
        for capture in [ArtifactCapture::Screenshots, ArtifactCapture::Failure] {
            let artifact_dir = std::env::temp_dir().join(format!(
                "butler-provider-service-verification-{}",
                rand::random::<u64>()
            ));
            let provider = test_provider(
                test_config(Url::parse("http://127.0.0.1:1/").unwrap(), true),
                artifact_dir.clone(),
                capture,
            );
            let attempted = provider
                .write_terminal_failure_artifact(
                    "run-id",
                    "ProviderPowerTimeout",
                    ProviderMutation::PowerStart,
                )
                .await;
            assert!(attempted.is_some());

            let confirmed = provider
                .write_verified_submission_artifact(
                    "run-id",
                    SubmissionVerificationSource::Minecraft,
                    "online",
                )
                .await;
            assert_eq!(confirmed, None);
            assert!(
                !artifact_dir
                    .join("run-id")
                    .join("provider_snapshot.png")
                    .exists()
            );
            let detail =
                std::fs::read_to_string(artifact_dir.join("run-id").join(STATE_ARTIFACT)).unwrap();
            assert!(detail.contains(r#""workflow_stage": "submission_verified""#));
            assert!(detail.contains(r#""current_state": "online""#));
            assert!(detail.contains(r#""power_submission_state": "confirmed""#));
            assert!(detail.contains(r#""power_confirmation_source": "minecraft""#));
            std::fs::remove_dir_all(artifact_dir).unwrap();
        }
    }

    #[test]
    fn validates_provider_server_references() {
        assert!(valid_server_reference("34634dd7"));
        assert!(valid_server_reference(
            "34634dd7-e564-480e-a3a7-84baf53c9328"
        ));
        assert!(!valid_server_reference("../other-server"));
        assert!(!valid_server_reference("server name"));
        assert!(!valid_server_reference(""));
    }
}
