use super::{
    background_verification,
    responses::{
        StartMessage, edit_start_message, format_failure_detail, format_provider_detail,
        send_start_message, start_final_content, start_progress_content, with_notice,
    },
    tracking::{RunTracker, run_context},
    types::StartOptions,
};
use crate::{
    framework::Context,
    minecraft::{self, ServerStatus},
    provider::{
        ProviderMutation, ProviderProgress, ProviderProgressStage, ProviderStartFailure,
        ProviderStartResult, StartOutcome, SubmissionVerificationSource,
    },
    run_history::now_ms,
    state::{ActiveStartLease, ActiveStartOperationGuard, ActiveStartRun, StartAdmissionError},
    terminal,
};
use anyhow::Result;
use std::{path::PathBuf, time::Duration};
use tokio::{
    sync::mpsc,
    time::{Instant, sleep, timeout},
};

const POST_CLICK_VERIFY_SECS: u64 = 60;
const PROVIDER_VERIFY_SLICE_SECS: u64 = 20;
const MINECRAFT_STATUS_POLL_SECS: u64 = 5;
const UNCONFIRMED_PROVIDER_STATUS: &str = "Unconfirmed (provider confirmation lost)";

struct SubmissionArtifactRequest {
    source: SubmissionVerificationSource,
    observed_state: String,
}

struct PostLeaseStartEdit {
    content: String,
    screenshot_path: Option<PathBuf>,
    submission_artifact: Option<SubmissionArtifactRequest>,
}

pub async fn start_server(ctx: Context<'_>, options: StartOptions) -> Result<()> {
    start_server_with_notice(ctx, options, None).await
}

pub async fn start_server_with_notice(
    ctx: Context<'_>,
    options: StartOptions,
    notice: Option<&str>,
) -> Result<()> {
    ctx.defer().await?;

    let data = ctx.data();
    let context = run_context(ctx, "server.start");

    if let Some(active_run) = data.active_start_run().await {
        terminal::emit(terminal::line_for_context(
            "BUSY",
            &context,
            format!("{} already running", active_run.run_id),
        ));
        ctx.say(with_notice(
            active_start_busy_message(ctx, &active_run, false),
            notice,
        ))
        .await?;
        return Ok(());
    }

    let active_lease = match data.try_begin_start_run(&context).await {
        Ok(lease) => lease,
        Err(StartAdmissionError::Busy(active_run)) => {
            terminal::emit(terminal::line_for_context(
                "BUSY",
                &context,
                format!("{} already running", active_run.run_id),
            ));
            ctx.say(with_notice(
                active_start_busy_message(ctx, &active_run, true),
                notice,
            ))
            .await?;
            return Ok(());
        }
        Err(StartAdmissionError::ShuttingDown) => {
            ctx.say(with_notice(
                "Butler is restarting; try again shortly.".to_string(),
                notice,
            ))
            .await?;
            return Ok(());
        }
        Err(StartAdmissionError::Unavailable) => {
            ctx.say(with_notice(
                "A start operation cannot be admitted right now; try again shortly.".to_string(),
                notice,
            ))
            .await?;
            return Ok(());
        }
    };

    let mut tracker = RunTracker::new(ctx, context.clone());
    let mut start_details = vec![context.run_id.clone()];
    if options.wait_online {
        start_details.push(format!(
            "wait {}",
            terminal::format_duration((data.config.start_wait_online_secs as u128) * 1000)
        ));
    }
    if options.force {
        start_details.push("force".to_string());
    }
    terminal::emit(terminal::line_for_context(
        "START",
        &context,
        start_details.join(" "),
    ));

    if let Err(error) = ctx
        .send(poise::CreateReply::default().content(with_notice(
            format!(
                "Preparing the start operation. Progress will be posted in a bot message.\nRun: `{}`",
                context.run_id
            ),
            notice,
        )))
        .await
    {
        active_lease.finish().await;
        return Err(error.into());
    }
    let progress_message =
        match send_start_message(ctx, start_progress_content(&context.run_id, notice)).await {
            Ok(message) => message,
            Err(error) => {
                active_lease.finish().await;
                return Err(error);
            }
        };

    let result = start_server_inner(
        ctx,
        options,
        &mut tracker,
        &progress_message,
        notice,
        &active_lease,
    )
    .await;

    let (edit_result, post_lease_edit) = match result {
        Ok(post_lease_edit) => (None, post_lease_edit),
        Err(error) => {
            tracker
                .finish("Failed", None, None, None, Some("CommandError".to_string()))
                .await;
            terminal::emit(terminal::line_for_context(
                "FAIL",
                &context,
                format!(
                    "{} error {}",
                    context.run_id,
                    terminal::clean(&error.to_string())
                ),
            ));
            (
                Some(
                    edit_start_message(
                        ctx,
                        &progress_message,
                        notice,
                        format!(
                            "Start failed.\nRun: `{}`\nError: `{}`",
                            context.run_id,
                            terminal::clean(&error.to_string())
                        ),
                        None,
                    )
                    .await,
                ),
                None,
            )
        }
    };
    let release_task = tokio::spawn(async move {
        active_lease.finish().await;
    });
    release_task
        .await
        .map_err(|error| anyhow::anyhow!("start lease release task failed: {error}"))?;

    if let Some(mut post_lease_edit) = post_lease_edit {
        if let Some(request) = post_lease_edit.submission_artifact
            && let Some(verified_screenshot) = data
                .provider
                .submission_verified_artifact(
                    &context.run_id,
                    request.source,
                    &request.observed_state,
                )
                .await
        {
            post_lease_edit.screenshot_path = Some(verified_screenshot);
        }
        if let Err(error) = edit_start_message(
            ctx,
            &progress_message,
            notice,
            post_lease_edit.content,
            post_lease_edit.screenshot_path,
        )
        .await
        {
            terminal::emit(terminal::line_for_context(
                "WARN",
                &context,
                format!(
                    "{} start was accepted, but the final Discord message could not be updated; error {}",
                    context.run_id,
                    terminal::clean(&error.to_string())
                ),
            ));
        }
    }

    if let Err(error) = data.run_store.prune_artifacts().await {
        terminal::emit(terminal::line_for_context(
            "WARN",
            &context,
            format!(
                "{} artifact retention failed; error {}",
                context.run_id,
                terminal::clean(&error.to_string())
            ),
        ));
    }

    if let Some(edit_result) = edit_result {
        edit_result?;
    }

    Ok(())
}

fn active_start_busy_message(
    ctx: Context<'_>,
    active_run: &ActiveStartRun,
    just_started: bool,
) -> String {
    active_start_busy_message_for(
        &active_run.run_id,
        caller_can_see_active_run_id(ctx, active_run),
        just_started,
    )
}

fn active_start_busy_message_for(run_id: &str, can_see_run_id: bool, just_started: bool) -> String {
    if can_see_run_id && just_started {
        format!(
            "A start operation began just now as `{run_id}`. Use `/bot run run_id:{run_id}` to inspect it after it finishes."
        )
    } else if can_see_run_id {
        format!(
            "A start operation is already running as `{run_id}`. Use `/bot run run_id:{run_id}` to inspect the last recorded details."
        )
    } else if just_started {
        "A start operation began just now. Try again after it finishes.".to_string()
    } else {
        "A start operation is already running. Try again after it finishes.".to_string()
    }
}

fn caller_can_see_active_run_id(ctx: Context<'_>, active_run: &ActiveStartRun) -> bool {
    let user_id = ctx.author().id.to_string();
    let caller_guild_id = ctx.guild_id().map(|guild_id| guild_id.to_string());
    caller_can_see_active_run_id_for(
        ctx.data().config.owner_user_ids.contains(&user_id),
        caller_guild_id.as_deref(),
        active_run.guild_id.as_deref(),
    )
}

fn caller_can_see_active_run_id_for(
    caller_is_owner: bool,
    caller_guild_id: Option<&str>,
    active_guild_id: Option<&str>,
) -> bool {
    caller_is_owner || (active_guild_id.is_some() && caller_guild_id == active_guild_id)
}

async fn start_server_inner(
    ctx: Context<'_>,
    options: StartOptions,
    tracker: &mut RunTracker<'_>,
    progress_message: &StartMessage,
    notice: Option<&str>,
    active_lease: &ActiveStartLease,
) -> Result<Option<PostLeaseStartEdit>> {
    let mut preflight_status = None;
    if !options.force {
        let minecraft_address = ctx.data().minecraft_address().await;
        match minecraft::get_status_for_addr(&minecraft_address).await {
            Ok(status) => {
                let status_text = status.to_string();
                tracker
                    .step(
                        "minecraft_preflight",
                        "ok",
                        Some(status_text.clone()),
                        None,
                        None,
                    )
                    .await;
                preflight_status = Some(status_text.clone());
                if !status.is_offline_like() {
                    edit_start_message(
                        ctx,
                        progress_message,
                        notice,
                        format!(
                            "Server is already **{}**.\nRun: `{}`",
                            status, tracker.context.run_id
                        ),
                        None,
                    )
                    .await?;
                    tracker
                        .finish("AlreadyOnline", None, Some(status_text), None, None)
                        .await;
                    return Ok(None);
                }
            }
            Err(error) => {
                terminal::emit(terminal::line_for_context(
                    "WARN",
                    &tracker.context,
                    format!(
                        "{} preflight failed; continuing; error {}",
                        tracker.context.run_id,
                        terminal::clean(&error.to_string())
                    ),
                ));
                tracker
                    .step(
                        "minecraft_preflight",
                        "warning",
                        Some(format!("Could not check status ({error}); proceeding")),
                        None,
                        None,
                    )
                    .await;
            }
        }
    } else {
        tracker
            .step(
                "minecraft_preflight",
                "skipped",
                Some("force=true".to_string()),
                None,
                None,
            )
            .await;
    }

    let provider = ctx.data().provider.clone();
    let state = ctx.data().clone();
    let run_id = tracker.context.run_id.clone();
    let operation_guard = active_lease.operation_guard();
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let mut provider_task = tokio::spawn(async move {
        let result = provider.start_with_progress(&run_id, progress_tx).await;
        let provider_address = match &result {
            Ok(result) => result.minecraft_address.as_ref(),
            Err(failure) => failure.minecraft_address.as_ref(),
        };
        if let Some(address) = provider_address {
            state.set_minecraft_address(address.to_string()).await;
        }
        let safety_guard = retain_operation_guard_for_uncertain_result(
            &result,
            operation_guard,
            Duration::from_secs(POST_CLICK_VERIFY_SECS),
        );
        (result, safety_guard)
    });
    let (provider_result, provider_safety_guard) = loop {
        tokio::select! {
            result = &mut provider_task => {
                break result
                    .map_err(|error| anyhow::anyhow!("provider operation task failed: {error}"))?;
            }
            Some(progress) = progress_rx.recv() => {
                handle_provider_progress(
                    ctx,
                    tracker,
                    progress,
                    progress_message,
                    notice,
                )
                .await;
            }
        }
    };

    let handler_result = match provider_result {
        Ok(provider_result) => {
            handle_provider_success(
                ctx,
                tracker,
                provider_result,
                preflight_status,
                options,
                progress_message,
                notice,
            )
            .await
        }
        Err(failure) => {
            handle_provider_failure(
                ctx,
                tracker,
                failure,
                preflight_status,
                options,
                (progress_message, notice),
                active_lease,
            )
            .await
        }
    };
    if let Some(task) = provider_safety_guard {
        task.abort();
        let _ = task.await;
    }

    handler_result
}

async fn handle_provider_progress(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    progress: ProviderProgress,
    progress_message: &StartMessage,
    notice: Option<&str>,
) {
    let stage = progress.stage.to_string();
    tracker
        .step(
            "provider_progress",
            &stage,
            Some(progress.detail.clone()),
            None,
            None,
        )
        .await;
    terminal::emit(terminal::line_for_context(
        "WAIT",
        &tracker.context,
        format!(
            "{} {} {}",
            tracker.context.run_id,
            stage,
            terminal::clean(&progress.detail)
        ),
    ));
    if let Err(error) = edit_start_message(
        ctx,
        progress_message,
        notice,
        provider_progress_content(&tracker.context.run_id, &progress),
        None,
    )
    .await
    {
        terminal::emit(terminal::line_for_context(
            "WARN",
            &tracker.context,
            format!(
                "{} could not update provider progress; error {}",
                tracker.context.run_id,
                terminal::clean(&error.to_string())
            ),
        ));
    }
}

fn provider_progress_content(run_id: &str, progress: &ProviderProgress) -> String {
    let message = match progress.stage {
        ProviderProgressStage::SolvingChallenge => "Connecting to Play Hosting...".to_string(),
        ProviderProgressStage::RequestingAllocation => {
            "Play Hosting is asleep. Requesting a host for the server...".to_string()
        }
        ProviderProgressStage::WaitingForAllocation => {
            discord_safe_provider_detail(&progress.detail)
        }
        ProviderProgressStage::RequestingPower => {
            "A host is ready. Sending the Minecraft start command...".to_string()
        }
        ProviderProgressStage::VerifyingPower => {
            "The start command may have gone through. Checking Play Hosting and Minecraft without sending it again...".to_string()
        }
    };
    format!("{message}\nRun: `{run_id}`")
}

fn discord_safe_provider_detail(detail: &str) -> String {
    terminal::clean(detail)
        .replace('@', "@\u{200b}")
        .replace('`', "'")
        .chars()
        .take(500)
        .collect()
}

async fn handle_provider_success(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    provider_result: ProviderStartResult,
    preflight_status: Option<String>,
    options: StartOptions,
    progress_message: &StartMessage,
    notice: Option<&str>,
) -> Result<Option<PostLeaseStartEdit>> {
    let screenshot_path = provider_result.screenshot_path.clone();
    let initial_outcome = match provider_result.outcome {
        StartOutcome::StartClicked => "StartClicked".to_string(),
        StartOutcome::DashboardChanged => "DashboardChanged".to_string(),
        StartOutcome::StartRequested => "StartRequested".to_string(),
        StartOutcome::AlreadyActive => "AlreadyActive".to_string(),
    };
    let background_job = background_verification::should_schedule(
        ctx.data().provider.name(),
        options.wait_online,
        &initial_outcome,
    )
    .then(|| {
        let minecraft_address = provider_result
            .minecraft_address
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| ctx.data().config.minecraft_server_addr.clone());
        background_verification::Job::new(
            tracker.context.clone(),
            minecraft_address,
            provider_result.provider_status.clone(),
            Duration::from_secs(ctx.data().config.start_wait_online_secs),
            tracker.started_at_ms(),
        )
        .reserve(ctx.data().clone())
    });
    tracker
        .step(
            "provider_start",
            &provider_result.outcome.to_string(),
            Some(format_provider_detail(
                &provider_result.provider_status,
                provider_result.detail_artifact_path.as_ref(),
            )),
            screenshot_path.clone(),
            None,
        )
        .await;

    let mut final_minecraft_status = preflight_status;
    let mut outcome = initial_outcome;
    let mut final_error_class = None;

    if options.wait_online {
        edit_start_message(
            ctx,
            progress_message,
            notice,
            format!(
                "Start accepted. Waiting up to {} for Minecraft to report online.\nRun: `{}`",
                terminal::format_duration(
                    (ctx.data().config.start_wait_online_secs as u128) * 1000
                ),
                tracker.context.run_id
            ),
            screenshot_path.clone(),
        )
        .await?;
        let wait_result = wait_for_minecraft_online(ctx, tracker).await;
        final_minecraft_status = wait_result
            .final_minecraft_status
            .or(final_minecraft_status);
        outcome = wait_result.outcome;
        final_error_class = wait_result.error_class;
    }

    if let Some(background_job) = background_job {
        tracker
            .step(
                "minecraft_background_verify",
                "scheduled",
                Some(format!(
                    "Will verify Minecraft for up to {} without delaying Discord",
                    terminal::format_duration(
                        (ctx.data().config.start_wait_online_secs as u128) * 1000
                    )
                )),
                None,
                None,
            )
            .await;
        let finish_task = tracker.finish_with_background(
            &outcome,
            Some(provider_result.provider_status.clone()),
            final_minecraft_status,
            screenshot_path.clone(),
            final_error_class,
            background_job,
        );
        finish_task
            .await
            .map_err(|error| anyhow::anyhow!("accepted start finalization task failed: {error}"))?;

        let content = start_final_content(
            &tracker.context.run_id,
            ctx.data().provider.name(),
            &provider_result.provider_status,
            &outcome,
        );
        return Ok(Some(PostLeaseStartEdit {
            content,
            screenshot_path,
            submission_artifact: None,
        }));
    }

    let content = start_final_content(
        &tracker.context.run_id,
        ctx.data().provider.name(),
        &provider_result.provider_status,
        &outcome,
    );
    edit_start_message(
        ctx,
        progress_message,
        notice,
        content,
        screenshot_path.clone(),
    )
    .await?;
    tracker
        .finish(
            &outcome,
            Some(provider_result.provider_status.clone()),
            final_minecraft_status,
            screenshot_path,
            final_error_class,
        )
        .await;
    Ok(None)
}

struct MinecraftWaitResult {
    outcome: String,
    final_minecraft_status: Option<String>,
    error_class: Option<String>,
}

async fn wait_for_minecraft_online(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
) -> MinecraftWaitResult {
    let mut final_minecraft_status = None;
    let mut outcome = "WaitOnlineTimeout".to_string();
    let mut error_class = Some("WaitOnlineTimeout".to_string());
    let wait_started_at_ms = now_ms();
    let deadline = wait_started_at_ms
        .saturating_add((ctx.data().config.start_wait_online_secs as u128) * 1000);

    loop {
        let minecraft_address = ctx.data().minecraft_address().await;
        match minecraft::get_status_for_addr(&minecraft_address).await {
            Ok(status @ ServerStatus::Online { .. }) => {
                final_minecraft_status = Some(status.to_string());
                outcome = "MinecraftOnline".to_string();
                error_class = None;
                tracker
                    .step(
                        "minecraft_wait_online",
                        "online",
                        Some(status.to_string()),
                        None,
                        None,
                    )
                    .await;
                break;
            }
            Ok(status) => {
                final_minecraft_status = Some(status.to_string());
                tracker
                    .step(
                        "minecraft_wait_online",
                        "waiting",
                        Some(status.to_string()),
                        None,
                        None,
                    )
                    .await;
            }
            Err(error) => {
                tracker
                    .step(
                        "minecraft_wait_online",
                        "warning",
                        Some(error.to_string()),
                        None,
                        Some("MinecraftStatusError".to_string()),
                    )
                    .await;
            }
        }

        if now_ms() >= deadline {
            tracker
                .step(
                    "minecraft_wait_online",
                    "timeout",
                    Some("Timed out waiting for Minecraft to report online".to_string()),
                    None,
                    Some("WaitOnlineTimeout".to_string()),
                )
                .await;
            break;
        }
        sleep(Duration::from_secs(MINECRAFT_STATUS_POLL_SECS)).await;
    }

    MinecraftWaitResult {
        outcome,
        final_minecraft_status,
        error_class,
    }
}

struct SubmissionVerificationResult {
    confirmation: Option<SubmissionConfirmation>,
    final_minecraft_status: Option<String>,
}

enum SubmissionConfirmation {
    Minecraft(ServerStatus),
    Provider(String),
}

async fn verify_submitted_start(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    mutation: ProviderMutation,
) -> SubmissionVerificationResult {
    let mut final_minecraft_status = None;
    let deadline = Instant::now() + Duration::from_secs(POST_CLICK_VERIFY_SECS);

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            tracker
                .step(
                    "minecraft_post_click_verify",
                    "timeout",
                    Some("Timed out verifying whether the start was submitted".to_string()),
                    None,
                    Some("StartSubmissionUnverified".to_string()),
                )
                .await;
            return SubmissionVerificationResult {
                confirmation: None,
                final_minecraft_status,
            };
        }

        if mutation == ProviderMutation::PowerStart {
            match timeout(
                std::cmp::min(remaining, Duration::from_secs(PROVIDER_VERIFY_SLICE_SECS)),
                ctx.data().provider.check(&tracker.context.run_id),
            )
            .await
            {
                Ok(Ok(result)) => {
                    let confirmed =
                        provider_status_confirms_start_submission(&result.provider_status);
                    tracker
                        .step(
                            "provider_post_submit_verify",
                            if confirmed { "accepted" } else { "waiting" },
                            Some(result.provider_status.clone()),
                            None,
                            None,
                        )
                        .await;
                    if confirmed {
                        return SubmissionVerificationResult {
                            confirmation: Some(SubmissionConfirmation::Provider(
                                result.provider_status,
                            )),
                            final_minecraft_status,
                        };
                    }
                }
                Ok(Err(error)) => {
                    tracker
                        .step(
                            "provider_post_submit_verify",
                            "warning",
                            Some(format_failure_detail(&error)),
                            None,
                            Some(error.error_class),
                        )
                        .await;
                }
                Err(_) => {
                    tracker
                        .step(
                            "provider_post_submit_verify",
                            "warning",
                            Some("Provider verification probe timed out".to_string()),
                            None,
                            Some("ProviderVerificationTimeout".to_string()),
                        )
                        .await;
                }
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            continue;
        }
        let minecraft_address = ctx.data().minecraft_address().await;
        match timeout(
            remaining,
            minecraft::get_status_for_addr(&minecraft_address),
        )
        .await
        {
            Ok(Ok(status)) => {
                let status_text = status.to_string();
                let confirmed = status_confirms_start_submission(&status);
                final_minecraft_status = Some(status_text.clone());
                tracker
                    .step(
                        "minecraft_post_click_verify",
                        if confirmed { "accepted" } else { "waiting" },
                        Some(status_text),
                        None,
                        None,
                    )
                    .await;
                if confirmed {
                    return SubmissionVerificationResult {
                        confirmation: Some(SubmissionConfirmation::Minecraft(status)),
                        final_minecraft_status,
                    };
                }
            }
            Ok(Err(error)) => {
                tracker
                    .step(
                        "minecraft_post_click_verify",
                        "warning",
                        Some(error.to_string()),
                        None,
                        Some("MinecraftStatusError".to_string()),
                    )
                    .await;
            }
            Err(_) => continue,
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        sleep(std::cmp::min(
            Duration::from_secs(MINECRAFT_STATUS_POLL_SECS),
            remaining,
        ))
        .await;
    }
}

async fn handle_provider_failure(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    failure: ProviderStartFailure,
    preflight_status: Option<String>,
    options: StartOptions,
    response_target: (&StartMessage, Option<&str>),
    active_lease: &ActiveStartLease,
) -> Result<Option<PostLeaseStartEdit>> {
    let (progress_message, notice) = response_target;
    if failure.uncertain_mutation.may_have_started_server() {
        return handle_unconfirmed_submitted_start(
            ctx,
            tracker,
            failure,
            preflight_status,
            options,
            response_target,
            active_lease,
        )
        .await;
    }

    let screenshot_path = failure.screenshot_path.clone();
    tracker
        .step(
            "provider_start",
            "failed",
            Some(format_failure_detail(&failure)),
            screenshot_path.clone(),
            Some(failure.error_class.clone()),
        )
        .await;

    let content = provider_failure_public_content(&failure.error_class, &tracker.context.run_id);
    edit_start_message(
        ctx,
        progress_message,
        notice,
        content,
        screenshot_path.clone(),
    )
    .await?;
    tracker
        .finish(
            "Failed",
            None,
            preflight_status,
            screenshot_path,
            Some(failure.error_class),
        )
        .await;
    Ok(None)
}

async fn handle_unconfirmed_submitted_start(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    failure: ProviderStartFailure,
    preflight_status: Option<String>,
    options: StartOptions,
    response_target: (&StartMessage, Option<&str>),
    active_lease: &ActiveStartLease,
) -> Result<Option<PostLeaseStartEdit>> {
    let (progress_message, notice) = response_target;
    let submitted_minecraft_address = failure
        .minecraft_address
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| ctx.data().config.minecraft_server_addr.clone());
    let pre_verification_guard = spawn_submission_verification_guard(active_lease);
    let mut screenshot_path = failure.screenshot_path.clone();
    tracker
        .step(
            "provider_start",
            "StartSubmittedUnconfirmed",
            Some(format_failure_detail(&failure)),
            screenshot_path.clone(),
            Some(failure.error_class.clone()),
        )
        .await;

    let checking_edit = timeout(
        Duration::from_secs(PROVIDER_VERIFY_SLICE_SECS),
        edit_start_message(
            ctx,
            progress_message,
            notice,
            start_submission_checking_content(&tracker.context.run_id, failure.uncertain_mutation),
            screenshot_path.clone(),
        ),
    )
    .await;
    if !matches!(checking_edit, Ok(Ok(()))) {
        let detail = match checking_edit {
            Ok(Err(error)) => format!("Could not update verification message ({error})"),
            Err(_) => "Timed out updating verification message".to_string(),
            Ok(Ok(())) => unreachable!(),
        };
        terminal::emit(terminal::line_for_context(
            "WARN",
            &tracker.context,
            format!(
                "{} {}; continuing verification",
                tracker.context.run_id,
                terminal::clean(&detail)
            ),
        ));
        tracker
            .step(
                "submission_verification_message",
                "warning",
                Some(detail),
                None,
                None,
            )
            .await;
    }

    let verification_guard = spawn_submission_verification_guard(active_lease);
    pre_verification_guard.abort();
    let _ = pre_verification_guard.await;
    let verification = verify_submitted_start(ctx, tracker, failure.uncertain_mutation).await;
    verification_guard.abort();
    let _ = verification_guard.await;
    if let Some(confirmation) = verification.confirmation {
        let (confirmed_provider_status, confirmed_minecraft_status) = match &confirmation {
            SubmissionConfirmation::Minecraft(status) => (
                UNCONFIRMED_PROVIDER_STATUS.to_string(),
                Some(status.to_string()),
            ),
            SubmissionConfirmation::Provider(status) => (status.clone(), None),
        };
        let mut outcome = match &confirmation {
            SubmissionConfirmation::Provider(_) => "StartSubmittedConfirmed".to_string(),
            SubmissionConfirmation::Minecraft(_) => "StartSubmittedUnconfirmed".to_string(),
        };
        let background_job = background_verification::should_schedule(
            ctx.data().provider.name(),
            options.wait_online,
            &outcome,
        )
        .then(|| {
            background_verification::Job::new(
                tracker.context.clone(),
                submitted_minecraft_address,
                confirmed_provider_status.clone(),
                Duration::from_secs(ctx.data().config.start_wait_online_secs),
                tracker.started_at_ms(),
            )
            .reserve(ctx.data().clone())
        });
        let mut final_minecraft_status =
            confirmed_minecraft_status.or(verification.final_minecraft_status);
        let mut final_error_class = None;

        if let Some(background_job) = background_job {
            tracker
                .step(
                    "minecraft_background_verify",
                    "scheduled",
                    Some(format!(
                        "Will verify Minecraft for up to {} without delaying Discord",
                        terminal::format_duration(
                            (ctx.data().config.start_wait_online_secs as u128) * 1000
                        )
                    )),
                    None,
                    None,
                )
                .await;
            let finish_task = tracker.finish_with_background(
                &outcome,
                Some(confirmed_provider_status.clone()),
                final_minecraft_status,
                screenshot_path.clone(),
                None,
                background_job,
            );
            finish_task.await.map_err(|error| {
                anyhow::anyhow!("accepted start finalization task failed: {error}")
            })?;
            let source = match &confirmation {
                SubmissionConfirmation::Provider(_) => SubmissionVerificationSource::Provider,
                SubmissionConfirmation::Minecraft(_) => SubmissionVerificationSource::Minecraft,
            };
            let observed_state = match &confirmation {
                SubmissionConfirmation::Provider(status) => status.clone(),
                SubmissionConfirmation::Minecraft(_) => "online".to_string(),
            };
            return Ok(Some(PostLeaseStartEdit {
                content: start_submitted_unconfirmed_content(
                    &tracker.context.run_id,
                    ctx.data().provider.name(),
                    &confirmation,
                ),
                screenshot_path,
                submission_artifact: Some(SubmissionArtifactRequest {
                    source,
                    observed_state,
                }),
            }));
        }

        let verification_source = match &confirmation {
            SubmissionConfirmation::Provider(_) => SubmissionVerificationSource::Provider,
            SubmissionConfirmation::Minecraft(_) => SubmissionVerificationSource::Minecraft,
        };
        let observed_state = match &confirmation {
            SubmissionConfirmation::Provider(status) => status.as_str(),
            SubmissionConfirmation::Minecraft(_) => "online",
        };
        if let Some(verified_screenshot) = ctx
            .data()
            .provider
            .submission_verified_artifact(
                &tracker.context.run_id,
                verification_source,
                observed_state,
            )
            .await
        {
            screenshot_path = Some(verified_screenshot);
        }

        if options.wait_online {
            edit_start_message(
                ctx,
                progress_message,
                notice,
                format!(
                    "Start appears submitted. Waiting up to {} for Minecraft to report online.\nRun: `{}`",
                    terminal::format_duration(
                        (ctx.data().config.start_wait_online_secs as u128) * 1000
                    ),
                    tracker.context.run_id
                ),
                screenshot_path.clone(),
            )
            .await?;
            let wait_result = wait_for_minecraft_online(ctx, tracker).await;
            final_minecraft_status = wait_result
                .final_minecraft_status
                .or(final_minecraft_status);
            outcome = wait_result.outcome;
            final_error_class = wait_result.error_class;
            edit_start_message(
                ctx,
                progress_message,
                notice,
                start_submitted_unconfirmed_wait_final_content(
                    &tracker.context.run_id,
                    ctx.data().provider.name(),
                    &confirmed_provider_status,
                    &outcome,
                ),
                screenshot_path.clone(),
            )
            .await?;
        } else {
            edit_start_message(
                ctx,
                progress_message,
                notice,
                start_submitted_unconfirmed_content(
                    &tracker.context.run_id,
                    ctx.data().provider.name(),
                    &confirmation,
                ),
                screenshot_path.clone(),
            )
            .await?;
        }

        tracker
            .finish(
                &outcome,
                Some(confirmed_provider_status.clone()),
                final_minecraft_status,
                screenshot_path,
                final_error_class,
            )
            .await;
        return Ok(None);
    }

    let final_minecraft_status = verification.final_minecraft_status.or(preflight_status);
    edit_start_message(
        ctx,
        progress_message,
        notice,
        start_submission_unverified_content(&tracker.context.run_id),
        screenshot_path.clone(),
    )
    .await?;
    tracker
        .finish(
            "StartSubmissionUnverified",
            Some(UNCONFIRMED_PROVIDER_STATUS.to_string()),
            final_minecraft_status,
            screenshot_path,
            Some("StartSubmissionUnverified".to_string()),
        )
        .await;
    Ok(None)
}

fn spawn_submission_verification_guard(
    active_lease: &ActiveStartLease,
) -> tokio::task::JoinHandle<()> {
    spawn_submission_verification_guard_for(
        active_lease,
        Duration::from_secs(POST_CLICK_VERIFY_SECS),
    )
}

fn spawn_submission_verification_guard_for(
    active_lease: &ActiveStartLease,
    duration: Duration,
) -> tokio::task::JoinHandle<()> {
    let guard = active_lease.operation_guard();
    tokio::spawn(async move {
        sleep(duration).await;
        drop(guard);
    })
}

fn retain_operation_guard_for_uncertain_result(
    result: &Result<ProviderStartResult, ProviderStartFailure>,
    guard: ActiveStartOperationGuard,
    duration: Duration,
) -> Option<tokio::task::JoinHandle<()>> {
    if result
        .as_ref()
        .is_err_and(|failure| failure.uncertain_mutation.may_have_started_server())
    {
        return Some(tokio::spawn(async move {
            sleep(duration).await;
            drop(guard);
        }));
    }
    drop(guard);
    None
}

fn provider_failure_public_content(error_class: &str, run_id: &str) -> String {
    if error_class == "ProviderAllocationTimeout" {
        return format!(
            "Play Hosting did not finish allocating a host before Butler's maximum wait. No server power command was sent. Retrying `/server start` is safe; Butler will inspect the current allocation before acting.\nRun: `{run_id}`"
        );
    }
    if error_class == "ProviderControlsTimeout" {
        return format!(
            "Play Hosting allocated a host, but its server controls timed out during repeated read-only checks. No server power command was sent. Retrying `/server start` is safe.\nRun: `{run_id}`"
        );
    }
    if error_class == "ProviderControlsUnavailable" {
        return format!(
            "Play Hosting allocated a host, but its server controls remained temporarily unavailable during repeated read-only checks. No server power command was sent. Retrying `/server start` is safe.\nRun: `{run_id}`"
        );
    }
    format!("Start failed: **{error_class}**\nRun: `{run_id}`")
}

fn status_confirms_start_submission(status: &ServerStatus) -> bool {
    !status.is_offline_like()
}

fn provider_status_confirms_start_submission(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "starting" | "running" | "online" | "preparing" | "loading"
    )
}

fn start_submission_checking_content(run_id: &str, mutation: ProviderMutation) -> String {
    let checks = match mutation {
        ProviderMutation::PowerStart => "Play Hosting and Minecraft",
        ProviderMutation::BrowserStart => "Minecraft status",
        ProviderMutation::None | ProviderMutation::WakeAllocation => "server status",
    };
    format!(
        "The start command may have gone through. Checking {checks} without sending it again...\nRun: `{run_id}`"
    )
}

fn start_submitted_unconfirmed_content(
    run_id: &str,
    provider_name: &str,
    confirmation: &SubmissionConfirmation,
) -> String {
    match confirmation {
        SubmissionConfirmation::Minecraft(status) if provider_name == "pterodactyl" => {
            format!(
                "Start confirmed by Minecraft after Play Hosting's response was lost.\nMinecraft: **{status}**\nRun: `{run_id}`"
            )
        }
        SubmissionConfirmation::Minecraft(status) => format!(
            "Start confirmed by Minecraft after the hosting provider's response was lost.\nMinecraft: **{status}**\nRun: `{run_id}`"
        ),
        SubmissionConfirmation::Provider(status) => {
            let description = super::responses::pterodactyl_status_description(status);
            format!(
                "Start confirmed after Play Hosting's response was lost.\nPlay Hosting state: **{description}**\nRun: `{run_id}`"
            )
        }
    }
}

fn start_submitted_unconfirmed_wait_final_content(
    run_id: &str,
    provider_name: &str,
    provider_status: &str,
    outcome: &str,
) -> String {
    start_final_content(run_id, provider_name, provider_status, outcome)
}

fn start_submission_unverified_content(run_id: &str) -> String {
    format!(
        "The start command may have been sent, but Butler could not confirm it. Check `/server status` again in a minute.\nRun: `{run_id}`"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[test]
    fn active_run_id_is_visible_to_owner_or_same_guild() {
        assert!(caller_can_see_active_run_id_for(
            true,
            None,
            Some("guild-1")
        ));
        assert!(caller_can_see_active_run_id_for(
            false,
            Some("guild-1"),
            Some("guild-1")
        ));
        assert!(!caller_can_see_active_run_id_for(
            false,
            Some("guild-2"),
            Some("guild-1")
        ));
        assert!(!caller_can_see_active_run_id_for(false, None, None));
    }

    #[test]
    fn active_busy_message_hides_cross_guild_run_id() {
        let hidden = active_start_busy_message_for("abc123", false, false);
        assert!(!hidden.contains("abc123"));
        let visible = active_start_busy_message_for("abc123", true, false);
        assert!(visible.contains("abc123"));
    }

    #[test]
    fn provider_failure_public_content_hides_raw_message() {
        let content = provider_failure_public_content("StartNotAccepted", "abc123");
        assert!(content.contains("StartNotAccepted"));
        assert!(content.contains("abc123"));
        assert!(!content.contains("selector"));
        assert!(!content.contains("/tmp"));
        assert!(!content.contains("An owner or server Administrator"));
        assert!(!content.contains("/bot run"));
        assert_eq!(content, "Start failed: **StartNotAccepted**\nRun: `abc123`");
    }

    #[test]
    fn allocation_timeout_content_explains_safe_retry() {
        let content = provider_failure_public_content("ProviderAllocationTimeout", "abc123");
        assert!(content.contains("did not finish"));
        assert!(content.contains("Retrying `/server start` is safe"));
        assert!(content.contains("No server power command was sent"));
        assert!(content.contains("abc123"));
        assert!(!content.contains("Start may have been submitted"));
    }

    #[test]
    fn provider_controls_failure_content_explains_safe_retry_without_power() {
        for (error_class, expected) in [
            ("ProviderControlsTimeout", "timed out"),
            ("ProviderControlsUnavailable", "temporarily unavailable"),
        ] {
            let content = provider_failure_public_content(error_class, "abc123");
            assert!(content.contains(expected));
            assert!(content.contains("repeated read-only checks"));
            assert!(content.contains("No server power command was sent"));
            assert!(content.contains("Retrying `/server start` is safe"));
            assert!(content.contains("abc123"));
        }
    }

    #[test]
    fn provider_progress_content_reports_allocation_state() {
        let content = provider_progress_content(
            "abc123",
            &ProviderProgress {
                stage: ProviderProgressStage::WaitingForAllocation,
                detail: "A host is waking up for the server. Estimated wait: about 3 minutes."
                    .to_string(),
            },
        );
        assert!(content.contains("A host is waking up"));
        assert!(content.contains("about 3 minutes"));
        assert!(content.contains("abc123"));
    }

    #[test]
    fn provider_progress_content_suppresses_mentions_and_bounds_text() {
        let content = provider_progress_content(
            "abc123",
            &ProviderProgress {
                stage: ProviderProgressStage::WaitingForAllocation,
                detail: format!("@everyone `{}`", "x".repeat(1_000)),
            },
        );
        assert!(!content.contains("@everyone"));
        assert!(!content.contains('`') || content.contains("`abc123`"));
        assert!(content.chars().count() < 650);
    }

    #[test]
    fn start_submission_status_detection_accepts_transitional_and_online() {
        assert!(status_confirms_start_submission(&ServerStatus::Queued));
        assert!(status_confirms_start_submission(&ServerStatus::Starting));
        assert!(status_confirms_start_submission(&ServerStatus::Preparing));
        assert!(status_confirms_start_submission(&ServerStatus::Loading));
        assert!(status_confirms_start_submission(&ServerStatus::Online {
            online: 0,
            max: 20,
            players: Vec::new(),
        }));
        assert!(!status_confirms_start_submission(&ServerStatus::Offline));
        assert!(!status_confirms_start_submission(
            &ServerStatus::Unreachable {
                reason: "timeout".to_string(),
            }
        ));
    }

    #[test]
    fn provider_submission_status_detection_accepts_only_active_states() {
        assert!(provider_status_confirms_start_submission("Starting"));
        assert!(provider_status_confirms_start_submission("Running"));
        assert!(provider_status_confirms_start_submission("Online"));
        assert!(!provider_status_confirms_start_submission("Offline"));
        assert!(!provider_status_confirms_start_submission(
            "Allocation active"
        ));
    }

    #[test]
    fn unconfirmed_start_public_content_is_short_and_scoped() {
        let checking = start_submission_checking_content("abc123", ProviderMutation::PowerStart);
        assert_eq!(
            checking,
            "The start command may have gone through. Checking Play Hosting and Minecraft without sending it again...\nRun: `abc123`"
        );
        let browser_checking =
            start_submission_checking_content("abc123", ProviderMutation::BrowserStart);
        assert_eq!(
            browser_checking,
            "The start command may have gone through. Checking Minecraft status without sending it again...\nRun: `abc123`"
        );

        let accepted = start_submitted_unconfirmed_content(
            "abc123",
            "pterodactyl",
            &SubmissionConfirmation::Minecraft(ServerStatus::Starting),
        );
        assert!(accepted.contains("Start confirmed by Minecraft"));
        assert!(accepted.contains("Minecraft: **Starting**"));
        assert!(accepted.contains("Run: `abc123`"));

        let provider_confirmed = start_submitted_unconfirmed_content(
            "abc123",
            "pterodactyl",
            &SubmissionConfirmation::Provider("Starting".to_string()),
        );
        assert!(provider_confirmed.contains("confirmed"));
        assert!(provider_confirmed.contains(
            "Play Hosting state: **The server process is starting and is not ready to join yet.**"
        ));

        let aternos_confirmed = start_submitted_unconfirmed_content(
            "abc123",
            "aternos",
            &SubmissionConfirmation::Minecraft(ServerStatus::Starting),
        );
        assert!(aternos_confirmed.contains("hosting provider's response"));
        assert!(!aternos_confirmed.contains("Play Hosting"));

        let wait_done = start_submitted_unconfirmed_wait_final_content(
            "abc123",
            "pterodactyl",
            "Starting",
            "MinecraftOnline",
        );
        assert!(wait_done.contains("Minecraft is online."));
        assert!(wait_done.contains(
            "Confirmation: **Play Hosting was still reporting startup, but Minecraft is now online.**"
        ));
        assert!(wait_done.contains("Run: `abc123`"));

        let unverified = start_submission_unverified_content("abc123");
        assert_eq!(
            unverified,
            "The start command may have been sent, but Butler could not confirm it. Check `/server status` again in a minute.\nRun: `abc123`"
        );
    }

    #[tokio::test]
    async fn submission_guard_handoff_keeps_admission_after_command_cancellation() {
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-submission-guard-cancel-{}",
            rand::random::<u64>()
        ));
        let (lease, active_run) = ActiveStartLease::for_test(&artifact_dir, "run-1");

        let provider_guard =
            spawn_submission_verification_guard_for(&lease, Duration::from_millis(250));
        let message_guard =
            spawn_submission_verification_guard_for(&lease, Duration::from_millis(250));
        provider_guard.abort();
        let _ = provider_guard.await;
        let verification_guard =
            spawn_submission_verification_guard_for(&lease, Duration::from_millis(80));
        message_guard.abort();
        let _ = message_guard.await;

        drop(lease);
        tokio::task::yield_now().await;
        assert!(active_run.lock().await.is_some());
        sleep(Duration::from_millis(40)).await;
        assert!(active_run.lock().await.is_some());
        sleep(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        assert!(active_run.lock().await.is_none());
        assert!(verification_guard.is_finished());
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn uncertain_provider_result_retains_admission_if_command_is_cancelled() {
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-provider-emergency-guard-{}",
            rand::random::<u64>()
        ));
        let (lease, active_run) = ActiveStartLease::for_test(&artifact_dir, "run-3");
        let result = Err(ProviderStartFailure {
            error_class: "ProviderPowerTimeout".to_string(),
            message: "response timed out".to_string(),
            screenshot_path: None,
            detail_artifact_path: None,
            minecraft_address: None,
            uncertain_mutation: ProviderMutation::PowerStart,
            retryable: false,
        });
        let emergency_guard = retain_operation_guard_for_uncertain_result(
            &result,
            lease.operation_guard(),
            Duration::from_millis(80),
        )
        .expect("uncertain result should retain admission");

        drop(lease);
        tokio::task::yield_now().await;
        assert!(active_run.lock().await.is_some());
        sleep(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(active_run.lock().await.is_none());
        assert!(emergency_guard.is_finished());
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }

    #[tokio::test]
    async fn submission_guard_releases_immediately_after_normal_completion() {
        let artifact_dir = std::env::temp_dir().join(format!(
            "butler-submission-guard-complete-{}",
            rand::random::<u64>()
        ));
        let (lease, active_run): (ActiveStartLease, Arc<Mutex<Option<ActiveStartRun>>>) =
            ActiveStartLease::for_test(&artifact_dir, "run-2");
        let verification_guard =
            spawn_submission_verification_guard_for(&lease, Duration::from_secs(60));

        verification_guard.abort();
        let _ = verification_guard.await;
        drop(lease);
        tokio::task::yield_now().await;

        assert!(active_run.lock().await.is_none());
        std::fs::remove_dir_all(artifact_dir).unwrap();
    }
}
