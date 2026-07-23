use super::{
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
        ProviderProgress, ProviderProgressStage, ProviderStartFailure, ProviderStartResult,
        StartOutcome,
    },
    run_history::now_ms,
    state::{ActiveStartLease, ActiveStartRun, StartAdmissionError},
    terminal,
};
use anyhow::Result;
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};

const POST_CLICK_VERIFY_SECS: u64 = 60;
const MINECRAFT_STATUS_POLL_SECS: u64 = 5;
const UNCONFIRMED_PROVIDER_STATUS: &str = "Unconfirmed (provider confirmation lost)";

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

    let edit_result = if let Err(error) = result {
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
        )
    } else {
        None
    };
    active_lease.finish().await;

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
) -> Result<()> {
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
                    return Ok(());
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
        drop(operation_guard);
        result
    });
    let provider_result = loop {
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

    match provider_result {
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
            .await?;
        }
        Err(failure) => {
            handle_provider_failure(
                ctx,
                tracker,
                failure,
                preflight_status,
                options,
                progress_message,
                notice,
            )
            .await?;
        }
    }

    Ok(())
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
    let headline = match progress.stage {
        ProviderProgressStage::SolvingChallenge => "Preparing provider access.",
        ProviderProgressStage::RequestingAllocation => "Requesting host allocation.",
        ProviderProgressStage::WaitingForAllocation => "Waiting for host allocation.",
        ProviderProgressStage::RequestingPower => "Host allocated; requesting server power.",
    };
    format!(
        "{headline}\n{}\nRun: `{run_id}`",
        discord_safe_provider_detail(&progress.detail)
    )
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
) -> Result<()> {
    let screenshot_path = provider_result.screenshot_path.clone();
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
    let mut outcome = match provider_result.outcome {
        StartOutcome::StartClicked => "StartClicked".to_string(),
        StartOutcome::DashboardChanged => "DashboardChanged".to_string(),
        StartOutcome::StartRequested => "StartRequested".to_string(),
        StartOutcome::AlreadyActive => "AlreadyActive".to_string(),
    };
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
            Some(provider_result.provider_status),
            final_minecraft_status,
            screenshot_path,
            final_error_class,
        )
        .await;
    Ok(())
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
    confirmed_status: Option<ServerStatus>,
    final_minecraft_status: Option<String>,
}

async fn verify_submitted_start(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
) -> SubmissionVerificationResult {
    let mut final_minecraft_status = None;
    let deadline = now_ms().saturating_add((POST_CLICK_VERIFY_SECS as u128) * 1000);

    loop {
        let minecraft_address = ctx.data().minecraft_address().await;
        match minecraft::get_status_for_addr(&minecraft_address).await {
            Ok(status) => {
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
                        confirmed_status: Some(status),
                        final_minecraft_status,
                    };
                }
            }
            Err(error) => {
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
        }

        if now_ms() >= deadline {
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
                confirmed_status: None,
                final_minecraft_status,
            };
        }
        sleep(Duration::from_secs(MINECRAFT_STATUS_POLL_SECS)).await;
    }
}

async fn handle_provider_failure(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    failure: ProviderStartFailure,
    preflight_status: Option<String>,
    options: StartOptions,
    progress_message: &StartMessage,
    notice: Option<&str>,
) -> Result<()> {
    if failure.uncertain_mutation.may_have_started_server() {
        return handle_unconfirmed_submitted_start(
            ctx,
            tracker,
            failure,
            preflight_status,
            options,
            progress_message,
            notice,
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
    Ok(())
}

async fn handle_unconfirmed_submitted_start(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    failure: ProviderStartFailure,
    preflight_status: Option<String>,
    options: StartOptions,
    progress_message: &StartMessage,
    notice: Option<&str>,
) -> Result<()> {
    let screenshot_path = failure.screenshot_path.clone();
    tracker
        .step(
            "provider_start",
            "StartSubmittedUnconfirmed",
            Some(format_failure_detail(&failure)),
            screenshot_path.clone(),
            Some(failure.error_class.clone()),
        )
        .await;

    edit_start_message(
        ctx,
        progress_message,
        notice,
        start_submission_checking_content(&tracker.context.run_id),
        screenshot_path.clone(),
    )
    .await?;

    let verification = verify_submitted_start(ctx, tracker).await;
    if let Some(status) = verification.confirmed_status {
        let mut final_minecraft_status = Some(status.to_string());
        let mut outcome = "StartSubmittedUnconfirmed".to_string();
        let mut final_error_class = None;

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
                start_submitted_unconfirmed_content(&tracker.context.run_id, &status),
                screenshot_path.clone(),
            )
            .await?;
        }

        tracker
            .finish(
                &outcome,
                Some(UNCONFIRMED_PROVIDER_STATUS.to_string()),
                final_minecraft_status,
                screenshot_path,
                final_error_class,
            )
            .await;
        return Ok(());
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
    Ok(())
}

fn provider_failure_public_content(error_class: &str, run_id: &str) -> String {
    if error_class == "ProviderAllocationTimeout" {
        return format!(
            "Host allocation did not finish before the timeout. Running `/server start` again is safe; Butler will inspect the current provider state before acting.\nRun: `{run_id}`"
        );
    }
    format!("Start failed: **{error_class}**\nRun: `{run_id}`")
}

fn status_confirms_start_submission(status: &ServerStatus) -> bool {
    !status.is_offline_like()
}

fn start_submission_checking_content(run_id: &str) -> String {
    format!("Start may have been submitted. Checking Minecraft status...\nRun: `{run_id}`")
}

fn start_submitted_unconfirmed_content(run_id: &str, status: &ServerStatus) -> String {
    format!(
        "Start appears submitted, but provider confirmation was lost.\nMinecraft: **{status}**\nRun: `{run_id}`"
    )
}

fn start_submitted_unconfirmed_wait_final_content(
    run_id: &str,
    provider_name: &str,
    outcome: &str,
) -> String {
    start_final_content(run_id, provider_name, UNCONFIRMED_PROVIDER_STATUS, outcome)
}

fn start_submission_unverified_content(run_id: &str) -> String {
    format!("Start could not be verified after provider confirmation was lost.\nRun: `{run_id}`")
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(content.contains("again is safe"));
        assert!(content.contains("abc123"));
        assert!(!content.contains("Start may have been submitted"));
    }

    #[test]
    fn provider_progress_content_reports_allocation_state() {
        let content = provider_progress_content(
            "abc123",
            &ProviderProgress {
                stage: ProviderProgressStage::WaitingForAllocation,
                detail: "phase waking, queue 1/4, estimated 3 minutes".to_string(),
            },
        );
        assert!(content.contains("Waiting for host allocation"));
        assert!(content.contains("queue 1/4"));
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
    fn unconfirmed_start_public_content_is_short_and_scoped() {
        let checking = start_submission_checking_content("abc123");
        assert_eq!(
            checking,
            "Start may have been submitted. Checking Minecraft status...\nRun: `abc123`"
        );

        let accepted = start_submitted_unconfirmed_content("abc123", &ServerStatus::Starting);
        assert!(accepted.contains("Start appears submitted"));
        assert!(accepted.contains("Minecraft: **Starting**"));
        assert!(accepted.contains("Run: `abc123`"));

        let wait_done = start_submitted_unconfirmed_wait_final_content(
            "abc123",
            "pterodactyl",
            "MinecraftOnline",
        );
        assert!(wait_done.contains("Server is online."));
        assert!(wait_done.contains("Provider (pterodactyl): **Unconfirmed"));
        assert!(wait_done.contains("Run: `abc123`"));

        let unverified = start_submission_unverified_content("abc123");
        assert_eq!(
            unverified,
            "Start could not be verified after provider confirmation was lost.\nRun: `abc123`"
        );
    }
}
