use super::{
    responses::{
        edit_start_message, format_dashboard_detail, format_failure_detail, start_final_content,
        start_progress_content, with_notice,
    },
    tracking::{RunTracker, run_context},
    types::StartOptions,
};
use crate::{
    aternos,
    framework::Context,
    minecraft::{self, ServerStatus},
    provider::{ProviderStartFailure, ProviderStartResult, ServerStartProvider, StartOutcome},
    run_history::now_ms,
    state::ActiveStartRun,
    terminal,
};
use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;

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

    if !data.begin_start_run(&context).await {
        let active_run = data
            .active_start_run()
            .await
            .unwrap_or_else(|| ActiveStartRun {
                run_id: "unknown".to_string(),
                guild_id: None,
            });
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

    let progress_message = match ctx
        .send(
            poise::CreateReply::default().content(start_progress_content(&context.run_id, notice)),
        )
        .await
    {
        Ok(message) => message,
        Err(error) => {
            data.finish_start_run(&context.run_id).await;
            return Err(error.into());
        }
    };

    let result = start_server_inner(ctx, options, &mut tracker, &progress_message, notice).await;

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
        data.finish_start_run(&context.run_id).await;
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
        data.finish_start_run(&context.run_id).await;
        None
    };

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
    progress_message: &poise::ReplyHandle<'_>,
    notice: Option<&str>,
) -> Result<()> {
    let config = ctx.data().config.clone();

    let mut preflight_status = None;
    if !options.force {
        match minecraft::get_configured_status(&config).await {
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

    match aternos::BrowserAternosProvider
        .start(&config, &tracker.context.run_id)
        .await
    {
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
                progress_message,
                notice,
            )
            .await?;
        }
    }

    Ok(())
}

async fn handle_provider_success(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    provider_result: ProviderStartResult,
    preflight_status: Option<String>,
    options: StartOptions,
    progress_message: &poise::ReplyHandle<'_>,
    notice: Option<&str>,
) -> Result<()> {
    let screenshot_path = provider_result.screenshot_path.clone();
    tracker
        .step(
            "aternos_dashboard",
            &provider_result.outcome.to_string(),
            Some(format_dashboard_detail(
                &provider_result.dashboard_status,
                provider_result.html_path.as_ref(),
            )),
            screenshot_path.clone(),
            None,
        )
        .await;

    let mut final_minecraft_status = preflight_status;
    let mut outcome = match provider_result.outcome {
        StartOutcome::StartClicked => "StartClicked".to_string(),
        StartOutcome::DashboardChanged => "DashboardChanged".to_string(),
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
        let wait_started_at_ms = now_ms();
        let deadline = wait_started_at_ms
            .saturating_add((ctx.data().config.start_wait_online_secs as u128) * 1000);
        loop {
            match minecraft::get_configured_status(&ctx.data().config).await {
                Ok(status @ ServerStatus::Online { .. }) => {
                    final_minecraft_status = Some(status.to_string());
                    outcome = "MinecraftOnline".to_string();
                    final_error_class = None;
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
                outcome = "WaitOnlineTimeout".to_string();
                final_error_class = Some("WaitOnlineTimeout".to_string());
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
            sleep(Duration::from_secs(5)).await;
        }
    }

    let content = start_final_content(
        &tracker.context.run_id,
        &provider_result.dashboard_status,
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
            Some(provider_result.dashboard_status),
            final_minecraft_status,
            screenshot_path,
            final_error_class,
        )
        .await;
    Ok(())
}

async fn handle_provider_failure(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    failure: ProviderStartFailure,
    preflight_status: Option<String>,
    progress_message: &poise::ReplyHandle<'_>,
    notice: Option<&str>,
) -> Result<()> {
    let screenshot_path = failure.screenshot_path.clone();
    tracker
        .step(
            "aternos_dashboard",
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

fn provider_failure_public_content(error_class: &str, run_id: &str) -> String {
    format!(
        "Start failed: **{error_class}**\nRun: `{run_id}`\nAn owner or server Administrator can inspect raw details with `/bot run run_id:{run_id}`."
    )
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
    }
}
