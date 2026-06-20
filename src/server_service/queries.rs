use super::{
    responses::{format_run_detail, send_text, send_with_optional_screenshot},
    tracking::run_context,
};
use crate::{
    auth::{self, SensitiveCommandAccess},
    framework::Context,
    run_history::{RunQueryScope, now_ms},
    terminal,
};
use anyhow::Result;
use std::path::PathBuf;

pub async fn runs(ctx: Context<'_>, limit: Option<u8>) -> Result<()> {
    let Some(access) = auth::require_sensitive_command_access(ctx).await? else {
        return Ok(());
    };
    ctx.defer_ephemeral().await?;
    let context = run_context(ctx, "bot.runs");
    let started_at_ms = now_ms();
    let limit = limit.unwrap_or(10).clamp(1, 20) as usize;
    let scope = run_query_scope(&access);
    let runs = ctx.data().run_store.recent_scoped(limit, &scope).await;
    if runs.is_empty() {
        send_text(ctx, "No completed runs recorded yet.", true).await?;
        terminal::emit(terminal::line_for_context(
            "OK",
            &context,
            format!(
                "0 runs {}",
                terminal::format_duration(now_ms().saturating_sub(started_at_ms))
            ),
        ));
        return Ok(());
    }

    let lines = runs
        .iter()
        .map(|run| {
            format!(
                "`{}` {} by {} in {} ({}ms){}",
                run.context.run_id,
                run.outcome,
                run.context.user_name,
                run.context.guild_name,
                run.duration_ms,
                run.error_class
                    .as_ref()
                    .map(|class| format!(" error={class}"))
                    .unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    send_text(
        ctx,
        format!("Last {} completed runs:\n{}", runs.len(), lines),
        true,
    )
    .await?;
    terminal::emit(terminal::line_for_context(
        "OK",
        &context,
        format!(
            "{} runs {}",
            runs.len(),
            terminal::format_duration(now_ms().saturating_sub(started_at_ms))
        ),
    ));
    Ok(())
}

pub async fn run(ctx: Context<'_>, run_id: String) -> Result<()> {
    let Some(access) = auth::require_sensitive_command_access(ctx).await? else {
        return Ok(());
    };
    ctx.defer_ephemeral().await?;
    let context = run_context(ctx, "bot.run");
    let started_at_ms = now_ms();
    let scope = run_query_scope(&access);
    if let Some(run) = ctx.data().run_store.find_scoped(&run_id, &scope).await {
        let outcome = run.outcome.clone();
        let detail = format_run_detail(&run);
        send_with_optional_screenshot(
            ctx,
            detail,
            run.screenshot_path.as_ref().map(PathBuf::from),
            true,
        )
        .await?;
        terminal::emit(terminal::line_for_context(
            "OK",
            &context,
            format!(
                "{} {} {}",
                run_id,
                outcome,
                terminal::format_duration(now_ms().saturating_sub(started_at_ms))
            ),
        ));
    } else if ctx
        .data()
        .active_start_run()
        .await
        .filter(|active_run| {
            active_run.run_id == run_id.as_str()
                && scope.allows_guild_id(active_run.guild_id.as_deref())
        })
        .is_some()
    {
        send_text(
            ctx,
            format!(
                "Run `{run_id}` is currently active. It will appear in `/bot runs` after it finishes."
            ),
            true,
        )
        .await?;
        terminal::emit(terminal::line_for_context(
            "OK",
            &context,
            format!(
                "{} active {}",
                run_id,
                terminal::format_duration(now_ms().saturating_sub(started_at_ms))
            ),
        ));
    } else {
        send_text(ctx, format!("No run found for `{run_id}`."), true).await?;
        terminal::emit(terminal::line_for_context(
            "MISS",
            &context,
            format!(
                "{} {}",
                run_id,
                terminal::format_duration(now_ms().saturating_sub(started_at_ms))
            ),
        ));
    }
    Ok(())
}

pub async fn last_error(ctx: Context<'_>) -> Result<()> {
    let Some(access) = auth::require_sensitive_command_access(ctx).await? else {
        return Ok(());
    };
    ctx.defer_ephemeral().await?;
    let context = run_context(ctx, "bot.last-error");
    let started_at_ms = now_ms();
    let scope = run_query_scope(&access);
    if let Some(run) = ctx.data().run_store.last_error_scoped(&scope).await {
        let last_error_run = run.context.run_id.clone();
        let outcome = run.outcome.clone();
        let detail = format_run_detail(&run);
        send_with_optional_screenshot(
            ctx,
            detail,
            run.screenshot_path.as_ref().map(PathBuf::from),
            true,
        )
        .await?;
        terminal::emit(terminal::line_for_context(
            "OK",
            &context,
            format!(
                "{} {} {}",
                last_error_run,
                outcome,
                terminal::format_duration(now_ms().saturating_sub(started_at_ms))
            ),
        ));
    } else {
        send_text(ctx, "No failed runs recorded yet.", true).await?;
        terminal::emit(terminal::line_for_context(
            "OK",
            &context,
            format!(
                "none {}",
                terminal::format_duration(now_ms().saturating_sub(started_at_ms))
            ),
        ));
    }
    Ok(())
}

fn run_query_scope(access: &SensitiveCommandAccess) -> RunQueryScope {
    match access {
        SensitiveCommandAccess::Owner => RunQueryScope::All,
        SensitiveCommandAccess::GuildAdministrator { guild_id } => {
            RunQueryScope::Guild(guild_id.clone())
        }
    }
}
