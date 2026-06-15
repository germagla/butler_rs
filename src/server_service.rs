use crate::{
    aternos::{self, BrowserStartFailure, BrowserStartResult, StartOutcome},
    framework::Context,
    minecraft::{self, ServerStatus},
    run_history::{RunContext, RunEvent, RunStep, RunSummary, now_ms},
    terminal,
};
use anyhow::Result;
use poise::serenity_prelude as serenity;
use rand::Rng;
use std::{path::PathBuf, time::Duration};
use tokio::time::sleep;

#[derive(Clone, Copy, Debug, Default)]
pub struct StartOptions {
    pub wait_online: bool,
    pub force: bool,
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

    if let Some(active_run_id) = data.active_start_run_id().await {
        terminal::emit(terminal::line_for_context(
            "BUSY",
            &context,
            format!("{active_run_id} already running"),
        ));
        ctx.say(with_notice(
            format!(
                "A start operation is already running as `{active_run_id}`. Use `/bot run run_id:{active_run_id}` to inspect the last recorded details."
            ),
            notice,
        ))
        .await?;
        return Ok(());
    }

    if !data.begin_start_run(&context.run_id).await {
        let active_run_id = data
            .active_start_run_id()
            .await
            .unwrap_or_else(|| "unknown".to_string());
        terminal::emit(terminal::line_for_context(
            "BUSY",
            &context,
            format!("{active_run_id} already running"),
        ));
        ctx.say(with_notice(
            format!(
                "A start operation began just now as `{active_run_id}`. Use `/bot run run_id:{active_run_id}` to inspect it after it finishes."
            ),
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
    data.finish_start_run(&context.run_id).await;

    if let Err(error) = result {
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
        .await?;
    }

    Ok(())
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
        Ok(browser_result) => {
            handle_browser_success(
                ctx,
                tracker,
                browser_result,
                preflight_status,
                options,
                progress_message,
                notice,
            )
            .await?;
        }
        Err(failure) => {
            handle_browser_failure(
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

async fn handle_browser_success(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    browser_result: BrowserStartResult,
    preflight_status: Option<String>,
    options: StartOptions,
    progress_message: &poise::ReplyHandle<'_>,
    notice: Option<&str>,
) -> Result<()> {
    let screenshot_path = browser_result.screenshot_path.clone();
    tracker
        .step(
            "aternos_dashboard",
            &browser_result.outcome.to_string(),
            Some(format_dashboard_detail(
                &browser_result.dashboard_status,
                browser_result.html_path.as_ref(),
            )),
            screenshot_path.clone(),
            None,
        )
        .await;

    let mut final_minecraft_status = preflight_status;
    let mut outcome = match browser_result.outcome {
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
        &browser_result.dashboard_status,
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
            Some(browser_result.dashboard_status),
            final_minecraft_status,
            screenshot_path,
            final_error_class,
        )
        .await;
    Ok(())
}

async fn handle_browser_failure(
    ctx: Context<'_>,
    tracker: &mut RunTracker<'_>,
    failure: BrowserStartFailure,
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

    let content = format!(
        "Start failed: **{}**\n{}\nRun: `{}`",
        failure.error_class, failure.message, tracker.context.run_id
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
            "Failed",
            None,
            preflight_status,
            screenshot_path,
            Some(failure.error_class),
        )
        .await;
    Ok(())
}

pub async fn status(ctx: Context<'_>) -> Result<()> {
    status_with_notice(ctx, None).await
}

pub async fn status_with_notice(ctx: Context<'_>, notice: Option<&str>) -> Result<()> {
    ctx.defer().await?;
    if let Some(notice) = notice {
        ctx.say(notice).await?;
    }

    let context = run_context(ctx, "server.status");
    let started_at_ms = now_ms();

    match minecraft::get_configured_status(&ctx.data().config).await {
        Ok(status) => {
            let status_text = status.to_string();
            ctx.say(format!(
                "Status for `{}`: **{}**",
                ctx.data().config.minecraft_server_addr,
                status_text
            ))
            .await?;
            terminal::emit(terminal::line_for_context(
                "OK",
                &context,
                format!(
                    "{} {}",
                    terminal::quote(&terminal::brief_minecraft_status(&status)),
                    terminal::format_duration(now_ms().saturating_sub(started_at_ms))
                ),
            ));
        }
        Err(error) => {
            terminal::emit(terminal::line_for_context(
                "FAIL",
                &context,
                format!(
                    "{} error {}",
                    terminal::format_duration(now_ms().saturating_sub(started_at_ms)),
                    terminal::clean(&error.to_string())
                ),
            ));
            ctx.say(format!("Status check failed: {error}")).await?;
        }
    }
    Ok(())
}

pub async fn diagnose(ctx: Context<'_>) -> Result<()> {
    ctx.defer().await?;
    let context = run_context(ctx, "server.diagnose");
    let started_at_ms = now_ms();
    let config = &ctx.data().config;
    let active = ctx
        .data()
        .active_start_run_id()
        .await
        .unwrap_or_else(|| "none".to_string());
    let status = minecraft::get_configured_status(config)
        .await
        .map(|status| status.to_string())
        .unwrap_or_else(|error| format!("error: {error}"));

    let response = format!(
        "Diagnostics\nServer address: `{}`\nAternos server id: `{}`\nHeadless: `{}`\nArtifact dir: `{}`\nActive start run: `{}`\nMinecraft status: **{}**",
        config.minecraft_server_addr,
        config.server_id.as_deref().unwrap_or("not configured"),
        config.headless,
        config.artifact_dir.display(),
        active,
        status
    );
    ctx.say(response).await?;
    terminal::emit(terminal::line_for_context(
        "OK",
        &context,
        format!(
            "active {} MC {} {}",
            active,
            terminal::quote(&status),
            terminal::format_duration(now_ms().saturating_sub(started_at_ms))
        ),
    ));
    Ok(())
}

pub async fn runs(ctx: Context<'_>, limit: Option<u8>) -> Result<()> {
    ctx.defer().await?;
    let context = run_context(ctx, "bot.runs");
    let started_at_ms = now_ms();
    let limit = limit.unwrap_or(10).clamp(1, 20) as usize;
    let runs = ctx.data().run_store.recent(limit).await;
    if runs.is_empty() {
        ctx.say("No completed runs recorded yet.").await?;
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

    ctx.say(format!("Last {} completed runs:\n{}", runs.len(), lines))
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
    ctx.defer().await?;
    let context = run_context(ctx, "bot.run");
    let started_at_ms = now_ms();
    if let Some(run) = ctx.data().run_store.find(&run_id).await {
        let outcome = run.outcome.clone();
        let detail = format_run_detail(&run);
        send_with_optional_screenshot(ctx, detail, run.screenshot_path.as_ref().map(PathBuf::from))
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
    } else if ctx.data().active_start_run_id().await.as_deref() == Some(run_id.as_str()) {
        ctx.say(format!(
            "Run `{run_id}` is currently active. It will appear in `/bot runs` after it finishes."
        ))
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
        ctx.say(format!("No run found for `{run_id}`.")).await?;
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
    ctx.defer().await?;
    let context = run_context(ctx, "bot.last-error");
    let started_at_ms = now_ms();
    if let Some(run) = ctx.data().run_store.last_error().await {
        let last_error_run = run.context.run_id.clone();
        let outcome = run.outcome.clone();
        let detail = format_run_detail(&run);
        send_with_optional_screenshot(ctx, detail, run.screenshot_path.as_ref().map(PathBuf::from))
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
        ctx.say("No failed runs recorded yet.").await?;
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

struct RunTracker<'a> {
    ctx: Context<'a>,
    context: RunContext,
    started_at_ms: u128,
    steps: Vec<RunStep>,
}

impl<'a> RunTracker<'a> {
    fn new(ctx: Context<'a>, context: RunContext) -> Self {
        Self {
            ctx,
            context,
            started_at_ms: now_ms(),
            steps: Vec::new(),
        }
    }

    async fn step(
        &mut self,
        step: &str,
        status: &str,
        detail: Option<String>,
        screenshot_path: Option<PathBuf>,
        error_class: Option<String>,
    ) {
        let run_step = RunStep {
            at_ms: now_ms(),
            step: step.to_string(),
            status: status.to_string(),
            detail,
            screenshot_path: screenshot_path
                .as_ref()
                .map(|path| path.display().to_string()),
            error_class,
        };

        terminal::emit_debug(terminal::line_for_context(
            "STEP",
            &self.context,
            format!(
                "{} {} {}{}",
                self.context.run_id,
                run_step.step,
                run_step.status,
                run_step
                    .error_class
                    .as_ref()
                    .map(|class| format!(" error {}", terminal::clean(class)))
                    .unwrap_or_default()
            ),
        ));

        let event = RunEvent {
            context: self.context.clone(),
            step: run_step.clone(),
        };
        if let Err(error) = self.ctx.data().run_store.append_event(&event) {
            terminal::emit(terminal::line_for_context(
                "WARN",
                &self.context,
                format!(
                    "{} could not write run history; error {}",
                    self.context.run_id,
                    terminal::clean(&error.to_string())
                ),
            ));
        }
        self.steps.push(run_step);
    }

    async fn finish(
        &mut self,
        outcome: &str,
        final_aternos_status: Option<String>,
        final_minecraft_status: Option<String>,
        screenshot_path: Option<PathBuf>,
        error_class: Option<String>,
    ) {
        let finished_at_ms = now_ms();
        let summary = RunSummary {
            context: self.context.clone(),
            started_at_ms: self.started_at_ms,
            finished_at_ms,
            duration_ms: finished_at_ms.saturating_sub(self.started_at_ms),
            outcome: outcome.to_string(),
            final_aternos_status,
            final_minecraft_status,
            screenshot_path: screenshot_path.map(|path| path.display().to_string()),
            error_class,
            steps: self.steps.clone(),
        };
        if summary.error_class.as_deref() != Some("CommandError") {
            let label = run_result_label(&summary);
            let line =
                terminal::line_for_context(label, &summary.context, run_summary_detail(&summary));
            terminal::emit(line);
        }
        self.ctx.data().run_store.push(summary).await;
    }
}

fn run_context(ctx: Context<'_>, command: &str) -> RunContext {
    let guild_id = ctx.guild_id().map(|id| id.to_string());
    let channel_id = ctx.channel_id();
    let mut channel_name = None;
    let guild_name = ctx
        .guild()
        .map(|guild| {
            channel_name = guild
                .channels
                .get(&channel_id)
                .map(|channel| channel.name.clone());
            guild.name.clone()
        })
        .unwrap_or_else(|| "DM".to_string());
    let author = ctx.author();

    RunContext {
        run_id: generate_run_id(),
        command: command.to_string(),
        guild_id,
        guild_name,
        channel_id: channel_id.to_string(),
        channel_name,
        user_id: author.id.to_string(),
        user_name: author.name.clone(),
    }
}

fn generate_run_id() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..6)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn run_result_label(summary: &RunSummary) -> &'static str {
    if summary.error_class.is_some() || summary.outcome.eq_ignore_ascii_case("failed") {
        "FAIL"
    } else if summary.outcome.eq_ignore_ascii_case("AlreadyOnline") {
        "SKIP"
    } else {
        "OK"
    }
}

fn run_summary_detail(summary: &RunSummary) -> String {
    let mut parts = vec![
        summary.context.run_id.clone(),
        summary.outcome.clone(),
        terminal::format_duration(summary.duration_ms),
    ];

    if let Some(error_class) = &summary.error_class {
        parts.push(format!("error {}", terminal::clean(error_class)));
    }

    parts.join(" ")
}

fn start_progress_content(run_id: &str, notice: Option<&str>) -> String {
    with_notice(format!("Starting server...\nRun: `{run_id}`"), notice)
}

fn start_final_content(run_id: &str, dashboard_status: &str, outcome: &str) -> String {
    let headline = match outcome {
        "MinecraftOnline" => "Server is online.",
        "WaitOnlineTimeout" => {
            "Aternos accepted the start, but Minecraft did not report online before timeout."
        }
        _ => "Start accepted.",
    };
    format!("{headline}\nAternos: **{dashboard_status}**\nRun: `{run_id}`")
}

fn with_notice(content: String, notice: Option<&str>) -> String {
    match notice {
        Some(notice) => format!("{notice}\n\n{content}"),
        None => content,
    }
}

async fn edit_start_message(
    ctx: Context<'_>,
    message: &poise::ReplyHandle<'_>,
    notice: Option<&str>,
    content: String,
    screenshot_path: Option<PathBuf>,
) -> Result<()> {
    let mut reply = poise::CreateReply::default().content(with_notice(content, notice));
    if let Some(path) = screenshot_path {
        let attachment = serenity::CreateAttachment::path(&path).await?;
        reply = reply.attachment(attachment);
    }
    message.edit(ctx, reply).await?;
    Ok(())
}

async fn send_with_optional_screenshot(
    ctx: Context<'_>,
    content: String,
    screenshot_path: Option<PathBuf>,
) -> Result<()> {
    if let Some(path) = screenshot_path {
        let attachment = serenity::CreateAttachment::path(&path).await?;
        ctx.send(
            poise::CreateReply::default()
                .content(content)
                .attachment(attachment),
        )
        .await?;
    } else {
        ctx.say(content).await?;
    }
    Ok(())
}

fn format_run_detail(run: &RunSummary) -> String {
    let mut lines = vec![
        format!("Run `{}`", run.context.run_id),
        format!("Command: `{}`", run.context.command),
        format!("Outcome: **{}**", run.outcome),
        format!(
            "User: `{}` (`{}`)",
            run.context.user_name, run.context.user_id
        ),
        format!("Guild: `{}`", run.context.guild_name),
        format!("Duration: `{}ms`", run.duration_ms),
    ];

    if let Some(status) = &run.final_aternos_status {
        lines.push(format!("Aternos dashboard: **{status}**"));
    }
    if let Some(status) = &run.final_minecraft_status {
        lines.push(format!("Minecraft status: **{status}**"));
    }
    if let Some(error_class) = &run.error_class {
        lines.push(format!("Error: **{error_class}**"));
    }

    lines.push("Steps:".to_string());
    for step in &run.steps {
        lines.push(format!(
            "- `{}` `{}`{}{}",
            step.step,
            step.status,
            step.detail
                .as_ref()
                .map(|detail| format!(" - {detail}"))
                .unwrap_or_default(),
            step.error_class
                .as_ref()
                .map(|class| format!(" error={class}"))
                .unwrap_or_default()
        ));
    }

    lines.join("\n")
}

fn format_dashboard_detail(status: &str, html_path: Option<&PathBuf>) -> String {
    match html_path {
        Some(path) => format!("Dashboard status: {status}; html={}", path.display()),
        None => format!("Dashboard status: {status}"),
    }
}

fn format_failure_detail(failure: &BrowserStartFailure) -> String {
    match &failure.html_path {
        Some(path) => format!("{}; html={}", failure.message, path.display()),
        None => failure.message.clone(),
    }
}
