use crate::{
    framework::Context,
    run_history::{RunContext, RunEvent, RunStep, RunSummary, now_ms},
    terminal,
};
use rand::Rng;
use std::path::PathBuf;

pub(super) struct RunTracker<'a> {
    ctx: Context<'a>,
    pub(super) context: RunContext,
    started_at_ms: u128,
    steps: Vec<RunStep>,
}

impl<'a> RunTracker<'a> {
    pub(super) fn new(ctx: Context<'a>, context: RunContext) -> Self {
        Self {
            ctx,
            context,
            started_at_ms: now_ms(),
            steps: Vec::new(),
        }
    }

    pub(super) async fn step(
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

    pub(super) async fn finish(
        &mut self,
        outcome: &str,
        final_provider_status: Option<String>,
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
            final_provider_status,
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

pub(super) fn run_context(ctx: Context<'_>, command: &str) -> RunContext {
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
        provider: ctx.data().provider.name().to_string(),
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
