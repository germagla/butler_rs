use crate::{
    framework::Context, provider::ProviderStartFailure, run_history::RunSummary, terminal,
};
use anyhow::Result;
use poise::serenity_prelude as serenity;
use std::{path::Path, path::PathBuf};

pub(super) fn start_progress_content(run_id: &str, notice: Option<&str>) -> String {
    with_notice(format!("Starting server...\nRun: `{run_id}`"), notice)
}

pub(super) fn start_final_content(run_id: &str, dashboard_status: &str, outcome: &str) -> String {
    let headline = match outcome {
        "MinecraftOnline" => "Server is online.",
        "WaitOnlineTimeout" => {
            "Aternos accepted the start, but Minecraft did not report online before timeout."
        }
        _ => "Start accepted.",
    };
    format!("{headline}\nAternos: **{dashboard_status}**\nRun: `{run_id}`")
}

pub(super) fn with_notice(content: String, notice: Option<&str>) -> String {
    match notice {
        Some(notice) => format!("{notice}\n\n{content}"),
        None => content,
    }
}

pub(super) async fn edit_start_message(
    ctx: Context<'_>,
    message: &poise::ReplyHandle<'_>,
    notice: Option<&str>,
    content: String,
    screenshot_path: Option<PathBuf>,
) -> Result<()> {
    let content = with_notice(content, notice);
    if let Some(path) =
        attachable_screenshot_path(ctx.data().config.attach_screenshots, screenshot_path)
    {
        match serenity::CreateAttachment::path(&path).await {
            Ok(attachment) => {
                let reply = poise::CreateReply::default()
                    .content(content.clone())
                    .attachment(attachment);
                match message.edit(ctx, reply).await {
                    Ok(()) => return Ok(()),
                    Err(error) => emit_attachment_warning(&path, &error.to_string()),
                }
            }
            Err(error) => emit_attachment_warning(&path, &error.to_string()),
        }
    }
    message
        .edit(ctx, poise::CreateReply::default().content(content))
        .await?;
    Ok(())
}

pub(super) async fn send_with_optional_screenshot(
    ctx: Context<'_>,
    content: String,
    screenshot_path: Option<PathBuf>,
    ephemeral: bool,
) -> Result<()> {
    if let Some(path) =
        attachable_screenshot_path(ctx.data().config.attach_screenshots, screenshot_path)
    {
        match serenity::CreateAttachment::path(&path).await {
            Ok(attachment) => {
                let reply = ctx
                    .send(
                        poise::CreateReply::default()
                            .content(content.clone())
                            .attachment(attachment)
                            .ephemeral(ephemeral),
                    )
                    .await;
                match reply {
                    Ok(_) => return Ok(()),
                    Err(error) => emit_attachment_warning(&path, &error.to_string()),
                }
            }
            Err(error) => emit_attachment_warning(&path, &error.to_string()),
        }
    }
    send_text(ctx, content, ephemeral).await?;
    Ok(())
}

pub(super) async fn send_text(
    ctx: Context<'_>,
    content: impl Into<String>,
    ephemeral: bool,
) -> Result<()> {
    ctx.send(
        poise::CreateReply::default()
            .content(content.into())
            .ephemeral(ephemeral),
    )
    .await?;
    Ok(())
}

fn attachable_screenshot_path(
    attach_screenshots: bool,
    screenshot_path: Option<PathBuf>,
) -> Option<PathBuf> {
    if !attach_screenshots {
        return None;
    }
    screenshot_path.filter(|path| path.is_file())
}

fn emit_attachment_warning(path: &Path, error: &str) {
    terminal::emit(terminal::line(
        "WARN",
        "discord.attachment",
        "",
        "",
        None,
        format!(
            "could not attach screenshot {}; error {}",
            path.display(),
            terminal::clean(error)
        ),
    ));
}

pub(super) fn format_run_detail(run: &RunSummary) -> String {
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

pub(super) fn format_dashboard_detail(status: &str, html_path: Option<&PathBuf>) -> String {
    match html_path {
        Some(path) => format!("Dashboard status: {status}; html={}", path.display()),
        None => format!("Dashboard status: {status}"),
    }
}

pub(super) fn format_failure_detail(failure: &ProviderStartFailure) -> String {
    match &failure.html_path {
        Some(path) => format!("{}; html={}", failure.message, path.display()),
        None => failure.message.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_history::now_ms;
    use std::fs;

    fn temp_file(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "butler_rs_{name}_{}_{}",
            std::process::id(),
            now_ms()
        ));
        fs::write(&path, "screenshot").unwrap();
        path
    }

    #[test]
    fn missing_screenshot_is_not_attachable() {
        let missing = std::env::temp_dir().join(format!(
            "butler_rs_missing_{}_{}",
            std::process::id(),
            now_ms()
        ));

        assert_eq!(attachable_screenshot_path(true, Some(missing)), None);
    }

    #[test]
    fn screenshot_attachment_respects_config_flag() {
        let path = temp_file("screenshot_flag");

        assert_eq!(
            attachable_screenshot_path(true, Some(path.clone())),
            Some(path.clone())
        );
        assert_eq!(attachable_screenshot_path(false, Some(path.clone())), None);

        fs::remove_file(path).unwrap();
    }
}
