use crate::{
    framework::Context, provider::ProviderStartFailure, run_history::RunSummary, terminal,
};
use anyhow::Result;
use poise::serenity_prelude as serenity;
use std::{path::Path, path::PathBuf};

#[derive(Clone, Copy)]
pub(super) struct StartMessage {
    channel_id: serenity::ChannelId,
    message_id: serenity::MessageId,
}

pub(super) fn start_progress_content(run_id: &str, notice: Option<&str>) -> String {
    with_notice(
        format!("Checking Minecraft and the hosting provider...\nRun: `{run_id}`"),
        notice,
    )
}

pub(super) fn start_final_content(
    run_id: &str,
    provider_name: &str,
    provider_status: &str,
    outcome: &str,
) -> String {
    let normalized_status = provider_status.trim().to_ascii_lowercase();
    let headline = match (outcome, normalized_status.as_str()) {
        ("AlreadyActive", "running" | "online") => "The server is already running.",
        ("AlreadyActive", "starting") => "The server is already starting.",
        ("StartRequested", "running" | "online") if provider_name == "pterodactyl" => {
            "Start accepted. The server process is running."
        }
        ("StartRequested", _) if provider_name == "pterodactyl" => {
            "Start accepted. Play Hosting is starting the server."
        }
        ("MinecraftOnline", _) => "Minecraft is online.",
        ("WaitOnlineTimeout", _) if provider_name == "pterodactyl" => {
            "Play Hosting accepted the start, but Minecraft is still not online."
        }
        ("WaitOnlineTimeout", _) => {
            "The hosting provider accepted the start, but Minecraft is still not online."
        }
        _ => "Start accepted.",
    };
    let (provider_label, state_description) =
        if provider_name == "pterodactyl" && outcome == "MinecraftOnline" {
            (
            "Confirmation",
            match normalized_status.as_str() {
                "running" | "online" => {
                    "Play Hosting reports the server process is running, and Minecraft is online."
                        .to_string()
                }
                "starting" => {
                    "Play Hosting was still reporting startup, but Minecraft is now online."
                        .to_string()
                }
                _ => {
                    "Minecraft is online. Play Hosting's last state was not recognized.".to_string()
                }
            },
        )
        } else if provider_name == "pterodactyl" {
            (
                "Play Hosting state",
                pterodactyl_status_description(provider_status),
            )
        } else {
            ("Provider state", provider_status.to_string())
        };
    format!("{headline}\n{provider_label}: **{state_description}**\nRun: `{run_id}`")
}

pub(super) fn pterodactyl_status_description(provider_status: &str) -> String {
    match provider_status.trim().to_ascii_lowercase().as_str() {
        "start requested" => {
            "Start accepted; Play Hosting should now move the server through its startup states."
                .to_string()
        }
        "running" | "online" => "The server process is running.".to_string(),
        "starting" => "The server process is starting and is not ready to join yet.".to_string(),
        "offline" | "stopped" => "The server process is offline.".to_string(),
        "suspended" => "The server is suspended and Play Hosting cannot start it yet.".to_string(),
        _ => "Play Hosting returned an unrecognized server state.".to_string(),
    }
}

pub(super) fn with_notice(content: String, notice: Option<&str>) -> String {
    match notice {
        Some(notice) => format!("{notice}\n\n{content}"),
        None => content,
    }
}

pub(super) async fn edit_start_message(
    ctx: Context<'_>,
    message: &StartMessage,
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
                let edit = serenity::EditMessage::new()
                    .content(content.clone())
                    .allowed_mentions(disabled_mentions())
                    .attachments(serenity::EditAttachments::new().add(attachment));
                match message
                    .channel_id
                    .edit_message(ctx.serenity_context(), message.message_id, edit)
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(error) => emit_attachment_warning(&path, &error.to_string()),
                }
            }
            Err(error) => emit_attachment_warning(&path, &error.to_string()),
        }
    }
    if let Err(error) = message
        .channel_id
        .edit_message(
            ctx.serenity_context(),
            message.message_id,
            serenity::EditMessage::new()
                .content(content)
                .allowed_mentions(disabled_mentions())
                .attachments(serenity::EditAttachments::new()),
        )
        .await
    {
        terminal::emit(terminal::line(
            "WARN",
            "discord.progress",
            "",
            "",
            None,
            format!(
                "could not edit start progress message; error {}",
                terminal::clean(&error.to_string())
            ),
        ));
    }
    Ok(())
}

pub(super) async fn send_start_message(ctx: Context<'_>, content: String) -> Result<StartMessage> {
    let message = ctx
        .channel_id()
        .send_message(
            ctx.serenity_context(),
            serenity::CreateMessage::new()
                .content(content)
                .allowed_mentions(disabled_mentions()),
        )
        .await?;
    Ok(StartMessage {
        channel_id: message.channel_id,
        message_id: message.id,
    })
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
                            .allowed_mentions(disabled_mentions())
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
            .allowed_mentions(disabled_mentions())
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

pub(super) fn disabled_mentions() -> serenity::CreateAllowedMentions {
    serenity::CreateAllowedMentions::new()
        .all_users(false)
        .all_roles(false)
        .everyone(false)
        .replied_user(false)
}

pub(super) fn format_run_detail(run: &RunSummary) -> String {
    let mut lines = vec![
        format!("Run `{}`", run.context.run_id),
        format!("Command: `{}`", run.context.command),
        format!("Outcome: **{}**", run.display_outcome()),
        format!(
            "User: `{}` (`{}`)",
            run.context.user_name, run.context.user_id
        ),
        format!("Guild: `{}`", run.context.guild_name),
        format!("Duration: `{}ms`", run.duration_ms),
    ];

    lines.push(format!("Provider: `{}`", run.context.provider));
    if let Some(status) = &run.final_provider_status {
        lines.push(format!("Provider status: **{status}**"));
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

pub(super) fn format_provider_detail(status: &str, artifact_path: Option<&PathBuf>) -> String {
    match artifact_path {
        Some(path) => format!("Provider status: {status}; artifact={}", path.display()),
        None => format!("Provider status: {status}"),
    }
}

pub(super) fn format_failure_detail(failure: &ProviderStartFailure) -> String {
    match &failure.detail_artifact_path {
        Some(path) => format!("{}; artifact={}", failure.message, path.display()),
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

    #[test]
    fn pterodactyl_final_copy_describes_dashboard_state() {
        assert_eq!(
            start_final_content("abc123", "pterodactyl", "Start requested", "StartRequested"),
            "Start accepted. Play Hosting is starting the server.\nPlay Hosting state: **Start accepted; Play Hosting should now move the server through its startup states.**\nRun: `abc123`"
        );
        assert!(
            start_final_content("abc123", "pterodactyl", "Running", "AlreadyActive")
                .starts_with("The server is already running.")
        );
        assert!(
            start_final_content("abc123", "pterodactyl", "Starting", "AlreadyActive")
                .starts_with("The server is already starting.")
        );
        assert!(
            start_final_content("abc123", "pterodactyl", "Running", "StartRequested")
                .starts_with("Start accepted. The server process is running.")
        );
        let online = start_final_content("abc123", "pterodactyl", "Starting", "MinecraftOnline");
        assert!(online.starts_with("Minecraft is online."));
        assert!(online.contains(
            "Confirmation: **Play Hosting was still reporting startup, but Minecraft is now online.**"
        ));
    }

    #[test]
    fn unknown_pterodactyl_state_is_not_reflected_into_discord_copy() {
        let untrusted = "@everyone **injected**";

        let description = pterodactyl_status_description(untrusted);
        let final_content =
            start_final_content("abc123", "pterodactyl", untrusted, "MinecraftOnline");

        assert_eq!(
            description,
            "Play Hosting returned an unrecognized server state."
        );
        assert!(!description.contains(untrusted));
        assert!(!final_content.contains(untrusted));
    }
}
