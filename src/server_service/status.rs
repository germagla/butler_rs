use super::tracking::run_context;
use crate::{
    auth::{self, SensitiveCommandAccess},
    framework::Context,
    minecraft,
    run_history::now_ms,
    state::ActiveStartRun,
    terminal,
};
use anyhow::Result;

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
    let Some(access) = auth::require_sensitive_command_access(ctx).await? else {
        return Ok(());
    };
    ctx.defer_ephemeral().await?;
    let context = run_context(ctx, "server.diagnose");
    let started_at_ms = now_ms();
    let config = &ctx.data().config;
    let active_run = ctx.data().active_start_run().await;
    let active = active_run_display(active_run.as_ref(), &access);
    let status = minecraft::get_configured_status(config)
        .await
        .map(|status| status.to_string())
        .unwrap_or_else(|error| format!("error: {error}"));

    let response = format!(
        "Diagnostics\nServer address: `{}`\nAternos server id: `{}`\nHeadless: `{}`\nArtifact dir: `{}`\nArtifact capture: `{}`\nAttach screenshots: `{}`\nPersist events: `{}`\nRedact events: `{}`\nConfigured owners: `{}`\nActive start run: `{}`\nMinecraft status: **{}**",
        config.minecraft_server_addr,
        config.server_id.as_deref().unwrap_or("not configured"),
        config.headless,
        config.artifact_dir.display(),
        config.artifact_capture,
        config.attach_screenshots,
        config.persist_run_events,
        config.redact_run_events,
        config.owner_user_ids.len(),
        active,
        status
    );
    ctx.send(
        poise::CreateReply::default()
            .content(response)
            .ephemeral(true),
    )
    .await?;
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

fn active_run_display(
    active_run: Option<&ActiveStartRun>,
    access: &SensitiveCommandAccess,
) -> String {
    let Some(active_run) = active_run else {
        return "none".to_string();
    };
    if can_view_active_run(access, active_run.guild_id.as_deref()) {
        active_run.run_id.clone()
    } else {
        "hidden".to_string()
    }
}

fn can_view_active_run(access: &SensitiveCommandAccess, active_guild_id: Option<&str>) -> bool {
    match access {
        SensitiveCommandAccess::Owner => true,
        SensitiveCommandAccess::GuildAdministrator { guild_id } => {
            active_guild_id == Some(guild_id.as_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_run(run_id: &str, guild_id: Option<&str>) -> ActiveStartRun {
        ActiveStartRun {
            run_id: run_id.to_string(),
            guild_id: guild_id.map(str::to_string),
        }
    }

    #[test]
    fn diagnose_active_run_display_is_scoped() {
        let owner = SensitiveCommandAccess::Owner;
        let admin = SensitiveCommandAccess::GuildAdministrator {
            guild_id: "guild-1".to_string(),
        };
        let other_admin = SensitiveCommandAccess::GuildAdministrator {
            guild_id: "guild-2".to_string(),
        };

        let active = active_run("abc123", Some("guild-1"));
        assert_eq!(active_run_display(Some(&active), &owner), "abc123");
        assert_eq!(active_run_display(Some(&active), &admin), "abc123");
        assert_eq!(active_run_display(Some(&active), &other_admin), "hidden");

        let dm_active = active_run("dm123", None);
        assert_eq!(active_run_display(Some(&dm_active), &owner), "dm123");
        assert_eq!(active_run_display(Some(&dm_active), &admin), "hidden");
        assert_eq!(active_run_display(None, &owner), "none");
    }
}
