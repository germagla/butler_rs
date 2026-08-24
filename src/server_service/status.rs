use super::{
    responses::{pterodactyl_status_description, send_text},
    tracking::run_context,
};
use crate::{
    auth::{self, SensitiveCommandAccess},
    config::ProviderConfig,
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

    let mut provider_status = None;
    let mut provider_error = None;
    if matches!(&ctx.data().config.provider, ProviderConfig::Pterodactyl(_)) {
        match ctx
            .data()
            .try_begin_read_only_provider_operation(&context.run_id)
            .await
        {
            Ok(_operation_lease) => match ctx.data().provider.check(&context.run_id).await {
                Ok(result) => {
                    if let Some(address) = result.minecraft_address {
                        ctx.data().set_minecraft_address(address.to_string()).await;
                    }
                    provider_status = Some(pterodactyl_status_description(&result.provider_status));
                }
                Err(error) => {
                    provider_status = Some(format!("Status unavailable ({})", error.error_class));
                    provider_error = Some(error.to_string());
                }
            },
            Err(error) => {
                provider_status = Some("Status temporarily busy".to_string());
                provider_error = Some(error.to_string());
            }
        }
    }

    let minecraft_address = ctx.data().minecraft_address().await;
    let minecraft_result = minecraft::get_status_for_addr(&minecraft_address).await;
    let (minecraft_status, minecraft_error) = match &minecraft_result {
        Ok(status) => (status.to_string(), None),
        Err(error) => ("Status unavailable".to_string(), Some(error.to_string())),
    };
    send_text(
        ctx,
        status_content(
            &minecraft_address,
            &minecraft_status,
            provider_status.as_deref(),
        ),
        false,
    )
    .await?;

    let severity = if minecraft_error.is_some() || provider_error.is_some() {
        "WARN"
    } else {
        "OK"
    };
    let mut terminal_detail = match &minecraft_result {
        Ok(status) => terminal::quote(&terminal::brief_minecraft_status(status)),
        Err(_) => "MC unavailable".to_string(),
    };
    if let Some(status) = provider_status {
        terminal_detail.push_str(&format!(" provider {}", terminal::quote(&status)));
    }
    if let Some(error) = provider_error {
        terminal_detail.push_str(&format!(" provider_error {}", terminal::clean(&error)));
    }
    if let Some(error) = minecraft_error {
        terminal_detail.push_str(&format!(" minecraft_error {}", terminal::clean(&error)));
    }
    terminal_detail.push_str(&format!(
        " {}",
        terminal::format_duration(now_ms().saturating_sub(started_at_ms))
    ));
    terminal::emit(terminal::line_for_context(
        severity,
        &context,
        terminal_detail,
    ));
    Ok(())
}

fn status_content(
    minecraft_address: &str,
    minecraft_status: &str,
    provider_status: Option<&str>,
) -> String {
    match provider_status {
        Some(provider_status) => format!(
            "Status for `{minecraft_address}`\nMinecraft: **{minecraft_status}**\nPlay Hosting: **{provider_status}**"
        ),
        None => format!("Status for `{minecraft_address}`: **{minecraft_status}**"),
    }
}

pub async fn diagnose(ctx: Context<'_>) -> Result<()> {
    let Some(access) = auth::require_sensitive_command_access(ctx).await? else {
        return Ok(());
    };
    ctx.defer_ephemeral().await?;
    let context = run_context(ctx, "server.diagnose");
    let started_at_ms = now_ms();
    let config = &ctx.data().config;
    let minecraft_address = ctx.data().minecraft_address().await;
    let active_run = ctx.data().active_start_run().await;
    let active = active_run_display(active_run.as_ref(), &access);
    let status = minecraft::get_status_for_addr(&minecraft_address)
        .await
        .map(|status| status.to_string())
        .unwrap_or_else(|error| format!("error: {error}"));
    let provider = provider_diagnostics(&config.provider);

    let response = format!(
        "Diagnostics\nServer address: `{}`\n{}\nArtifact dir: `{}`\nArtifact capture: `{}`\nAttach screenshots: `{}`\nPersist events: `{}`\nRedact events: `{}`\nConfigured owners: `{}`\nActive start run: `{}`\nMinecraft status: **{}**",
        minecraft_address,
        provider,
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

fn provider_diagnostics(provider: &ProviderConfig) -> String {
    match provider {
        ProviderConfig::Aternos(config) => format!(
            "Provider: `aternos`\nAternos server id: `{}`\nHeadless: `{}`",
            config.server_id.as_deref().unwrap_or("not configured"),
            config.headless
        ),
        ProviderConfig::Pterodactyl(config) => format!(
            "Provider: `pterodactyl`\nPanel: `{}`\nPterodactyl server id: `{}`\nPower enabled: `{}`\nFlareSolverr: `{}`\nFlareSolverr container: `{}`",
            config.panel_origin,
            config.server_id,
            config.power_enabled,
            config.flaresolverr_url,
            config.flaresolverr_container
        ),
    }
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

    #[test]
    fn pterodactyl_status_content_describes_both_views() {
        assert_eq!(
            status_content(
                "server.example",
                "Offline",
                Some("In Play Hosting's queue: position 3 of 8. Estimated wait: about 6 minutes.")
            ),
            "Status for `server.example`\nMinecraft: **Offline**\nPlay Hosting: **In Play Hosting's queue: position 3 of 8. Estimated wait: about 6 minutes.**"
        );
        assert_eq!(
            status_content(
                "server.example",
                "Status unavailable",
                Some("Status unavailable (ProviderUnavailable)")
            ),
            "Status for `server.example`\nMinecraft: **Status unavailable**\nPlay Hosting: **Status unavailable (ProviderUnavailable)**"
        );
    }

    #[test]
    fn aternos_status_content_keeps_compact_format() {
        assert_eq!(
            status_content("server.example", "Online", None),
            "Status for `server.example`: **Online**"
        );
    }
}
