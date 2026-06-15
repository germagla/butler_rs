use crate::framework::Context;
use std::env;

/// HTTP-based alternative to /aternos_start.
///
/// Instead of launching a headless Chrome browser, this command talks directly
/// to Aternos' internal HTTP API.  It is faster, uses far less memory, and
/// doesn't require Chromium to be installed on the host.
#[poise::command(slash_command)]
pub async fn new_aternos_start_deepseek(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    // Let Discord know we're working on it (avoids "interaction failed").
    ctx.defer().await?;

    // ── Pre-flight: check if the Minecraft server is already online ─────────
    let full_addr =
        env::var("MINECRAFT_SERVER_ADDR").unwrap_or_else(|_| "localhost:25565".to_string());
    let addr = full_addr
        .split_once(':')
        .map(|(host, _)| host)
        .unwrap_or(&full_addr)
        .to_string();

    match crate::aternos::get_minecraft_status(&addr).await {
        Ok(status) => {
            if !status.starts_with("Offline") {
                ctx.say(format!(
                    "⚠️ Server is already **{}**. Aborting start.",
                    status
                ))
                .await?;
                return Ok(());
            }
        }
        Err(e) => {
            ctx.say(format!(
                "⚠️ Could not check server status ({}), proceeding with start anyway...",
                e
            ))
            .await?;
        }
    }

    // ── Start the server via HTTP API ───────────────────────────────────────
    let username = env::var("ATERNOS_USER")?;
    let password = env::var("ATERNOS_PASS")?;

    match crate::aternos_http::start(&username, &password).await {
        Ok(msg) => ctx.say(format!("✅ {}", msg)).await?,
        Err(e) => ctx.say(format!("❌ Error: {}", e)).await?,
    };

    Ok(())
}
