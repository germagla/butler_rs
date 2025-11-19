use crate::framework::Context;
use std::env;

#[poise::command(slash_command)]
pub async fn aternos_start(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    ctx.say("🚀 Checking server status before starting...").await?;

    let full_addr = env::var("MINECRAFT_SERVER_ADDR")
        .unwrap_or_else(|_| "localhost:25565".to_string());
    let addr = full_addr.split_once(':').map(|(host, _)| host).unwrap_or(&full_addr).to_string();

    // Pre-flight check
    match crate::aternos::get_minecraft_status(&addr).await {
        Ok(status) => {
            if !status.starts_with("Offline") {
                ctx.say(format!("⚠️ Server is already **{}**. Aborting start.", status)).await?;
                return Ok(());
            }
        },
        Err(e) => {
            ctx.say(format!("⚠️ Could not check status ({}), proceeding with start anyway...", e)).await?;
        }
    }

    ctx.say("🚀 Server is offline. Starting Aternos server...").await?;

    let username = env::var("ATERNOS_USER")?;
    let password = env::var("ATERNOS_PASS")?;

    match crate::aternos::start(&username, &password).await {
        Ok(msg) => ctx.say(format!("✅ {}", msg)).await?,
        Err(e) => ctx.say(format!("❌ Error: {}", e)).await?,
    };

    Ok(())
}

#[poise::command(slash_command)]
pub async fn aternos_status(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    ctx.say("🔍 Pinging Minecraft server...").await?;

    let full_addr = env::var("MINECRAFT_SERVER_ADDR")
        .unwrap_or_else(|_| "localhost:25565".to_string());
    let addr = full_addr.split_once(':').map(|(host, _)| host).unwrap_or(&full_addr).to_string();

    match crate::aternos::get_minecraft_status(&addr).await {
        Ok(status) => ctx.say(format!("📊 Status for `{}`: **{}**", addr, status)).await?,
        Err(e) => ctx.say(format!("❌ Error: {}", e)).await?,
    };

    Ok(())
}
