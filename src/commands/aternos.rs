use crate::framework::Context;
use std::env;

#[poise::command(slash_command)]
pub async fn start_aternos(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    ctx.say("🚀 Starting Aternos server…").await?;

    let username = env::var("ATERNOS_USER")?;
    let password = env::var("ATERNOS_PASS")?;

    match crate::aternos::start(&username, &password).await {
        Ok(msg) => ctx.say(format!("✅ {}", msg)).await?,
        Err(e) => ctx.say(format!("❌ Error: {}", e)).await?,
    };

    Ok(())
}
