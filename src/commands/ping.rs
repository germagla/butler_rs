use crate::framework::Context;

/// Replies with "Pong!"
#[poise::command(slash_command)]
pub async fn ping(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    ctx.say("🏓 Pong!").await?;
    Ok(())
}
