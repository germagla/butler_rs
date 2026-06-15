use crate::{framework::Context, server_service};

#[poise::command(slash_command)]
pub async fn aternos_start(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    server_service::start_server_with_notice(
        ctx,
        server_service::StartOptions::default(),
        Some("`/aternos_start` is deprecated. Use `/server start` instead."),
    )
    .await
}

#[poise::command(slash_command)]
pub async fn aternos_status(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    server_service::status_with_notice(
        ctx,
        Some("`/aternos_status` is deprecated. Use `/server status` instead."),
    )
    .await
}
