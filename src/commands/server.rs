use crate::{framework::Context, server_service};

#[poise::command(slash_command, subcommands("start", "status", "diagnose"))]
pub async fn server(_ctx: Context<'_>) -> Result<(), anyhow::Error> {
    Ok(())
}

#[poise::command(slash_command)]
pub async fn start(
    ctx: Context<'_>,
    #[description = "Wait until Minecraft itself reports online. Defaults to false."]
    wait_online: Option<bool>,
    #[description = "Skip the preflight status check. Defaults to false."] force: Option<bool>,
) -> Result<(), anyhow::Error> {
    server_service::start_server(
        ctx,
        server_service::StartOptions {
            wait_online: wait_online.unwrap_or(false),
            force: force.unwrap_or(false),
        },
    )
    .await
}

#[poise::command(slash_command)]
pub async fn status(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    server_service::status(ctx).await
}

#[poise::command(slash_command)]
pub async fn diagnose(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    server_service::diagnose(ctx).await
}
