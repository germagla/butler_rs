use crate::{framework::Context, server_service};

#[poise::command(slash_command, subcommands("runs", "run", "last_error"))]
pub async fn bot(_ctx: Context<'_>) -> Result<(), anyhow::Error> {
    Ok(())
}

#[poise::command(slash_command)]
pub async fn runs(
    ctx: Context<'_>,
    #[description = "Number of completed runs to show. Defaults to 10."] limit: Option<u8>,
) -> Result<(), anyhow::Error> {
    server_service::runs(ctx, limit).await
}

#[poise::command(slash_command)]
pub async fn run(
    ctx: Context<'_>,
    #[description = "Run ID returned by /server start or /bot runs"] run_id: String,
) -> Result<(), anyhow::Error> {
    server_service::run(ctx, run_id).await
}

#[poise::command(slash_command, rename = "last-error")]
pub async fn last_error(ctx: Context<'_>) -> Result<(), anyhow::Error> {
    server_service::last_error(ctx).await
}
