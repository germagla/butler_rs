use dotenvy::dotenv;
use poise::serenity_prelude as serenity;
use std::env;

mod aternos;
mod commands;
mod framework;
mod state;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let token = env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN must be set in .env");

    let framework = framework::create_framework();

    let mut client =
        serenity::ClientBuilder::new(token, serenity::GatewayIntents::non_privileged())
            .framework(framework)
            .await?;

    client.start().await?;

    Ok(())
}
