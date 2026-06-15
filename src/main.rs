use dotenvy::dotenv;
use poise::serenity_prelude as serenity;
use tracing::{Level, metadata::Metadata};
use tracing_subscriber::{
    Layer,
    filter::{LevelFilter, filter_fn},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

mod aternos;
mod commands;
mod config;
mod framework;
mod minecraft;
mod run_history;
mod server_service;
mod state;
mod terminal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    init_tracing();

    let config = config::Config::from_env()?;
    let framework = framework::create_framework(config.clone());

    let mut client = serenity::ClientBuilder::new(
        config.discord_token.clone(),
        serenity::GatewayIntents::non_privileged(),
    )
    .framework(framework)
    .await?;

    client.start().await?;

    Ok(())
}

fn init_tracing() {
    let app_level = butler_log_level();
    let filter = filter_fn(move |metadata| {
        let target = metadata.target();
        let max_level = if target.starts_with("butler_rs") || target.starts_with("status_debug") {
            app_level
        } else {
            LevelFilter::OFF
        };
        metadata_enabled(metadata, max_level)
    });

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_target(false)
                .with_level(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .compact()
                .with_filter(filter),
        )
        .init();
}

fn butler_log_level() -> LevelFilter {
    match std::env::var("BUTLER_LOG")
        .unwrap_or_else(|_| "info".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" => LevelFilter::OFF,
        "error" => LevelFilter::ERROR,
        "warn" | "warning" => LevelFilter::WARN,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        _ => LevelFilter::INFO,
    }
}

fn metadata_enabled(metadata: &Metadata<'_>, max_level: LevelFilter) -> bool {
    match max_level {
        LevelFilter::OFF => false,
        LevelFilter::ERROR => matches!(*metadata.level(), Level::ERROR),
        LevelFilter::WARN => matches!(*metadata.level(), Level::ERROR | Level::WARN),
        LevelFilter::INFO => matches!(*metadata.level(), Level::ERROR | Level::WARN | Level::INFO),
        LevelFilter::DEBUG => matches!(
            *metadata.level(),
            Level::ERROR | Level::WARN | Level::INFO | Level::DEBUG
        ),
        LevelFilter::TRACE => true,
    }
}
