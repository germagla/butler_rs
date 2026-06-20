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
mod auth;
mod commands;
mod config;
mod framework;
mod minecraft;
mod provider;
mod run_history;
mod server_service;
mod state;
mod terminal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    init_tracing()?;

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

fn init_tracing() -> anyhow::Result<()> {
    let app_level = butler_log_level()?;
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
    Ok(())
}

fn butler_log_level() -> anyhow::Result<LevelFilter> {
    parse_butler_log_level(std::env::var("BUTLER_LOG").ok().as_deref())
}

fn parse_butler_log_level(value: Option<&str>) -> anyhow::Result<LevelFilter> {
    let value = value.unwrap_or("info").trim().to_ascii_lowercase();
    match value.as_str() {
        "off" => Ok(LevelFilter::OFF),
        "error" => Ok(LevelFilter::ERROR),
        "warn" | "warning" => Ok(LevelFilter::WARN),
        "info" => Ok(LevelFilter::INFO),
        "debug" => Ok(LevelFilter::DEBUG),
        "trace" => Ok(LevelFilter::TRACE),
        other => anyhow::bail!(
            "BUTLER_LOG must be one of off, error, warn, info, debug, trace; got `{other}`"
        ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_butler_log_levels_strictly() {
        assert_eq!(parse_butler_log_level(None).unwrap(), LevelFilter::INFO);
        assert_eq!(
            parse_butler_log_level(Some("warning")).unwrap(),
            LevelFilter::WARN
        );
        assert!(parse_butler_log_level(Some("verbose")).is_err());
    }
}
