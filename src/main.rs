use anyhow::Context as _;
use dotenvy::dotenv;
use poise::serenity_prelude as serenity;
use std::{sync::Arc, time::Duration};
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
mod pterodactyl;
mod run_history;
mod server_service;
mod state;
mod terminal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    init_tracing()?;

    let config = config::Config::from_env()?;
    let mut args = std::env::args().skip(1);
    match args.next() {
        Some(argument) if argument == "--check-config" => {
            if args.next().is_some() {
                anyhow::bail!("--check-config does not accept additional arguments");
            }
            println!("Butler configuration is valid.");
            return Ok(());
        }
        Some(argument) => anyhow::bail!("unknown argument `{argument}`"),
        None => {}
    }
    let provider: Arc<dyn provider::ServerStartProvider> = match &config.provider {
        config::ProviderConfig::Aternos(provider_config) => {
            Arc::new(aternos::BrowserAternosProvider::new(
                provider_config.clone(),
                config.artifact_dir.clone(),
                config.artifact_capture,
            ))
        }
        config::ProviderConfig::Pterodactyl(provider_config) => {
            Arc::new(pterodactyl::PterodactylProvider::new(
                provider_config.clone(),
                config.artifact_dir.clone(),
                config.artifact_capture,
            )?)
        }
    };
    let state = state::BotState::new(config.clone(), provider)?;
    let shutdown_state = state.clone();
    let framework = framework::create_framework(state);

    let mut client = serenity::ClientBuilder::new(
        config.discord_token.clone(),
        serenity::GatewayIntents::non_privileged(),
    )
    .framework(framework)
    .await?;
    let shard_manager = client.shard_manager.clone();
    let mut shutdown = Box::pin(shutdown_signal());
    let supervisor = match &config.provider {
        config::ProviderConfig::Pterodactyl(provider_config) => {
            let mut startup = tokio::spawn(pterodactyl::FlareSolverrSupervisor::start(
                pterodactyl::FlareSolverrRuntimeConfig::new(provider_config, &config.artifact_dir),
            ));
            tokio::select! {
                result = &mut startup => Some(
                    result.context("FlareSolverr supervisor startup task failed")??
                ),
                signal_result = &mut shutdown => {
                    signal_result?;
                    shutdown_state.begin_shutdown().await;
                    shard_manager.shutdown_all().await;
                    if let Ok(Ok(supervisor)) = startup.await {
                        supervisor.shutdown().await;
                    }
                    return Ok(());
                }
            }
        }
        config::ProviderConfig::Aternos(_) => None,
    };

    let client_result: anyhow::Result<()> = tokio::select! {
        result = client.start() => result.map_err(Into::into),
        signal_result = &mut shutdown => {
            if signal_result.is_ok() {
                shutdown_state.begin_shutdown().await;
                shard_manager.shutdown_all().await;
            }
            signal_result
        }
    };

    shutdown_state.begin_shutdown().await;
    if !shutdown_state
        .wait_for_provider_operations(Duration::from_secs(420))
        .await
    {
        terminal::emit(terminal::line(
            "WARN",
            "shutdown",
            "",
            "",
            None,
            "timed out waiting for an active provider operation",
        ));
    }
    if let Some(supervisor) = supervisor {
        supervisor.shutdown().await;
    }
    client_result?;

    Ok(())
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
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
