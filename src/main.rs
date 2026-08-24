use anyhow::Context as _;
use dotenvy::dotenv;
use poise::serenity_prelude as serenity;
use rand::Rng;
use std::{
    ffi::OsString,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
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
mod runtime_lock;
mod server_service;
mod state;
mod terminal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dotenv_path = dotenv().ok();
    init_tracing()?;

    let command = parse_cli_command(std::env::args_os().skip(1))?;
    let mut config = match command {
        CliCommand::ProviderCheck | CliCommand::ProviderStartOnce => {
            config::Config::from_provider_env()?
        }
        _ => config::Config::from_env()?,
    };
    resolve_artifact_dir(&mut config, dotenv_path.as_deref())?;
    match command {
        CliCommand::CheckConfig => {
            println!("Butler configuration is valid.");
            return Ok(());
        }
        CliCommand::PrintArtifactDir => {
            println!("{}", config.artifact_dir.display());
            return Ok(());
        }
        CliCommand::ProviderCheck | CliCommand::ProviderStartOnce => {
            return run_provider_cli(command, config).await;
        }
        CliCommand::RunBot => {}
    }
    if let config::ProviderConfig::Pterodactyl(provider_config) = &config.provider
        && let Err(error) = pterodactyl::FlareSolverrSupervisor::reconcile_stale_ownership(
            pterodactyl::FlareSolverrRuntimeConfig::new(provider_config, &config.artifact_dir),
        )
        .await
    {
        terminal::emit(terminal::line(
            "WARN",
            "flaresolverr",
            "",
            "",
            None,
            format!(
                "could not reconcile stale owned container; error {}",
                terminal::clean(&error.to_string())
            ),
        ));
    }
    let provider = build_provider(&config)?;
    let provider_shutdown_wait_secs = match &config.provider {
        config::ProviderConfig::Pterodactyl(provider_config) => {
            provider_config.allocation_max_wait_secs
        }
        config::ProviderConfig::Aternos(_) => 0,
    }
    .saturating_add(config.start_wait_online_secs)
    .saturating_add(420);
    let provider_shutdown_wait = Duration::from_secs(provider_shutdown_wait_secs);
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
        .wait_for_provider_operations(provider_shutdown_wait)
        .await
    {
        terminal::emit(terminal::line(
            "WARN",
            "shutdown",
            "",
            "",
            None,
            "timed out waiting for an active provider or verification operation",
        ));
    }
    client_result?;

    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CliCommand {
    RunBot,
    CheckConfig,
    PrintArtifactDir,
    ProviderCheck,
    ProviderStartOnce,
}

fn parse_cli_command<I>(args: I) -> anyhow::Result<CliCommand>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let Some(argument) = args.next() else {
        return Ok(CliCommand::RunBot);
    };
    let argument = argument
        .into_string()
        .map_err(|_| anyhow::anyhow!("command-line arguments must be valid UTF-8"))?;
    if args.next().is_some() {
        anyhow::bail!("`{argument}` does not accept additional arguments");
    }
    match argument.as_str() {
        "--check-config" => Ok(CliCommand::CheckConfig),
        "--print-artifact-dir" => Ok(CliCommand::PrintArtifactDir),
        "--provider-check" => Ok(CliCommand::ProviderCheck),
        "--provider-start-once" => Ok(CliCommand::ProviderStartOnce),
        _ => anyhow::bail!("unknown argument `{argument}`"),
    }
}

fn resolve_artifact_dir(
    config: &mut config::Config,
    dotenv_path: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    if config.artifact_dir.is_absolute() {
        return Ok(());
    }
    let base = dotenv_path
        .and_then(std::path::Path::parent)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ARTIFACT_DIR must be absolute when Butler cannot locate a project .env file"
            )
        })?
        .to_path_buf();
    config.artifact_dir = base.join(&config.artifact_dir);
    Ok(())
}

fn build_provider(
    config: &config::Config,
) -> anyhow::Result<Arc<dyn provider::ServerStartProvider>> {
    match &config.provider {
        config::ProviderConfig::Aternos(provider_config) => {
            Ok(Arc::new(aternos::BrowserAternosProvider::new(
                provider_config.clone(),
                config.artifact_dir.clone(),
                config.artifact_capture,
            )))
        }
        config::ProviderConfig::Pterodactyl(provider_config) => {
            Ok(Arc::new(pterodactyl::PterodactylProvider::new(
                provider_config.as_ref().clone(),
                config.artifact_dir.clone(),
                config.artifact_capture,
            )?))
        }
    }
}

async fn run_provider_cli(command: CliCommand, config: config::Config) -> anyhow::Result<()> {
    run_history::verify_artifact_dir_writable(&config.artifact_dir)?;
    let run_id = generate_cli_run_id();
    let _admission = runtime_lock::StartAdmissionFileLease::acquire(&config.artifact_dir, &run_id)
        .context("could not admit provider smoke test")?;
    let provider = build_provider(&config)?;
    println!(
        "{} provider smoke test\nRun: `{run_id}`\nProvider: `{}`",
        match command {
            CliCommand::ProviderCheck => "Read-only",
            CliCommand::ProviderStartOnce => "Mutating",
            _ => unreachable!("provider CLI runner requires a provider command"),
        },
        provider.name()
    );
    let interrupted = Arc::new(AtomicBool::new(false));
    let signal_interrupted = interrupted.clone();
    let (interrupt_tx, mut interrupt_rx) = mpsc::channel(1);
    let interrupt_task = tokio::spawn(async move {
        if shutdown_signal().await.is_ok() {
            signal_interrupted.store(true, Ordering::Release);
            let _ = interrupt_tx.send(()).await;
        }
    });

    let preflight = match command {
        CliCommand::ProviderStartOnce => cli_minecraft_preflight(&config).await,
        CliCommand::ProviderCheck => None,
        _ => unreachable!("provider CLI runner requires a provider command"),
    };
    let result = if command == CliCommand::ProviderStartOnce && interrupted.load(Ordering::Acquire)
    {
        Err(interrupted_before_mutation_failure())
    } else {
        match (command, preflight) {
            (_, Some(result)) => Ok(result),
            (CliCommand::ProviderCheck, None) => {
                let mut operation = provider.check(&run_id);
                loop {
                    tokio::select! {
                        result = &mut operation => break result.map(CliResult::Check),
                        Some(()) = interrupt_rx.recv() => emit_cli_interrupt_notice(),
                    }
                }
            }
            (CliCommand::ProviderStartOnce, None) => {
                let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
                let mut operation = provider.start_with_progress(&run_id, progress_tx);
                let result = loop {
                    tokio::select! {
                        result = &mut operation => break result,
                        Some(progress) = progress_rx.recv() => {
                            println!(
                                "Progress: {} - {}",
                                progress.stage,
                                cli_safe_text(&progress.detail)
                            );
                        }
                        Some(()) = interrupt_rx.recv() => emit_cli_interrupt_notice(),
                    }
                };
                match result {
                    Ok(result) => Ok(CliResult::Start(result)),
                    Err(failure) if failure.uncertain_mutation.may_have_started_server() => {
                        match verify_cli_submitted_start(
                            &config,
                            provider.as_ref(),
                            &run_id,
                            &failure,
                            &mut interrupt_rx,
                        )
                        .await
                        {
                            Some((address, source, status)) => {
                                Ok(CliResult::StartSubmittedUnconfirmed {
                                    address,
                                    source,
                                    status,
                                    outcome: cli_submission_outcome(source),
                                    screenshot_path: failure.screenshot_path,
                                    detail_artifact_path: failure.detail_artifact_path,
                                })
                            }
                            None => Err(start_submission_unverified_failure(failure)),
                        }
                    }
                    Err(failure) => Err(failure),
                }
            }
            _ => unreachable!("provider CLI runner requires a provider command"),
        }
    };

    interrupt_task.abort();
    if let Err(error) =
        run_history::prune_run_artifact_dirs(&config.artifact_dir, config.run_history_limit)
    {
        eprintln!(
            "Warning: artifact retention failed: {}",
            cli_safe_text(&error.to_string())
        );
    }

    let final_result = match result {
        Ok(CliResult::Check(result)) => {
            println!(
                "Provider status: {}",
                cli_safe_text(&result.provider_status)
            );
            print_optional_artifacts(
                result.screenshot_path.as_ref(),
                result.detail_artifact_path.as_ref(),
            );
            let minecraft_address = result
                .minecraft_address
                .as_deref()
                .unwrap_or(&config.minecraft_server_addr);
            println!("Minecraft address: {}", cli_safe_text(minecraft_address));
            let status = minecraft::get_status_for_addr(minecraft_address).await?;
            println!("Minecraft status: {}", cli_safe_text(&status.to_string()));
            println!("Result: OK");
            Ok(())
        }
        Ok(CliResult::Start(result)) => {
            println!("Outcome: {}", result.outcome);
            println!(
                "Provider status: {}",
                cli_safe_text(&result.provider_status)
            );
            print_optional_artifacts(
                result.screenshot_path.as_ref(),
                result.detail_artifact_path.as_ref(),
            );
            if let Some(address) = result.minecraft_address.as_deref() {
                println!("Minecraft address: {}", cli_safe_text(address));
                let status = minecraft::get_status_for_addr(address).await?;
                println!("Minecraft status: {}", cli_safe_text(&status.to_string()));
            }
            println!("Result: OK");
            Ok(())
        }
        Ok(CliResult::AlreadyOnline { address, status }) => {
            println!("Minecraft address: {}", cli_safe_text(&address));
            println!("Minecraft status: {}", cli_safe_text(&status));
            println!("Result: SKIPPED (already active)");
            Ok(())
        }
        Ok(CliResult::StartSubmittedUnconfirmed {
            address,
            source,
            status,
            outcome,
            screenshot_path,
            detail_artifact_path,
        }) => {
            println!("Outcome: {outcome}");
            println!("Minecraft address: {}", cli_safe_text(&address));
            println!("{source} status: {}", cli_safe_text(&status));
            print_optional_artifacts(screenshot_path.as_ref(), detail_artifact_path.as_ref());
            println!("Result: OK");
            Ok(())
        }
        Err(failure) => {
            eprintln!("Result: FAILED");
            eprintln!("Error class: {}", cli_safe_text(&failure.error_class));
            eprintln!("Detail: {}", cli_safe_text(&failure.message));
            print_optional_artifacts(
                failure.screenshot_path.as_ref(),
                failure.detail_artifact_path.as_ref(),
            );
            anyhow::bail!(
                "provider smoke test failed ({})",
                cli_safe_text(&failure.error_class)
            )
        }
    };
    if interrupted.load(Ordering::Acquire) {
        final_result?;
        anyhow::bail!("provider smoke test interrupted after safe completion");
    }
    final_result
}

enum CliResult {
    Check(provider::ProviderCheckResult),
    Start(provider::ProviderStartResult),
    AlreadyOnline {
        address: String,
        status: String,
    },
    StartSubmittedUnconfirmed {
        address: String,
        source: &'static str,
        status: String,
        outcome: &'static str,
        screenshot_path: Option<std::path::PathBuf>,
        detail_artifact_path: Option<std::path::PathBuf>,
    },
}

async fn cli_minecraft_preflight(config: &config::Config) -> Option<CliResult> {
    match minecraft::get_status_for_addr(&config.minecraft_server_addr).await {
        Ok(status) if cli_status_confirms_activity(&status) => Some(CliResult::AlreadyOnline {
            address: config.minecraft_server_addr.clone(),
            status: status.to_string(),
        }),
        Ok(status) => {
            println!(
                "Minecraft preflight: {}",
                cli_safe_text(&status.to_string())
            );
            None
        }
        Err(error) => {
            eprintln!(
                "Warning: Minecraft preflight failed; continuing: {}",
                cli_safe_text(&error.to_string())
            );
            None
        }
    }
}

async fn verify_cli_submitted_start(
    config: &config::Config,
    provider: &dyn provider::ServerStartProvider,
    run_id: &str,
    failure: &provider::ProviderStartFailure,
    interrupt_rx: &mut mpsc::Receiver<()>,
) -> Option<(String, &'static str, String)> {
    const VERIFY_TIMEOUT: Duration = Duration::from_secs(60);
    const VERIFY_INTERVAL: Duration = Duration::from_secs(5);
    const PROVIDER_PROBE_TIMEOUT: Duration = Duration::from_secs(20);

    let address = failure
        .minecraft_address
        .as_deref()
        .unwrap_or(&config.minecraft_server_addr)
        .to_string();
    eprintln!(
        "Provider confirmation was lost after a possible start submission; verifying provider and Minecraft state for up to 60 seconds."
    );
    let deadline = tokio::time::Instant::now() + VERIFY_TIMEOUT;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if failure.uncertain_mutation == provider::ProviderMutation::PowerStart {
            let check_result = tokio::select! {
                result = tokio::time::timeout(
                    std::cmp::min(remaining, PROVIDER_PROBE_TIMEOUT),
                    provider.check(run_id),
                ) => result,
                Some(()) = interrupt_rx.recv() => {
                    emit_cli_interrupt_notice();
                    continue;
                }
            };
            match check_result {
                Ok(Ok(result)) if provider_status_confirms_activity(&result.provider_status) => {
                    return Some((address, "Provider", result.provider_status));
                }
                Ok(Ok(result)) => {
                    println!(
                        "Provider verification: {}",
                        cli_safe_text(&result.provider_status)
                    );
                }
                Ok(Err(error)) => {
                    eprintln!(
                        "Provider verification warning: {}",
                        cli_safe_text(&error.to_string())
                    );
                }
                Err(_) => {
                    eprintln!("Provider verification warning: probe timed out");
                }
            }
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let status_result = tokio::select! {
            result = tokio::time::timeout(
                remaining,
                minecraft::get_status_for_addr(&address),
            ) => match result {
                Ok(result) => result,
                Err(_) => return None,
            },
            Some(()) = interrupt_rx.recv() => {
                emit_cli_interrupt_notice();
                continue;
            }
        };
        match status_result {
            Ok(status) if cli_status_confirms_activity(&status) => {
                return Some((address, "Minecraft", status.to_string()));
            }
            Ok(status) => {
                println!("Verification: {}", cli_safe_text(&status.to_string()));
            }
            Err(error) => {
                eprintln!(
                    "Verification warning: {}",
                    cli_safe_text(&error.to_string())
                );
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        tokio::select! {
            _ = tokio::time::sleep(std::cmp::min(VERIFY_INTERVAL, remaining)) => {}
            Some(()) = interrupt_rx.recv() => emit_cli_interrupt_notice(),
        }
    }
}

fn cli_status_confirms_activity(status: &minecraft::ServerStatus) -> bool {
    !status.is_offline_like()
}

fn provider_status_confirms_activity(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "starting" | "running" | "online" | "preparing" | "loading"
    )
}

fn cli_submission_outcome(source: &str) -> &'static str {
    if source == "Provider" {
        "StartSubmittedConfirmed"
    } else {
        "StartSubmittedUnconfirmed"
    }
}

fn start_submission_unverified_failure(
    mut failure: provider::ProviderStartFailure,
) -> provider::ProviderStartFailure {
    let original_error = format!("{}: {}", failure.error_class, failure.message);
    failure.error_class = "StartSubmissionUnverified".to_string();
    failure.message = format!(
        "The provider start may have been submitted, but neither provider nor Minecraft state confirmed it; original failure: {original_error}"
    );
    failure.retryable = false;
    failure
}

fn interrupted_before_mutation_failure() -> provider::ProviderStartFailure {
    provider::ProviderStartFailure {
        error_class: "Interrupted".to_string(),
        message: "The smoke test was interrupted before any provider mutation".to_string(),
        screenshot_path: None,
        detail_artifact_path: None,
        minecraft_address: None,
        uncertain_mutation: provider::ProviderMutation::None,
        retryable: true,
    }
}

fn emit_cli_interrupt_notice() {
    eprintln!(
        "Interrupt requested; waiting for the provider operation and safety checks to finish."
    );
}

fn generate_cli_run_id() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..6)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn cli_safe_text(value: &str) -> String {
    let without_controls: String = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    terminal::clean(&without_controls)
        .chars()
        .take(1_000)
        .collect()
}

fn print_optional_artifacts(
    screenshot_path: Option<&std::path::PathBuf>,
    detail_artifact_path: Option<&std::path::PathBuf>,
) {
    if let Some(path) = screenshot_path {
        println!("Screenshot: {}", cli_safe_text(&path.display().to_string()));
    }
    if let Some(path) = detail_artifact_path {
        println!("Artifact: {}", cli_safe_text(&path.display().to_string()));
    }
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

    #[test]
    fn parses_cli_commands_strictly() {
        assert_eq!(
            parse_cli_command(Vec::<OsString>::new()).unwrap(),
            CliCommand::RunBot
        );
        assert_eq!(
            parse_cli_command([OsString::from("--provider-check")]).unwrap(),
            CliCommand::ProviderCheck
        );
        assert_eq!(
            parse_cli_command([OsString::from("--provider-start-once")]).unwrap(),
            CliCommand::ProviderStartOnce
        );
        assert!(parse_cli_command([OsString::from("--unknown")]).is_err());
        assert!(
            parse_cli_command([OsString::from("--provider-check"), OsString::from("extra")])
                .is_err()
        );
    }

    #[test]
    fn cli_output_sanitizes_control_characters_and_bounds_length() {
        let value = format!("token\nsecret\r\u{1b}[31m{}", "x".repeat(2_000));
        let sanitized = cli_safe_text(&value);
        assert!(!sanitized.contains('\n'));
        assert!(!sanitized.contains('\r'));
        assert!(!sanitized.contains('\u{1b}'));
        assert!(sanitized.chars().count() <= 1_000);
        assert!(sanitized.ends_with("..."));
    }

    #[test]
    fn cli_activity_check_accepts_transitional_and_online_states() {
        use minecraft::ServerStatus;

        assert!(!cli_status_confirms_activity(&ServerStatus::Offline));
        assert!(!cli_status_confirms_activity(&ServerStatus::Unreachable {
            reason: "test".to_string()
        }));
        assert!(cli_status_confirms_activity(&ServerStatus::Queued));
        assert!(cli_status_confirms_activity(&ServerStatus::Starting));
        assert!(cli_status_confirms_activity(&ServerStatus::Preparing));
        assert!(cli_status_confirms_activity(&ServerStatus::Loading));
        assert!(cli_status_confirms_activity(&ServerStatus::Online {
            online: 0,
            max: 20,
            players: Vec::new()
        }));
    }

    #[test]
    fn provider_activity_check_accepts_only_active_states() {
        assert!(provider_status_confirms_activity("Starting"));
        assert!(provider_status_confirms_activity("Running"));
        assert!(provider_status_confirms_activity("Online"));
        assert!(!provider_status_confirms_activity("Offline"));
        assert!(!provider_status_confirms_activity("Allocation active"));
    }

    #[test]
    fn cli_recovery_outcome_distinguishes_provider_confirmation() {
        assert_eq!(
            cli_submission_outcome("Provider"),
            "StartSubmittedConfirmed"
        );
        assert_eq!(
            cli_submission_outcome("Minecraft"),
            "StartSubmittedUnconfirmed"
        );
    }

    #[test]
    fn exhausted_cli_submission_verification_has_explicit_failure_class() {
        let failure = provider::ProviderStartFailure {
            error_class: "ProviderUnavailable".to_string(),
            message: "original".to_string(),
            screenshot_path: None,
            detail_artifact_path: None,
            minecraft_address: None,
            uncertain_mutation: provider::ProviderMutation::PowerStart,
            retryable: true,
        };

        let failure = start_submission_unverified_failure(failure);

        assert_eq!(failure.error_class, "StartSubmissionUnverified");
        assert!(!failure.retryable);
        assert_eq!(
            failure.uncertain_mutation,
            provider::ProviderMutation::PowerStart
        );
        assert!(failure.message.contains("ProviderUnavailable: original"));
    }

    #[test]
    fn interrupt_before_provider_call_is_non_mutating() {
        let failure = interrupted_before_mutation_failure();

        assert_eq!(failure.error_class, "Interrupted");
        assert_eq!(failure.uncertain_mutation, provider::ProviderMutation::None);
    }
}
