use crate::{
    minecraft::{self, ServerStatus},
    run_history::{RunContext, RunStep, now_ms},
    state::BotState,
    terminal,
};
use std::{future::Future, time::Duration};
use tokio::time::{Instant, sleep, timeout};

const MINECRAFT_STATUS_POLL_SECS: u64 = 5;

#[derive(Debug)]
struct MinecraftPollObservation {
    online: Option<ServerStatus>,
    last_status: Option<ServerStatus>,
    last_error: Option<String>,
    successful_probes: usize,
    unreachable_probes: usize,
    interrupted_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackgroundVerificationOutcome {
    MinecraftOnline,
    StartVerificationTimeout,
    MinecraftStatusError,
    StartVerificationInterrupted,
}

impl BackgroundVerificationOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::MinecraftOnline => "MinecraftOnline",
            Self::StartVerificationTimeout => "StartVerificationTimeout",
            Self::MinecraftStatusError => "MinecraftStatusError",
            Self::StartVerificationInterrupted => "StartVerificationInterrupted",
        }
    }

    fn step_status(self) -> &'static str {
        match self {
            Self::MinecraftOnline => "online",
            Self::StartVerificationTimeout => "timeout",
            Self::MinecraftStatusError => "status_error",
            Self::StartVerificationInterrupted => "interrupted",
        }
    }

    fn error_class(self) -> Option<String> {
        (!matches!(
            self,
            Self::MinecraftOnline | Self::StartVerificationInterrupted
        ))
        .then(|| self.as_str().to_string())
    }
}

pub(super) struct Job {
    context: RunContext,
    minecraft_address: String,
    initial_provider_status: String,
    max_wait: Duration,
    command_started_at_ms: u128,
}

pub(super) struct ReservedJob {
    inner: Option<ReservedJobInner>,
}

struct ReservedJobInner {
    state: BotState,
    job: Job,
    operation_guard: crate::state::ProviderOperationGuard,
}

impl Job {
    pub(super) fn new(
        context: RunContext,
        minecraft_address: String,
        initial_provider_status: String,
        max_wait: Duration,
        command_started_at_ms: u128,
    ) -> Self {
        Self {
            context,
            minecraft_address,
            initial_provider_status: safe_provider_status(&initial_provider_status),
            max_wait,
            command_started_at_ms,
        }
    }

    pub(super) fn reserve(self, state: BotState) -> ReservedJob {
        let operation_guard = state.begin_background_operation();
        ReservedJob {
            inner: Some(ReservedJobInner {
                state,
                job: self,
                operation_guard,
            }),
        }
    }
}

impl ReservedJob {
    pub(super) fn spawn(mut self) {
        if let Some(inner) = self.inner.take() {
            spawn(inner);
        }
    }
}

pub(super) fn should_schedule(provider_name: &str, wait_online: bool, outcome: &str) -> bool {
    provider_name == "pterodactyl"
        && !wait_online
        && matches!(
            outcome,
            "StartRequested" | "StartSubmittedConfirmed" | "StartSubmittedUnconfirmed"
        )
}

fn spawn(reserved: ReservedJobInner) {
    let ReservedJobInner {
        state,
        job,
        operation_guard,
    } = reserved;
    tokio::spawn(async move {
        let context = job.context.clone();
        let worker_state = state.clone();
        let worker = tokio::spawn(async move { run(worker_state, job).await });
        if let Err(error) = worker.await {
            let detail = if error.is_panic() {
                "Background verification task panicked"
            } else {
                "Background verification task was cancelled"
            };
            record_terminal(
                &state,
                &context,
                BackgroundVerificationOutcome::MinecraftStatusError,
                detail.to_string(),
                None,
                None,
            )
            .await;
        }
        drop(operation_guard);
    });
}

async fn run(state: BotState, job: Job) {
    let poll = poll_minecraft(&state, &job.minecraft_address, job.max_wait).await;
    let outcome = classify(&poll);
    let elapsed_ms = now_ms().saturating_sub(job.command_started_at_ms);
    let final_minecraft_status = poll
        .online
        .as_ref()
        .or(poll.last_status.as_ref())
        .map(ToString::to_string);
    let detail = verification_detail(outcome, &poll, &job.initial_provider_status, elapsed_ms);
    record_terminal(
        &state,
        &job.context,
        outcome,
        detail,
        Some(job.initial_provider_status),
        final_minecraft_status,
    )
    .await;
}

async fn record_terminal(
    state: &BotState,
    context: &RunContext,
    outcome: BackgroundVerificationOutcome,
    detail: String,
    final_provider_status: Option<String>,
    final_minecraft_status: Option<String>,
) {
    let error_class = outcome.error_class();
    let step = RunStep {
        at_ms: now_ms(),
        step: "minecraft_background_verify".to_string(),
        status: outcome.step_status().to_string(),
        detail: Some(detail.clone()),
        screenshot_path: None,
        error_class: error_class.clone(),
    };

    let apply_result = state
        .run_store
        .apply_follow_up(
            context.clone(),
            step,
            outcome.as_str(),
            final_provider_status,
            final_minecraft_status,
            error_class,
        )
        .await;
    if !apply_result.event_persisted {
        let persistence_error = apply_result
            .event_error
            .as_deref()
            .unwrap_or("unknown event persistence error");
        let memory_result = if apply_result.summary_updated {
            "updated the in-memory run but"
        } else {
            "could not update the in-memory run and"
        };
        terminal::emit(terminal::line_for_context(
            "WARN",
            context,
            format!(
                "{} {} could not persist background verification; error {}",
                context.run_id,
                memory_result,
                terminal::clean(persistence_error)
            ),
        ));
    } else if !apply_result.summary_updated {
        terminal::emit(terminal::line_for_context(
            "WARN",
            context,
            format!(
                "{} background verification event was durable, but the completed summary was unavailable or already finalized",
                context.run_id
            ),
        ));
    }

    terminal::emit(terminal::line_for_context(
        match outcome {
            BackgroundVerificationOutcome::MinecraftOnline => "OK",
            BackgroundVerificationOutcome::StartVerificationInterrupted => "SKIP",
            _ => "WARN",
        },
        context,
        format!(
            "{} {} {}",
            context.run_id,
            outcome.as_str(),
            terminal::clean(&detail)
        ),
    ));
}

async fn poll_minecraft(
    state: &BotState,
    address: &str,
    max_wait: Duration,
) -> MinecraftPollObservation {
    poll_minecraft_with(
        max_wait,
        Duration::from_secs(MINECRAFT_STATUS_POLL_SECS),
        || state.is_shutting_down(),
        || state.shutdown_notified(),
        || minecraft::get_status_for_addr(address),
    )
    .await
}

async fn poll_minecraft_with<Shutdown, ShutdownWait, ShutdownFuture, Probe, ProbeFuture>(
    max_wait: Duration,
    poll_interval: Duration,
    mut is_shutting_down: Shutdown,
    mut wait_for_shutdown: ShutdownWait,
    mut probe: Probe,
) -> MinecraftPollObservation
where
    Shutdown: FnMut() -> bool,
    ShutdownWait: FnMut() -> ShutdownFuture,
    ShutdownFuture: Future<Output = ()>,
    Probe: FnMut() -> ProbeFuture,
    ProbeFuture: Future<Output = anyhow::Result<ServerStatus>>,
{
    let deadline = Instant::now() + max_wait;
    let mut observation = MinecraftPollObservation {
        online: None,
        last_status: None,
        last_error: None,
        successful_probes: 0,
        unreachable_probes: 0,
        interrupted_reason: None,
    };

    loop {
        if is_shutting_down() {
            mark_interrupted(&mut observation);
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let shutdown = wait_for_shutdown();
        tokio::pin!(shutdown);
        if is_shutting_down() {
            mark_interrupted(&mut observation);
            break;
        }
        let probe_result = tokio::select! {
            _ = &mut shutdown => {
                mark_interrupted(&mut observation);
                break;
            }
            result = timeout(remaining, probe()) => result,
        };
        match probe_result {
            Ok(Ok(status @ ServerStatus::Online { .. })) => {
                observation.successful_probes += 1;
                observation.online = Some(status);
                break;
            }
            Ok(Ok(status @ ServerStatus::Unreachable { .. })) => {
                observation.successful_probes += 1;
                observation.unreachable_probes += 1;
                observation.last_status = Some(status);
            }
            Ok(Ok(status)) => {
                observation.successful_probes += 1;
                observation.last_status = Some(status);
            }
            Ok(Err(error)) => observation.last_error = Some(error.to_string()),
            Err(_) => {
                observation.last_error = Some("Minecraft status probe timed out".to_string());
                break;
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let shutdown = wait_for_shutdown();
        tokio::pin!(shutdown);
        if is_shutting_down() {
            mark_interrupted(&mut observation);
            break;
        }
        tokio::select! {
            _ = &mut shutdown => {
                mark_interrupted(&mut observation);
                break;
            }
            _ = sleep(std::cmp::min(poll_interval, remaining)) => {}
        }
    }

    observation
}

fn mark_interrupted(observation: &mut MinecraftPollObservation) {
    observation.interrupted_reason =
        Some("Butler began shutting down during background verification".to_string());
}

fn classify(poll: &MinecraftPollObservation) -> BackgroundVerificationOutcome {
    if poll.online.is_some() {
        return BackgroundVerificationOutcome::MinecraftOnline;
    }
    if poll.interrupted_reason.is_some() {
        return BackgroundVerificationOutcome::StartVerificationInterrupted;
    }
    if poll.successful_probes == 0 && (poll.last_error.is_some() || poll.unreachable_probes > 0) {
        return BackgroundVerificationOutcome::MinecraftStatusError;
    }
    BackgroundVerificationOutcome::StartVerificationTimeout
}

fn safe_provider_status(status: &str) -> String {
    let normalized = status.trim().to_ascii_lowercase();
    if normalized.contains("running") || normalized.contains("online") {
        "Running".to_string()
    } else if normalized.contains("starting")
        || normalized.contains("waking")
        || normalized.contains("queued")
    {
        "Starting".to_string()
    } else if normalized.contains("requested") || normalized.contains("accepted") {
        "Start requested".to_string()
    } else {
        "Unknown".to_string()
    }
}

fn verification_detail(
    outcome: BackgroundVerificationOutcome,
    poll: &MinecraftPollObservation,
    initial_provider_status: &str,
    elapsed_ms: u128,
) -> String {
    let elapsed = terminal::format_duration(elapsed_ms);
    match outcome {
        BackgroundVerificationOutcome::MinecraftOnline => format!(
            "Minecraft became playable {elapsed} after the Discord command began: {}",
            terminal::brief_minecraft_status(
                poll.online
                    .as_ref()
                    .expect("online outcome requires an online status")
            )
        ),
        BackgroundVerificationOutcome::StartVerificationTimeout => format!(
            "Minecraft did not become playable within {elapsed}; last reported status: {}; accepted Play Hosting state: {}",
            poll.last_status
                .as_ref()
                .map(terminal::brief_minecraft_status)
                .unwrap_or_else(|| "unavailable".to_string()),
            initial_provider_status
        ),
        BackgroundVerificationOutcome::MinecraftStatusError if poll.unreachable_probes > 0 => {
            format!(
                "Minecraft status remained unreachable for {elapsed} after the Discord command began"
            )
        }
        BackgroundVerificationOutcome::MinecraftStatusError => {
            format!("Minecraft status checks failed {elapsed} after the Discord command began")
        }
        BackgroundVerificationOutcome::StartVerificationInterrupted => {
            format!(
                "Background verification stopped {elapsed} after the Discord command began: {}",
                poll.interrupted_reason
                    .as_deref()
                    .unwrap_or("verification was interrupted")
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    fn sample_context() -> RunContext {
        RunContext {
            run_id: "abc123".to_string(),
            command: "server.start".to_string(),
            provider: "pterodactyl".to_string(),
            guild_id: Some("guild".to_string()),
            guild_name: "Guild".to_string(),
            channel_id: "channel".to_string(),
            channel_name: Some("ops".to_string()),
            user_id: "user".to_string(),
            user_name: "User".to_string(),
        }
    }

    fn observation(
        online: Option<ServerStatus>,
        last_status: Option<ServerStatus>,
        last_error: Option<&str>,
        successful_probes: usize,
    ) -> MinecraftPollObservation {
        MinecraftPollObservation {
            online,
            last_status,
            last_error: last_error.map(str::to_string),
            successful_probes,
            unreachable_probes: 0,
            interrupted_reason: None,
        }
    }

    #[test]
    fn schedules_only_unwaited_pterodactyl_submissions() {
        assert!(should_schedule("pterodactyl", false, "StartRequested"));
        assert!(should_schedule(
            "pterodactyl",
            false,
            "StartSubmittedConfirmed"
        ));
        assert!(!should_schedule("pterodactyl", true, "StartRequested"));
        assert!(!should_schedule("aternos", false, "StartClicked"));
        assert!(!should_schedule("pterodactyl", false, "AlreadyActive"));
    }

    #[test]
    fn classifies_online_minecraft() {
        let poll = observation(
            Some(ServerStatus::Online {
                online: 1,
                max: 20,
                players: vec!["Player".to_string()],
            }),
            None,
            None,
            1,
        );

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::MinecraftOnline
        );
    }

    #[test]
    fn classifies_offline_minecraft_as_timeout() {
        let poll = observation(None, Some(ServerStatus::Offline), None, 3);

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::StartVerificationTimeout
        );
    }

    #[test]
    fn classifies_unconfirmed_timeout() {
        let poll = observation(None, Some(ServerStatus::Starting), None, 3);

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::StartVerificationTimeout
        );
    }

    #[test]
    fn classifies_unreachable_status_as_timeout() {
        let mut poll = observation(
            None,
            Some(ServerStatus::Unreachable {
                reason: "network detail".to_string(),
            }),
            None,
            1,
        );
        poll.unreachable_probes = 3;

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::StartVerificationTimeout
        );
    }

    #[test]
    fn provider_status_is_allowlisted_before_persistence() {
        let job = Job::new(
            sample_context(),
            "minecraft.example".to_string(),
            "Running secret-sentinel".to_string(),
            Duration::from_secs(10),
            1,
        );

        assert_eq!(job.initial_provider_status, "Running");
        assert!(!job.initial_provider_status.contains("secret-sentinel"));
    }

    #[test]
    fn classifies_status_errors_before_provider_state() {
        let poll = observation(None, None, Some("invalid address"), 0);

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::MinecraftStatusError
        );
    }

    #[test]
    fn classifies_shutdown_as_interrupted() {
        let mut poll = observation(None, Some(ServerStatus::Offline), None, 1);
        poll.interrupted_reason = Some("shutting down".to_string());

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::StartVerificationInterrupted
        );
        assert_eq!(poll.interrupted_reason.as_deref(), Some("shutting down"));
    }

    #[test]
    fn persisted_online_detail_omits_player_names() {
        let poll = observation(
            Some(ServerStatus::Online {
                online: 1,
                max: 20,
                players: vec!["SensitivePlayer".to_string()],
            }),
            None,
            None,
            1,
        );

        let detail = verification_detail(
            BackgroundVerificationOutcome::MinecraftOnline,
            &poll,
            "Running",
            8_000,
        );

        assert!(detail.contains("Online 1/20"));
        assert!(!detail.contains("SensitivePlayer"));
    }

    #[tokio::test(start_paused = true)]
    async fn polling_observes_offline_to_online_transition() {
        let responses = Arc::new(StdMutex::new(VecDeque::from([
            Ok(ServerStatus::Offline),
            Ok(ServerStatus::Starting),
            Ok(ServerStatus::Online {
                online: 1,
                max: 20,
                players: Vec::new(),
            }),
        ])));
        let probe_responses = responses.clone();

        let poll = poll_minecraft_with(
            Duration::from_secs(30),
            Duration::from_secs(5),
            || false,
            std::future::pending::<()>,
            move || {
                let response = probe_responses.lock().unwrap().pop_front().unwrap();
                async move { response }
            },
        )
        .await;

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::MinecraftOnline
        );
        assert_eq!(poll.successful_probes, 3);
        assert!(responses.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn polling_treats_all_unreachable_results_as_timeout() {
        let probe = || async {
            Ok(ServerStatus::Unreachable {
                reason: "raw network error".to_string(),
            })
        };

        let poll = poll_minecraft_with(
            Duration::from_secs(11),
            Duration::from_secs(5),
            || false,
            std::future::pending::<()>,
            probe,
        )
        .await;

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::StartVerificationTimeout
        );
        assert_eq!(poll.successful_probes, 3);
        assert_eq!(poll.unreachable_probes, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn polling_stops_cleanly_on_shutdown() {
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutdown_signal = shutting_down.clone();
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let signal_notify = shutdown_notify.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(6)).await;
            shutdown_signal.store(true, Ordering::Release);
            signal_notify.notify_waiters();
        });
        let shutdown_check = shutting_down.clone();
        let wait_notify = shutdown_notify.clone();

        let poll = poll_minecraft_with(
            Duration::from_secs(30),
            Duration::from_secs(5),
            move || shutdown_check.load(Ordering::Acquire),
            move || wait_notify.clone().notified_owned(),
            std::future::pending::<anyhow::Result<ServerStatus>>,
        )
        .await;

        assert_eq!(
            classify(&poll),
            BackgroundVerificationOutcome::StartVerificationInterrupted
        );
        assert!(poll.interrupted_reason.is_some());
    }
}
