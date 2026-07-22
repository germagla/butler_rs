use crate::{
    config::{ArtifactCapture, AternosConfig},
    provider::{
        ProviderStartFailure, ProviderStartFuture, ProviderStartResult, ServerStartProvider,
        StartOutcome,
    },
    run_history::{ensure_owner_only_file, mark_run_artifact_dir},
    terminal,
};
use anyhow::{Context, Result, anyhow};
use headless_chrome::{Browser, Element, LaunchOptions, browser::tab::point::Point};
use rand::Rng;
use std::{
    collections::HashMap,
    ffi::OsStr,
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};

const ATERNOS_LOGIN_URL: &str = "https://aternos.org/go/";
const ATERNOS_SERVER_URL: &str = "https://aternos.org/server/";
const ATERNOS_SERVERS_URL: &str = "https://aternos.org/servers/";
const START_BUTTON_SELECTOR: &str = "#start";
const USERNAME_SELECTOR: &str = ".username";
const PASSWORD_SELECTOR: &str = ".password";
const LOGIN_BUTTON_SELECTOR: &str = ".login-button";
const DASHBOARD_SUCCESS_SCREENSHOT: &str = "dashboard_after_start.png";
const DASHBOARD_SUCCESS_HTML: &str = "dashboard_after_start.html";
const DASHBOARD_BEFORE_START_SCREENSHOT: &str = "dashboard_before_start.png";
const FAILURE_SCREENSHOT: &str = "failure.png";
const FAILURE_HTML: &str = "failure.html";
const BROWSER_WINDOW_SIZE: &str = "--window-size=1920,1080";
const BROWSER_USER_AGENT: &str = "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const BROWSER_START_ATTEMPTS: usize = 2;
const START_RETRY_ATTEMPTS: usize = 5;
const DASHBOARD_OPEN_ATTEMPTS: usize = 3;
const DASHBOARD_READY_WAIT_SECS: u64 = 10;
const DASHBOARD_POLL_MS: u64 = 500;
const PAGE_SETTLE_SECS: u64 = 2;
const START_CLICK_SETTLE_MS: u64 = 500;
const START_ACCEPT_WAIT_SECS: u64 = 5;
const DIALOG_SETTLE_SECS: u64 = 3;
const BLOCKER_DISMISS_ATTEMPTS: usize = 3;
const BLOCKER_SETTLE_SECS: u64 = 1;
const ESCAPE_SETTLE_MS: u64 = 300;
const OVERLAY_CLICK_SETTLE_MS: u64 = 500;
const FINAL_CAPTURE_SETTLE_MS: u64 = 500;
const RANDOM_DELAY_MIN_MS: u64 = 500;
const RANDOM_DELAY_MAX_MS: u64 = 1500;

/// Browser-backed Aternos integration adapter.
///
/// The rest of the bot treats this as a server start provider. A future HTTP or
/// first-party provider should preserve this result/error contract instead of
/// leaking provider-specific details into command handling.
pub struct BrowserAternosProvider {
    config: AternosConfig,
    artifact_dir: PathBuf,
    artifact_capture: ArtifactCapture,
}

impl BrowserAternosProvider {
    pub fn new(
        config: AternosConfig,
        artifact_dir: PathBuf,
        artifact_capture: ArtifactCapture,
    ) -> Self {
        Self {
            config,
            artifact_dir,
            artifact_capture,
        }
    }
}

impl ServerStartProvider for BrowserAternosProvider {
    fn name(&self) -> &'static str {
        "aternos"
    }

    fn start<'a>(&'a self, run_id: &'a str) -> ProviderStartFuture<'a> {
        Box::pin(async move {
            let username = self.config.user.clone();
            let password = self.config.password.clone();
            let server_id = self.config.server_id.clone();
            let headless = self.config.headless;
            let chrome_path = self.config.chrome_path.clone();
            let run_dir = self.artifact_dir.join(run_id);
            let artifact_capture = self.artifact_capture;

            tokio::task::spawn_blocking(move || {
                run_browser_start(
                    username,
                    password,
                    server_id,
                    headless,
                    chrome_path,
                    run_dir,
                    artifact_capture,
                )
            })
            .await
            .map_err(|error| ProviderStartFailure {
                error_class: "BrowserThreadJoin".to_string(),
                message: error.to_string(),
                screenshot_path: None,
                detail_artifact_path: None,
                minecraft_address: None,
                start_may_have_been_submitted: false,
            })?
        })
    }
}

fn run_browser_start(
    username: String,
    password: String,
    server_id: Option<String>,
    headless: bool,
    chrome_path: Option<PathBuf>,
    run_dir: PathBuf,
    artifact_capture: ArtifactCapture,
) -> Result<ProviderStartResult, ProviderStartFailure> {
    let context = BrowserStartContext {
        username: &username,
        password: &password,
        server_id: server_id.as_deref(),
        headless,
        chrome_path: chrome_path.as_deref(),
        run_dir: &run_dir,
        artifact_capture,
    };
    run_with_browser_disconnect_retry(
        |capture_retryable_failure_artifacts| {
            run_browser_start_once(&context, capture_retryable_failure_artifacts)
        },
        BROWSER_START_ATTEMPTS,
        |attempt, failure| emit_browser_retry_warning(&run_dir, attempt, failure),
    )
}

struct BrowserStartContext<'a> {
    username: &'a str,
    password: &'a str,
    server_id: Option<&'a str>,
    headless: bool,
    chrome_path: Option<&'a Path>,
    run_dir: &'a Path,
    artifact_capture: ArtifactCapture,
}

fn run_browser_start_once(
    context: &BrowserStartContext<'_>,
    capture_retryable_failure_artifacts: bool,
) -> Result<ProviderStartResult, ProviderStartFailure> {
    let args = vec![
        OsStr::new("--disable-blink-features=AutomationControlled"),
        OsStr::new("--disable-notifications"),
        OsStr::new(BROWSER_WINDOW_SIZE),
        OsStr::new(BROWSER_USER_AGENT),
    ];

    let options = LaunchOptions {
        headless: context.headless,
        path: context.chrome_path.map(Path::to_path_buf),
        args,
        ignore_certificate_errors: false,
        process_envs: Some(HashMap::from([
            ("DISCORD_TOKEN".to_string(), String::new()),
            ("ATERNOS_USER".to_string(), String::new()),
            ("ATERNOS_PASS".to_string(), String::new()),
            ("PTERODACTYL_API_TOKEN".to_string(), String::new()),
        ])),
        ..Default::default()
    };

    let browser = Browser::new(options).map_err(|error| ProviderStartFailure {
        error_class: browser_setup_error_class(&error.to_string(), "BrowserLaunch"),
        message: error.to_string(),
        screenshot_path: None,
        detail_artifact_path: None,
        minecraft_address: None,
        start_may_have_been_submitted: false,
    })?;
    let tab = browser.new_tab().map_err(|error| ProviderStartFailure {
        error_class: browser_setup_error_class(&error.to_string(), "BrowserTab"),
        message: error.to_string(),
        screenshot_path: None,
        detail_artifact_path: None,
        minecraft_address: None,
        start_may_have_been_submitted: false,
    })?;

    match run_dashboard_flow(
        &tab,
        context.username,
        context.password,
        context.server_id,
        context.run_dir,
        context.artifact_capture,
    ) {
        Ok(result) => Ok(result),
        Err(flow_failure) => {
            let mut failure = ProviderStartFailure {
                error_class: classify_browser_error(&flow_failure.error),
                message: browser_failure_message(&flow_failure.error),
                screenshot_path: None,
                detail_artifact_path: None,
                minecraft_address: None,
                start_may_have_been_submitted: false,
            };
            apply_post_click_submission_metadata(&mut failure, flow_failure.start_clicked);
            if !is_retryable_browser_failure(&failure) || capture_retryable_failure_artifacts {
                failure.screenshot_path = capture_screenshot_best_effort(
                    &tab,
                    context.run_dir,
                    FAILURE_SCREENSHOT,
                    context.artifact_capture.capture_failure_screenshot(),
                );
                failure.detail_artifact_path = capture_html_best_effort(
                    &tab,
                    context.run_dir,
                    FAILURE_HTML,
                    context.artifact_capture.capture_failure_html(),
                );
            }
            failure.screenshot_path = failure_screenshot_or_checkpoint(
                failure.screenshot_path,
                failure.start_may_have_been_submitted,
                flow_failure.checkpoint_screenshot_path,
            );
            Err(failure)
        }
    }
}

fn run_dashboard_flow(
    tab: &headless_chrome::Tab,
    username: &str,
    password: &str,
    server_id: Option<&str>,
    run_dir: &Path,
    artifact_capture: ArtifactCapture,
) -> Result<ProviderStartResult, BrowserFlowFailure> {
    let mut start_clicked = false;
    let mut checkpoint_screenshot_path = None;
    macro_rules! try_flow {
        ($expr:expr) => {
            match $expr {
                Ok(value) => value,
                Err(error) => {
                    return Err(BrowserFlowFailure {
                        error: error.into(),
                        start_clicked,
                        checkpoint_screenshot_path,
                    });
                }
            }
        };
    }

    try_flow!(
        tab.navigate_to(ATERNOS_LOGIN_URL)
            .context("LoginPageUnavailable: could not open the Aternos login page")
    );
    random_delay();
    sleep(Duration::from_secs(PAGE_SETTLE_SECS));
    try_flow!(click_cookie_consent(tab));
    try_flow!(dismiss_notification_prompt(tab));
    try_flow!(dismiss_page_blockers(tab));
    random_delay();
    try_flow!(fail_if_challenge_present(tab));

    let user_field = try_flow!(wait_for_browser_element(
        tab,
        USERNAME_SELECTOR,
        "LoginFormUnavailable",
        "username field was not available",
    ));
    random_delay();
    try_flow!(user_field.click());
    random_delay();
    try_flow!(user_field.type_into(username));
    random_delay();

    let pass_field = try_flow!(wait_for_browser_element(
        tab,
        PASSWORD_SELECTOR,
        "LoginFormUnavailable",
        "password field was not available",
    ));
    try_flow!(pass_field.click());
    random_delay();
    try_flow!(pass_field.type_into(password));
    random_delay();

    let login_button = try_flow!(wait_for_browser_element(
        tab,
        LOGIN_BUTTON_SELECTOR,
        "LoginFormUnavailable",
        "login button was not available",
    ));
    try_flow!(login_button.click());
    random_delay();
    try_flow!(dismiss_page_blockers(tab));
    try_flow!(fail_if_challenge_present(tab));

    try_flow!(open_server_dashboard(tab, server_id));
    try_flow!(dismiss_notification_prompt(tab));
    try_flow!(dismiss_page_blockers(tab));
    try_flow!(fail_if_challenge_present(tab));

    let mut dashboard_status = try_flow!(dashboard_status(tab));
    let mut accepted = false;

    for _ in 1..=START_RETRY_ATTEMPTS {
        try_flow!(dismiss_page_blockers(tab));
        try_flow!(fail_if_challenge_present(tab));
        let state = try_flow!(dashboard_state(tab));
        dashboard_status = state.status.clone();

        if state.accepted {
            accepted = true;
            break;
        }

        if state.start_button_visible {
            try_flow!(scroll_start_into_view(tab));
            sleep(Duration::from_millis(START_CLICK_SETTLE_MS));
            if let Ok(button) = tab.find_element(START_BUTTON_SELECTOR) {
                if checkpoint_screenshot_path.is_none() {
                    checkpoint_screenshot_path = capture_screenshot_best_effort(
                        tab,
                        run_dir,
                        DASHBOARD_BEFORE_START_SCREENSHOT,
                        artifact_capture.capture_success_screenshot()
                            || artifact_capture.capture_failure_screenshot(),
                    );
                }
                start_clicked = true;
                try_flow!(button.click());
            }
        }

        sleep(Duration::from_secs(START_ACCEPT_WAIT_SECS));
        try_flow!(dismiss_notification_prompt(tab));
        try_flow!(dismiss_page_blockers(tab));
        try_flow!(click_known_dialogs(tab));
        sleep(Duration::from_secs(DIALOG_SETTLE_SECS));
        try_flow!(dismiss_notification_prompt(tab));
        try_flow!(dismiss_page_blockers(tab));

        let state = try_flow!(dashboard_state(tab));
        dashboard_status = state.status.clone();
        if state.accepted {
            accepted = true;
            break;
        }
    }

    if !accepted && !start_clicked {
        if try_flow!(visible_ad_overlay_present(tab)) {
            return Err(BrowserFlowFailure {
                error: anyhow!(
                    "AdOverlayBlocked: an advertisement overlay blocked the Aternos dashboard"
                ),
                start_clicked,
                checkpoint_screenshot_path,
            });
        }
        return Err(BrowserFlowFailure {
            error: anyhow!("StartButtonUnavailable: could not click the Aternos start button"),
            start_clicked,
            checkpoint_screenshot_path,
        });
    }

    if !accepted {
        if try_flow!(visible_ad_overlay_present(tab)) {
            return Err(BrowserFlowFailure {
                error: anyhow!(
                    "AdOverlayBlocked: an advertisement overlay blocked start confirmation"
                ),
                start_clicked,
                checkpoint_screenshot_path,
            });
        }
        return Err(BrowserFlowFailure {
            error: anyhow!(
                "StartNotAccepted: dashboard still appears offline after clicking start"
            ),
            start_clicked,
            checkpoint_screenshot_path,
        });
    }

    try_flow!(dismiss_notification_prompt(tab));
    sleep(Duration::from_millis(FINAL_CAPTURE_SETTLE_MS));

    let screenshot_path = capture_screenshot_best_effort(
        tab,
        run_dir,
        DASHBOARD_SUCCESS_SCREENSHOT,
        artifact_capture.capture_success_screenshot(),
    );
    let html_path = capture_html_best_effort(
        tab,
        run_dir,
        DASHBOARD_SUCCESS_HTML,
        artifact_capture.capture_success_html(),
    );

    Ok(ProviderStartResult {
        outcome: if start_clicked {
            StartOutcome::StartClicked
        } else {
            StartOutcome::DashboardChanged
        },
        provider_status: dashboard_status,
        minecraft_address: None,
        screenshot_path,
        detail_artifact_path: html_path,
    })
}

#[derive(Debug)]
struct BrowserFlowFailure {
    error: anyhow::Error,
    start_clicked: bool,
    checkpoint_screenshot_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct DashboardState {
    status: String,
    start_button_visible: bool,
    accepted: bool,
}

fn dashboard_state_from_parts(status: &str, start_button_visible: bool) -> DashboardState {
    DashboardState {
        status: status.to_string(),
        start_button_visible,
        accepted: status != "unknown" && (!status.contains("Offline") || !start_button_visible),
    }
}

fn dashboard_state(tab: &headless_chrome::Tab) -> Result<DashboardState> {
    let value = tab.evaluate(
        r#"
        (function() {
            const status = document.querySelector('.statuslabel-label');
            const startBtn = document.querySelector('#start');
            const statusText = status ? status.innerText.trim() : 'unknown';
            const display = startBtn ? window.getComputedStyle(startBtn).display : 'none';
            const visible = !!startBtn && display !== 'none' && startBtn.offsetParent !== null;
            const accepted = statusText !== 'unknown' &&
                (!statusText.includes('Offline') || !visible);
            return JSON.stringify({ status: statusText, visible, accepted });
        })()
        "#,
        false,
    )?;
    let raw = value
        .value
        .as_ref()
        .and_then(|value| value.as_str())
        .unwrap_or("{}");
    let parsed: serde_json::Value = serde_json::from_str(raw)?;

    Ok(dashboard_state_from_parts(
        parsed
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown"),
        parsed
            .get("visible")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
    ))
}

fn dashboard_status(tab: &headless_chrome::Tab) -> Result<String> {
    Ok(dashboard_state(tab)?.status)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DashboardPageState {
    url: String,
    title: String,
    has_start_button: bool,
    start_button_visible: bool,
    has_status_label: bool,
    status_text: String,
    has_server_cards: bool,
    server_card_count: usize,
    has_target_server_card: Option<bool>,
    has_dashboard_server_id: bool,
    dashboard_server_id_matches: Option<bool>,
}

impl DashboardPageState {
    fn dashboard_controls_ready(&self) -> bool {
        self.start_button_visible || (self.has_status_label && !self.has_server_cards)
    }

    fn dashboard_ready_for(&self, server_id: Option<&str>) -> bool {
        self.dashboard_controls_ready()
            && (server_id.is_none() || self.dashboard_server_id_matches == Some(true))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServerCardSelection {
    Configured,
    Only,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DashboardOpenAction {
    Ready,
    ClickServerCard(ServerCardSelection),
    NavigateToServer,
    NavigateToServerPicker,
    Wait,
    Fail(&'static str),
}

fn dashboard_open_action(
    state: &DashboardPageState,
    server_id: Option<&str>,
    final_snapshot: bool,
    allow_single_card_without_id: bool,
) -> DashboardOpenAction {
    if state.dashboard_ready_for(server_id) {
        return DashboardOpenAction::Ready;
    }

    if state.dashboard_controls_ready() && server_id.is_some() {
        return match state.dashboard_server_id_matches {
            Some(false) if final_snapshot => {
                DashboardOpenAction::Fail("dashboard server id did not match configured SERVER_ID")
            }
            None if final_snapshot => {
                DashboardOpenAction::Fail("dashboard server id was not available")
            }
            Some(false) => DashboardOpenAction::NavigateToServerPicker,
            None => DashboardOpenAction::Wait,
            Some(true) => DashboardOpenAction::Ready,
        };
    }

    if state.has_server_cards {
        if server_id.is_some() {
            return if state.has_target_server_card == Some(true) {
                DashboardOpenAction::ClickServerCard(ServerCardSelection::Configured)
            } else if final_snapshot {
                DashboardOpenAction::Fail("configured SERVER_ID was not found on the server picker")
            } else {
                DashboardOpenAction::Wait
            };
        }

        return match state.server_card_count {
            0 => DashboardOpenAction::NavigateToServer,
            1 if allow_single_card_without_id => {
                DashboardOpenAction::ClickServerCard(ServerCardSelection::Only)
            }
            1 => DashboardOpenAction::NavigateToServer,
            _ if final_snapshot => {
                DashboardOpenAction::Fail("multiple server cards; configure SERVER_ID")
            }
            _ => DashboardOpenAction::NavigateToServer,
        };
    }

    DashboardOpenAction::NavigateToServer
}

fn open_server_dashboard(tab: &headless_chrome::Tab, server_id: Option<&str>) -> Result<()> {
    let mut last_error = None;
    let mut last_state = None;

    for _ in 1..=DASHBOARD_OPEN_ATTEMPTS {
        dismiss_notification_prompt(tab)?;
        dismiss_page_blockers(tab)?;
        fail_if_challenge_present(tab)?;

        if handle_current_dashboard_state(tab, server_id, &mut last_error, &mut last_state, false)?
        {
            return Ok(());
        }

        random_delay();
        if let Err(error) = tab.navigate_to(ATERNOS_SERVER_URL) {
            remember_first_error(
                &mut last_error,
                format!("navigation failed: {}", terminal::clean(&error.to_string())),
            );
        }
        sleep(Duration::from_secs(PAGE_SETTLE_SECS));

        dismiss_notification_prompt(tab)?;
        dismiss_page_blockers(tab)?;
        fail_if_challenge_present(tab)?;

        if handle_current_dashboard_state(tab, server_id, &mut last_error, &mut last_state, true)? {
            return Ok(());
        }
    }

    let state = dashboard_page_state(tab, server_id).ok().or(last_state);
    Err(anyhow!(
        "DashboardUnavailable: {}; {}",
        dashboard_terminal_failure_hint(state.as_ref(), server_id),
        dashboard_failure_detail(state.as_ref(), last_error.as_deref())
    ))
}

fn handle_current_dashboard_state(
    tab: &headless_chrome::Tab,
    server_id: Option<&str>,
    last_error: &mut Option<String>,
    last_state: &mut Option<DashboardPageState>,
    allow_single_card_without_id: bool,
) -> Result<bool> {
    let state = match dashboard_page_state(tab, server_id) {
        Ok(state) => state,
        Err(error) => {
            remember_first_error(
                last_error,
                format!(
                    "page inspection failed: {}",
                    terminal::clean(&error.to_string())
                ),
            );
            return Ok(false);
        }
    };
    let action = dashboard_open_action(&state, server_id, false, allow_single_card_without_id);
    *last_state = Some(state.clone());

    match action {
        DashboardOpenAction::Ready => Ok(true),
        DashboardOpenAction::ClickServerCard(selection) => {
            if !click_server_card(tab, selection, server_id)? {
                remember_first_error(last_error, "server card click returned false".to_string());
                return Ok(false);
            }
            random_delay();
            wait_for_dashboard_ready(
                tab,
                server_id,
                DASHBOARD_READY_WAIT_SECS,
                last_error,
                last_state,
                allow_single_card_without_id,
            )
        }
        DashboardOpenAction::NavigateToServer => Ok(false),
        DashboardOpenAction::Wait => wait_for_dashboard_ready(
            tab,
            server_id,
            DASHBOARD_READY_WAIT_SECS,
            last_error,
            last_state,
            allow_single_card_without_id,
        ),
        DashboardOpenAction::NavigateToServerPicker => {
            navigate_to_server_picker(tab, last_error);
            wait_for_dashboard_ready(
                tab,
                server_id,
                DASHBOARD_READY_WAIT_SECS,
                last_error,
                last_state,
                allow_single_card_without_id,
            )
        }
        DashboardOpenAction::Fail(detail) => Err(anyhow!(
            "DashboardUnavailable: {}; {}",
            detail,
            dashboard_failure_detail(Some(&state), last_error.as_deref())
        )),
    }
}

fn wait_for_dashboard_ready(
    tab: &headless_chrome::Tab,
    server_id: Option<&str>,
    timeout_secs: u64,
    last_error: &mut Option<String>,
    last_state: &mut Option<DashboardPageState>,
    allow_single_card_without_id: bool,
) -> Result<bool> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match dashboard_page_state(tab, server_id) {
            Ok(state) => {
                let final_snapshot = std::time::Instant::now() >= deadline;
                let action = dashboard_open_action(
                    &state,
                    server_id,
                    final_snapshot,
                    allow_single_card_without_id,
                );
                *last_state = Some(state.clone());
                match action {
                    DashboardOpenAction::Ready => return Ok(true),
                    DashboardOpenAction::ClickServerCard(selection) => {
                        if !click_server_card(tab, selection, server_id)? {
                            remember_first_error(
                                last_error,
                                "server card click returned false".to_string(),
                            );
                        }
                    }
                    DashboardOpenAction::NavigateToServerPicker => {
                        navigate_to_server_picker(tab, last_error);
                    }
                    DashboardOpenAction::Fail(detail) => {
                        return Err(anyhow!(
                            "DashboardUnavailable: {}; {}",
                            detail,
                            dashboard_failure_detail(Some(&state), last_error.as_deref())
                        ));
                    }
                    DashboardOpenAction::NavigateToServer | DashboardOpenAction::Wait => {}
                }
            }
            Err(error) => {
                remember_first_error(
                    last_error,
                    format!(
                        "page inspection failed: {}",
                        terminal::clean(&error.to_string())
                    ),
                );
            }
        }

        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        sleep(Duration::from_millis(DASHBOARD_POLL_MS));
    }
}

fn navigate_to_server_picker(tab: &headless_chrome::Tab, last_error: &mut Option<String>) {
    if let Err(error) = tab.navigate_to(ATERNOS_SERVERS_URL) {
        remember_first_error(
            last_error,
            format!(
                "server picker navigation failed: {}",
                terminal::clean(&error.to_string())
            ),
        );
    }
    sleep(Duration::from_secs(PAGE_SETTLE_SECS));
}

fn dashboard_page_state(
    tab: &headless_chrome::Tab,
    server_id: Option<&str>,
) -> Result<DashboardPageState> {
    let server_id_json = match server_id {
        Some(server_id) => serde_json::to_string(server_id)?,
        None => "null".to_string(),
    };
    let script = format!(
        r#"
        (function() {{
            const targetServerId = {server_id_json};
            const startButton = document.querySelector('#start');
            const statusLabel = document.querySelector('.statuslabel-label');
            const cards = Array.from(document.querySelectorAll('.servercard'));
            const targetCard = targetServerId === null
                ? null
                : cards.find((card) => card.getAttribute('data-id') === targetServerId);
            const startButtonStyle = startButton ? window.getComputedStyle(startButton) : null;
            const startButtonVisible = !!startButton &&
                startButtonStyle.display !== 'none' &&
                startButtonStyle.visibility !== 'hidden' &&
                startButton.offsetParent !== null;
            const dashboardServerId = typeof lastStatus !== 'undefined' &&
                lastStatus &&
                lastStatus.id
                    ? String(lastStatus.id)
                    : '';
            return JSON.stringify({{
                url: window.location.href,
                title: document.title,
                hasStartButton: !!startButton,
                startButtonVisible,
                hasStatusLabel: !!statusLabel,
                statusText: statusLabel ? statusLabel.innerText.trim() : '',
                hasServerCards: cards.length > 0,
                serverCardCount: cards.length,
                hasTargetServerCard: targetServerId === null ? null : !!targetCard,
                hasDashboardServerId: dashboardServerId !== '',
                dashboardServerIdMatches: targetServerId === null || dashboardServerId === ''
                    ? null
                    : dashboardServerId === targetServerId
            }});
        }})()
        "#
    );
    let value = tab.evaluate(&script, false)?;
    let raw = value
        .value
        .as_ref()
        .and_then(|value| value.as_str())
        .unwrap_or("{}");
    let parsed: serde_json::Value = serde_json::from_str(raw)?;
    let server_card_count = parsed
        .get("serverCardCount")
        .and_then(|value| value.as_u64())
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();

    Ok(DashboardPageState {
        url: parsed
            .get("url")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        title: parsed
            .get("title")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        has_start_button: parsed
            .get("hasStartButton")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        start_button_visible: parsed
            .get("startButtonVisible")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        has_status_label: parsed
            .get("hasStatusLabel")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        status_text: parsed
            .get("statusText")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        has_server_cards: parsed
            .get("hasServerCards")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        server_card_count,
        has_target_server_card: parsed
            .get("hasTargetServerCard")
            .and_then(|value| value.as_bool()),
        has_dashboard_server_id: parsed
            .get("hasDashboardServerId")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        dashboard_server_id_matches: parsed
            .get("dashboardServerIdMatches")
            .and_then(|value| value.as_bool()),
    })
}

fn click_server_card(
    tab: &headless_chrome::Tab,
    selection: ServerCardSelection,
    server_id: Option<&str>,
) -> Result<bool> {
    if matches!(selection, ServerCardSelection::Configured) && server_id.is_none() {
        return Ok(false);
    }
    let selection_json = serde_json::to_string(match selection {
        ServerCardSelection::Configured => "configured",
        ServerCardSelection::Only => "only",
    })?;
    let server_id_json = match server_id {
        Some(server_id) => serde_json::to_string(server_id)?,
        None => "null".to_string(),
    };
    let script = format!(
        r#"
        (function() {{
            const selection = {selection_json};
            const targetServerId = {server_id_json};
            const cards = Array.from(document.querySelectorAll('.servercard'));
            const targetCard = selection === 'configured'
                ? cards.find((card) => card.getAttribute('data-id') === targetServerId)
                : (cards.length === 1 ? cards[0] : null);
            if (!targetCard) return false;
            targetCard.scrollIntoView({{ block: 'center', inline: 'center' }});
            targetCard.click();
            return true;
        }})()
        "#
    );
    let value = tab.evaluate(&script, false)?;
    Ok(value
        .value
        .as_ref()
        .and_then(|value| value.as_bool())
        .unwrap_or(false))
}

fn dashboard_failure_detail(
    state: Option<&DashboardPageState>,
    last_error: Option<&str>,
) -> String {
    let mut parts = Vec::new();
    if let Some(state) = state {
        parts.push(format!("url={}", terminal::quote(&state.url)));
        parts.push(format!("title={}", terminal::quote(&state.title)));
        parts.push(format!("has_server_cards={}", state.has_server_cards));
        parts.push(format!("server_cards={}", state.server_card_count));
        if let Some(has_target_server_card) = state.has_target_server_card {
            parts.push(format!("target_card={has_target_server_card}"));
        }
        parts.push(format!(
            "dashboard_id_present={}",
            state.has_dashboard_server_id
        ));
        if let Some(dashboard_server_id_matches) = state.dashboard_server_id_matches {
            parts.push(format!(
                "dashboard_id_matches={dashboard_server_id_matches}"
            ));
        }
        if !state.status_text.is_empty() {
            parts.push(format!("status={}", terminal::quote(&state.status_text)));
        }
    } else {
        parts.push("page_state=unavailable".to_string());
    }
    if let Some(last_error) = last_error {
        parts.push(format!("last_error={}", terminal::quote(last_error)));
    }
    parts.join("; ")
}

fn dashboard_terminal_failure_hint(
    state: Option<&DashboardPageState>,
    server_id: Option<&str>,
) -> &'static str {
    let Some(state) = state else {
        return "dashboard controls did not load";
    };
    if state.dashboard_controls_ready()
        && server_id.is_some()
        && state.dashboard_server_id_matches == Some(false)
    {
        return "dashboard server id did not match configured SERVER_ID";
    }
    if state.dashboard_controls_ready() && server_id.is_some() && !state.has_dashboard_server_id {
        return "dashboard server id was not available";
    }
    if state.has_server_cards && server_id.is_some() && state.has_target_server_card != Some(true) {
        return "configured SERVER_ID was not found on the server picker";
    }
    if state.has_server_cards && server_id.is_none() && state.server_card_count > 1 {
        return "multiple server cards; configure SERVER_ID";
    }
    "dashboard controls did not load"
}

fn remember_first_error(last_error: &mut Option<String>, error: String) {
    if last_error.is_none() {
        *last_error = Some(error);
    }
}

fn click_cookie_consent(tab: &headless_chrome::Tab) -> Result<()> {
    let _ = tab.evaluate(
        r#"
        const buttons = document.querySelectorAll('button');
        for (const button of buttons) {
            const text = button.innerText || '';
            if (text.includes('Consent') || text.includes('Accept')) {
                button.click();
                break;
            }
        }
        "#,
        false,
    )?;
    Ok(())
}

fn dismiss_notification_prompt(tab: &headless_chrome::Tab) -> Result<()> {
    let _ = tab.evaluate(
        r#"
        (function() {
            const isVisible = (element) => {
                if (!element) return false;
                const style = window.getComputedStyle(element);
                const rect = element.getBoundingClientRect();
                return style.display !== 'none' &&
                    style.visibility !== 'hidden' &&
                    rect.width > 0 &&
                    rect.height > 0;
            };

            const dialogSelectors = [
                '[role="dialog"]',
                '.modal',
                '.modal-content',
                '.swal2-popup',
                '.alert',
                '.alert-body',
                '.bootbox',
                '.dialog',
                '.popup'
            ];

            const dialogs = Array.from(document.querySelectorAll(dialogSelectors.join(',')));
            const dialog = dialogs.find((candidate) => {
                if (!isVisible(candidate)) return false;
                const text = (candidate.innerText || candidate.textContent || '').toLowerCase();
                return text.includes('send you notifications') ||
                    text.includes('notify you when your server is online') ||
                    text.includes('please allow us to send you notifications');
            });

            if (!dialog) return false;

            const dismissVia = (element) => {
                if (!element || !isVisible(element)) return false;
                element.click();
                if (isVisible(dialog)) {
                    if (typeof dialog.close === 'function') {
                        dialog.close();
                    }
                    dialog.remove();
                }
                return true;
            };

            const controls = Array.from(dialog.querySelectorAll('button, .btn, [role="button"], a'));
            const decline = controls.find((control) => {
                const text = (control.innerText || control.textContent || '').trim().toLowerCase();
                return text === 'no' ||
                    text === 'no.' ||
                    text.includes('no thanks') ||
                    text.includes('deny') ||
                    text.includes('later');
            });
            if (dismissVia(decline)) return true;

            const close = dialog.querySelector(
                '[aria-label="Close"], [data-dismiss="modal"], .btn-close, .close, .fa-times, .fa-xmark'
            );
            if (dismissVia(close)) return true;

            if (typeof dialog.close === 'function') {
                dialog.close();
            }
            dialog.remove();
            return true;
        })()
        "#,
        false,
    )?;
    Ok(())
}

fn click_known_dialogs(tab: &headless_chrome::Tab) -> Result<()> {
    let _ = tab.evaluate(
        r#"
        (function() {
            const selectors = ['.alert-body .btn', '.modal .btn', '#confirm', '.btn-success', '.btn-danger', 'button'];
            const buttons = document.querySelectorAll(selectors.join(','));
            for (const btn of buttons) {
                const text = (btn.innerText || '').toLowerCase();
                const isVisible = btn.offsetParent !== null;
                if (!isVisible) continue;
                if (text.includes('yes') || text.includes('confirm') || text.includes('accept') || text.includes('i accept')) {
                    btn.click();
                } else if (
                    text.includes('no thanks') ||
                    text.includes('later') ||
                    text.includes('deny') ||
                    text.includes('block') ||
                    text.includes('close')
                ) {
                    btn.click();
                }
            }
        })()
        "#,
        false,
    )?;
    Ok(())
}

fn dismiss_page_blockers(tab: &headless_chrome::Tab) -> Result<()> {
    for _ in 0..BLOCKER_DISMISS_ATTEMPTS {
        let dismissed_adblock = dismiss_adblock_prompt(tab)?;
        dismiss_notification_prompt(tab)?;
        click_known_dialogs(tab)?;

        if visible_ad_overlay_present(tab)? {
            click_rewarded_ad_close(tab)?;
            sleep(Duration::from_secs(BLOCKER_SETTLE_SECS));
        }

        if !dismissed_adblock && !visible_ad_overlay_present(tab)? {
            break;
        }
    }
    Ok(())
}

fn dismiss_adblock_prompt(tab: &headless_chrome::Tab) -> Result<bool> {
    let value = tab.evaluate(
        r#"
        (function() {
            const isVisible = (element) => {
                if (!element) return false;
                const style = window.getComputedStyle(element);
                const rect = element.getBoundingClientRect();
                return style.display !== 'none' &&
                    style.visibility !== 'hidden' &&
                    rect.width > 0 &&
                    rect.height > 0;
            };

            const controls = Array.from(document.querySelectorAll('button, .btn, [role="button"], a, div'));
            const skip = controls.find((control) => {
                if (!isVisible(control)) return false;
                const text = (control.innerText || control.textContent || '').trim().toLowerCase();
                return text.includes('continue with adblocker anyway') ||
                    text.includes('adblocker anyway');
            });
            if (skip) {
                skip.click();
                return true;
            }
            return false;
        })()
        "#,
        false,
    )?;
    Ok(value
        .value
        .as_ref()
        .and_then(|value| value.as_bool())
        .unwrap_or(false))
}

fn visible_ad_overlay_present(tab: &headless_chrome::Tab) -> Result<bool> {
    let value = tab.evaluate(
        r#"
        (function() {
            const isVisible = (element) => {
                if (!element) return false;
                const style = window.getComputedStyle(element);
                const rect = element.getBoundingClientRect();
                return style.display !== 'none' &&
                    style.visibility !== 'hidden' &&
                    rect.width > window.innerWidth * 0.45 &&
                    rect.height > window.innerHeight * 0.45;
            };

            const selectors = [
                'ins[id*="Aternos_Rewarded_Video"]',
                'iframe[aria-label="Advertisement"]',
                'iframe[title*="ad content" i]'
            ];
            return selectors.some((selector) => {
                return Array.from(document.querySelectorAll(selector)).some(isVisible);
            });
        })()
        "#,
        false,
    )?;
    Ok(value
        .value
        .as_ref()
        .and_then(|value| value.as_bool())
        .unwrap_or(false))
}

fn click_rewarded_ad_close(tab: &headless_chrome::Tab) -> Result<()> {
    let value = tab.evaluate(
        r#"
        (function() {
            const isVisible = (element) => {
                if (!element) return false;
                const style = window.getComputedStyle(element);
                const rect = element.getBoundingClientRect();
                return style.display !== 'none' &&
                    style.visibility !== 'hidden' &&
                    rect.width > 0 &&
                    rect.height > 0;
            };

            const controls = Array.from(document.querySelectorAll('button, [role="button"], a, div, span'));
            const close = controls.find((control) => {
                if (!isVisible(control)) return false;
                const text = (control.innerText || control.textContent || '').trim().toLowerCase();
                const rect = control.getBoundingClientRect();
                return text === 'close' &&
                    rect.top < window.innerHeight * 0.2 &&
                    rect.right > window.innerWidth * 0.55;
            });
            if (close) {
                close.click();
                return JSON.stringify({ clicked: true, width: window.innerWidth, height: window.innerHeight });
            }
            return JSON.stringify({ clicked: false, width: window.innerWidth, height: window.innerHeight });
        })()
        "#,
        false,
    )?;
    let raw = value
        .value
        .as_ref()
        .and_then(|value| value.as_str())
        .unwrap_or("{}");
    let parsed: serde_json::Value = serde_json::from_str(raw)?;
    if parsed
        .get("clicked")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return Ok(());
    }

    let width = parsed
        .get("width")
        .and_then(|value| value.as_f64())
        .unwrap_or(1920.0);
    let height = parsed
        .get("height")
        .and_then(|value| value.as_f64())
        .unwrap_or(937.0);

    tab.press_key("Escape")?;
    sleep(Duration::from_millis(ESCAPE_SETTLE_MS));

    for point in [
        Point {
            x: width * 0.80,
            y: height * 0.115,
        },
        Point {
            x: width * 0.82,
            y: height * 0.115,
        },
        Point {
            x: width * 0.95,
            y: height * 0.06,
        },
    ] {
        tab.click_point(point)?;
        sleep(Duration::from_millis(OVERLAY_CLICK_SETTLE_MS));
        if !visible_ad_overlay_present(tab)? {
            break;
        }
    }

    Ok(())
}

fn scroll_start_into_view(tab: &headless_chrome::Tab) -> Result<()> {
    let _ = tab.evaluate(
        r#"
        const btn = document.querySelector('#start');
        if (btn) btn.scrollIntoView({ block: 'center', inline: 'center' });
        "#,
        false,
    )?;
    Ok(())
}

fn fail_if_challenge_present(tab: &headless_chrome::Tab) -> Result<()> {
    let value = tab.evaluate(
        r#"
        (function() {
            const isVisible = (element) => {
                if (!element) return false;
                const style = window.getComputedStyle(element);
                const rect = element.getBoundingClientRect();
                return style.display !== 'none' &&
                    style.visibility !== 'hidden' &&
                    rect.width > 0 &&
                    rect.height > 0;
            };

            const selectors = [
                'iframe[src*="captcha"]',
                'iframe[src*="challenge"]',
                '.g-recaptcha',
                '[class*="captcha" i]',
                '[id*="captcha" i]',
                '[name="cf-turnstile-response"]',
                '.cf-challenge'
            ];
            return selectors.some((selector) => {
                return Array.from(document.querySelectorAll(selector)).some(isVisible);
            });
        })()
        "#,
        false,
    )?;
    if value
        .value
        .as_ref()
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return Err(anyhow!(
            "ChallengeRequired: Aternos displayed a browser challenge or CAPTCHA"
        ));
    }
    Ok(())
}

fn wait_for_browser_element<'a>(
    tab: &'a headless_chrome::Tab,
    selector: &str,
    error_class: &str,
    detail: &str,
) -> Result<Element<'a>> {
    tab.wait_for_element(selector)
        .with_context(|| format!("{error_class}: {detail}"))
}

fn capture_screenshot_if(
    tab: &headless_chrome::Tab,
    run_dir: &Path,
    filename: &str,
    enabled: bool,
) -> Result<Option<PathBuf>> {
    if !enabled {
        return Ok(None);
    }
    capture_screenshot(tab, run_dir, filename).map(Some)
}

fn capture_screenshot_best_effort(
    tab: &headless_chrome::Tab,
    run_dir: &Path,
    filename: &str,
    enabled: bool,
) -> Option<PathBuf> {
    artifact_capture_result_best_effort(
        capture_screenshot_if(tab, run_dir, filename, enabled),
        run_dir,
        filename,
    )
}

fn capture_html_if(
    tab: &headless_chrome::Tab,
    run_dir: &Path,
    filename: &str,
    enabled: bool,
) -> Result<Option<PathBuf>> {
    if !enabled {
        return Ok(None);
    }
    capture_html(tab, run_dir, filename).map(Some)
}

fn capture_html_best_effort(
    tab: &headless_chrome::Tab,
    run_dir: &Path,
    filename: &str,
    enabled: bool,
) -> Option<PathBuf> {
    artifact_capture_result_best_effort(
        capture_html_if(tab, run_dir, filename, enabled),
        run_dir,
        filename,
    )
}

fn artifact_capture_result_best_effort(
    result: Result<Option<PathBuf>>,
    run_dir: &Path,
    target: &str,
) -> Option<PathBuf> {
    match result {
        Ok(path) => path,
        Err(error) => {
            emit_artifact_warning(run_dir, target, &error);
            None
        }
    }
}

fn capture_screenshot(
    tab: &headless_chrome::Tab,
    run_dir: &Path,
    filename: &str,
) -> Result<PathBuf> {
    use headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption;

    std::fs::create_dir_all(run_dir)
        .with_context(|| format!("ArtifactWrite: could not write screenshot {filename}"))?;
    mark_run_dir_best_effort(run_dir);
    let path = run_dir.join(filename);
    let png_data = tab
        .capture_screenshot(CaptureScreenshotFormatOption::Png, None, None, true)
        .with_context(|| format!("BrowserCapture: could not capture screenshot {filename}"))?;
    std::fs::write(&path, png_data)
        .with_context(|| format!("ArtifactWrite: could not write screenshot {filename}"))?;
    ensure_owner_only_file(&path)
        .with_context(|| format!("ArtifactWrite: could not protect screenshot {filename}"))?;
    Ok(path)
}

fn capture_html(tab: &headless_chrome::Tab, run_dir: &Path, filename: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(run_dir)
        .with_context(|| format!("ArtifactWrite: could not write HTML {filename}"))?;
    mark_run_dir_best_effort(run_dir);
    let path = run_dir.join(filename);
    let content = tab
        .get_content()
        .with_context(|| format!("BrowserCapture: could not capture HTML {filename}"))?;
    std::fs::write(&path, content)
        .with_context(|| format!("ArtifactWrite: could not write HTML {filename}"))?;
    ensure_owner_only_file(&path)
        .with_context(|| format!("ArtifactWrite: could not protect HTML {filename}"))?;
    Ok(path)
}

fn mark_run_dir_best_effort(run_dir: &Path) {
    if let Err(error) = mark_run_artifact_dir(run_dir) {
        emit_artifact_warning(run_dir, ".butler-run", &error);
    }
}

fn emit_artifact_warning(run_dir: &Path, target: &str, error: &anyhow::Error) {
    terminal::emit(terminal::line(
        "WARN",
        "artifacts",
        "",
        "",
        None,
        artifact_warning_message(run_dir, target, error),
    ));
}

fn artifact_warning_message(run_dir: &Path, target: &str, error: &anyhow::Error) -> String {
    let error_class = artifact_warning_error_class(error);
    let action = if error_class == "BrowserCapture" {
        "could not capture"
    } else {
        "could not write"
    };
    let source = if action == "could not capture" {
        " from browser"
    } else {
        ""
    };
    let run_id = run_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("unknown");
    format!("{action} {target}{source} for run {run_id}; error_class {error_class}")
}

fn artifact_warning_error_class(error: &anyhow::Error) -> &'static str {
    if error.chain().any(|cause| {
        cause
            .to_string()
            .to_ascii_lowercase()
            .contains("browsercapture:")
    }) {
        "BrowserCapture"
    } else if error.chain().any(|cause| {
        cause
            .to_string()
            .to_ascii_lowercase()
            .contains("artifactwrite:")
    }) {
        "ArtifactWrite"
    } else {
        "ArtifactWarning"
    }
}

fn run_with_browser_disconnect_retry<F, W>(
    mut run_once: F,
    max_attempts: usize,
    mut warn_retry: W,
) -> Result<ProviderStartResult, ProviderStartFailure>
where
    F: FnMut(bool) -> Result<ProviderStartResult, ProviderStartFailure>,
    W: FnMut(usize, &ProviderStartFailure),
{
    let max_attempts = max_attempts.max(1);
    let mut attempt = 1;
    loop {
        let final_attempt = attempt >= max_attempts;
        match run_once(final_attempt) {
            Ok(result) => return Ok(result),
            Err(failure) if attempt < max_attempts && is_retryable_browser_failure(&failure) => {
                warn_retry(attempt, &failure);
                attempt += 1;
            }
            Err(mut failure) => {
                if is_retryable_browser_failure(&failure) && attempt >= max_attempts {
                    annotate_browser_retry_exhausted(&mut failure, max_attempts);
                }
                return Err(failure);
            }
        }
    }
}

fn browser_setup_error_class(message: &str, fallback: &str) -> String {
    if is_browser_connection_closed_text(message) {
        "BrowserConnectionClosed".to_string()
    } else if is_browser_event_timeout_text(message) {
        "BrowserEventTimeout".to_string()
    } else {
        fallback.to_string()
    }
}

fn is_retryable_browser_failure(failure: &ProviderStartFailure) -> bool {
    if failure.start_may_have_been_submitted {
        return false;
    }
    match failure.error_class.as_str() {
        "BrowserConnectionClosed" | "BrowserEventTimeout" => true,
        "BrowserConnectionClosedAfterStartClick" => false,
        _ => is_ambiguous_browser_failure(failure),
    }
}

fn is_ambiguous_browser_failure(failure: &ProviderStartFailure) -> bool {
    is_browser_connection_closed_text(&failure.message)
        || is_browser_event_timeout_text(&failure.message)
        || matches!(
            failure.error_class.as_str(),
            "BrowserConnectionClosed" | "BrowserEventTimeout"
        )
}

fn apply_post_click_submission_metadata(failure: &mut ProviderStartFailure, start_clicked: bool) {
    if !start_clicked || !is_ambiguous_browser_failure(failure) {
        return;
    }
    failure.start_may_have_been_submitted = true;
    if failure.error_class == "BrowserConnectionClosed" {
        failure.error_class = "BrowserConnectionClosedAfterStartClick".to_string();
    }
    if !failure
        .message
        .contains("start click may have been submitted")
    {
        failure.message = format!("{}; start click may have been submitted", failure.message);
    }
}

fn failure_screenshot_or_checkpoint(
    failure_screenshot_path: Option<PathBuf>,
    start_may_have_been_submitted: bool,
    checkpoint_screenshot_path: Option<PathBuf>,
) -> Option<PathBuf> {
    if failure_screenshot_path.is_none() && start_may_have_been_submitted {
        checkpoint_screenshot_path
    } else {
        failure_screenshot_path
    }
}

fn is_browser_connection_closed_text(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("underlying connection is closed")
        || message.contains("connection is closed")
        || message.contains("connection closed")
        || message.contains("target closed")
        || message.contains("browser closed")
        || message.contains("browser has been closed")
        || message.contains("browser has disconnected")
        || message.contains("channel closed")
        || (message.contains("websocket") && message.contains("closed"))
}

fn is_browser_event_timeout_text(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("the event waited for never came")
}

fn annotate_browser_retry_exhausted(failure: &mut ProviderStartFailure, max_attempts: usize) {
    if is_browser_event_timeout_text(&failure.message) {
        failure.error_class = "BrowserEventTimeout".to_string();
    } else {
        failure.error_class = "BrowserConnectionClosed".to_string();
    }
    if !failure.message.contains("after ") {
        failure.message = format!("{} (after {max_attempts} attempts)", failure.message);
    }
}

fn emit_browser_retry_warning(run_dir: &Path, attempt: usize, failure: &ProviderStartFailure) {
    terminal::emit(terminal::line(
        "WARN",
        "aternos",
        "",
        "",
        None,
        format!(
            "{} browser automation lost control on attempt {}; retrying; class {}",
            run_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("run"),
            attempt,
            terminal::clean(&failure.error_class)
        ),
    ));
}

fn classify_browser_error(error: &anyhow::Error) -> String {
    if browser_error_has_connection_closed(error) {
        return "BrowserConnectionClosed".to_string();
    }
    if browser_error_has_event_timeout(error) {
        return "BrowserEventTimeout".to_string();
    }

    error
        .to_string()
        .split_once(':')
        .map(|(class, _)| class.trim().to_string())
        .filter(|class| {
            matches!(
                class.as_str(),
                "LoginPageUnavailable"
                    | "LoginFormUnavailable"
                    | "DashboardUnavailable"
                    | "ChallengeRequired"
                    | "AdOverlayBlocked"
                    | "StartButtonUnavailable"
                    | "StartNotAccepted"
                    | "BrowserConnectionClosed"
                    | "BrowserEventTimeout"
                    | "ArtifactWrite"
            )
        })
        .unwrap_or_else(|| "BrowserAutomation".to_string())
}

fn browser_error_has_connection_closed(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| is_browser_connection_closed_text(&cause.to_string()))
}

fn browser_error_has_event_timeout(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| is_browser_event_timeout_text(&cause.to_string()))
}

fn browser_failure_message(error: &anyhow::Error) -> String {
    let message = error.to_string();
    let has_known_browser_failure =
        browser_error_has_connection_closed(error) || browser_error_has_event_timeout(error);
    if !has_known_browser_failure
        || is_browser_connection_closed_text(&message)
        || is_browser_event_timeout_text(&message)
    {
        return message;
    }

    let source = error
        .chain()
        .find_map(|cause| {
            let cause = cause.to_string();
            (is_browser_connection_closed_text(&cause) || is_browser_event_timeout_text(&cause))
                .then_some(cause)
        })
        .unwrap_or_else(|| "browser connection closed".to_string());
    format!("{message}; source: {source}")
}

fn random_delay() {
    let mut rng = rand::thread_rng();
    let delay = rng.gen_range(RANDOM_DELAY_MIN_MS..RANDOM_DELAY_MAX_MS);
    sleep(Duration::from_millis(delay));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn artifact_capture_errors_are_best_effort() {
        let run_dir = std::env::temp_dir().join("butler_rs_best_effort");
        let path = artifact_capture_result_best_effort(
            Err(anyhow!("permission denied")),
            &run_dir,
            DASHBOARD_SUCCESS_SCREENSHOT,
        );

        assert_eq!(path, None);
    }

    #[test]
    fn dashboard_state_does_not_accept_unknown_hidden_start_button() {
        let unknown_hidden = dashboard_state_from_parts("unknown", false);
        assert!(!unknown_hidden.accepted);

        let offline_hidden = dashboard_state_from_parts("Offline", false);
        assert!(offline_hidden.accepted);

        let offline_visible = dashboard_state_from_parts("Offline", true);
        assert!(!offline_visible.accepted);
    }

    #[test]
    fn dashboard_page_state_is_ready_when_dashboard_controls_exist() {
        assert!(
            DashboardPageState {
                has_start_button: true,
                start_button_visible: true,
                has_server_cards: true,
                ..DashboardPageState::default()
            }
            .dashboard_controls_ready()
        );
        assert!(
            !DashboardPageState {
                has_start_button: true,
                start_button_visible: false,
                ..DashboardPageState::default()
            }
            .dashboard_controls_ready()
        );
        assert!(
            DashboardPageState {
                has_status_label: true,
                ..DashboardPageState::default()
            }
            .dashboard_ready_for(None)
        );
        assert!(
            !DashboardPageState {
                has_status_label: true,
                has_server_cards: true,
                server_card_count: 2,
                ..DashboardPageState::default()
            }
            .dashboard_controls_ready()
        );
        assert!(!DashboardPageState::default().dashboard_controls_ready());
        assert!(
            !DashboardPageState {
                has_start_button: true,
                start_button_visible: true,
                has_dashboard_server_id: true,
                dashboard_server_id_matches: Some(false),
                ..DashboardPageState::default()
            }
            .dashboard_ready_for(Some("server-1"))
        );
        assert!(
            DashboardPageState {
                has_start_button: true,
                start_button_visible: true,
                has_dashboard_server_id: true,
                dashboard_server_id_matches: Some(true),
                ..DashboardPageState::default()
            }
            .dashboard_ready_for(Some("server-1"))
        );
    }

    #[test]
    fn dashboard_open_action_selects_expected_next_step() {
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_start_button: true,
                    start_button_visible: true,
                    has_dashboard_server_id: true,
                    dashboard_server_id_matches: Some(true),
                    ..DashboardPageState::default()
                },
                Some("server-1"),
                false,
                false,
            ),
            DashboardOpenAction::Ready
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_status_label: true,
                    ..DashboardPageState::default()
                },
                None,
                false,
                false,
            ),
            DashboardOpenAction::Ready
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_start_button: true,
                    start_button_visible: true,
                    has_dashboard_server_id: true,
                    dashboard_server_id_matches: Some(false),
                    ..DashboardPageState::default()
                },
                Some("server-1"),
                false,
                false,
            ),
            DashboardOpenAction::NavigateToServerPicker
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_start_button: true,
                    start_button_visible: true,
                    has_dashboard_server_id: false,
                    ..DashboardPageState::default()
                },
                Some("server-1"),
                false,
                false,
            ),
            DashboardOpenAction::Wait
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_start_button: true,
                    start_button_visible: true,
                    has_dashboard_server_id: true,
                    dashboard_server_id_matches: Some(false),
                    ..DashboardPageState::default()
                },
                Some("server-1"),
                true,
                false,
            ),
            DashboardOpenAction::Fail("dashboard server id did not match configured SERVER_ID")
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_server_cards: true,
                    server_card_count: 2,
                    has_target_server_card: Some(true),
                    ..DashboardPageState::default()
                },
                Some("server-1"),
                false,
                false,
            ),
            DashboardOpenAction::ClickServerCard(ServerCardSelection::Configured)
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_server_cards: true,
                    server_card_count: 2,
                    has_target_server_card: Some(false),
                    ..DashboardPageState::default()
                },
                Some("server-1"),
                false,
                false,
            ),
            DashboardOpenAction::Wait
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_server_cards: true,
                    server_card_count: 2,
                    has_target_server_card: Some(false),
                    ..DashboardPageState::default()
                },
                Some("server-1"),
                true,
                false,
            ),
            DashboardOpenAction::Fail("configured SERVER_ID was not found on the server picker")
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_server_cards: true,
                    server_card_count: 1,
                    ..DashboardPageState::default()
                },
                None,
                false,
                true,
            ),
            DashboardOpenAction::ClickServerCard(ServerCardSelection::Only)
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_server_cards: true,
                    server_card_count: 2,
                    ..DashboardPageState::default()
                },
                None,
                false,
                true,
            ),
            DashboardOpenAction::NavigateToServer
        );
        assert_eq!(
            dashboard_open_action(
                &DashboardPageState {
                    has_server_cards: true,
                    server_card_count: 2,
                    ..DashboardPageState::default()
                },
                None,
                true,
                true,
            ),
            DashboardOpenAction::Fail("multiple server cards; configure SERVER_ID")
        );
    }

    #[test]
    fn dashboard_failure_detail_includes_page_context() {
        let detail = dashboard_failure_detail(
            Some(&DashboardPageState {
                url: "https://aternos.org/servers/".to_string(),
                title: "Servers | Aternos".to_string(),
                has_server_cards: true,
                server_card_count: 2,
                has_target_server_card: Some(true),
                ..DashboardPageState::default()
            }),
            Some("navigation failed"),
        );

        assert!(detail.contains("url=\"https://aternos.org/servers/\""));
        assert!(detail.contains("title=\"Servers | Aternos\""));
        assert!(detail.contains("has_server_cards=true"));
        assert!(detail.contains("server_cards=2"));
        assert!(detail.contains("target_card=true"));
        assert!(detail.contains("last_error=\"navigation failed\""));
    }

    #[test]
    fn remember_first_error_preserves_navigation_failure() {
        let mut last_error = None;
        remember_first_error(&mut last_error, "navigation failed".to_string());
        remember_first_error(&mut last_error, "page inspection failed".to_string());

        assert_eq!(last_error.as_deref(), Some("navigation failed"));
    }

    #[test]
    fn browser_disconnect_text_is_retryable() {
        let failure = ProviderStartFailure {
            error_class: "BrowserAutomation".to_string(),
            message: "Unable to make method calls because underlying connection is closed"
                .to_string(),
            screenshot_path: None,
            detail_artifact_path: None,
            minecraft_address: None,
            start_may_have_been_submitted: false,
        };

        assert!(is_retryable_browser_failure(&failure));
        assert_eq!(
            browser_setup_error_class(&failure.message, "BrowserTab"),
            "BrowserConnectionClosed"
        );
    }

    #[test]
    fn browser_error_classification_inspects_source_chain() {
        let error = anyhow!("Unable to make method calls because underlying connection is closed")
            .context("DashboardUnavailable: could not open the Aternos dashboard");

        assert_eq!(classify_browser_error(&error), "BrowserConnectionClosed");
        assert!(browser_failure_message(&error).contains("source: Unable to make method calls"));
    }

    #[test]
    fn browser_event_timeout_is_classified_and_retryable_before_click() {
        let error = anyhow!("The event waited for never came");
        let failure = provider_failure("BrowserEventTimeout", "The event waited for never came");

        assert_eq!(classify_browser_error(&error), "BrowserEventTimeout");
        assert!(is_retryable_browser_failure(&failure));
    }

    #[test]
    fn browser_disconnect_retry_succeeds_on_second_attempt() {
        let mut attempts = VecDeque::new();
        attempts.push_back(Err(provider_failure(
            "BrowserConnectionClosed",
            "Unable to make method calls because underlying connection is closed",
        )));
        attempts.push_back(Ok(provider_success()));
        let mut retry_warnings = 0;

        let result = run_with_browser_disconnect_retry(
            |_| attempts.pop_front().expect("attempt available"),
            2,
            |_, _| retry_warnings += 1,
        )
        .unwrap();

        assert_eq!(result.outcome, StartOutcome::StartClicked);
        assert_eq!(retry_warnings, 1);
        assert!(attempts.is_empty());
    }

    #[test]
    fn browser_disconnect_retry_exhaustion_is_classified() {
        let mut attempts = VecDeque::new();
        attempts.push_back(Err(provider_failure(
            "BrowserAutomation",
            "Unable to make method calls because underlying connection is closed",
        )));
        attempts.push_back(Err(provider_failure(
            "BrowserAutomation",
            "Unable to make method calls because underlying connection is closed",
        )));
        let mut retry_warnings = 0;

        let failure = run_with_browser_disconnect_retry(
            |_| attempts.pop_front().expect("attempt available"),
            2,
            |_, _| retry_warnings += 1,
        )
        .unwrap_err();

        assert_eq!(failure.error_class, "BrowserConnectionClosed");
        assert!(failure.message.contains("after 2 attempts"));
        assert_eq!(retry_warnings, 1);
        assert!(attempts.is_empty());
    }

    #[test]
    fn non_retryable_failures_do_not_retry() {
        for error_class in [
            "DashboardUnavailable",
            "StartNotAccepted",
            "ChallengeRequired",
            "BrowserConnectionClosedAfterStartClick",
        ] {
            let message = if error_class == "BrowserConnectionClosedAfterStartClick" {
                "Unable to make method calls because underlying connection is closed"
            } else {
                "not retryable"
            };
            let failure = provider_failure(error_class, message);
            let mut calls = 0;

            let result = run_with_browser_disconnect_retry(
                |_| {
                    calls += 1;
                    Err(failure.clone())
                },
                2,
                |_, _| panic!("unexpected retry for {error_class}"),
            )
            .unwrap_err();

            assert_eq!(result.error_class, error_class);
            assert_eq!(calls, 1);
        }
    }

    #[test]
    fn retry_wrapper_marks_only_final_attempt_for_artifact_capture() {
        let mut final_attempts = Vec::new();

        let failure = run_with_browser_disconnect_retry(
            |final_attempt| {
                final_attempts.push(final_attempt);
                Err(provider_failure(
                    "BrowserConnectionClosed",
                    "Unable to make method calls because underlying connection is closed",
                ))
            },
            2,
            |_, _| {},
        )
        .unwrap_err();

        assert_eq!(failure.error_class, "BrowserConnectionClosed");
        assert_eq!(final_attempts, vec![false, true]);
    }

    #[test]
    fn retry_wrapper_retries_event_timeout_before_click() {
        let mut attempts = VecDeque::new();
        attempts.push_back(Err(provider_failure(
            "BrowserEventTimeout",
            "The event waited for never came",
        )));
        attempts.push_back(Ok(provider_success()));
        let mut retry_warnings = 0;

        let result = run_with_browser_disconnect_retry(
            |_| attempts.pop_front().expect("attempt available"),
            2,
            |_, _| retry_warnings += 1,
        )
        .unwrap();

        assert_eq!(result.outcome, StartOutcome::StartClicked);
        assert_eq!(retry_warnings, 1);
    }

    #[test]
    fn post_click_ambiguous_failure_is_not_retryable() {
        let mut failure =
            provider_failure("BrowserEventTimeout", "The event waited for never came");
        failure.start_may_have_been_submitted = true;

        assert!(!is_retryable_browser_failure(&failure));
    }

    #[test]
    fn post_click_metadata_marks_ambiguous_failures() {
        let mut event_timeout =
            provider_failure("BrowserEventTimeout", "The event waited for never came");
        apply_post_click_submission_metadata(&mut event_timeout, true);
        assert!(event_timeout.start_may_have_been_submitted);
        assert_eq!(event_timeout.error_class, "BrowserEventTimeout");
        assert!(
            event_timeout
                .message
                .contains("start click may have been submitted")
        );

        let mut connection_closed = provider_failure(
            "BrowserConnectionClosed",
            "Unable to make method calls because underlying connection is closed",
        );
        apply_post_click_submission_metadata(&mut connection_closed, true);
        assert!(connection_closed.start_may_have_been_submitted);
        assert_eq!(
            connection_closed.error_class,
            "BrowserConnectionClosedAfterStartClick"
        );
    }

    #[test]
    fn checkpoint_screenshot_fills_missing_post_click_failure_capture() {
        let checkpoint = Some(PathBuf::from("dashboard_before_start.png"));

        assert_eq!(
            failure_screenshot_or_checkpoint(None, true, checkpoint.clone()),
            checkpoint
        );
        assert_eq!(
            failure_screenshot_or_checkpoint(
                Some(PathBuf::from("failure.png")),
                true,
                Some(PathBuf::from("dashboard_before_start.png")),
            ),
            Some(PathBuf::from("failure.png"))
        );
        assert_eq!(
            failure_screenshot_or_checkpoint(None, false, Some(PathBuf::from("checkpoint.png"))),
            None
        );
    }

    #[test]
    fn artifact_warning_distinguishes_browser_capture_from_write() {
        let run_dir = PathBuf::from("artifacts/runs/example");
        let browser_error =
            anyhow!("BrowserCapture: could not capture screenshot failure.png: raw cdp detail");
        let write_error =
            anyhow!("ArtifactWrite: could not write screenshot failure.png: permission denied");

        let browser_message = artifact_warning_message(&run_dir, "failure.png", &browser_error);
        assert!(browser_message.contains("could not capture failure.png from browser"));
        assert!(browser_message.contains("for run example"));
        assert!(browser_message.contains("error_class BrowserCapture"));
        assert!(!browser_message.contains("raw cdp detail"));

        let write_message = artifact_warning_message(&run_dir, "failure.png", &write_error);
        assert!(write_message.contains("could not write failure.png"));
        assert!(write_message.contains("for run example"));
        assert!(write_message.contains("error_class ArtifactWrite"));
        assert!(!write_message.contains("permission denied"));
    }

    fn provider_success() -> ProviderStartResult {
        ProviderStartResult {
            outcome: StartOutcome::StartClicked,
            provider_status: "Starting".to_string(),
            minecraft_address: None,
            screenshot_path: None,
            detail_artifact_path: None,
        }
    }

    fn provider_failure(error_class: &str, message: &str) -> ProviderStartFailure {
        ProviderStartFailure {
            error_class: error_class.to_string(),
            message: message.to_string(),
            screenshot_path: None,
            detail_artifact_path: None,
            minecraft_address: None,
            start_may_have_been_submitted: false,
        }
    }
}
