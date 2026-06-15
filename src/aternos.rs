use crate::config::Config;
use anyhow::{Result, anyhow};
use headless_chrome::{Browser, LaunchOptions, browser::tab::point::Point};
use rand::Rng;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    StartClicked,
    DashboardChanged,
}

impl std::fmt::Display for StartOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartClicked => write!(f, "StartClicked"),
            Self::DashboardChanged => write!(f, "DashboardChanged"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BrowserStartResult {
    pub outcome: StartOutcome,
    pub dashboard_status: String,
    pub screenshot_path: Option<PathBuf>,
    pub html_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct BrowserStartFailure {
    pub error_class: String,
    pub message: String,
    pub screenshot_path: Option<PathBuf>,
    pub html_path: Option<PathBuf>,
}

impl std::fmt::Display for BrowserStartFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_class, self.message)
    }
}

impl std::error::Error for BrowserStartFailure {}

pub async fn start_browser(
    config: &Config,
    run_id: &str,
) -> Result<BrowserStartResult, BrowserStartFailure> {
    let username = config.aternos_user.clone();
    let password = config.aternos_pass.clone();
    let server_id = config.server_id.clone();
    let headless = config.headless;
    let run_dir = config.artifact_dir.join(run_id);

    tokio::task::spawn_blocking(move || {
        run_browser_start(username, password, server_id, headless, run_dir)
    })
    .await
    .map_err(|error| BrowserStartFailure {
        error_class: "BrowserThreadJoin".to_string(),
        message: error.to_string(),
        screenshot_path: None,
        html_path: None,
    })?
}

fn run_browser_start(
    username: String,
    password: String,
    server_id: Option<String>,
    headless: bool,
    run_dir: PathBuf,
) -> Result<BrowserStartResult, BrowserStartFailure> {
    std::fs::create_dir_all(&run_dir).map_err(|error| BrowserStartFailure {
        error_class: "ArtifactWrite".to_string(),
        message: error.to_string(),
        screenshot_path: None,
        html_path: None,
    })?;

    let args = vec![
        OsStr::new("--disable-blink-features=AutomationControlled"),
        OsStr::new("--disable-notifications"),
        OsStr::new("--window-size=1920,1080"),
        OsStr::new(
            "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ),
    ];

    let options = LaunchOptions {
        headless,
        args,
        ..Default::default()
    };

    let browser = Browser::new(options).map_err(|error| BrowserStartFailure {
        error_class: "BrowserLaunch".to_string(),
        message: error.to_string(),
        screenshot_path: None,
        html_path: None,
    })?;
    let tab = browser.new_tab().map_err(|error| BrowserStartFailure {
        error_class: "BrowserTab".to_string(),
        message: error.to_string(),
        screenshot_path: None,
        html_path: None,
    })?;

    match run_dashboard_flow(&tab, &username, &password, server_id.as_deref(), &run_dir) {
        Ok(result) => Ok(result),
        Err(error) => {
            let screenshot_path = capture_screenshot(&tab, &run_dir, "failure.png").ok();
            let html_path = capture_html(&tab, &run_dir, "failure.html").ok();
            Err(BrowserStartFailure {
                error_class: classify_browser_error(&error),
                message: error.to_string(),
                screenshot_path,
                html_path,
            })
        }
    }
}

fn run_dashboard_flow(
    tab: &headless_chrome::Tab,
    username: &str,
    password: &str,
    server_id: Option<&str>,
    run_dir: &Path,
) -> Result<BrowserStartResult> {
    tab.navigate_to("https://aternos.org/go/")?;
    random_delay();
    sleep(Duration::from_secs(2));
    click_cookie_consent(tab)?;
    dismiss_notification_prompt(tab)?;
    dismiss_page_blockers(tab)?;
    random_delay();
    fail_if_challenge_present(tab)?;

    let user_field = tab.wait_for_element(".username")?;
    random_delay();
    user_field.click()?;
    random_delay();
    user_field.type_into(username)?;
    random_delay();

    let pass_field = tab.wait_for_element(".password")?;
    pass_field.click()?;
    random_delay();
    pass_field.type_into(password)?;
    random_delay();

    tab.wait_for_element(".login-button")?.click()?;
    random_delay();
    dismiss_page_blockers(tab)?;
    fail_if_challenge_present(tab)?;

    if let Some(server_id) = server_id {
        let selector = format!(".servercard[data-id='{server_id}']");
        if let Ok(card) = tab.wait_for_element(&selector) {
            random_delay();
            card.click()?;
        }
    }

    random_delay();
    tab.navigate_to("https://aternos.org/server/")?;
    tab.wait_for_element("#start")?;
    dismiss_notification_prompt(tab)?;
    dismiss_page_blockers(tab)?;
    fail_if_challenge_present(tab)?;

    let mut start_clicked = false;
    let mut dashboard_status = dashboard_status(tab)?;
    let mut accepted = false;

    for _ in 1..=5 {
        dismiss_page_blockers(tab)?;
        fail_if_challenge_present(tab)?;
        let state = dashboard_state(tab)?;
        dashboard_status = state.status.clone();

        if state.accepted {
            accepted = true;
            break;
        }

        if state.start_button_visible {
            scroll_start_into_view(tab)?;
            sleep(Duration::from_millis(500));
            if let Ok(button) = tab.find_element("#start") {
                button.click()?;
                start_clicked = true;
            }
        }

        sleep(Duration::from_secs(5));
        dismiss_notification_prompt(tab)?;
        dismiss_page_blockers(tab)?;
        click_known_dialogs(tab)?;
        sleep(Duration::from_secs(3));
        dismiss_notification_prompt(tab)?;
        dismiss_page_blockers(tab)?;

        let state = dashboard_state(tab)?;
        dashboard_status = state.status.clone();
        if state.accepted {
            accepted = true;
            break;
        }
    }

    if !accepted && !start_clicked {
        if visible_ad_overlay_present(tab)? {
            return Err(anyhow!(
                "AdOverlayBlocked: an advertisement overlay blocked the Aternos dashboard"
            ));
        }
        return Err(anyhow!(
            "StartButtonUnavailable: could not click the Aternos start button"
        ));
    }

    if !accepted {
        if visible_ad_overlay_present(tab)? {
            return Err(anyhow!(
                "AdOverlayBlocked: an advertisement overlay blocked start confirmation"
            ));
        }
        return Err(anyhow!(
            "StartNotAccepted: dashboard still appears offline after clicking start"
        ));
    }

    dismiss_notification_prompt(tab)?;
    sleep(Duration::from_millis(500));

    let screenshot_path = capture_screenshot(tab, run_dir, "dashboard_after_start.png").ok();
    let html_path = capture_html(tab, run_dir, "dashboard_after_start.html").ok();

    Ok(BrowserStartResult {
        outcome: if start_clicked {
            StartOutcome::StartClicked
        } else {
            StartOutcome::DashboardChanged
        },
        dashboard_status,
        screenshot_path,
        html_path,
    })
}

#[derive(Clone, Debug)]
struct DashboardState {
    status: String,
    start_button_visible: bool,
    accepted: bool,
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
            const accepted = (statusText !== 'unknown' && !statusText.includes('Offline')) || !visible;
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

    Ok(DashboardState {
        status: parsed
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string(),
        start_button_visible: parsed
            .get("visible")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        accepted: parsed
            .get("accepted")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
    })
}

fn dashboard_status(tab: &headless_chrome::Tab) -> Result<String> {
    Ok(dashboard_state(tab)?.status)
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
    for _ in 0..3 {
        let dismissed_adblock = dismiss_adblock_prompt(tab)?;
        dismiss_notification_prompt(tab)?;
        click_known_dialogs(tab)?;

        if visible_ad_overlay_present(tab)? {
            click_rewarded_ad_close(tab)?;
            sleep(Duration::from_secs(1));
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
    sleep(Duration::from_millis(300));

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
        sleep(Duration::from_millis(500));
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

fn capture_screenshot(
    tab: &headless_chrome::Tab,
    run_dir: &Path,
    filename: &str,
) -> Result<PathBuf> {
    use headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption;

    std::fs::create_dir_all(run_dir)?;
    let path = run_dir.join(filename);
    let png_data = tab.capture_screenshot(CaptureScreenshotFormatOption::Png, None, None, true)?;
    std::fs::write(&path, png_data)?;
    Ok(path)
}

fn capture_html(tab: &headless_chrome::Tab, run_dir: &Path, filename: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(run_dir)?;
    let path = run_dir.join(filename);
    let content = tab.get_content()?;
    std::fs::write(&path, content)?;
    Ok(path)
}

fn classify_browser_error(error: &anyhow::Error) -> String {
    let message = error.to_string();
    message
        .split_once(':')
        .map(|(class, _)| class.trim().to_string())
        .filter(|class| {
            matches!(
                class.as_str(),
                "ChallengeRequired"
                    | "AdOverlayBlocked"
                    | "StartButtonUnavailable"
                    | "StartNotAccepted"
            )
        })
        .unwrap_or_else(|| "BrowserAutomation".to_string())
}

fn random_delay() {
    let mut rng = rand::thread_rng();
    let delay = rng.gen_range(500..1500);
    sleep(Duration::from_millis(delay));
}
