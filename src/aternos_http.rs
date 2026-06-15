use anyhow::{Context as _, Result};
use regex::Regex;
use reqwest::Client;
use serde_json::Value;

/// Starts an Aternos Minecraft server using only HTTP requests (no headless browser).
///
/// Flow:
/// 1. GET  /go/                     → extract AJAX_TOKEN from the page
/// 2. POST /panel/ajax/account/login.php → authenticate, receive session cookie
/// 3. GET  /server/                 → extract per-server SEC token
/// 4. POST /panel/ajax/start.php    → send the start command
pub async fn start(username: &str, password: &str) -> Result<String> {
    // Build a client that keeps cookies across requests (needed for the session).
    let client = Client::builder()
        .cookie_store(true)
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
             AppleWebKit/537.36 (KHTML, like Gecko) \
             Chrome/120.0.0.0 Safari/537.36",
        )
        .build()?;

    // ── Step 1: Fetch login page & extract CSRF token ──────────────────────
    let login_page = client
        .get("https://aternos.org/go/")
        .send()
        .await?
        .text()
        .await?;

    let token_re = Regex::new(r#"AJAX_TOKEN\s*=\s*"([^"]+)""#)?;
    let token = token_re
        .captures(&login_page)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .context("Could not find AJAX_TOKEN on the Aternos login page. The page structure may have changed.")?;

    // ── Step 2: Login ──────────────────────────────────────────────────────
    let login_resp = client
        .post("https://aternos.org/panel/ajax/account/login.php")
        .form(&[
            ("user", username),
            ("password", password),
            ("token", &token),
        ])
        .send()
        .await?;

    let login_json: Value = login_resp.json().await?;

    if !login_json
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        let error_msg = login_json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown login error");
        anyhow::bail!("Aternos login failed: {}", error_msg);
    }

    // ── Step 3: Navigate to server page & extract SEC token ────────────────
    // If SERVER_ID is set, visit the specific server first so the session
    // knows which server we're targeting.
    if let Ok(server_id) = std::env::var("SERVER_ID") {
        client
            .get(&format!("https://aternos.org/server/{}/", server_id))
            .send()
            .await?;
    }

    let server_page = client
        .get("https://aternos.org/server/")
        .send()
        .await?
        .text()
        .await?;

    let sec_re = Regex::new(r#"SEC\s*=\s*"([^"]+)""#)?;
    let sec = sec_re
        .captures(&server_page)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .context("Could not find SEC token on the Aternos server page. You may not have a server, or the page structure may have changed.")?;

    // ── Step 4: Send the start command ─────────────────────────────────────
    let start_resp = client
        .post("https://aternos.org/panel/ajax/start.php")
        .form(&[("SEC", &sec), ("token", &token)])
        .send()
        .await?;

    let start_json: Value = start_resp.json().await?;

    if start_json
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        Ok("Server start command sent successfully!".to_string())
    } else {
        let error_msg = start_json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");

        // If the server is already running, treat it as a "soft success"
        // rather than an error so the user isn't confused.
        let lower = error_msg.to_lowercase();
        if lower.contains("already") || lower.contains("running") || lower.contains("online") {
            Ok(format!(
                "Server is already running (Aternos said: {})",
                error_msg
            ))
        } else {
            anyhow::bail!("Failed to start server: {}", error_msg);
        }
    }
}
