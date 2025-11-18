use anyhow::Result;
use playwright::Playwright;
use tokio::time::{sleep, Duration};

pub async fn start(username: &str, password: &str) -> Result<String> {
    let playwright = Playwright::initialize().await?;
    playwright.prepare()?;

    let browser = playwright
        .chromium()
        .launcher()
        .headless(true)
        .launch()
        .await?;

    let context = browser.context_builder().build().await?;
    let page = context.new_page().await?;

    // 1. Login page
    page.goto_builder("https://aternos.org/go/").goto().await?;

    page.fill_builder("#user", username).fill().await?;
    page.fill_builder("#password", password).fill().await?;
    page.click_builder("#login-button").click().await?;

    // 2. Navigate to server page
    page.goto_builder("https://aternos.org/server/")
        .goto()
        .await?;

    // Sometimes the button takes a moment to load
    page.wait_for_selector_builder(".server-start")
        .wait_for_selector()
        .await?;

    page.click_builder(".server-start").click().await?;

    // Optional: wait a moment for feedback text
    sleep(Duration::from_secs(3)).await;

    Ok("Aternos start command sent successfully".to_string())
}
