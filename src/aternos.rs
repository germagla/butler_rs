use anyhow::Result;
use headless_chrome::{Browser, LaunchOptions};
use std::time::Duration;
use std::thread::sleep;
use rand::Rng;
use std::ffi::OsStr;

pub async fn start(username: &str, password: &str) -> Result<String> {
    let mut args = Vec::new();
    args.push(OsStr::new("--disable-blink-features=AutomationControlled"));
    args.push(OsStr::new("--window-size=1920,1080"));
    args.push(OsStr::new("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));

    let options = LaunchOptions {
        headless: true,
        args: args,
        ..Default::default()
    };
    let browser = Browser::new(options)?;
    let tab = browser.new_tab()?;

    let run = || -> Result<()> {
        // Helper for random delays
        let random_delay = || {
            let mut rng = rand::thread_rng();
            let delay = rng.gen_range(500..1500);
            sleep(Duration::from_millis(delay));
        };

        // 1. Login page
        tab.navigate_to("https://aternos.org/go/")?;
        random_delay();

        // Try to handle cookie consent if present (best effort)
        // Common selector for CMP or just wait a bit
        sleep(Duration::from_secs(2));
        
        // Check if we are on the login page or if there is a cookie banner blocking
        // We can try to find a button with text "Consent" or "Accept" using JS
        let _ = tab.evaluate(r#"
            const buttons = document.querySelectorAll('button');
            for (const button of buttons) {
                if (button.innerText.includes('Consent') || button.innerText.includes('Accept')) {
                    button.click();
                    break;
                }
            }
        "#, false);
        
        random_delay();
        
        // Check for cookie error
        if tab.find_element(".go-cookie-error").is_ok() {
             println!("⚠️ Warning: Cookie error detected on login page. This might cause issues.");
        }

        println!("Waiting for username field...");
        let user_field = tab.wait_for_element(".username")?;
        random_delay();

        println!("Clicking username field...");
        user_field.click()?;
        random_delay();
        println!("Typing username...");
        user_field.type_into(username)?;
        random_delay();

        println!("Waiting for password field...");
        let pass_field = tab.wait_for_element(".password")?;
        pass_field.click()?;
        random_delay();
        println!("Typing password...");
        pass_field.type_into(password)?;
        random_delay();

        println!("Clicking login button...");
        let login_btn = tab.wait_for_element(".login-button")?;
        login_btn.click()?;
        
        // 2. Select server if SERVER_ID is set
        if let Ok(server_id) = std::env::var("SERVER_ID") {
             println!("Selecting server with ID: {}", server_id);
             random_delay();
             let selector = format!(".servercard[data-id='{}']", server_id);
             tab.wait_for_element(&selector)?;
             let server_card = tab.find_element(&selector)?;
             server_card.click()?;
        }

        // 3. Navigate to server page (if not already there)
        // If we clicked a server card, we should be redirected.
        // If we didn't (e.g. only one server), we might need to go there manually or we might already be there.
        // Let's wait a bit and check if we need to navigate.
        random_delay();
        
        // Explicitly go to server page to be safe, or just wait for the start button.
        // If we are on the server list, clicking the card takes us to /server/
        
        // Let's try to go to /server/ directly to ensure we are on the right page
        tab.navigate_to("https://aternos.org/server/")?;
        tab.wait_for_element("#start")?;

        // Retry loop for starting the server
        let max_retries = 5;
        for attempt in 1..=max_retries {
            println!("🔄 Start attempt {}/{}", attempt, max_retries);

            // 1. Check server status and Start button visibility
            let state_check = tab.evaluate(r#"
                (function() {
                    const status = document.querySelector('.statuslabel-label');
                    const startBtn = document.querySelector('#start');
                    
                    const statusText = status ? status.innerText.trim() : 'unknown';
                    const btnDisplay = startBtn ? window.getComputedStyle(startBtn).display : 'unknown';
                    
                    return {
                        status: statusText,
                        btn_display: btnDisplay
                    };
                })()
            "#, false)?;
            
            let state_val = state_check.value.as_ref().expect("Failed to get state value");
            let status_text = state_val.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");
            let btn_display = state_val.get("btn_display").and_then(|v| v.as_str()).unwrap_or("unknown");

            println!("Current server status: '{}', Start button display: '{}'", status_text, btn_display);

            // Success conditions:
            // 1. Status is NOT "Offline" and NOT "unknown" (e.g. "Starting", "Queue", "Loading")
            // 2. Start button is hidden (display: none) which usually happens after clicking
            if (!status_text.contains("Offline") && status_text != "unknown") || btn_display == "none" {
                println!("✅ Server start initiated! (Status: {}, Button hidden: {})", status_text, btn_display == "none");
                break;
            }

            // 2. Click Start (CDP Click with Scroll)
            if btn_display != "none" {
                println!("Clicking start button (CDP)...");
                
                // Scroll into view first
                let _ = tab.evaluate(r#"
                    const btn = document.querySelector('#start');
                    if (btn) {
                        btn.scrollIntoView({block: 'center', inline: 'center'});
                        btn.style.border = '5px solid blue';
                    }
                "#, false);
                
                sleep(Duration::from_millis(500)); // Wait for scroll

                match tab.find_element("#start") {
                    Ok(btn) => {
                        if let Err(e) = btn.click() {
                            println!("❌ Failed to click button via CDP: {}", e);
                        } else {
                            println!("✅ CDP Click sent.");
                        }
                    },
                    Err(e) => println!("❌ Could not find #start element for CDP click: {}", e),
                }
            } else {
                println!("Start button is hidden, skipping click.");
            }

            // 3. Wait for reaction
            sleep(Duration::from_secs(5));

            // 4. Handle Popups (Notifications, Confirmations, Queue)
            println!("Checking for popups...");
            let popup_result = tab.evaluate(r#"
                (function() {
                    let clicked = [];
                    
                    // Selectors for various popups
                    const selectors = [
                        '.alert-body .btn', 
                        '.modal .btn', 
                        '#confirm', 
                        '.btn-success',
                        '.btn-danger',
                        'button' // Generic fallback for notification popups
                    ];
                    
                    const buttons = document.querySelectorAll(selectors.join(','));
                    for (const btn of buttons) {
                        const text = btn.innerText.toLowerCase();
                        const isVisible = btn.offsetParent !== null;
                        
                        if (!isVisible) continue;

                        // Confirmations (Yes, Confirm, I accept)
                        if (text.includes('yes') || text.includes('confirm') || text.includes('accept') || text.includes('i accept')) {
                            btn.click();
                            clicked.push('Confirmed: ' + btn.innerText);
                        }
                        // Notification / Ad Dismissals
                        // "Please allow us to send you notifications" often has "Continue" or similar, 
                        // but we want to block/close. Sometimes it's just a generic close 'x' or "No thanks".
                        else if (text.includes('no thanks') || text.includes('later') || text.includes('deny') || text.includes('block') || text.includes('close') || text.includes('continue')) {
                             // Be careful with "continue", only click if it looks like a dismissal or the only way forward
                             // For notifications, "Continue" might trigger the browser prompt. 
                             // Let's prioritize "No thanks", "Block", "Deny".
                             if (!text.includes('continue')) {
                                btn.click();
                                clicked.push('Dismissed: ' + btn.innerText);
                             }
                        }
                    }
                    return clicked.join(', ');
                })()
            "#, false)?;
            
            if let Some(actions) = popup_result.value.as_ref().and_then(|v| v.as_str()) {
                if !actions.is_empty() {
                    println!("👉 Popup actions: {}", actions);
                }
            }

            // Wait before next retry
            sleep(Duration::from_secs(3));
        }

        // Final status check
        let final_status = tab.evaluate(r#"
            const status = document.querySelector('.statuslabel-label');
            status ? status.innerText.trim() : 'unknown'
        "#, false)?;
        println!("Final server status: {}", final_status.value.as_ref().and_then(|v| v.as_str()).unwrap_or("unknown"));

        // Capture final state
        use headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption;
        if let Ok(png_data) = tab.capture_screenshot(CaptureScreenshotFormatOption::Png, None, None, true) {
            std::fs::write("final_state_screenshot.png", png_data)?;
        }
        if let Ok(content) = tab.get_content() {
            std::fs::write("final_state_dump.html", content)?;
        }

        Ok(())
    };

    match run() {
        Ok(_) => Ok("Aternos start command sent successfully".to_string()),
        Err(e) => {
            use headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption;
            
            if let Ok(png_data) = tab.capture_screenshot(CaptureScreenshotFormatOption::Png, None, None, true) {
                std::fs::write("screenshot.png", png_data)?;
                println!("❌ Error occurred. Screenshot saved to 'screenshot.png'.");
            }
            
            if let Ok(content) = tab.get_content() {
                std::fs::write("page_dump.html", content)?;
                println!("❌ Error occurred. HTML content saved to 'page_dump.html'.");
            }
            
            Err(e)
        }
    }
}
