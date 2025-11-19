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

    let headless = std::env::var("HEADLESS").unwrap_or_else(|_| "true".to_string()) != "false";
    println!("DEBUG: HEADLESS env var is set to: {:?}, resulting in headless={}", std::env::var("HEADLESS"), headless);
    
    let options = LaunchOptions {
        headless,
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
            // Return JSON string to avoid parsing issues with complex objects
            let state_check = tab.evaluate(r#"
                (function() {
                    const status = document.querySelector('.statuslabel-label');
                    const startBtn = document.querySelector('#start');
                    
                    const statusText = status ? status.innerText.trim() : 'unknown';
                    const btnDisplay = startBtn ? window.getComputedStyle(startBtn).display : 'unknown';
                    
                    return JSON.stringify({
                        status: statusText,
                        btn_display: btnDisplay
                    });
                })()
            "#, false)?;
            
            let state_json = state_check.value.as_ref().and_then(|v| v.as_str()).unwrap_or("{}");
            let state_val: serde_json::Value = serde_json::from_str(state_json).unwrap_or(serde_json::json!({}));
            
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

pub async fn get_minecraft_status(addr: &str) -> Result<String> {
    use regex::Regex;

    // Parse initial address
    let parts: Vec<&str> = addr.split(':').collect();
    let mut current_host = parts[0].to_string();
    let mut current_port = if parts.len() > 1 {
        parts[1].parse().unwrap_or(25565)
    } else {
        25565
    };

    // Allow up to 1 redirect
    for _ in 0..2 {
        println!("Pinging Minecraft server at {}:{}", current_host, current_port);

        let mut ping_result = Err(anyhow::anyhow!("Ping failed"));
        
        // Retry loop for the ping itself
        for attempt in 1..=3 {
            if attempt > 1 {
                println!("Retrying ping (attempt {}/3)...", attempt);
                sleep(Duration::from_millis(1000));
            }

            let timeout = Duration::from_secs(5);
            let result = tokio::time::timeout(timeout, async {
                let mut stream = tokio::net::TcpStream::connect((current_host.as_str(), current_port)).await?;
                let response = craftping::tokio::ping(&mut stream, current_host.as_str(), current_port).await?;
                Ok::<craftping::Response, anyhow::Error>(response)
            }).await;
            
            if let Ok(inner_res) = result {
                ping_result = Ok(inner_res);
                if ping_result.as_ref().unwrap().is_ok() {
                    break;
                }
            }
        }

        match ping_result {
            Ok(Ok(response)) => {
                println!("Ping success: {:?}", response);
                
                // Extract text from description
                let description_text;
                if let Some(serde_json::Value::Object(map)) = &response.description {
                    if let Some(serde_json::Value::String(text)) = map.get("text") {
                        description_text = text.clone();
                    } else if let Some(serde_json::Value::Object(_extra)) = map.get("extra") {
                        description_text = serde_json::to_string(&response.description).unwrap_or_default();
                    } else {
                         description_text = serde_json::to_string(&response.description).unwrap_or_default();
                    }
                } else if let Some(serde_json::Value::String(text)) = &response.description {
                    description_text = text.clone();
                } else {
                    description_text = serde_json::to_string(&response.description).unwrap_or_default();
                }

                println!("Raw description text: {}", description_text);

                // Clean color codes (section sign + char)
                let re_color = Regex::new(r"§.").unwrap();
                let clean_text = re_color.replace_all(&description_text, "");
                println!("Cleaned description text: {}", clean_text);

                // Check for explicit status indicators in version or description
                let lower_version = response.version.to_lowercase();
                let lower_desc = clean_text.to_lowercase();

                if lower_version.contains("offline") || lower_desc.contains("offline") {
                    println!("Detected 'Offline' indicator.");
                    return Ok("Offline".to_string());
                }

                if lower_version.contains("starting") || lower_desc.contains("starting") {
                     println!("Detected 'Starting' indicator.");
                     return Ok("Starting".to_string());
                }

                if lower_version.contains("loading") || lower_desc.contains("loading") {
                     println!("Detected 'Loading' indicator.");
                     return Ok("Loading".to_string());
                }

                if lower_version.contains("queue") || lower_desc.contains("queue") {
                     println!("Detected 'Queue' indicator.");
                     return Ok("In Queue".to_string());
                }

                if lower_version.contains("preparing") || lower_desc.contains("preparing") {
                     println!("Detected 'Preparing' indicator.");
                     return Ok("Preparing".to_string());
                }
                
                // Regex for "host:port" (e.g. dynip)
                let re_host_port = Regex::new(r"([a-zA-Z0-9.-]+\.[a-zA-Z]{2,}):(\d{4,5})").unwrap();
                // Regex for "Port: 12345"
                let re_port_only = Regex::new(r"[Pp]ort:?\s*(\d{4,5})").unwrap();

                let mut redirect_found = false;

                if let Some(caps) = re_host_port.captures(&clean_text) {
                    let new_host = caps.get(1).unwrap().as_str().to_string();
                    let new_port = caps.get(2).unwrap().as_str().parse::<u16>().unwrap_or(0);
                    
                    if new_port != 0 && (new_host != current_host || new_port != current_port) {
                        println!("Redirection detected to {}:{}", new_host, new_port);
                        current_host = new_host;
                        current_port = new_port;
                        redirect_found = true;
                    }
                } else if let Some(caps) = re_port_only.captures(&clean_text) {
                    let new_port = caps.get(1).unwrap().as_str().parse::<u16>().unwrap_or(0);
                    if new_port != 0 && new_port != current_port {
                        println!("Port redirection detected to {}", new_port);
                        current_port = new_port;
                        redirect_found = true;
                    }
                }

                if redirect_found {
                    continue; // Retry with new address
                }

                // No redirect, return result
                let players: Vec<String> = response.sample
                    .unwrap_or_default()
                    .iter()
                    .map(|p| p.name.clone())
                    .collect();
                
                let player_list = if players.is_empty() {
                    "None".to_string()
                } else {
                    players.join(", ")
                };

                return Ok(format!(
                    "Online ({}/{} players)\nPlayers: {}", 
                    response.online_players, 
                    response.max_players,
                    player_list
                ));
            },
            Ok(Err(e)) => {
                println!("Ping failed: {}", e);
                // Only return offline if we are on the last redirect attempt or if it's a hard failure
                // But here we are inside the redirect loop.
                // If it fails, we might want to try the next redirect loop iteration? 
                // No, if ping fails, we can't read description to find redirect.
                // So we just return Offline.
                return Ok("Offline (Unreachable)".to_string());
            },
            Err(_) => {
                println!("Ping timed out after retries");
                return Ok("Offline (Timeout)".to_string());
            }
        }
    }

    Ok("Offline (Max redirects reached)".to_string())
}
