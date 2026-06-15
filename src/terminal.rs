use crate::{minecraft::ServerStatus, run_history::RunContext};
use std::{
    io::{self, Write},
    time::{SystemTime, UNIX_EPOCH},
};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";

pub fn ready(bot_name: &str) -> String {
    line("READY", "", bot_name, "", None, "")
}

pub fn emit(line: impl AsRef<str>) {
    let mut stdout = io::stdout().lock();
    let _ = writeln!(stdout, "{}", line.as_ref());
    let _ = stdout.flush();
}

pub fn emit_debug(line: impl AsRef<str>) {
    if debug_enabled() {
        emit(line);
    }
}

pub fn line_for_context(label: &str, context: &RunContext, detail: impl AsRef<str>) -> String {
    line(
        label,
        &slash_command_name(&context.command),
        &context.user_name,
        &context.guild_name,
        context.channel_name.as_deref(),
        detail,
    )
}

pub fn line(
    label: &str,
    command: &str,
    user_name: &str,
    guild_name: &str,
    channel_name: Option<&str>,
    detail: impl AsRef<str>,
) -> String {
    let mut parts = vec![time_token(), label_token(label)];

    if !command.is_empty() {
        parts.push(command.to_string());
    }
    if !user_name.is_empty() {
        parts.push(user_token(user_name));
    }

    let mut metadata = Vec::new();
    if !guild_name.is_empty() {
        metadata.push(quote(guild_name));
    }
    if let Some(channel_name) = channel_name {
        metadata.push(channel_token(channel_name));
    }
    if !metadata.is_empty() {
        parts.push(dim(&metadata.join(" ")));
    }

    let detail = detail.as_ref().trim();
    if !detail.is_empty() {
        parts.push(detail.to_string());
    }

    let prefix = if matches!(label, "START" | "FAIL") {
        "\n"
    } else {
        ""
    };
    format!("{prefix}{}", parts.join(" "))
}

pub fn format_duration(duration_ms: u128) -> String {
    if duration_ms >= 60_000 {
        let minutes = duration_ms / 60_000;
        let seconds = (duration_ms % 60_000) / 1_000;
        if seconds == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m{seconds}s")
        }
    } else if duration_ms >= 1_000 {
        format!("{}s", duration_ms / 1_000)
    } else {
        format!("{duration_ms}ms")
    }
}

pub fn brief_minecraft_status(status: &ServerStatus) -> String {
    match status {
        ServerStatus::Offline => "Offline".to_string(),
        ServerStatus::Unreachable { .. } => "Offline".to_string(),
        ServerStatus::Queued => "Queued".to_string(),
        ServerStatus::Starting => "Starting".to_string(),
        ServerStatus::Preparing => "Preparing".to_string(),
        ServerStatus::Loading => "Loading".to_string(),
        ServerStatus::Online { online, max, .. } => format!("Online {online}/{max}"),
    }
}

pub fn quote(value: &str) -> String {
    format!("\"{}\"", clean(value))
}

pub fn clean(value: &str) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let escaped = compact.replace('"', "'");
    let max_len: usize = 160;
    if escaped.chars().count() <= max_len {
        return escaped;
    }

    let mut clipped = escaped
        .chars()
        .take(max_len.saturating_sub(3))
        .collect::<String>();
    clipped.push_str("...");
    clipped
}

fn slash_command_name(command: &str) -> String {
    format!("/{}", command.replace('.', " "))
}

fn time_token() -> String {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = elapsed.as_secs() % 86_400;
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    dim(&format!("{hours:02}:{minutes:02}"))
}

fn label_token(label: &str) -> String {
    let padded = format!("{label:<5}");
    match label {
        "READY" | "OK" => paint(&padded, &[BOLD, GREEN]),
        "START" => paint(&padded, &[BOLD, CYAN]),
        "FAIL" => paint(&padded, &[BOLD, RED]),
        "WARN" | "BUSY" | "MISS" | "SKIP" => paint(&padded, &[BOLD, YELLOW]),
        _ => paint(&padded, &[DIM]),
    }
}

fn user_token(user_name: &str) -> String {
    format!("@{}", clean_token(user_name))
}

fn channel_token(channel_name: &str) -> String {
    format!("#{}", clean_token(channel_name))
}

fn clean_token(value: &str) -> String {
    clean(value)
        .chars()
        .map(|ch| if ch.is_whitespace() { '-' } else { ch })
        .collect()
}

fn dim(value: &str) -> String {
    paint(value, &[DIM])
}

fn paint(value: &str, codes: &[&str]) -> String {
    if !ansi_enabled() {
        return value.to_string();
    }

    format!("{}{}{}", codes.join(""), value, RESET)
}

fn ansi_enabled() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if std::env::var("TERM")
        .map(|term| term.eq_ignore_ascii_case("dumb"))
        .unwrap_or(false)
    {
        return false;
    }
    std::env::var("BUTLER_COLOR")
        .map(|value| !matches!(value.trim(), "0" | "false" | "off" | "no"))
        .unwrap_or(true)
}

fn debug_enabled() -> bool {
    std::env::var("BUTLER_LOG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "debug" | "trace"
            )
        })
        .unwrap_or(false)
}
