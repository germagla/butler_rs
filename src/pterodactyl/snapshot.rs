use super::{PowerConfirmationSource, PowerSubmissionState, StateArtifact};
use anyhow::{Context, Result};
use font8x8::{BASIC_FONTS, UnicodeFonts};
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use std::io::Cursor;

const WIDTH: u32 = 1200;
const HEIGHT: u32 = 675;

const BACKGROUND: Rgba<u8> = Rgba([17, 20, 24, 255]);
const PANEL: Rgba<u8> = Rgba([27, 31, 37, 255]);
const PANEL_ALT: Rgba<u8> = Rgba([34, 39, 46, 255]);
const BORDER: Rgba<u8> = Rgba([68, 76, 86, 255]);
const TEXT: Rgba<u8> = Rgba([238, 241, 245, 255]);
const MUTED: Rgba<u8> = Rgba([156, 164, 175, 255]);
const GREEN: Rgba<u8> = Rgba([69, 186, 116, 255]);
const AMBER: Rgba<u8> = Rgba([235, 174, 66, 255]);
const RED: Rgba<u8> = Rgba([224, 92, 92, 255]);
const BLUE: Rgba<u8> = Rgba([79, 148, 235, 255]);

#[cfg(test)]
pub(super) fn render_snapshot(run_id: &str, artifact: &StateArtifact<'_>) -> Result<Vec<u8>> {
    let view = SnapshotView::from_artifact(artifact);
    render_snapshot_view(run_id, &view)
}

pub(super) fn render_snapshot_view(run_id: &str, view: &SnapshotView) -> Result<Vec<u8>> {
    let mut image = RgbaImage::from_pixel(WIDTH, HEIGHT, BACKGROUND);
    fill_rect(&mut image, 0, 0, WIDTH, 8, view.color);

    draw_text(&mut image, 54, 42, "BUTLER", 4, TEXT);
    draw_text(&mut image, 54, 83, "PLAY HOSTING / SERVER START", 2, MUTED);
    draw_text(
        &mut image,
        800,
        52,
        &format!("RUN {}", safe_ascii(run_id, 16)),
        2,
        MUTED,
    );

    fill_rect(&mut image, 48, 130, 1104, 112, PANEL);
    stroke_rect(&mut image, 48, 130, 1104, 112, BORDER);
    draw_text(&mut image, 78, 156, "CURRENT RESULT", 2, MUTED);
    draw_text(&mut image, 78, 190, &view.headline, 4, view.color);

    draw_metric(
        &mut image,
        48,
        266,
        344,
        view.state_label,
        &view.provider_state,
    );
    draw_metric(&mut image, 428, 266, 344, "ALLOCATION", &view.allocation);
    draw_metric(&mut image, 808, 266, 344, "SUBMISSION", &view.submission);

    fill_rect(&mut image, 48, 420, 1104, 166, PANEL);
    stroke_rect(&mut image, 48, 420, 1104, 166, BORDER);
    draw_text(&mut image, 78, 446, "DIAGNOSTIC CONTEXT", 2, MUTED);
    draw_labeled_value(&mut image, 78, 486, "ACCESS", "Managed by Butler", 460);
    draw_labeled_value(&mut image, 700, 486, "MODE", &view.mode, 307);

    if let Some(error_class) = &view.error_class {
        draw_text(&mut image, 78, 530, "PROVIDER ERROR", 2, RED);
        draw_text(&mut image, 310, 530, error_class, 2, TEXT);
    } else {
        draw_text(&mut image, 78, 530, "No provider error recorded.", 2, MUTED);
    }

    draw_text(
        &mut image,
        48,
        625,
        "SANITIZED API STATE / NO CREDENTIALS OR COOKIES INCLUDED",
        2,
        MUTED,
    );

    let mut bytes = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(image)
        .write_to(&mut bytes, ImageFormat::Png)
        .context("could not encode provider snapshot")?;
    Ok(bytes.into_inner())
}

pub(super) struct SnapshotView {
    headline: String,
    color: Rgba<u8>,
    state_label: &'static str,
    provider_state: String,
    allocation: String,
    submission: String,
    mode: String,
    error_class: Option<String>,
}

impl SnapshotView {
    pub(super) fn from_artifact(artifact: &StateArtifact<'_>) -> Self {
        Self {
            headline: safe_ascii(&headline(artifact), 32),
            color: status_color(artifact),
            state_label: provider_state_label(artifact),
            provider_state: display_provider_state(artifact.current_state).to_string(),
            allocation: safe_ascii(&allocation_value(artifact), 32),
            submission: safe_ascii(&submission_value(artifact), 32),
            mode: format!(
                "limbo={} suspended={}",
                yes_no(artifact.is_limbo),
                yes_no(artifact.is_suspended)
            ),
            error_class: artifact.last_error.map(display_error_class),
        }
    }
}

fn display_provider_state(state: Option<&str>) -> &'static str {
    match state.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("running") => "Running",
        Some("starting") => "Starting",
        Some("offline") => "Offline",
        Some("stopped") => "Stopped",
        Some("stopping") => "Stopping",
        Some("suspended") => "Suspended",
        Some(_) => "Other",
        None => "Not reported",
    }
}

fn display_error_class(error: &str) -> String {
    let class: String = error
        .split([':', ';'])
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        .take(48)
        .collect();
    if class.is_empty() {
        "ProviderError".to_string()
    } else {
        class
    }
}

fn provider_state_label(artifact: &StateArtifact<'_>) -> &'static str {
    if artifact.workflow_stage == "submission_verified" {
        "VERIFIED STATE"
    } else if artifact.power_submission_state.is_some() {
        "STATE AT REQUEST"
    } else {
        "PROVIDER STATE"
    }
}

fn draw_metric(image: &mut RgbaImage, x: u32, y: u32, width: u32, label: &str, value: &str) {
    fill_rect(image, x, y, width, 126, PANEL_ALT);
    stroke_rect(image, x, y, width, 126, BORDER);
    draw_text(image, x + 24, y + 24, label, 2, MUTED);
    draw_text_fit(
        image,
        x + 24,
        y + 67,
        value,
        width.saturating_sub(48),
        2,
        TEXT,
    );
}

fn draw_labeled_value(
    image: &mut RgbaImage,
    x: u32,
    y: u32,
    label: &str,
    value: &str,
    max_width: u32,
) {
    draw_text(image, x, y, label, 2, MUTED);
    draw_text_fit(image, x + 145, y, value, max_width, 2, TEXT);
}

fn headline(artifact: &StateArtifact<'_>) -> String {
    if artifact.workflow_stage == "provider_submission_attempted" {
        return "START REQUEST ATTEMPTED".to_string();
    }
    if artifact.workflow_stage == "provider_failure" && artifact.power_submission_state.is_none() {
        return "PROVIDER START FAILED".to_string();
    }
    match artifact.power_submission_state {
        Some(PowerSubmissionState::Confirmed) => "START REQUEST CONFIRMED".to_string(),
        Some(PowerSubmissionState::Dispatching) => "SENDING START REQUEST".to_string(),
        Some(PowerSubmissionState::Ambiguous) => "START REQUEST ATTEMPTED".to_string(),
        Some(PowerSubmissionState::Rejected) => "START REQUEST REJECTED".to_string(),
        None => match artifact
            .current_state
            .unwrap_or("unknown")
            .to_ascii_lowercase()
            .as_str()
        {
            "running" | "online" => "SERVER RUNNING".to_string(),
            "starting" => "SERVER STARTING".to_string(),
            "offline" | "stopped" => "SERVER OFFLINE".to_string(),
            _ if artifact.workflow_stage == "allocation" => {
                "HOST ALLOCATION IN PROGRESS".to_string()
            }
            _ => "PROVIDER STATE CAPTURED".to_string(),
        },
    }
}

fn status_color(artifact: &StateArtifact<'_>) -> Rgba<u8> {
    if artifact.workflow_stage == "provider_submission_attempted" {
        return AMBER;
    }
    if artifact.workflow_stage == "provider_failure" && artifact.power_submission_state.is_none() {
        return RED;
    }
    match artifact.power_submission_state {
        Some(PowerSubmissionState::Confirmed) => GREEN,
        Some(PowerSubmissionState::Dispatching) => BLUE,
        Some(PowerSubmissionState::Ambiguous) => AMBER,
        Some(PowerSubmissionState::Rejected) => RED,
        None => match artifact
            .current_state
            .unwrap_or("unknown")
            .to_ascii_lowercase()
            .as_str()
        {
            "running" | "online" => GREEN,
            "starting" => BLUE,
            "offline" | "stopped" => AMBER,
            _ => BLUE,
        },
    }
}

fn allocation_value(artifact: &StateArtifact<'_>) -> String {
    let Some(phase) = artifact.phase else {
        return "Not applicable".to_string();
    };
    let mut value = match phase.trim().to_ascii_lowercase().as_str() {
        "active" => "Active",
        "queued" => "Queued",
        "waking" => "Waking",
        "suspended" => "Suspended",
        "sleeping" => "Sleeping",
        "asleep" => "Asleep",
        "attention" => "Attention",
        _ => "Other",
    }
    .to_string();
    if let Some(position) = artifact.position {
        value.push_str(&match artifact.total {
            Some(total) => format!(" {position}/{total}"),
            None => format!(" #{position}"),
        });
    }
    if let Some(minutes) = artifact.estimated_minutes {
        value.push_str(&format!(" ~{minutes}m"));
    }
    value
}

fn submission_value(artifact: &StateArtifact<'_>) -> String {
    if artifact.workflow_stage == "provider_submission_attempted" {
        return "Provider reply unavailable".to_string();
    }
    let state = match artifact.power_submission_state {
        Some(PowerSubmissionState::Dispatching) => "Dispatching",
        Some(PowerSubmissionState::Ambiguous) => "Ambiguous",
        Some(PowerSubmissionState::Confirmed) => "Confirmed",
        Some(PowerSubmissionState::Rejected) => "Rejected",
        None => "Not attempted",
    };
    let source = match artifact.power_confirmation_source {
        Some(PowerConfirmationSource::Response) => "response",
        Some(PowerConfirmationSource::Resources) => "resources",
        Some(PowerConfirmationSource::Activity) => "activity",
        Some(PowerConfirmationSource::ProviderVerification) => "provider verification",
        Some(PowerConfirmationSource::Minecraft) => "Minecraft verification",
        None => return state.to_string(),
    };
    format!("{state} / {source}")
}

fn yes_no(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "unknown",
    }
}

fn safe_ascii(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() || character == ' ' {
                character
            } else {
                '?'
            }
        })
        .take(max_chars)
        .collect()
}

fn fill_rect(image: &mut RgbaImage, x: u32, y: u32, width: u32, height: u32, color: Rgba<u8>) {
    let max_x = x.saturating_add(width).min(image.width());
    let max_y = y.saturating_add(height).min(image.height());
    for pixel_y in y..max_y {
        for pixel_x in x..max_x {
            image.put_pixel(pixel_x, pixel_y, color);
        }
    }
}

fn stroke_rect(image: &mut RgbaImage, x: u32, y: u32, width: u32, height: u32, color: Rgba<u8>) {
    fill_rect(image, x, y, width, 1, color);
    fill_rect(
        image,
        x,
        y.saturating_add(height).saturating_sub(1),
        width,
        1,
        color,
    );
    fill_rect(image, x, y, 1, height, color);
    fill_rect(
        image,
        x.saturating_add(width).saturating_sub(1),
        y,
        1,
        height,
        color,
    );
}

fn draw_text(image: &mut RgbaImage, x: u32, y: u32, text: &str, scale: u32, color: Rgba<u8>) {
    let mut cursor_x = x;
    for character in text.chars() {
        if let Some(glyph) = BASIC_FONTS.get(character) {
            for (row, bits) in glyph.iter().enumerate() {
                for column in 0..8 {
                    if bits & (1u8 << column) != 0 {
                        fill_rect(
                            image,
                            cursor_x + column * scale,
                            y + row as u32 * scale,
                            scale,
                            scale,
                            color,
                        );
                    }
                }
            }
        }
        cursor_x = cursor_x.saturating_add(9 * scale);
        if cursor_x >= image.width() {
            break;
        }
    }
}

fn draw_text_fit(
    image: &mut RgbaImage,
    x: u32,
    y: u32,
    text: &str,
    max_width: u32,
    preferred_scale: u32,
    color: Rgba<u8>,
) {
    let text = safe_ascii(text, 96);
    let preferred_width = text.chars().count() as u32 * 9 * preferred_scale;
    let scale = if preferred_width <= max_width {
        preferred_scale
    } else {
        1
    };
    let max_chars = (max_width / (9 * scale)) as usize;
    draw_text(image, x, y, &safe_ascii(&text, max_chars), scale, color);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_nonempty_png_snapshot() {
        let artifact = StateArtifact {
            provider: "pterodactyl",
            workflow_stage: "power_start",
            current_state: Some("offline"),
            is_suspended: Some(false),
            is_limbo: Some(true),
            phase: Some("active"),
            connection: Some("server.example"),
            position: None,
            total: None,
            estimated_minutes: None,
            blocked: Some(false),
            enabled: Some(true),
            wake_requested: true,
            last_error: None,
            power_submission_state: Some(PowerSubmissionState::Confirmed),
            power_confirmation_source: Some(PowerConfirmationSource::Response),
        };

        let bytes = render_snapshot("abc123", &artifact).unwrap();

        let decoded = image::load_from_memory(&bytes).unwrap();
        assert_eq!(decoded.width(), WIDTH);
        assert_eq!(decoded.height(), HEIGHT);
        assert!(bytes.len() > 10_000);
        if let Some(output) = std::env::var_os("BUTLER_SNAPSHOT_TEST_OUTPUT") {
            std::fs::write(output, bytes).unwrap();
        }
    }

    #[test]
    fn sanitizes_non_ascii_and_bounds_text() {
        assert_eq!(safe_ascii("hello\nworld", 20), "hello?world");
        assert_eq!(safe_ascii("abcdefgh", 4), "abcd");
    }

    #[test]
    fn display_view_excludes_raw_connection_and_error_detail() {
        let artifact = StateArtifact {
            provider: "pterodactyl",
            workflow_stage: "provider_failure",
            current_state: Some("token=test-token"),
            is_suspended: None,
            is_limbo: None,
            phase: Some("cookie=secret"),
            connection: Some("secret-user:secret-password@private.example"),
            position: None,
            total: None,
            estimated_minutes: None,
            blocked: None,
            enabled: None,
            wake_requested: false,
            last_error: Some(
                "ProviderPowerTimeout: token=test-token; cookie=secret; /Users/private/path",
            ),
            power_submission_state: None,
            power_confirmation_source: None,
        };

        let view = SnapshotView::from_artifact(&artifact);

        assert_eq!(view.headline, "PROVIDER START FAILED");
        assert_eq!(view.color, RED);
        assert_eq!(view.error_class.as_deref(), Some("ProviderPowerTimeout"));
        assert_eq!(view.provider_state, "Other");
        assert_eq!(view.allocation, "Other");
        assert!(!view.mode.contains("secret"));
        assert!(!view.provider_state.contains("secret"));
        assert!(!view.allocation.contains("secret"));
        assert!(!view.submission.contains("secret"));
    }

    #[test]
    fn attempted_submission_snapshot_remains_accurate_after_later_verification() {
        let artifact = StateArtifact {
            provider: "pterodactyl",
            workflow_stage: "provider_submission_attempted",
            current_state: None,
            is_suspended: None,
            is_limbo: None,
            phase: None,
            connection: None,
            position: None,
            total: None,
            estimated_minutes: None,
            blocked: None,
            enabled: None,
            wake_requested: false,
            last_error: Some("ProviderPowerTimeout"),
            power_submission_state: None,
            power_confirmation_source: None,
        };

        let view = SnapshotView::from_artifact(&artifact);

        assert_eq!(view.headline, "START REQUEST ATTEMPTED");
        assert_eq!(view.submission, "Provider reply unavailable");
        assert_eq!(view.color, AMBER);
    }
}
