#!/bin/sh
set -eu

LABEL="com.germagla.butler-rs"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
ENV_FILE="$ROOT_DIR/.env"
BINARY="$ROOT_DIR/target/release/butler_rs"
LOG_DIR="$ROOT_DIR/artifacts/launchd"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST="$PLIST_DIR/$LABEL.plist"
DOMAIN="gui/$(id -u)"

if [ "$(uname -s)" != "Darwin" ]; then
    echo "This installer only supports macOS." >&2
    exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
    echo "Missing $ENV_FILE; configure Butler before installing the service." >&2
    exit 1
fi

if [ -n "${CARGO:-}" ]; then
    CARGO_BIN="$CARGO"
elif command -v cargo >/dev/null 2>&1; then
    CARGO_BIN=$(command -v cargo)
elif [ -x "$HOME/.cargo/bin/cargo" ]; then
    CARGO_BIN="$HOME/.cargo/bin/cargo"
else
    echo "Could not find cargo; install Rust or set CARGO=/path/to/cargo." >&2
    exit 1
fi

echo "Building Butler release binary..."
"$CARGO_BIN" build --release --manifest-path "$ROOT_DIR/Cargo.toml"

if [ ! -x "$BINARY" ]; then
    echo "Release build did not create $BINARY." >&2
    exit 1
fi

chmod 600 "$ENV_FILE"
mkdir -p "$LOG_DIR" "$PLIST_DIR"
chmod 700 "$LOG_DIR"

umask 077
TEMP_PLIST=$(mktemp "${TMPDIR:-/tmp}/$LABEL.XXXXXX")
trap 'rm -f "$TEMP_PLIST"' EXIT HUP INT TERM

/usr/bin/plutil -create xml1 "$TEMP_PLIST"
/usr/bin/plutil -insert Label -string "$LABEL" "$TEMP_PLIST"
/usr/bin/plutil -insert ProgramArguments -array "$TEMP_PLIST"
/usr/bin/plutil -insert ProgramArguments.0 -string "$BINARY" "$TEMP_PLIST"
/usr/bin/plutil -insert WorkingDirectory -string "$ROOT_DIR" "$TEMP_PLIST"
/usr/bin/plutil -insert RunAtLoad -bool true "$TEMP_PLIST"
/usr/bin/plutil -insert KeepAlive -bool true "$TEMP_PLIST"
/usr/bin/plutil -insert ThrottleInterval -integer 10 "$TEMP_PLIST"
/usr/bin/plutil -insert ProcessType -string Background "$TEMP_PLIST"
/usr/bin/plutil -insert StandardOutPath -string "$LOG_DIR/stdout.log" "$TEMP_PLIST"
/usr/bin/plutil -insert StandardErrorPath -string "$LOG_DIR/stderr.log" "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables -dictionary "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables.HOME -string "$HOME" "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables.NO_COLOR -string "1" "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables.PATH -string "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" "$TEMP_PLIST"
/usr/bin/plutil -lint "$TEMP_PLIST"
/usr/bin/install -m 600 "$TEMP_PLIST" "$PLIST"

/bin/launchctl bootout "$DOMAIN/$LABEL" >/dev/null 2>&1 || true
/bin/launchctl enable "$DOMAIN/$LABEL"
/bin/launchctl bootstrap "$DOMAIN" "$PLIST"

echo "Butler is installed and running as $LABEL."
echo "Status: launchctl print $DOMAIN/$LABEL"
echo "Logs:   $LOG_DIR"
