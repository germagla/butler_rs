#!/bin/sh
set -eu

LABEL="com.germagla.butler-rs"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
ENV_FILE="$ROOT_DIR/.env"
BINARY="$ROOT_DIR/target/release/butler_rs"
LOG_DIR="$ROOT_DIR/artifacts/launchd"
STDOUT_LOG="$LOG_DIR/stdout.log"
STDERR_LOG="$LOG_DIR/stderr.log"
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
chmod 600 "$ENV_FILE"

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

echo "Checking Butler configuration..."
(
    cd "$ROOT_DIR"
    "$BINARY" --check-config
)

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
/usr/bin/plutil -insert ExitTimeOut -integer 450 "$TEMP_PLIST"
/usr/bin/plutil -insert Umask -integer 63 "$TEMP_PLIST"
/usr/bin/plutil -insert ProcessType -string Background "$TEMP_PLIST"
/usr/bin/plutil -insert StandardOutPath -string "$STDOUT_LOG" "$TEMP_PLIST"
/usr/bin/plutil -insert StandardErrorPath -string "$STDERR_LOG" "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables -dictionary "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables.HOME -string "$HOME" "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables.NO_COLOR -string "1" "$TEMP_PLIST"
/usr/bin/plutil -insert EnvironmentVariables.PATH -string "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" "$TEMP_PLIST"
/usr/bin/plutil -lint "$TEMP_PLIST"
/usr/bin/install -m 600 "$TEMP_PLIST" "$PLIST"

WAS_LOADED=false
if /bin/launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    WAS_LOADED=true
    /bin/launchctl bootout "$DOMAIN/$LABEL"
    WAIT_COUNT=0
    while /bin/launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; do
        WAIT_COUNT=$((WAIT_COUNT + 1))
        if [ "$WAIT_COUNT" -ge 1840 ]; then
            echo "Existing Butler service did not finish unloading." >&2
            exit 1
        fi
        /bin/sleep 0.25
    done
fi
/bin/launchctl enable "$DOMAIN/$LABEL"
LOG_OFFSET=0
if [ -f "$STDOUT_LOG" ]; then
    LOG_OFFSET=$(/usr/bin/wc -c < "$STDOUT_LOG" | /usr/bin/tr -d ' ')
fi
if ! /bin/launchctl bootstrap "$DOMAIN" "$PLIST"; then
    if [ "$WAS_LOADED" != true ]; then
        exit 1
    fi
    echo "Initial bootstrap failed after unload; retrying once..." >&2
    /bin/sleep 2
    /bin/launchctl bootstrap "$DOMAIN" "$PLIST"
fi
/bin/sleep 1
READY=false
READY_WAIT_COUNT=0
while [ "$READY_WAIT_COUNT" -lt 240 ]; do
    if /bin/launchctl print "$DOMAIN/$LABEL" 2>/dev/null | /usr/bin/grep -q 'state = running' \
        && /usr/bin/tail -c "+$((LOG_OFFSET + 1))" "$STDOUT_LOG" 2>/dev/null \
            | /usr/bin/grep -q ' READY '; then
        READY=true
        break
    fi
    READY_WAIT_COUNT=$((READY_WAIT_COUNT + 1))
    /bin/sleep 1
done
if [ "$READY" != true ]; then
    echo "Butler did not report ready after installation; inspect $STDERR_LOG." >&2
    exit 1
fi

echo "Butler is installed and running as $LABEL."
echo "Status: launchctl print $DOMAIN/$LABEL"
echo "Logs:   $LOG_DIR"
