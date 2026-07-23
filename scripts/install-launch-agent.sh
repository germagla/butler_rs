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
TEMP_PLIST=""
INSTALL_LOCK_HELD=false
INSTALL_LOCK_OWNER=""
RECLAIM_LOCK_HELD=false
RECLAIM_LOCK_OWNER=""

cleanup() {
    if [ "$INSTALL_LOCK_HELD" = true ]; then
        CURRENT_LOCK_OWNER=$(/bin/cat "$START_ADMISSION_LOCK" 2>/dev/null || true)
        if [ "$CURRENT_LOCK_OWNER" = "$INSTALL_LOCK_OWNER" ]; then
            /bin/rm -f "$START_ADMISSION_LOCK"
        fi
    fi
    if [ "$RECLAIM_LOCK_HELD" = true ]; then
        CURRENT_RECLAIM_OWNER=$(/bin/cat "$START_ADMISSION_RECLAIM_LOCK" 2>/dev/null || true)
        if [ "$CURRENT_RECLAIM_OWNER" = "$RECLAIM_LOCK_OWNER" ]; then
            /bin/rm -f "$START_ADMISSION_RECLAIM_LOCK"
        fi
    fi
    if [ -n "$TEMP_PLIST" ]; then
        /bin/rm -f "$TEMP_PLIST"
    fi
}
trap cleanup EXIT HUP INT TERM

acquire_start_admission_lock() {
    ATTEMPT=0
    while [ "$ATTEMPT" -lt 2 ]; do
        TEMP_LOCK=$(mktemp "$ARTIFACT_DIR/.start-admission.XXXXXX")
        LOCK_TOKEN=${TEMP_LOCK##*/}
        printf '%s\n%s\n' "$$" "$LOCK_TOKEN" > "$TEMP_LOCK"
        chmod 600 "$TEMP_LOCK"
        if /bin/ln "$TEMP_LOCK" "$START_ADMISSION_LOCK" 2>/dev/null; then
            INSTALL_LOCK_OWNER=$(/bin/cat "$TEMP_LOCK")
            /bin/rm -f "$TEMP_LOCK"
            INSTALL_LOCK_HELD=true
            if [ "$RECLAIM_LOCK_HELD" = true ]; then
                CURRENT_RECLAIM_OWNER=$(/bin/cat "$START_ADMISSION_RECLAIM_LOCK" 2>/dev/null || true)
                if [ "$CURRENT_RECLAIM_OWNER" = "$RECLAIM_LOCK_OWNER" ]; then
                    /bin/rm -f "$START_ADMISSION_RECLAIM_LOCK"
                fi
                RECLAIM_LOCK_HELD=false
            fi
            return
        fi
        /bin/rm -f "$TEMP_LOCK"
        if [ "$RECLAIM_LOCK_HELD" != true ]; then
            TEMP_RECLAIM=$(mktemp "$ARTIFACT_DIR/.start-admission-reclaim.XXXXXX")
            RECLAIM_TOKEN=${TEMP_RECLAIM##*/}
            printf '%s\n%s\n' "$$" "$RECLAIM_TOKEN" > "$TEMP_RECLAIM"
            chmod 600 "$TEMP_RECLAIM"
            if ! /bin/ln "$TEMP_RECLAIM" "$START_ADMISSION_RECLAIM_LOCK" 2>/dev/null; then
                /bin/rm -f "$TEMP_RECLAIM"
                echo "Another process is inspecting a stale start-admission lock." >&2
                exit 1
            fi
            RECLAIM_LOCK_OWNER=$(/bin/cat "$TEMP_RECLAIM")
            /bin/rm -f "$TEMP_RECLAIM"
            RECLAIM_LOCK_HELD=true
        fi
        if [ ! -e "$START_ADMISSION_LOCK" ]; then
            ATTEMPT=$((ATTEMPT + 1))
            continue
        fi
        if [ ! -f "$START_ADMISSION_LOCK" ] || [ -L "$START_ADMISSION_LOCK" ]; then
            echo "Unsafe start-admission lock at $START_ADMISSION_LOCK." >&2
            exit 1
        fi
        LOCK_OWNER=$(/bin/cat "$START_ADMISSION_LOCK" 2>/dev/null || true)
        LOCK_PID=$(printf '%s\n' "$LOCK_OWNER" | /usr/bin/head -n 1)
        case "$LOCK_PID" in
            ''|*[!0-9]*)
                echo "Start-admission lock has invalid ownership." >&2
                exit 1
                ;;
        esac
        if /bin/kill -0 "$LOCK_PID" 2>/dev/null; then
            RUN_ID=$(/usr/bin/head -n 1 "$ACTIVE_START_MARKER" 2>/dev/null \
                | /usr/bin/tr -cd 'A-Za-z0-9_-' || true)
            echo "Butler start run ${RUN_ID:-unknown} is active; retry installation after it finishes." >&2
            exit 1
        fi
        CURRENT_LOCK_OWNER=$(/bin/cat "$START_ADMISSION_LOCK" 2>/dev/null || true)
        if [ "$CURRENT_LOCK_OWNER" != "$LOCK_OWNER" ]; then
            echo "Start-admission lock ownership changed during inspection." >&2
            exit 1
        fi
        /bin/rm -f "$START_ADMISSION_LOCK"
        ATTEMPT=$((ATTEMPT + 1))
    done
    echo "Could not acquire start-admission lock." >&2
    exit 1
}

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
ARTIFACT_DIR=$(
    cd "$ROOT_DIR"
    "$BINARY" --print-artifact-dir
)
ACTIVE_START_MARKER="$ARTIFACT_DIR/.active-start"
START_ADMISSION_LOCK="$ARTIFACT_DIR/.start-admission.lock"
START_ADMISSION_RECLAIM_LOCK="$ARTIFACT_DIR/.start-admission.lock.reclaim"
mkdir -p "$ARTIFACT_DIR"
chmod 700 "$ARTIFACT_DIR"
acquire_start_admission_lock

mkdir -p "$LOG_DIR" "$PLIST_DIR"
chmod 700 "$LOG_DIR"

umask 077
TEMP_PLIST=$(mktemp "${TMPDIR:-/tmp}/$LABEL.XXXXXX")

/usr/bin/plutil -create xml1 "$TEMP_PLIST"
/usr/bin/plutil -insert Label -string "$LABEL" "$TEMP_PLIST"
/usr/bin/plutil -insert ProgramArguments -array "$TEMP_PLIST"
/usr/bin/plutil -insert ProgramArguments.0 -string "$BINARY" "$TEMP_PLIST"
/usr/bin/plutil -insert WorkingDirectory -string "$ROOT_DIR" "$TEMP_PLIST"
/usr/bin/plutil -insert RunAtLoad -bool true "$TEMP_PLIST"
/usr/bin/plutil -insert KeepAlive -bool true "$TEMP_PLIST"
/usr/bin/plutil -insert ThrottleInterval -integer 10 "$TEMP_PLIST"
/usr/bin/plutil -insert ExitTimeOut -integer 8100 "$TEMP_PLIST"
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
        if [ "$WAIT_COUNT" -ge 32440 ]; then
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
