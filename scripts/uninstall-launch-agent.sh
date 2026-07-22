#!/bin/sh
set -eu

LABEL="com.germagla.butler-rs"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"

if [ "$(uname -s)" != "Darwin" ]; then
    echo "This uninstaller only supports macOS." >&2
    exit 1
fi

/bin/launchctl bootout "$DOMAIN/$LABEL" >/dev/null 2>&1 || true
rm -f "$PLIST"

echo "Butler LaunchAgent removed. Existing logs and artifacts were kept."
