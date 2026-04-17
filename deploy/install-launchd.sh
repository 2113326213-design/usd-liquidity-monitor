#!/usr/bin/env bash
#
# Render the launchd plist template with the current user's paths and
# install it to ~/Library/LaunchAgents/. Makes the monitor auto-start on
# login and restart on crash.
#
# Usage:
#   ./deploy/install-launchd.sh            # install / reinstall
#   ./deploy/install-launchd.sh --uninstall
#
# Safe to re-run; it unloads any previously-installed copy first.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_PARENT="$(dirname "$PROJECT_DIR")"
BUNDLE_ID="com.$(whoami).usd-liquidity-monitor"
PLIST_DEST="$HOME/Library/LaunchAgents/${BUNDLE_ID}.plist"
TEMPLATE="$SCRIPT_DIR/launchd-template.plist"

if [ "${1:-}" = "--uninstall" ]; then
    if [ -f "$PLIST_DEST" ]; then
        launchctl unload "$PLIST_DEST" 2>/dev/null || true
        rm -f "$PLIST_DEST"
        echo "✅ Uninstalled $PLIST_DEST"
    else
        echo "Nothing to uninstall (not installed)."
    fi
    exit 0
fi

# Sanity checks
if [ ! -f "$TEMPLATE" ]; then
    echo "ERROR: template not found at $TEMPLATE" >&2
    exit 1
fi
if [ ! -x "$PROJECT_DIR/.venv/bin/python3" ]; then
    echo "ERROR: venv not found at $PROJECT_DIR/.venv" >&2
    echo "Run install.sh first (or create the venv manually)." >&2
    exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents"
mkdir -p "$PROJECT_DIR/data/logs"

# Unload any previous version
if [ -f "$PLIST_DEST" ]; then
    launchctl unload "$PLIST_DEST" 2>/dev/null || true
fi

# Substitute template placeholders
sed \
    -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
    -e "s|__PROJECT_PARENT__|$PROJECT_PARENT|g" \
    -e "s|__BUNDLE_ID__|$BUNDLE_ID|g" \
    "$TEMPLATE" > "$PLIST_DEST"

launchctl load "$PLIST_DEST"

echo ""
echo "✅ Installed LaunchAgent at $PLIST_DEST"
echo ""
echo "Bundle id : $BUNDLE_ID"
echo "Logs      : $PROJECT_DIR/data/logs/launchd-{stdout,stderr}.log"
echo ""
echo "Manage with:"
echo "  launchctl start   $BUNDLE_ID     # one-off start (don't normally need)"
echo "  launchctl stop    $BUNDLE_ID     # stop (KeepAlive will restart on crash)"
echo "  launchctl list | grep $BUNDLE_ID # check status"
echo ""
echo "To uninstall:  $0 --uninstall"
