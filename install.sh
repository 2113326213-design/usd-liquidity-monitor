#!/usr/bin/env bash
#
# One-shot installer for a fresh clone.
# Creates venv, installs deps, seeds .env from template, prints next steps.
# Safe to re-run.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo ""
echo "──────────────────────────────────────────────────────"
echo " 💧 USD Liquidity Monitor — install"
echo "──────────────────────────────────────────────────────"

# 1. Python version check
if ! command -v python3 &>/dev/null; then
    echo "❌ python3 not found. Install Python 3.11+ first." >&2
    exit 1
fi
PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJ=$(echo "$PY_VER" | cut -d. -f1)
PY_MIN=$(echo "$PY_VER" | cut -d. -f2)
if [ "$PY_MAJ" -lt 3 ] || { [ "$PY_MAJ" -eq 3 ] && [ "$PY_MIN" -lt 11 ]; }; then
    echo "❌ Python $PY_VER is too old. Need 3.11+." >&2
    exit 1
fi
echo "✓ Python $PY_VER"

# 2. venv
if [ ! -d .venv ]; then
    echo "Creating venv..."
    python3 -m venv .venv
fi
# shellcheck source=/dev/null
source .venv/bin/activate
echo "✓ venv active at .venv"

# 3. pip install
echo "Installing dependencies (this takes ~30s)..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
if [ -f requirements-dev.txt ]; then
    pip install -q -r requirements-dev.txt
fi
echo "✓ dependencies installed"

# 4. .env seed
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✓ created .env from template (FILL IN KEYS BEFORE RUNNING)"
else
    echo "✓ .env already exists, not overwriting"
fi

# 5. data dirs
mkdir -p data/raw/proxy data/derived data/logs data/state
echo "✓ data directories ready"

# 6. summary
cat <<EOF

──────────────────────────────────────────────────────
 ✅ install complete
──────────────────────────────────────────────────────

Next steps:

  1. Fill in your API keys:
     $EDITOR .env
     (Required: FRED_API_KEY + at least one of BARK_DEVICE_KEY / TELEGRAM_*)

  2. (Optional) Backfill 5 years of history:
     python3 -m usd_liquidity_monitor.scripts.backfill

  3. Run it:
     python3 -m usd_liquidity_monitor.main

  4. In another terminal, open the dashboard:
     streamlit run usd_liquidity_monitor/dashboard/app.py
     → http://127.0.0.1:8501

  5. (Optional, macOS) Auto-start on login:
     ./deploy/install-launchd.sh

  6. (Optional, Linux VPS) systemd:
     see README.md § 6b

Run tests:
  python3 -m pytest tests/ -v

EOF
