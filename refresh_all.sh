#!/usr/bin/env bash
# Refresh Coralogix dashboard: refresh.py fetches APIs and merges latest AHC_*_output.json when present.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Common Dashboard — full refresh"
echo "══════════════════════════════════════════════════════════"
echo ""

REFRESH_OK=0
python3 refresh.py "$@" || REFRESH_OK=$?
if [ "$REFRESH_OK" -ne 0 ]; then
  echo "  ⚠ refresh.py exited with code $REFRESH_OK — some sections may have failed; data.json may still be updated."
fi

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Done. Start the UI:"
echo "    python3 serve.py"
echo "  → http://127.0.0.1:8765/coralogix-dashboard.html"
echo "══════════════════════════════════════════════════════════"
echo ""
