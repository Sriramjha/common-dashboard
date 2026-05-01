#!/usr/bin/env bash
# Build a zip you can upload to Drive / Slack / email for teammates (Option B).
# Excludes secrets and regeneratable files — see SHARING.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

STAMP="$(date +%Y%m%d-%H%M%S)"
DEFAULT_OUT="$(cd "$SCRIPT_DIR/.." && pwd)/coralogix-dashboard-share-${STAMP}.zip"
OUT="${1:-$DEFAULT_OUT}"

if ! command -v rsync >/dev/null 2>&1; then
  echo "ERROR: rsync is required (install Xcode CLT on macOS: xcode-select --install)" >&2
  exit 1
fi

if ! command -v zip >/dev/null 2>&1; then
  echo "ERROR: zip is required" >&2
  exit 1
fi

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

PKG_NAME="coralogix-dashboard"
mkdir -p "$TMP/$PKG_NAME"

rsync -a \
  --exclude='.env' \
  --exclude='data.json' \
  --exclude='dashboard_snapshot.json' \
  --exclude='.DS_Store' \
  --exclude='**/.DS_Store' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='agent-tools' \
  --exclude='.git' \
  "$SCRIPT_DIR/" "$TMP/$PKG_NAME/"

mkdir -p "$(dirname "$OUT")"
(cd "$TMP" && zip -r -q "$OUT" "$PKG_NAME")

BYTES="$(wc -c < "$OUT" | tr -d ' ')"
echo "OK — shareable archive created"
echo "  Path: $OUT"
echo "  Size: $BYTES bytes"
echo ""
echo "Next: upload this file, then send your teammate SHARING.md (or README.md § Option B)."
