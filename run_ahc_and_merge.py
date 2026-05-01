#!/usr/bin/env python3
"""
Run Snowbit AHC (ahc_runner.py) using credentials from .env, then run refresh.py
(API snapshot + merge of the new AHC file into data.json).

Required in .env:
  CORALOGIX_API_KEY
  CORALOGIX_COMPANY_ID
  CORALOGIX_SESSION_TOKEN

Optional:
  CORALOGIX_REGION=EU1   (or derived from CORALOGIX_API_BASE)
"""
from __future__ import annotations

import os
import re
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
AUTOMATOR = ROOT / "sb-ahc-automator-main"
RUNNER = AUTOMATOR / "ahc_runner.py"
REFRESH = ROOT / "refresh.py"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def derive_region(api_base: str) -> str:
    b = (api_base or "").lower()
    if "eu2" in b:
        return "EU2"
    if "eu1" in b or "api.eu1" in b:
        return "EU1"
    if "ap1" in b:
        return "AP1"
    if "ap2" in b:
        return "AP2"
    if "ap3" in b or "cx440" in b:
        return "AP3"
    if "cx498" in b or "us2" in b:
        return "US2"
    if "coralogix.us" in b:
        return "US1"
    return "EU1"


def main() -> int:
    load_env(ENV_PATH)

    api_key = os.environ.get("CORALOGIX_API_KEY", "").strip()
    company_id = os.environ.get("CORALOGIX_COMPANY_ID", "").strip()
    session = os.environ.get("CORALOGIX_SESSION_TOKEN", "").strip()
    region = (os.environ.get("CORALOGIX_REGION") or "").strip().upper()
    if not region:
        region = derive_region(os.environ.get("CORALOGIX_API_BASE", ""))

    missing = []
    if not api_key or api_key == "your_api_key_here":
        missing.append("CORALOGIX_API_KEY")
    if not company_id:
        missing.append("CORALOGIX_COMPANY_ID")
    if not session:
        missing.append("CORALOGIX_SESSION_TOKEN")

    if missing:
        print("ERROR: Cannot run AHC — add to .env:\n", file=sys.stderr)
        for m in missing:
            print(f"  {m}=...", file=sys.stderr)
        print(
            "\n  CORALOGIX_COMPANY_ID — Coralogix company ID (numeric).\n"
            "  CORALOGIX_SESSION_TOKEN — browser session cookie (see sb-ahc-automator-main/README.md).\n"
            "  Optional: CORALOGIX_REGION=EU1 if auto-detect from CORALOGIX_API_BASE is wrong.\n",
            file=sys.stderr,
        )
        return 1

    if not RUNNER.is_file():
        print(f"ERROR: Missing {RUNNER}", file=sys.stderr)
        return 1

    out_dir = AUTOMATOR / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(RUNNER),
        "--region",
        region,
        "--company-id",
        company_id,
        "--cx-api-key",
        api_key,
        "--session-token",
        session,
        "--output-dir",
        str(out_dir),
    ]

    print(f"\n  Running AHC (region={region}, company_id={company_id}) …\n", flush=True)
    r = subprocess.run(cmd, cwd=str(AUTOMATOR))
    if r.returncode != 0:
        print(f"\n  ahc_runner.py exited {r.returncode}", file=sys.stderr)
        return r.returncode

    print("\n  refresh.py — full dashboard data.json (APIs + AHC merge) …\n", flush=True)
    r2 = subprocess.run([sys.executable, str(REFRESH)], cwd=str(ROOT))
    return r2.returncode


if __name__ == "__main__":
    sys.exit(main())
