"""
Shared account manifest for multi-tenant dashboard refresh + serve.py API.

Manifest path: accounts/manifest.json (gitignored — copy from accounts/manifest.example.json)
Secrets:      accounts/secrets/<id>.env (gitignored; *.env already in .gitignore)
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = ROOT / "accounts" / "manifest.json"
SECRETS_DIR = ROOT / "accounts" / "secrets"


def default_manifest() -> Dict[str, Any]:
    return {
        "accounts": [
            {
                "id": "default",
                "label": "Snowbit-boutique",
                "dataFile": "data.json",
                "secretsFile": ".env",
            }
        ]
    }


def load_manifest() -> Dict[str, Any]:
    if not MANIFEST_PATH.exists():
        return default_manifest()
    try:
        data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("accounts"), list):
            return default_manifest()
        return data
    except Exception:
        return default_manifest()


def validate_account_id(aid: str) -> bool:
    return bool(aid and re.match(r"^[a-zA-Z0-9_-]{1,64}$", aid))


def _safe_under_root(rel: str) -> Path:
    """Resolve a project-relative path; reject traversal."""
    rel = str(rel or "").strip().replace("\\", "/")
    if not rel or Path(rel).is_absolute() or ".." in Path(rel).parts:
        raise ValueError(f"Invalid relative path: {rel!r}")
    p = (ROOT / rel).resolve()
    p.relative_to(ROOT.resolve())
    return p


def account_by_id(manifest: Dict[str, Any], account_id: str) -> Optional[Dict[str, Any]]:
    for a in manifest.get("accounts") or []:
        if isinstance(a, dict) and str(a.get("id")) == account_id:
            return a
    return None


def account_data_path(account: Dict[str, Any]) -> Path:
    aid = str(account.get("id") or "account")
    rel = str(account.get("dataFile") or "").strip() or f"data.{aid}.json"
    return _safe_under_root(rel)


def account_secrets_path(account: Dict[str, Any]) -> Path:
    """
    Resolve secrets env path for a manifest row.
    If ``secretsFile`` is omitted, default is root ``.env`` for ``default`` only;
    for any other account id, default is ``accounts/secrets/<id>.env`` (so UI Edit env
    and refresh stay consistent when the manifest was hand-edited without secretsFile).
    """
    aid = str(account.get("id") or "").strip()
    rel = str(account.get("secretsFile") or "").strip()
    if not rel:
        rel = ".env" if aid == "default" else f"accounts/secrets/{aid}.env"
    return _safe_under_root(rel)


def save_manifest(manifest: Dict[str, Any]) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def append_account(
    account_id: str,
    label: str,
    env_text: str,
    *,
    coralogix_region: str | None = None,
) -> None:
    """Create accounts/secrets/<id>.env and add row to manifest (localhost admin API)."""
    if not validate_account_id(account_id):
        raise ValueError("invalid account id")
    text = str(env_text or "").strip()
    if len(text) > 262_144:
        raise ValueError("env text too large")
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    sec_path = SECRETS_DIR / f"{account_id}.env"
    sec_path.write_text(text + ("\n" if text else ""), encoding="utf-8")
    man = load_manifest()
    accs = [
        a
        for a in man.get("accounts", [])
        if isinstance(a, dict) and str(a.get("id")) != account_id
    ]
    row: Dict[str, Any] = {
        "id": account_id,
        "label": (label or "").strip() or account_id,
        "dataFile": f"data.{account_id}.json",
        "secretsFile": f"accounts/secrets/{account_id}.env",
    }
    cr = str(coralogix_region or "").strip().upper()
    if cr:
        row["coralogixRegion"] = cr
    accs.append(row)
    man["accounts"] = accs
    save_manifest(man)


def _secrets_file_is_ui_editable(account: Dict[str, Any]) -> bool:
    """True if this account’s env file lives under accounts/secrets/ (not root .env)."""
    aid = str(account.get("id") or "").strip()
    if aid == "default":
        return False
    try:
        sec_path = account_secrets_path(account)
    except ValueError:
        return False
    try:
        sec_path.resolve().relative_to(SECRETS_DIR.resolve())
    except ValueError:
        return False
    return True


def read_account_env_text(account_id: str) -> str:
    """Return full secrets file text for manifest account; raises ValueError if not UI-editable."""
    if not validate_account_id(account_id):
        raise ValueError("invalid account id")
    acc = account_by_id(load_manifest(), account_id)
    if not acc:
        raise ValueError("account not in manifest")
    if not _secrets_file_is_ui_editable(acc):
        raise ValueError("this account uses root .env — edit .env on disk, not via dashboard")
    p = account_secrets_path(acc)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8")


def write_account_env_text(account_id: str, env_text: str) -> None:
    """Overwrite secrets file for manifest account (same size limit as append_account)."""
    if not validate_account_id(account_id):
        raise ValueError("invalid account id")
    text = str(env_text or "")
    if len(text) > 262_144:
        raise ValueError("env text too large")
    acc = account_by_id(load_manifest(), account_id)
    if not acc:
        raise ValueError("account not in manifest")
    if not _secrets_file_is_ui_editable(acc):
        raise ValueError("this account uses root .env — edit .env on disk, not via dashboard")
    p = account_secrets_path(acc)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text.rstrip() + ("\n" if text.strip() else ""), encoding="utf-8")


def update_account_manifest_meta(
    account_id: str,
    *,
    label: Optional[str] = None,
    coralogix_region: Optional[str] = None,
) -> None:
    """Update label and/or coralogixRegion on a manifest row (empty region removes key)."""
    if not validate_account_id(account_id):
        raise ValueError("invalid account id")
    man = load_manifest()
    accs = man.get("accounts") or []
    found = False
    for a in accs:
        if not isinstance(a, dict) or str(a.get("id")) != account_id:
            continue
        found = True
        if label is not None:
            a["label"] = str(label).strip() or account_id
        if coralogix_region is not None:
            cr = str(coralogix_region).strip().upper()
            if cr:
                a["coralogixRegion"] = cr
            else:
                a.pop("coralogixRegion", None)
        break
    if not found:
        raise ValueError("account not in manifest")
    save_manifest(man)


def list_accounts_public(manifest: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Safe for JSON API — no secret values; may include secretsEditable: true per row."""
    m = manifest or load_manifest()
    out: List[Dict[str, str]] = []
    for a in m.get("accounts") or []:
        if not isinstance(a, dict):
            continue
        aid = str(a.get("id") or "").strip()
        if not aid:
            continue
        item: Dict[str, str] = {
            "id": aid,
            "label": str(a.get("label") or aid),
            "dataFile": str(a.get("dataFile") or f"data.{aid}.json"),
        }
        cr = str(a.get("coralogixRegion") or a.get("coralogix_region") or "").strip().upper()
        if cr:
            item["coralogixRegion"] = cr
        if _secrets_file_is_ui_editable(a):
            item["secretsEditable"] = True
        out.append(item)
    return out
