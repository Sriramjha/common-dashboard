"""Per-account Customer Info manual overrides.

Stored under ``accounts/overrides/<account_id>.json`` (gitignored). Each field is
``{"value": <str>, "updated_at": <ISO-Z>}``. ``contracted_units`` accepts numeric
input only; everything else is free text up to 256 chars.

When a field has an override saved, the dashboard displays the manual value and
ignores the source (Monday / C4C / Prometheus / derived) until the override is
cleared. ``contracted_units`` is manual-only — no source.

This module is intentionally small and dependency-free so it can be imported by
``serve.py`` without affecting refresh.py runs.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parent
OVERRIDES_DIR = ROOT / "accounts" / "overrides"

_FIELDS: tuple[str, ...] = (
    "account_manager",
    "tam",
    "arr",
    "devops_assignee",
    "src_consultant",
    "daily_plan_units",
    "src_customer",
    "contracted_units",
)

_VALID_FIELD = set(_FIELDS)
_MAX_VALUE_LEN = 256
_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
_LOCK = Lock()


def fields() -> tuple[str, ...]:
    return _FIELDS


def is_valid_field(field: str) -> bool:
    return isinstance(field, str) and field in _VALID_FIELD


def _sanitize_account_id(account_id: Optional[str]) -> str:
    aid = (account_id or "").strip()
    if not aid:
        return "default"
    if not _ID_RE.match(aid):
        raise ValueError("Invalid account id")
    return aid


def _path_for(account_id: str) -> Path:
    aid = _sanitize_account_id(account_id)
    return OVERRIDES_DIR / f"{aid}.json"


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=p.name + ".", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_path, p)
    except Exception:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def _coerce_value(field: str, raw: Any) -> str:
    """Validate / normalize ``raw`` for ``field``. Returns the string to store.

    Raises ValueError for invalid input. Empty / whitespace-only → ValueError so
    callers explicitly use ``clear_override`` to remove a value.
    """
    if raw is None:
        raise ValueError("value is required")
    s = str(raw).strip()
    if not s:
        raise ValueError("value is required")
    if len(s) > _MAX_VALUE_LEN:
        raise ValueError(f"value too long (max {_MAX_VALUE_LEN} chars)")
    if field == "contracted_units":
        compact = s.replace(",", "").replace(" ", "")
        try:
            n = float(compact)
        except ValueError as e:
            raise ValueError("contracted_units must be a number") from e
        if n < 0:
            raise ValueError("contracted_units must be >= 0")
        if n.is_integer():
            return str(int(n))
        return ("%.6f" % n).rstrip("0").rstrip(".")
    return s


def read_overrides(account_id: Optional[str]) -> Dict[str, Any]:
    """Return the override map for the account. Always returns a dict; missing file → empty."""
    p = _path_for(account_id or "default")
    if not p.is_file():
        return {"account": _sanitize_account_id(account_id), "fields": {}}
    try:
        with _LOCK:
            data = json.loads(p.read_text(encoding="utf-8") or "{}")
    except (OSError, json.JSONDecodeError):
        return {"account": _sanitize_account_id(account_id), "fields": {}, "error": "corrupt overrides file"}
    fields_map: Dict[str, Any] = {}
    raw = data.get("fields") if isinstance(data, dict) else None
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k in _VALID_FIELD and isinstance(v, dict):
                val = v.get("value")
                ts = v.get("updated_at")
                if isinstance(val, str) and val.strip():
                    fields_map[k] = {
                        "value": val.strip(),
                        "updated_at": str(ts) if ts else None,
                    }
    return {"account": _sanitize_account_id(account_id), "fields": fields_map}


def upsert_override(account_id: Optional[str], field: str, value: Any) -> Dict[str, Any]:
    if not is_valid_field(field):
        raise ValueError(f"Unknown field {field!r}")
    norm = _coerce_value(field, value)
    aid = _sanitize_account_id(account_id)
    p = _path_for(aid)
    with _LOCK:
        try:
            existing = json.loads(p.read_text(encoding="utf-8")) if p.is_file() else {}
        except (OSError, json.JSONDecodeError):
            existing = {}
        if not isinstance(existing, dict):
            existing = {}
        fields_map = existing.get("fields") if isinstance(existing.get("fields"), dict) else {}
        ts = _now_iso_z()
        fields_map[field] = {"value": norm, "updated_at": ts}
        existing["account"] = aid
        existing["fields"] = fields_map
        existing["updated_at"] = ts
        _atomic_write(p, json.dumps(existing, indent=2, sort_keys=True))
    return {"account": aid, "field": field, "value": norm, "updated_at": ts}


def clear_override(account_id: Optional[str], field: str) -> Dict[str, Any]:
    if not is_valid_field(field):
        raise ValueError(f"Unknown field {field!r}")
    aid = _sanitize_account_id(account_id)
    p = _path_for(aid)
    with _LOCK:
        if not p.is_file():
            return {"account": aid, "field": field, "cleared": False}
        try:
            existing = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = {}
        if not isinstance(existing, dict):
            existing = {}
        fields_map = existing.get("fields") if isinstance(existing.get("fields"), dict) else {}
        had = field in fields_map
        if had:
            fields_map.pop(field, None)
        existing["account"] = aid
        existing["fields"] = fields_map
        existing["updated_at"] = _now_iso_z()
        if fields_map:
            _atomic_write(p, json.dumps(existing, indent=2, sort_keys=True))
        else:
            try:
                p.unlink()
            except FileNotFoundError:
                pass
    return {"account": aid, "field": field, "cleared": bool(had)}
