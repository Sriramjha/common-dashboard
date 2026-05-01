"""
Parse / build account secrets .env files for the dashboard Add / Edit env UI.

Structured keys match coralogix-dashboard.html field order; other lines stay in an
“extra” block. Sensitive values are masked as **** in API responses; POST treats
**** as “keep existing value”.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

# Order matches Add-account form (plus CORALOGIX_API_BASE, often present in secrets files).
KNOWN_ENV_KEYS_ORDER: Tuple[str, ...] = (
    "CORALOGIX_API_KEY",
    "CORALOGIX_REGION",
    "CORALOGIX_API_BASE",
    "CORALOGIX_COMPANY_ID",
    "CORALOGIX_TEAM_ENRICHMENT",
    "CORALOGIX_TEAM_ENRICHMENT_DAYS",
    "CORALOGIX_TEAM_ENRICHMENT_TIER",
    "CORALOGIX_SESSION_TOKEN",
    "CORALOGIX_AUDIT_DATAPRIME_API_KEY",
    "CORALOGIX_AUDIT_DATAPRIME_HOST",
    "CORALOGIX_AUDIT_DATAPRIME_TIER",
    "CORALOGIX_AUDIT_ACTIVE_USERS_DAYS",
    "CORALOGIX_AUDIT_ACTIVE_USERS",
    "MONDAY_FILTER_GROUP_NAMES",
    "MONDAY_GROUP_TITLE_CONTAINS",
    "MONDAY_GROUP_TITLE_EXCLUDE",
    "MONDAY_DEVOPS_COLUMN_TITLE",
    "MONDAY_SRC_COLUMN_TITLE",
    "MONDAY_DEVOPS_COLUMN_ID",
    "MONDAY_SRC_COLUMN_ID",
    "CORALOGIX_LOG_INGESTION_AGGREGATE",
    "CORALOGIX_DEBUG_DATA_USAGE",
)

KNOWN_SET = frozenset(KNOWN_ENV_KEYS_ORDER)
MASK_SENTINEL = "****"


def is_sensitive_env_key(key: str) -> bool:
    k = (key or "").strip().upper()
    if not k:
        return False
    if k in ("CORALOGIX_API_KEY", "CORALOGIX_AUDIT_DATAPRIME_API_KEY"):
        return True
    if "TOKEN" in k or "SECRET" in k or "PASSWORD" in k or "PRIVATE" in k:
        return True
    if k.endswith("_KEY"):
        return True
    return False


def _split_env_line(line: str) -> Tuple[str, str, str] | None:
    s = line.strip()
    if not s or s.startswith("#") or "=" not in s:
        return None
    k, _, v = s.partition("=")
    ks = k.strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", ks):
        return None
    return ks, v.strip(), line


def parse_account_env(text: str) -> Tuple[Dict[str, str], List[str]]:
    """Split file into known-key dict (last wins) and extra lines (order preserved)."""
    known: Dict[str, str] = {}
    extra: List[str] = []
    for line in (text or "").splitlines():
        raw = line
        p = _split_env_line(line)
        if p:
            ks, v, _ = p
            v = v.strip().strip('"').strip("'")
            if ks in KNOWN_SET:
                known[ks] = v
            else:
                extra.append(raw)
        else:
            extra.append(raw)
    return known, extra


def build_account_env(known: Dict[str, str], extra_lines: List[str]) -> str:
    lines: List[str] = []
    for k in KNOWN_ENV_KEYS_ORDER:
        v = (known.get(k) or "").strip()
        if v:
            lines.append(f"{k}={v}")
    if extra_lines:
        while extra_lines and not str(extra_lines[-1]).strip():
            extra_lines = extra_lines[:-1]
        if extra_lines:
            if lines:
                lines.append("")
            lines.extend(extra_lines)
    out = "\n".join(lines)
    return out + ("\n" if out.strip() else "")


def mask_extra_lines(extra_lines: List[str]) -> List[str]:
    out: List[str] = []
    for line in extra_lines:
        p = _split_env_line(line)
        if p:
            ks, v, _ = p
            if is_sensitive_env_key(ks) and v:
                out.append(f"{ks}={MASK_SENTINEL}")
            else:
                out.append(line.rstrip("\n"))
        else:
            out.append(line.rstrip("\n"))
    return out


def variables_for_get_response(known: Dict[str, str]) -> Dict[str, str]:
    """JSON object for dashboard: sensitive values shown as ****."""
    out: Dict[str, str] = {}
    for k in KNOWN_ENV_KEYS_ORDER:
        v = (known.get(k) or "").strip()
        if not v:
            continue
        if is_sensitive_env_key(k):
            out[k] = MASK_SENTINEL
        else:
            out[k] = v
    return out


def substitute_stars_in_extra(old_extra_lines: List[str], client_extra: str) -> List[str]:
    """Replace KEY=**** lines in client extra with original lines from old file."""
    old_by_key: Dict[str, str] = {}
    for line in old_extra_lines:
        p = _split_env_line(line)
        if p:
            ks, _, _ = p
            old_by_key[ks] = line.rstrip("\n")

    new_lines: List[str] = []
    for line in client_extra.splitlines():
        p = _split_env_line(line)
        if p:
            ks, v, _ = p
            vs = v.strip().strip('"').strip("'")
            if vs == MASK_SENTINEL and ks in old_by_key:
                new_lines.append(old_by_key[ks])
            else:
                new_lines.append(line.rstrip("\n"))
        else:
            new_lines.append(line.rstrip("\n"))
    return new_lines


def merge_account_env_from_post(
    old_text: str,
    client_vars: Dict[str, Any],
    client_extra: str,
) -> str:
    old_known, old_extra = parse_account_env(old_text)
    merged: Dict[str, str] = dict(old_known)

    for k in KNOWN_ENV_KEYS_ORDER:
        if k not in client_vars:
            continue
        raw = client_vars[k]
        cv = "" if raw is None else str(raw).strip()
        if cv == "" or cv.lower() == "(empty)":
            merged.pop(k, None)
        elif cv == MASK_SENTINEL:
            pass
        else:
            merged[k] = cv

    extra_merged = substitute_stars_in_extra(old_extra, client_extra)
    return build_account_env(merged, extra_merged)
