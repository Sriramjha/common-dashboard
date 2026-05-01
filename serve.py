#!/usr/bin/env python3
"""HTTP server for the Coralogix dashboard + account list API (writes locked to localhost unless allowlisted).

Auto-refresh: by default, ``refresh.py`` runs **once per week** (Monday 00:00 UTC) while this process is up.
Set ``CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC=1`` for daily 00:00 UTC instead.
Set ``CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_UTC=0`` and ``CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC=0`` to use
``CORALOGIX_DASH_AUTO_REFRESH_SEC`` (interval mode) or disable both plus ``CORALOGIX_DASH_AUTO_REFRESH_SEC=0``.
"""
from __future__ import annotations

import http.server
import ipaddress
import json
import os
import pathlib
import re
import socketserver
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer
from urllib.parse import unquote, urlparse

PORT = 8765

ROOT = pathlib.Path(__file__).resolve().parent


def _env_truthy(key: str) -> bool:
    return (os.environ.get(key) or "").strip().lower() in ("1", "true", "yes", "on")


def _parse_admin_allow_networks() -> tuple[ipaddress._BaseNetwork, ...]:
    """
    CORALOGIX_DASH_ADMIN_ALLOW_CIDR — comma-separated IPs or CIDRs (e.g. 18.192.144.83/32,10.0.0.0/8).
    Combined with loopback. Real client IP usually comes from nginx (X-Forwarded-For) when trust is on.
    """
    out: list[ipaddress._BaseNetwork] = []
    raw = (os.environ.get("CORALOGIX_DASH_ADMIN_ALLOW_CIDR") or "").strip()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(ipaddress.ip_network(p, strict=False))
        except ValueError:
            print(f"[serve] WARN: invalid CORALOGIX_DASH_ADMIN_ALLOW_CIDR token {p!r} — skipped", file=sys.stderr)
    return tuple(out)


_TRUST_X_FORWARDED_FOR = _env_truthy("CORALOGIX_DASH_TRUST_X_FORWARDED_FOR")
_ADMIN_ALLOW_NETWORKS = _parse_admin_allow_networks()


class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    """Allow concurrent API calls; a long refresh.py must not block Add account / env / GET."""

    daemon_threads = True

# Serialized refresh runs (button + auto + concurrent accounts)
_REFRESH_LOCK = threading.Lock()


def _refresh_command_for_account(account_id: str | None) -> list[str]:
    cmd = [sys.executable, str(ROOT / "refresh.py")]
    if account_id and str(account_id).strip().lower() not in ("", "default"):
        cmd.extend(["--account", str(account_id).strip()])
    return cmd


def run_refresh_py(account_id: str | None = None, timeout_sec: int | None = None) -> dict:
    """
    Run refresh.py for one dashboard account. Uses lock so auto + manual never overlap.
    account_id None or 'default' → root .env + data.json.
    """
    if timeout_sec is None:
        timeout_sec = int(os.environ.get("CORALOGIX_DASH_REFRESH_TIMEOUT_SEC", "3600"))
    cmd = _refresh_command_for_account(account_id)
    with _REFRESH_LOCK:
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                timeout=max(60, timeout_sec),
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": f"refresh.py timed out after {timeout_sec}s", "exitCode": -1}
        except OSError as e:
            return {"ok": False, "error": str(e), "exitCode": -1}
        tail = (proc.stdout or "") + (proc.stderr or "")
        tail = tail[-12000:] if len(tail) > 12000 else tail
        ok = proc.returncode == 0
        return {
            "ok": ok,
            "exitCode": proc.returncode,
            "logTail": tail,
            "error": None if ok else f"refresh.py exited {proc.returncode}",
        }


def _seconds_until_next_utc_midnight() -> float:
    """Wall-clock seconds until the next 00:00:00 UTC boundary (exclusive of current instant at midnight)."""
    now = datetime.now(timezone.utc)
    today_mid = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if now >= today_mid:
        nxt = today_mid + timedelta(days=1)
    else:
        nxt = today_mid
    return max(0.5, (nxt - now).total_seconds())


def _parse_auto_refresh_weekday() -> int:
    """CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_WEEKDAY: 0=Monday … 6=Sunday (datetime.weekday()). Default 0."""
    raw = (os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_WEEKDAY") or "0").strip()
    try:
        d = int(raw, 10)
    except ValueError:
        d = 0
    return max(0, min(6, d))


def _parse_auto_refresh_weekly_hour() -> int:
    """CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_HOUR: 0–23 UTC. Default 0."""
    raw = (os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_HOUR") or "0").strip()
    try:
        h = int(raw, 10)
    except ValueError:
        h = 0
    return max(0, min(23, h))


def _seconds_until_next_weekly_utc_slot() -> float:
    """Seconds until the next CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_WEEKDAY at CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_HOUR UTC."""
    now = datetime.now(timezone.utc)
    wday = _parse_auto_refresh_weekday()
    hour = _parse_auto_refresh_weekly_hour()
    days_ahead = wday - now.weekday()
    if days_ahead < 0:
        days_ahead += 7
    nxt = (now + timedelta(days=days_ahead)).replace(hour=hour, minute=0, second=0, microsecond=0)
    if nxt <= now:
        nxt += timedelta(days=7)
    return max(0.5, (nxt - now).total_seconds())


def _run_auto_refresh_accounts_pass() -> None:
    """One refresh cycle for all accounts selected by CORALOGIX_DASH_AUTO_REFRESH_ACCOUNTS."""
    mode = os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_ACCOUNTS", "default").strip().lower()
    if mode == "all":
        from accounts_config import list_accounts_public, load_manifest

        for a in list_accounts_public(load_manifest()):
            aid = a.get("id") or "default"
            run_refresh_py(None if aid == "default" else aid)
    elif mode == "default":
        run_refresh_py(None)
    else:
        for part in mode.split(","):
            aid = part.strip()
            if aid:
                run_refresh_py(None if aid == "default" else aid)


def _auto_refresh_worker_interval() -> None:
    """Background: re-run refresh.py every CORALOGIX_DASH_AUTO_REFRESH_SEC seconds."""
    interval = int(os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_SEC", "3600"))
    if interval <= 0:
        return
    time.sleep(interval)
    while True:
        try:
            _run_auto_refresh_accounts_pass()
        except Exception as exc:
            print(f"[serve] auto-refresh error: {exc}", file=sys.stderr)
        time.sleep(interval)


def _auto_refresh_worker_daily_utc() -> None:
    """Background: re-run refresh.py once per calendar day at 00:00 UTC."""
    logged = False
    while True:
        wait = _seconds_until_next_utc_midnight()
        if not logged:
            nxt = datetime.now(timezone.utc) + timedelta(seconds=wait)
            print(
                f"[serve] daily UTC refresh: next 00:00 UTC run in {wait / 3600:.2f} h "
                f"(~{nxt.strftime('%Y-%m-%d %H:%M')} UTC)",
                file=sys.stderr,
            )
            logged = True
        time.sleep(wait)
        try:
            print("[serve] scheduled refresh at 00:00 UTC …", file=sys.stderr)
            _run_auto_refresh_accounts_pass()
        except Exception as exc:
            print(f"[serve] daily UTC refresh error: {exc}", file=sys.stderr)


def _auto_refresh_worker_weekly_utc() -> None:
    """Background: re-run refresh.py once per week on a fixed weekday/hour UTC."""
    wday = _parse_auto_refresh_weekday()
    hour = _parse_auto_refresh_weekly_hour()
    logged = False
    while True:
        wait = _seconds_until_next_weekly_utc_slot()
        if not logged:
            nxt = datetime.now(timezone.utc) + timedelta(seconds=wait)
            print(
                f"[serve] weekly UTC refresh: next weekday={wday} hour={hour:02d} UTC run in {wait / 3600:.2f} h "
                f"(~{nxt.strftime('%Y-%m-%d %H:%M')} UTC)",
                file=sys.stderr,
            )
            logged = True
        time.sleep(wait)
        try:
            print(
                f"[serve] scheduled refresh (weekly, weekday={wday} {hour:02d}:00 UTC) …",
                file=sys.stderr,
            )
            _run_auto_refresh_accounts_pass()
        except Exception as exc:
            print(f"[serve] weekly UTC refresh error: {exc}", file=sys.stderr)


class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def log_message(self, fmt: str, *args: object) -> None:
        try:
            if args and str(args[0]).startswith('"GET /data'):
                sys.stderr.write(f"[serve] {self.address_string()} {fmt % args}\n")
        except Exception:
            pass


_ACCOUNT_ENV_PATH_RE = re.compile(r"^/api/accounts/([^/]+)/env$")


class DashboardHandler(NoCacheHandler):
    def _peer_is_loopback(self) -> bool:
        host = str(self.client_address[0] or "")
        if host in ("127.0.0.1", "::1"):
            return True
        if host.startswith("::ffff:") and host[7:] == "127.0.0.1":
            return True
        return False

    def _effective_client_ip_for_admin(self) -> str:
        """
        When nginx proxies to 127.0.0.1:8765, the TCP peer is loopback. With CORALOGIX_DASH_TRUST_X_FORWARDED_FOR=1,
        use X-Real-IP / first X-Forwarded-For hop as the browser client (nginx must set these).
        """
        peer = str(self.client_address[0] or "")
        if _TRUST_X_FORWARDED_FOR and self._peer_is_loopback():
            rip = (self.headers.get("X-Real-IP") or "").strip()
            if rip:
                return rip.split(",")[0].strip()
            xff = (self.headers.get("X-Forwarded-For") or "").strip()
            if xff:
                return xff.split(",")[0].strip()
        return peer

    @staticmethod
    def _ip_matches_admin_allowlist(ip_s: str) -> bool:
        if not _ADMIN_ALLOW_NETWORKS:
            return False
        try:
            addr = ipaddress.ip_address(ip_s)
        except ValueError:
            return False
        return any(addr in net for net in _ADMIN_ALLOW_NETWORKS)

    def _is_admin_client(self) -> bool:
        eff = self._effective_client_ip_for_admin()
        if eff in ("127.0.0.1", "::1"):
            return True
        if eff.startswith("::ffff:") and eff[7:] == "127.0.0.1":
            return True
        try:
            if ipaddress.ip_address(eff).is_loopback:
                return True
        except ValueError:
            pass
        return self._ip_matches_admin_allowlist(eff)

    def _request_path(self) -> str:
        """Normalize path: strip query, handle proxy absolute URLs, collapse slashes."""
        raw = (self.path or "").split("?", 1)[0].strip()
        if not raw:
            return "/"
        if raw.startswith(("http://", "https://")):
            u = urlparse(raw)
            raw = u.path or "/"
        while "//" in raw:
            raw = raw.replace("//", "/")
        if len(raw) > 1 and raw.endswith("/"):
            raw = raw.rstrip("/")
        return raw or "/"

    @staticmethod
    def _account_id_from_env_api_path(path: str) -> str | None:
        """Parse /api/accounts/<id>/env → account id, or None."""
        m = _ACCOUNT_ENV_PATH_RE.match(path)
        if not m:
            return None
        aid = unquote(m.group(1).strip())
        return aid or None

    def do_GET(self) -> None:
        path = self._request_path()
        if path == "/api/accounts":
            self._send_accounts_json()
            return
        aid = self._account_id_from_env_api_path(path)
        if aid is not None:
            self._get_account_env(aid)
            return
        super().do_GET()

    def do_POST(self) -> None:
        path = self._request_path()
        if path == "/api/accounts/add":
            self._post_add_account()
            return
        aid = self._account_id_from_env_api_path(path)
        if aid is not None:
            self._post_account_env(aid)
            return
        if path == "/api/refresh":
            self._post_refresh()
            return
        print(f"[serve] POST unmatched path {path!r} (raw {self.path!r})", file=sys.stderr)
        self.send_error(404, "Not Found")

    def _send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_accounts_json(self) -> None:
        try:
            from accounts_config import list_accounts_public, load_manifest

            m = load_manifest()
            self._send_json({"accounts": list_accounts_public(m)})
        except Exception as e:
            self._send_json({"error": str(e), "accounts": []}, status=500)

    def _post_add_account(self) -> None:
        if not self._is_admin_client():
            self.send_error(403, "Admin only: localhost or CORALOGIX_DASH_ADMIN_ALLOW_CIDR + nginx X-Forwarded-For trust")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > 262_144:
            self.send_error(400, "Bad Content-Length")
            return
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return
        aid = str(data.get("id") or "").strip()
        label = str(data.get("label") or "").strip()
        env_text = str(data.get("envText") or "")
        cgx_region = str(data.get("coralogixRegion") or data.get("coralogix_region") or "").strip().upper() or None
        if aid == "default":
            self._send_json({"ok": False, "error": "Choose an id other than 'default' (that id is reserved for root .env + data.json)."}, status=400)
            return
        try:
            from accounts_config import append_account

            append_account(aid, label or aid, env_text, coralogix_region=cgx_region)
        except ValueError as e:
            self._send_json({"ok": False, "error": str(e)}, status=400)
            return
        except OSError as e:
            self._send_json({"ok": False, "error": str(e)}, status=500)
            return
        # Do not block HTTP on refresh.py (often several minutes: incidents, never-triggered correlation, etc.).
        # Browsers/proxies typically time out long POSTs — same pattern as env save + refresh.
        def _run_first_refresh() -> None:
            print(f"[serve] background first refresh.py for new account {aid!r} …", file=sys.stderr)
            res = run_refresh_py(aid)
            if not res.get("ok"):
                print(
                    f"[serve] first refresh failed for {aid!r}: {res.get('error')}",
                    file=sys.stderr,
                )

        threading.Thread(target=_run_first_refresh, name=f"add-account-refresh-{aid}", daemon=True).start()
        self._send_json({"ok": True, "id": aid, "refresh": {"ok": True, "async": True}})

    def _post_refresh(self) -> None:
        if not self._is_admin_client():
            self.send_error(403, "Admin only: localhost or CORALOGIX_DASH_ADMIN_ALLOW_CIDR + nginx X-Forwarded-For trust")
            return
        account_id: str | None = None
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length > 0 and length < 8192:
            raw = self.rfile.read(length).decode("utf-8", errors="replace")
            try:
                data = json.loads(raw)
                if isinstance(data, dict) and data.get("accountId") is not None:
                    aid = str(data.get("accountId") or "").strip()
                    account_id = None if aid in ("", "default") else aid
            except json.JSONDecodeError:
                pass
        def _run_refresh() -> None:
            label = "default" if account_id is None else str(account_id)
            print(f"[serve] background refresh.py ({label}) …", file=sys.stderr)
            res = run_refresh_py(account_id)
            if not res.get("ok"):
                print(f"[serve] refresh.py failed ({label}): {res.get('error')}", file=sys.stderr)
                if res.get("logTail"):
                    print(res["logTail"][-4000:], file=sys.stderr)

        threading.Thread(target=_run_refresh, name="api-refresh", daemon=True).start()
        self._send_json({"ok": True, "async": True})

    def _get_account_env(self, account_id: str) -> None:
        if not self._is_admin_client():
            self.send_error(403, "Admin only: localhost or CORALOGIX_DASH_ADMIN_ALLOW_CIDR + nginx X-Forwarded-For trust")
            return
        try:
            from account_env_form import mask_extra_lines, parse_account_env, variables_for_get_response
            from accounts_config import account_by_id, load_manifest, read_account_env_text

            man = load_manifest()
            acc = account_by_id(man, account_id)
            if not acc:
                self._send_json({"ok": False, "error": "Unknown account id"}, status=404)
                return
            text = read_account_env_text(account_id)
            cr = str(acc.get("coralogixRegion") or acc.get("coralogix_region") or "").strip().upper()
            known, extra_lines = parse_account_env(text)
            variables = variables_for_get_response(known)
            extra_masked = "\n".join(mask_extra_lines(extra_lines))
            self._send_json(
                {
                    "ok": True,
                    "id": account_id,
                    "label": str(acc.get("label") or account_id),
                    "coralogixRegion": cr or None,
                    "variables": variables,
                    "extraText": extra_masked,
                    "maskSentinel": "****",
                }
            )
        except ValueError as e:
            self._send_json({"ok": False, "error": str(e)}, status=400)

    def _post_account_env(self, account_id: str) -> None:
        if not self._is_admin_client():
            self.send_error(403, "Admin only: localhost or CORALOGIX_DASH_ADMIN_ALLOW_CIDR + nginx X-Forwarded-For trust")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > 262_144:
            self._send_json(
                {"ok": False, "error": "Bad Content-Length (need JSON body; if using a proxy, preserve POST body)."},
                status=400,
            )
            return
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self._send_json({"ok": False, "error": "Invalid JSON body"}, status=400)
            return
        if not isinstance(data, dict):
            self._send_json({"ok": False, "error": "JSON object expected"}, status=400)
            return
        refresh_after = bool(data.get("refreshAfterSave"))
        try:
            from account_env_form import merge_account_env_from_post
            from accounts_config import read_account_env_text, update_account_manifest_meta, write_account_env_text

            if isinstance(data.get("variables"), dict):
                old = read_account_env_text(account_id)
                extra_t = str(data.get("extraText") if data.get("extraText") is not None else "")
                env_text = merge_account_env_from_post(old, data["variables"], extra_t)
            else:
                env_text = str(data.get("envText") if data.get("envText") is not None else "")
            write_account_env_text(account_id, env_text)
            if "label" in data or "coralogixRegion" in data or "coralogix_region" in data:
                lbl = None
                if "label" in data:
                    lbl = str(data.get("label") or "").strip() or account_id
                cr_part: str | None = None
                if "coralogixRegion" in data:
                    cr_part = str(data.get("coralogixRegion") or "")
                elif "coralogix_region" in data:
                    cr_part = str(data.get("coralogix_region") or "")
                update_account_manifest_meta(
                    account_id,
                    label=lbl,
                    coralogix_region=cr_part,
                )
        except ValueError as e:
            self._send_json({"ok": False, "error": str(e)}, status=400)
            return
        except OSError as e:
            self._send_json({"ok": False, "error": str(e)}, status=500)
            return
        except Exception as e:
            print(f"[serve] POST env {account_id!r}: {e}", file=sys.stderr)
            self._send_json({"ok": False, "error": str(e)[:500]}, status=500)
            return
        out: dict = {"ok": True, "id": account_id}
        if refresh_after:
            # Never block the HTTP response on refresh.py (can run many minutes) — avoids UI stuck on "Saving…"
            def _run_bg() -> None:
                print(f"[serve] background refresh.py after env edit for {account_id!r} …", file=sys.stderr)
                run_refresh_py(account_id)

            threading.Thread(target=_run_bg, name=f"env-edit-refresh-{account_id}", daemon=True).start()
            out["refresh"] = {"ok": True, "async": True}
        self._send_json(out)


def main() -> None:
    os.chdir(ROOT)
    _data = ROOT / "data.json"
    _sz = _data.stat().st_size / 1024 if _data.exists() else 0.0
    print(f"\n  Serving directory: {ROOT}")
    print(f"  data.json:         {_data}  ({_sz:.1f} KB)" if _data.exists() else f"  data.json: MISSING — run python3 refresh.py here first")
    print(f"\n  → http://127.0.0.1:{PORT}/coralogix-dashboard.html (or via nginx on :80)")
    print(
        "  Multi-account admin API: POST …/add · env GET|POST · POST /api/refresh "
        "(localhost OR CORALOGIX_DASH_TRUST_X_FORWARDED_FOR=1 + CORALOGIX_DASH_ADMIN_ALLOW_CIDR behind nginx)"
    )
    print(f"  POST /api/refresh — queue refresh.py in background (does not block other API calls)")
    _daily_key = os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC")
    daily_on = _env_truthy("CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC") if _daily_key is not None else False

    _w_raw = (os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_UTC") or "").strip()
    weekly_off = _w_raw.lower() in ("0", "false", "no", "off")

    ar = int(os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_SEC", "3600"))
    acc_mode = os.environ.get("CORALOGIX_DASH_AUTO_REFRESH_ACCOUNTS", "default")

    daily_explicit_off = _daily_key is not None and not daily_on
    legacy_interval_only = daily_explicit_off and _w_raw == ""

    if daily_on:
        threading.Thread(
            target=_auto_refresh_worker_daily_utc, name="dash-auto-refresh-daily-utc", daemon=True
        ).start()
        print(
            f"  Auto-refresh: daily at 00:00 UTC · accounts={acc_mode!r} "
            f"(set CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_UTC=0 and CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC=0 for interval)"
        )
    elif legacy_interval_only:
        if ar > 0:
            threading.Thread(
                target=_auto_refresh_worker_interval, name="dash-auto-refresh-interval", daemon=True
            ).start()
            print(
                f"  Auto-refresh: every {ar}s · accounts={acc_mode!r} "
                f"(legacy: CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC=0, no weekly env; "
                f"omit DAILY key for weekly Mon 00 UTC)"
            )
        else:
            print(
                "  Auto-refresh: off (CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC=0, "
                "CORALOGIX_DASH_AUTO_REFRESH_SEC=0)"
            )
    elif not weekly_off:
        threading.Thread(
            target=_auto_refresh_worker_weekly_utc, name="dash-auto-refresh-weekly-utc", daemon=True
        ).start()
        wd, wh = _parse_auto_refresh_weekday(), _parse_auto_refresh_weekly_hour()
        print(
            f"  Auto-refresh: weekly (weekday={wd} {wh:02d}:00 UTC) · accounts={acc_mode!r} "
            f"(CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_WEEKDAY / _HOUR; "
            f"set CORALOGIX_DASH_AUTO_REFRESH_DAILY_UTC=1 for daily, or WEEKLY_UTC=0 for interval)"
        )
    elif ar > 0:
        threading.Thread(
            target=_auto_refresh_worker_interval, name="dash-auto-refresh-interval", daemon=True
        ).start()
        print(
            f"  Auto-refresh: every {ar}s · accounts={acc_mode!r} "
            f"(set CORALOGIX_DASH_AUTO_REFRESH_SEC=0 to disable)"
        )
    else:
        print(
            "  Auto-refresh: off (set CORALOGIX_DASH_AUTO_REFRESH_WEEKLY_UTC=0, "
            "omit or disable daily, and CORALOGIX_DASH_AUTO_REFRESH_SEC=0)"
        )
    print(f"  See accounts/manifest.example.json\n")
    server = ThreadingHTTPServer(("127.0.0.1", PORT), DashboardHandler)
    try:
        print(f"  HTTP server (threaded) on http://127.0.0.1:{PORT}/  — Ctrl+C to stop\n")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopping…", file=sys.stderr)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
