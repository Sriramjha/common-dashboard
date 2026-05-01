# Coralogix dashboard (local)

Static **HTML** dashboard fed by **`data.json`**.  
**`refresh.py`** pulls from Coralogix APIs (API key stays on your machine) **and** merges the latest **`AHC_*_output.json`** into `data.json` when that file exists under `sb-ahc-automator-main/output/`, so one command refreshes the whole dashboard snapshot.  
**`serve.py`** serves files over HTTP so the browser can load `data.json`.

---

## Team collaboration (add a developer)

This project uses **Git** so a teammate can branch, push, and ship changes safely.

1. **You:** Create a **private** repo on GitHub/GitLab → push this code → **invite** your teammate as collaborator.  
   **Step-by-step:** [`docs/COLLABORATION.md`](docs/COLLABORATION.md)
2. **Teammate:** Clone → `cp .env.example .env` → add **their** API key → `python3 refresh.py` → `python3 serve.py`  
3. **Workflow:** [`CONTRIBUTING.md`](CONTRIBUTING.md)

> **Zip-only handoff** (no Git): see [`SHARING.md`](SHARING.md) and `./package-for-sharing.sh`.

---

## Quick start

```bash
cp .env.example .env   # add CORALOGIX_API_KEY + CORALOGIX_API_BASE
python3 refresh.py
python3 serve.py
```

**Shell wrapper** (same as `python3 refresh.py`; optional args forwarded):

```bash
./refresh_all.sh
# e.g.  ./refresh_all.sh -- --section alerts
```

Open **http://127.0.0.1:8765/coralogix-dashboard.html**

## Snowbit AHC Automator (combined view)

The repo includes **`sb-ahc-automator-main/`** — the full Account Health Check runner (gRPC/REST/MCP). It writes JSON under **`sb-ahc-automator-main/output/AHC_*_output.json`** (see that project’s README; it needs **session token** + **company id** + **API key**).

To show those results in this dashboard:

1. Run the automator (example):  
   `python3 sb-ahc-automator-main/ahc_runner.py --region EU1 --company-id YOUR_ID --cx-api-key YOUR_KEY --session-token "YOUR_TOKEN"`
2. Merge into `data.json`: run **`python3 refresh.py`** — it picks up the newest `AHC_*_output.json` automatically.  
   You can still run **`python3 merge_ahc_into_data_json.py`** alone if you only want to re-merge AHC without refetching APIs.  
   Or **`python3 run_ahc_and_merge.py`** to run the automator then **`refresh.py`** (full `data.json`, including the new AHC file).  
   (Automator requires `pip install -r sb-ahc-automator-main/requirements.txt` and `grpcurl` — see that README.)
3. Reload the browser. AHC-backed rows and the Platform panel AHC strip use `data.json.ahc`.

## Verify

```bash
python3 test_dashboard.py
```

(Start `serve.py` first if you want the server check to pass.)

## Files

| File | Role |
|------|------|
| `refresh.py` | Fetch Coralogix → merge latest AHC JSON if present → write `data.json` |
| `serve.py` | Local HTTP server (port 8765); auto-runs `refresh.py` **daily at 00:00 UTC** by default (see `.env.example`) |
| `coralogix-dashboard.html` | UI |
| `merge_ahc_into_data_json.py` | Merge AHC only (optional; also used internally by `refresh.py`) |
| `refresh_all.sh` | Wrapper around `python3 refresh.py` |
| `sb-ahc-automator-main/` | Snowbit AHC runner (separate `requirements.txt`) |
| `test_dashboard.py` | Data / wiring checks |
| `.env.example` | Env template (no secrets) |
| `SECURITY.md` | Key handling & safety notes |
| `docs/COLLABORATION.md` | Invite teammate + push to Git host |
| `CONTRIBUTING.md` | How to develop & test |

## Requirements

- Python **3.9+**
- No third-party packages (stdlib only)
