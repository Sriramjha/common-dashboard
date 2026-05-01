# Sharing this project (Option B — Zip / Drive / Slack)

Use this when you are **not** using a git remote yet, or you want a **one-off handoff**.

---

## For you (sender)

### 1. Create the archive

From this folder in Terminal:

```bash
cd "/path/to/coralogix-dashboard"
chmod +x package-for-sharing.sh
./package-for-sharing.sh
```

By default the zip is written to the **parent directory**:

`../coralogix-dashboard-share-YYYYMMDD-HHMMSS.zip`

To choose the output path:

```bash
./package-for-sharing.sh ~/Desktop/coralogix-dashboard-handoff.zip
```

### 2. What is **excluded** (on purpose)

| Excluded | Why |
|----------|-----|
| `.env` | Contains your **API key** — never share |
| `data.json` | Large, **regenerated** by `python3 refresh.py` |
| `dashboard_snapshot.json` | Optional test baseline; teammate can run `--save-snapshot` |
| `.git` | Optional; omit so the zip is smaller (teammate can `git init` later) |
| `__pycache__`, `*.pyc` | Junk |

**Included:** `coralogix-dashboard.html`, `refresh.py`, `serve.py`, `test_dashboard.py`, `.env.example`, `.gitignore`, `SECURITY.md`, this file, `README.md`.

### 3. Upload & message

- Upload the zip to **Google Drive / Dropbox / internal file share**.
- **Do not** paste API keys in Slack/email.
- Send the link + tell them to read **§ For your teammate** below (or send `SHARING.md`).

---

## For your teammate (receiver)

### 1. Unzip

Unzip to a folder of your choice, e.g. `~/Projects/coralogix-dashboard`.

### 2. Python

You need **Python 3.9+**. No `pip install` is required for the core tools.

```bash
python3 --version
```

### 3. API key (your own)

1. Coralogix → **Data flow → API keys** → create a key with **`alerts:read`** and **`incidents:read`** (and your org’s usual scopes).
2. In the project folder:

```bash
cp .env.example .env
```

3. Edit `.env`: set `CORALOGIX_API_KEY` and the correct `CORALOGIX_API_BASE` for your region (see comments in `.env.example`).

### 4. Generate data and run the dashboard

```bash
cd /path/to/coralogix-dashboard
python3 refresh.py
python3 serve.py
```

Open: **http://127.0.0.1:8765/coralogix-dashboard.html**

(Use this URL — opening the HTML as `file://` often breaks loading `data.json`.)

### 5. Optional: tests

In a **second** terminal, keep `serve.py` running, then:

```bash
python3 test_dashboard.py
```

If **T10 (snapshot)** warns, create a baseline once:

```bash
python3 test_dashboard.py --save-snapshot
```

---

## Troubleshooting

| Issue | What to try |
|-------|-------------|
| `CORALOGIX_API_KEY is not set` | Create `.env` from `.env.example` |
| Dashboard empty / old data | Run `python3 refresh.py` again |
| `package-for-sharing.sh` fails | Install **rsync** + **zip** (macOS: Xcode Command Line Tools) |
| Windows | Use **WSL** or **Git Bash** to run the script, or zip manually excluding `.env` and `data.json` |

---

## Security reminder

Read **`SECURITY.md`**. Never commit `.env`; never put keys in the HTML or in `data.json`.
