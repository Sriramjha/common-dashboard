# Contributing & development workflow

## Environment

- **Python 3.9+**, stdlib only (no `pip install` for core tools).
- **Never commit** `.env`, API keys, or `data.json`. See `SECURITY.md`.

```bash
cp .env.example .env
# Edit .env — your personal Coralogix API key + region base URL
python3 refresh.py    # generates local data.json (gitignored)
python3 serve.py      # http://127.0.0.1:8765/coralogix-dashboard.html
```

## Typical change flow

1. **Pull latest** `main` (or your team’s default branch).
2. **Create a branch**  
   `git checkout -b feature/short-description` or `fix/…`
3. **Edit** `coralogix-dashboard.html`, `refresh.py`, `serve.py`, or `test_dashboard.py` as needed.
4. **Refresh data** locally if your change depends on live JSON shape: `python3 refresh.py`
5. **Run tests** (with `serve.py` running in another terminal for the server check):  
   `python3 test_dashboard.py`
6. **Commit** with a clear message; **open a PR** (or merge policy your lead defines).

## What to change for “builds as per requirement”

| Need | Where to work |
|------|----------------|
| UI, layout, charts, health cards | `coralogix-dashboard.html` (JS blocks + HTML) |
| API fetch shape, new endpoints, `data.json` fields | `refresh.py` |
| Port, headers, caching | `serve.py` |
| Automated checks vs live API / `data.json` | `test_dashboard.py` |

## Code style

- Prefer **small, focused commits**.
- Keep **secrets out of** HTML, Python strings, and tests — use `.env` / env vars only.
- If you add new env vars, document them in **`.env.example`**.

## Questions?

Ask the repo owner or open a discussion in your team’s chat — include branch name and what you’re trying to ship.
