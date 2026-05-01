# Add a teammate to this project (Git + host)

This repo is set up so **both of you use Git**: history, branches, reviews, and no accidental sharing of `.env`.

---

## Part A — You (project owner)

### 1. Create a remote repository

On **GitHub**, **GitLab**, or **Azure DevOps**:

- New **private** repository (recommended), e.g. `coralogix-dashboard`
- **Do not** add a README/license on the host if you already have files locally (avoids merge noise), *or* choose “empty repo” and push below.

### 2. Push this folder (first time)

From your machine, in the project directory:

```bash
cd "/path/to/coralogix-dashboard"

git status                    # should show branch main (or master)
git remote add origin https://github.com/YOUR_ORG/coralogix-dashboard.git
git push -u origin main
```

Use your host’s real HTTPS or SSH URL. If the default branch is `master`, rename or push accordingly:

```bash
git branch -M main
git push -u origin main
```

### 3. Invite your teammate

- **GitHub:** Repo → **Settings** → **Collaborators** (or use a **Team** if your org uses them) → invite by username or email.  
- **GitLab:** **Project information** → **Members**.  
- They must **accept** the invite before they can push.

### 4. Optional protections

- **Branch protection** on `main`: require PR, or allow direct pushes for a tiny team.  
- Confirm **`.env`** is never committed (already in `.gitignore`).

---

## Part B — Your teammate

### 1. Clone

```bash
git clone https://github.com/YOUR_ORG/coralogix-dashboard.git
cd coralogix-dashboard
```

### 2. Local secrets & data

```bash
cp .env.example .env
# Put their own CORALOGIX_API_KEY + CORALOGIX_API_BASE in .env
python3 refresh.py
```

### 3. Run & develop

```bash
python3 serve.py
# Browser: http://127.0.0.1:8765/coralogix-dashboard.html
```

Read **`CONTRIBUTING.md`** for branch/commit/test habits.

---

## If you are not using Git (one-off handoff only)

Use **`SHARING.md`** and `package-for-sharing.sh` — that path is for zip/Drive, not ongoing co-development.

---

## Checklist

| Step | Owner | Teammate |
|------|--------|----------|
| Remote repo created | ✅ | |
| Code pushed | ✅ | |
| Collaborator invited | ✅ | |
| Invite accepted | | ✅ |
| Repo cloned | | ✅ |
| `.env` created locally | | ✅ |
| `refresh.py` + `serve.py` work | | ✅ |

If anything fails (auth, 403), check SSH keys / PAT and that the invite was accepted.
