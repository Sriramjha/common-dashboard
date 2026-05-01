# Deploy: GitHub Actions → Ubuntu EC2 (ap-south-1)

Targets your setup:

| Item | Value |
|------|--------|
| Region | `ap-south-1` |
| OS | Ubuntu |
| Listener | nginx **HTTP :80** → `serve.py` on `127.0.0.1:8765` (app is localhost-only by design) |
| Branch | `main` |
| Method | SSH + rsync from **GitHub-hosted** runners |

## 1. Create the Git repository

Your GitHub profile is [github.com/Sriramjha](https://github.com/Sriramjha). Create a **new repository** (e.g. `common-dashboard`) and push **this codebase** — **never** commit `.env`, `data*.json`, real `accounts/manifest.json`, or `accounts/secrets/`.

---

## 2. Security Group (AWS)

Because `serve.py` binds to **localhost only**, **nginx :80** is what users/VPN hit.

Suggested rules:

| Direction | Port | Source | Purpose |
|-----------|------|--------|---------|
| Inbound | **80/tcp** | **18.192.144.83/32** | Dashboard HTTP (your VPN egress) |
| Inbound | **22/tcp** | See warning below | **Deploy SSH** |

**Important — GitHub Actions vs your VPN IP**

- **18.192.144.83** is suitable for **you** accessing the dashboard.
- **GitHub-hosted runners** exit through [GitHub’s own IP ranges](https://docs.github.com/en/actions/using-github-hosted-runners/using-github-hosted-runners/about-github-hosted-runners), **not** through your VPN. So **`18.192.144.83/32` alone will NOT allow Actions to SSH in.**

Pick **one**:

1. **Allow GitHub Actions IP ranges on port 22** (large surface; automate updates from GitHub `meta API`),  
2. **Self-hosted runner** inside your VPC (recommended with strict SG),  
3. **No inbound SSH**: use **AWS SSM** + OIDC later (different workflow).

Until you widen **22** appropriately or use SSM/routed runner, the workflow SSH step will fail.

---

## 3. One-time EC2 bootstrap (Ubuntu)

On the instance (SSH as `ubuntu` or your admin user):

```bash
# Copy repo (or unzip) then:
sudo bash /opt/common-dashboard/deploy/bootstrap-ec2-ubuntu.sh
```

If the repo is not there yet, clone once (or SCP), then run the script from `./deploy/`:

```bash
sudo mkdir -p /opt/common-dashboard
sudo chown ubuntu:ubuntu /opt/common-dashboard
cd /opt/common-dashboard && git clone <YOUR_REPO_URL> .   # example
sudo bash deploy/bootstrap-ec2-ubuntu.sh
```

Set secrets **only on EC2** (Coralogix + VPN admin for Add/Edit/Refresh from the browser):

```bash
sudo install -o root -g dashboard -m 640 /dev/null /etc/common-dashboard/env
sudo nano /etc/common-dashboard/env
# Example:
#   CORALOGIX_API_KEY=...
#   CORALOGIX_API_BASE=https://api.../api/v2/external
#   CORALOGIX_DASH_TRUST_X_FORWARDED_FOR=1
#   CORALOGIX_DASH_ADMIN_ALLOW_CIDR=18.192.144.83/32
sudo chown root:dashboard /etc/common-dashboard/env
sudo chmod 640 /etc/common-dashboard/env
sudo systemctl restart common-dashboard
```

Nginx must set `X-Real-IP` / `X-Forwarded-For` (see `deploy/nginx-common-dashboard.conf`). Without trust + allowlist, only `127.0.0.1` can use those admin APIs (e.g. SSH tunnel).

---

## 4. Deploy user & SSH key

1. Generate a deploy keypair (keep private key for GitHub secret only):

   ```bash
   ssh-keygen -t ed25519 -f ~/.ssh/gh-deploy-common-dashboard -C "github-actions-deploy" -N ""
   ```

2. On EC2 `~ubuntu/.ssh/authorized_keys`, add **`gh-deploy-common-dashboard.pub`** (restrict with `command=`, `from=` if desired).

3. In GitHub: **Repo → Settings → Secrets and variables → Actions → New**

   | Name | Example |
   |------|---------|
   | `EC2_SSH_PRIVATE_KEY` | Contents of **`gh-deploy-common-dashboard`** (PEM/private) |
   | `EC2_HOST` | EC2 **public IPv4** or reachable DNS (must be reachable from runner for SSH) |
   | `EC2_USER` | `ubuntu` |

If the EC2 **private IP** is not routable from the internet, **`EC2_HOST` must be a public IP**, **VPN-attached bastion hostname**, **Site-to-Site egress**, **self-hosted runner in VPC**, or similar.

---

## 5. What the workflow does

File: [.github/workflows/deploy.yml](../.github/workflows/deploy.yml)

- On **push to `main`** (or manual **workflow_dispatch**): checkout → `rsync` to `/opt/common-dashboard/` with [--delete](../deploy/rsync-filters.txt), **excluding deletion** of `.env`, `accounts/secrets/`, `accounts/manifest.json`, `data.json`, `data.*.json` on the server.
- Restart **`common-dashboard`** and **nginx**.

---

## 6. Optional follow-ups

- **HTTPS**: switch nginx to TLS (Let’s Encrypt or ACM behind a load balancer).
- **Stronger CI**: compile check job on PRs before merge (no secrets required).
- **SSM/OIDC**: replace inbound SSH from GitHub with **Session Manager**.

---

## 7. Smoke test locally on EC2

```bash
curl -sSf http://127.0.0.1:8765/coralogix-dashboard.html | head -5
curl -sSf http://127.0.0.1/ | head -5
```

(from VPN:) `curl -sSf http://<EC2_IP>/coralogix-dashboard.html`
