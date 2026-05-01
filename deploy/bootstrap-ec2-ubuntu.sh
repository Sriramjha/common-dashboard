#!/usr/bin/env bash
# One-time setup on Ubuntu 22.04+ EC2 for Common Dashboard + nginx reverse proxy.
# Run with: sudo bash deploy/bootstrap-ec2-ubuntu.sh
set -euo pipefail

APP_USER="${APP_USER:-dashboard}"
APP_DIR="${APP_DIR:-/opt/common-dashboard}"

if [[ "$(id -u)" != "0" ]]; then
  echo "Run as root: sudo bash $0"
  exit 1
fi

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nginx python3 ca-certificates rsync openssh-client

if ! id -u "$APP_USER" &>/dev/null; then
  useradd --system --home "$APP_DIR" --create-home "$APP_USER" || true
fi

mkdir -p "$APP_DIR"
mkdir -p "$APP_DIR/accounts/secrets"
chown -R "$APP_USER:$APP_USER" "$APP_DIR"

# Environment file readable only by root + dashboard (populate with CORALOGIX_* etc.)
mkdir -p /etc/common-dashboard
touch /etc/common-dashboard/env
chown root:"$APP_USER" /etc/common-dashboard/env
chmod 640 /etc/common-dashboard/env

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
install -o root -g root -m 0644 "$SCRIPT_DIR/common-dashboard.service" /etc/systemd/system/common-dashboard.service
install -o root -g root -m 0644 "$SCRIPT_DIR/nginx-common-dashboard.conf" /etc/nginx/sites-available/common-dashboard
ln -sf /etc/nginx/sites-available/common-dashboard /etc/nginx/sites-enabled/common-dashboard
rm -f /etc/nginx/sites-enabled/default

nginx -t
systemctl daemon-reload
systemctl enable nginx common-dashboard
systemctl restart nginx

echo ""
echo "Next steps:"
echo "  1. Edit secrets: sudoeditor /etc/common-dashboard/env   (# KEY=value , chmod finalized below)"
echo "  2. chown root:dashboard /etc/common-dashboard/env && chmod 640 /etc/common-dashboard/env"
echo "  3. Place manifest & account secrets ONLY on this host under $APP_DIR/accounts/"
echo "  4. From GitHub Actions, rsync fills $APP_DIR; then: sudo systemctl restart common-dashboard"
echo "  5. Restrict SG: tcp/80 from your VPN egress only; tcp/22 for deploy (see deploy/README-DEPLOY.md)"
