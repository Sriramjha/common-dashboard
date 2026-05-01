#!/usr/bin/env bash
# Provision Ubuntu EC2 in ap-south-1, bootstrap Common Dashboard, and prepare GitHub Actions SSH deploy.
#
# Usage:
#   bash deploy/provision-ec2-from-scratch.sh [path/to/accessKeys.csv]
#
# Env:
#   AWS_REGION      default ap-south-1
#   INSTANCE_TYPE   default t3.micro
#   HTTP_CIDR       default 0.0.0.0/0 (nginx :80)
#
# Flags (optional):
#   --ssh-cidr CIDR     Restrict SSH :22 (default: this machine's public /32 from checkip.amazonaws.com)
#   --ssh-open          Allow SSH from 0.0.0.0/0 (INSECURE; only for quick demos)
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

AWS_REGION="${AWS_REGION:-ap-south-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t3.micro}"
HTTP_CIDR="${HTTP_CIDR:-0.0.0.0/0}"
CRED_CSV="$ROOT/Sri_User_accessKeys.csv"
SSH_CIDR=""
SSH_OPEN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssh-cidr)
      SSH_CIDR="${2:?}"
      shift 2
      ;;
    --ssh-open)
      SSH_OPEN=1
      shift
      ;;
    -*)
      echo "Unknown option: $1"
      exit 1
      ;;
    *)
      CRED_CSV="$1"
      shift
      ;;
  esac
done

if [[ ! -f "$CRED_CSV" ]]; then
  echo "Credentials CSV not found: $CRED_CSV"
  echo "Usage: bash deploy/provision-ec2-from-scratch.sh [path/to/accessKeys.csv]"
  exit 1
fi

if [[ "$SSH_OPEN" -eq 1 ]]; then
  SSH_CIDR="0.0.0.0/0"
elif [[ -z "$SSH_CIDR" ]]; then
  echo "Detecting public IP for SSH security group rule..."
  MYIP="$(curl -fsSL --max-time 15 https://checkip.amazonaws.com | tr -d '[:space:]')"
  SSH_CIDR="${MYIP}/32"
fi

echo "Loading AWS credentials from CSV (not printed)..."
# UTF-8 BOM-safe parse; supports header: Access key ID, Secret access key
eval "$(
  python3 <<'PY' "$CRED_CSV"
import csv, shlex, sys
path = sys.argv[1]
with open(path, newline="", encoding="utf-8-sig") as f:
    rows = list(csv.DictReader(f))
if not rows:
    sys.exit("No rows in CSV")
r = rows[0]
keys = {k.strip().lstrip("\ufeff"): v.strip() for k, v in r.items()}
ak = keys.get("Access key ID") or keys.get("AWSAccessKeyId")
sk = keys.get("Secret access key") or keys.get("AWSSecretKey")
if not ak or not sk:
    sys.exit("CSV must contain Access key ID and Secret access key columns")
print(f"export AWS_ACCESS_KEY_ID={shlex.quote(ak)}")
print(f"export AWS_SECRET_ACCESS_KEY={shlex.quote(sk)}")
PY
)"
export AWS_DEFAULT_REGION="$AWS_REGION"

echo "Caller identity:"
aws sts get-caller-identity

mkdir -p "$ROOT/deploy/.keys"
chmod 700 "$ROOT/deploy/.keys"

KP_NAME="common-dashboard-$(date +%Y%m%d%H%M)"
PEM_OUT="$ROOT/deploy/.keys/${KP_NAME}.pem"

echo "Creating key pair $KP_NAME..."
aws ec2 create-key-pair --region "$AWS_REGION" --key-name "$KP_NAME" \
  --query 'KeyMaterial' --output text > "$PEM_OUT"
chmod 600 "$PEM_OUT"

VPC_ID="$(aws ec2 describe-vpcs --region "$AWS_REGION" --filters Name=isDefault,Values=true \
  --query 'Vpcs[0].VpcId' --output text)"
if [[ -z "$VPC_ID" || "$VPC_ID" == "None" ]]; then
  echo "No default VPC found. Create a VPC or set a non-default default in this account/region."
  exit 1
fi

SUBNET_ID="$(aws ec2 describe-subnets --region "$AWS_REGION" \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=default-for-az,Values=true" \
  --query 'Subnets[0].SubnetId' --output text)"
if [[ -z "$SUBNET_ID" || "$SUBNET_ID" == "None" ]]; then
  echo "No default subnet in VPC $VPC_ID. Create a public subnet or pick one manually."
  exit 1
fi

SG_NAME="common-dashboard-sg"
SG_ID="$(aws ec2 describe-security-groups --region "$AWS_REGION" \
  --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$VPC_ID" \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
if [[ -z "$SG_ID" || "$SG_ID" == "None" ]]; then
  SG_ID="$(aws ec2 create-security-group --region "$AWS_REGION" \
    --group-name "$SG_NAME" --description "Common Dashboard + deploy" \
    --vpc-id "$VPC_ID" --query 'GroupId' --output text)"
fi

authorize_once() {
  local port="$1" cidr="$2"
  aws ec2 authorize-security-group-ingress --region "$AWS_REGION" \
    --group-id "$SG_ID" --protocol tcp --port "$port" --cidr "$cidr" 2>/dev/null || true
}

authorize_once 22 "$SSH_CIDR"
authorize_once 80 "$HTTP_CIDR"

AMI="$(aws ec2 describe-images --region "$AWS_REGION" --owners 099720109477 \
  --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
  --query 'sort_by(Images, &CreationDate)[-1].ImageId' --output text)"

run_id="$(aws ec2 run-instances --region "$AWS_REGION" \
  --image-id "$AMI" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KP_NAME" \
  --subnet-id "$SUBNET_ID" \
  --associate-public-ip-address \
  --security-group-ids "$SG_ID" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=CommonDashboard}]" \
  --query 'Instances[0].InstanceId' --output text)"

echo "Waiting for instance $run_id to be running..."
aws ec2 wait instance-running --region "$AWS_REGION" --instance-ids "$run_id"

PUB_IP="$(aws ec2 describe-instances --region "$AWS_REGION" --instance-ids "$run_id" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"

if [[ -z "$PUB_IP" || "$PUB_IP" == "None" ]]; then
  echo "Instance has no public IP. Assign an Elastic IP or use a public subnet."
  exit 1
fi

echo "Waiting for SSH on $PUB_IP ..."
for i in $(seq 1 40); do
  if ssh -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new \
      -i "$PEM_OUT" "ubuntu@$PUB_IP" "echo ok" 2>/dev/null; then
    break
  fi
  sleep 5
  if [[ "$i" -eq 40 ]]; then
    echo "SSH did not become ready. Check SG (port 22 from $SSH_CIDR) and instance status."
    exit 1
  fi
done

DEPLOY_PRIV="$ROOT/deploy/.keys/gh-actions-deploy"
DEPLOY_PUB="$ROOT/deploy/.keys/gh-actions-deploy.pub"
if [[ ! -f "$DEPLOY_PRIV" ]]; then
  ssh-keygen -t ed25519 -f "$DEPLOY_PRIV" -C "github-actions-deploy-common-dashboard" -N ""
  chmod 600 "$DEPLOY_PRIV"
fi

echo "Syncing application to EC2 (excluding secrets / venv)..."
RSYNC_EXCLUDES=(
  --exclude '.git/'
  --exclude '.venv/'
  --exclude '.DS_Store'
  --exclude 'Sri_User_accessKeys.csv'
  --exclude '*accessKeys*.csv'
  --exclude 'deploy/.keys/'
  --exclude '.cursor/'
)
rsync -avz "${RSYNC_EXCLUDES[@]}" -e "ssh -i $PEM_OUT -o StrictHostKeyChecking=accept-new" \
  "$ROOT/" "ubuntu@$PUB_IP:/opt/common-dashboard/"

echo "Running bootstrap on EC2..."
ssh -i "$PEM_OUT" -o StrictHostKeyChecking=accept-new "ubuntu@$PUB_IP" "sudo bash /opt/common-dashboard/deploy/bootstrap-ec2-ubuntu.sh"
scp -i "$PEM_OUT" -o StrictHostKeyChecking=accept-new "$DEPLOY_PUB" "ubuntu@$PUB_IP:/tmp/gh-actions.pub"
ssh -i "$PEM_OUT" -o StrictHostKeyChecking=accept-new "ubuntu@$PUB_IP" 'bash -s' <<'REMOTE'
set -euo pipefail
install -d -m 700 "$HOME/.ssh"
touch "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"
KEY="$(cat /tmp/gh-actions.pub)"
grep -qxF "$KEY" "$HOME/.ssh/authorized_keys" || printf '%s\n' "$KEY" >> "$HOME/.ssh/authorized_keys"
rm -f /tmp/gh-actions.pub
REMOTE

echo ""
echo "==================== DONE ===================="
echo "EC2 public IP:     $PUB_IP"
echo "Instance ID:      $run_id"
echo "Region:           $AWS_REGION"
echo "SSH (you):        ssh -i \"$PEM_OUT\" ubuntu@$PUB_IP"
echo "HTTP:             http://$PUB_IP/"
echo ""
echo "GitHub Actions secrets (Settings → Secrets → Actions):"
echo "  EC2_HOST=$PUB_IP"
echo "  EC2_USER=ubuntu"
echo "  EC2_SSH_PRIVATE_KEY  ← paste ENTIRE contents of:"
echo "       $DEPLOY_PRIV"
echo ""
echo "Important: SSH :22 is allowed only from: $SSH_CIDR"
echo "GitHub-hosted runners use different IPs — deploy workflow will fail until you either:"
echo "  • widen port 22 for GitHub (see deploy/README-DEPLOY.md), use a self-hosted runner in VPC, or use SSM;"
echo "  • or temporarily: recreate SG rule ssh from 0.0.0.0/0 (re-run with --ssh-open) — not for production."
echo ""
echo "Next — publish to GitHub (after: gh auth login):"
echo "  cd \"$ROOT\""
echo "  git remote add origin https://github.com/Sriramjha/common-dashboard.git"
echo "  git branch -M main && git push -u origin main"
echo ""
