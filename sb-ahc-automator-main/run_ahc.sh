#!/bin/bash
# Run AHC - avoids shell line-continuation issues with multi-line commands
# Usage: ./run_ahc.sh REGION COMPANY_ID CX_API_KEY SESSION_TOKEN
# Example: ./run_ahc.sh AP1 1010786 cxup_xxx "eyJ..."

set -e
cd "$(dirname "$0")"
python3 ahc_runner.py --region "$1" --company-id "$2" --cx-api-key "$3" --session-token "$4"
