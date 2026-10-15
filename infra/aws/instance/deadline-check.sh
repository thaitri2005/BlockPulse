#!/usr/bin/env bash
# Run every 15 minutes by blockpulse-deadline.timer. Once the collection deadline has passed it stops the
# capture (cleanly), runs a final process-and-upload pass, and powers the instance off, so a forgotten
# instance stops costing compute. Stopped instances still pay for their disk until `terraform destroy`.
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/blockpulse/collector.env}"
POWEROFF_COMMAND="${POWEROFF_COMMAND:-systemctl poweroff}"
# shellcheck source=/dev/null
source "$ENV_FILE"

now=$(date -u +%s)
deadline=$(date -u -d "$BLOCKPULSE_DEADLINE" +%s)
if (( now < deadline )); then
    echo "collection deadline in $(( (deadline - now) / 60 )) minutes ($BLOCKPULSE_DEADLINE)"
    exit 0
fi

echo "deadline $BLOCKPULSE_DEADLINE has passed: stopping capture, uploading, powering off"
systemctl stop blockpulse-collect.service || true
systemctl start blockpulse-process.service || echo "final processing reported a problem; powering off anyway"
# shellcheck disable=SC2086  # POWEROFF_COMMAND is a command line on purpose
$POWEROFF_COMMAND
