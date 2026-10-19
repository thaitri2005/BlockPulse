#!/usr/bin/env bash
# Run the collector's real systemd units inside an Amazon Linux 2023 container booted with systemd.
# Driven by run_container_tests.sh (see docs/guides/08-aws.md). Ends by triggering the deadline poweroff,
# so a passing run finishes with the container shutting itself down.
set -euo pipefail

pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*" >&2; journalctl --no-pager -n 40 >&2 || true; exit 1; }

# A fake AWS CLI that records calls inside the service's writable folder (services get a private /tmp).
cat > /usr/local/bin/aws <<'EOF'
#!/usr/bin/env bash
echo "$*" >> /srv/blockpulse/aws-calls.log
EOF
chmod 0755 /usr/local/bin/aws

mkdir -p /opt/blockpulse
cp /bundle/blockpulse-app.zip /opt/blockpulse/app.zip
python3 -m zipfile -e /opt/blockpulse/app.zip /opt/blockpulse/app
BLOCKPULSE_BUCKET=test-bucket BLOCKPULSE_DEADLINE=2030-01-01T00:00:00Z BLOCKPULSE_URL=wss://127.0.0.1:9/ws \
    BLOCKPULSE_SESSION_SECONDS=300 AWS_REGION=us-west-2 APP_DIR=/opt/blockpulse/app \
    bash /opt/blockpulse/app/infra/aws/instance/install.sh

sleep 5
[[ "$(systemctl is-active blockpulse-collect.service)" == active ]] || fail "collect service not active"
for timer in blockpulse-process.timer blockpulse-deadline.timer; do
    [[ "$(systemctl is-enabled "$timer")" == enabled && "$(systemctl is-active "$timer")" == active ]] || fail "$timer"
done
pass "install.sh started the capture service and both timers under systemd"

# Processing and upload under the full sandbox.
session="session-aws-20261015T000000Z"
mkdir -p "/srv/blockpulse/data/raw/$session"
cp /sample/messages.jsonl /sample/summary.json "/srv/blockpulse/data/raw/$session/"
chown -R blockpulse:blockpulse /srv/blockpulse
systemctl start blockpulse-process.service || true
[[ "$(systemctl show -p Result --value blockpulse-process.service)" == success ]] || fail "process service result"
[[ -f "/srv/blockpulse/data/screened/$session/.uploaded" ]] || fail "session not uploaded"
grep -q "s3://test-bucket/data/screened/$session/" /srv/blockpulse/aws-calls.log || fail "upload call"
grep -q "s3://test-bucket/data/lake/transactions/dt=" /srv/blockpulse/aws-calls.log || fail "lake upload call"
pass "blockpulse-process.service: processed, lake built, compacted and uploaded inside the sandbox"

# A normal stop must be clean: SIGINT, summary written, unit inactive rather than failed.
systemctl stop blockpulse-collect.service
state=$(systemctl show -p ActiveState --value blockpulse-collect.service)
result=$(systemctl show -p Result --value blockpulse-collect.service)
[[ "$state" == inactive && "$result" == success ]] || fail "stop left the unit $state/$result"
latest=$(find /srv/blockpulse/data/raw -maxdepth 1 -name "session-aws-2*" ! -name "$session" | sort | tail -1)
grep -q '"stop_reason": "interrupted"' "$latest/summary.json" || fail "stopped capture has no clean summary"
pass "systemctl stop: clean interrupt, summary written, unit inactive/success"

# Deadline: set it in the past and run the check; the container should power itself off.
sed -i 's/^BLOCKPULSE_DEADLINE=.*/BLOCKPULSE_DEADLINE=2020-01-01T00:00:00Z/' /etc/blockpulse/collector.env
echo "TRIGGERING DEADLINE POWEROFF"
systemctl start --no-block blockpulse-deadline.service
