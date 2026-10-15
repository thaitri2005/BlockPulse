#!/usr/bin/env bash
# Rehearse the instance bootstrap inside an Amazon Linux 2023 container (no AWS account needed).
# Run from the repository root:
#   docker run --rm -v "$PWD/infra/aws/build:/bundle:ro" -v "$PWD/infra/aws/tests:/tests:ro" \
#     -v "$PWD/infra/aws/tests/fixtures/session:/sample:ro" public.ecr.aws/amazonlinux/amazonlinux:2023 bash /tests/instance_smoke.sh
# It unpacks the bundle the way user data does, runs install.sh (without starting systemd, which containers
# lack), then processes a real saved capture as the service user, uploads through a fake AWS CLI, checks the
# deadline script, and checks that SIGINT stops a capture cleanly.
set -euo pipefail

pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

dnf install -y -q --setopt=install_weak_deps=False shadow-utils util-linux procps-ng findutils >/dev/null

# 1. Unpack exactly as the user data does, then install.
mkdir -p /opt/blockpulse
cp /bundle/blockpulse-app.zip /opt/blockpulse/app.zip
python3 -m zipfile -e /opt/blockpulse/app.zip /opt/blockpulse/app
BLOCKPULSE_BUCKET=test-bucket BLOCKPULSE_DEADLINE=2030-01-01T00:00:00Z AWS_REGION=us-west-2 \
    APP_DIR=/opt/blockpulse/app BLOCKPULSE_SKIP_SYSTEMD=1 bash /opt/blockpulse/app/infra/aws/instance/install.sh
grep -q '^BLOCKPULSE_BUCKET=test-bucket$' /etc/blockpulse/collector.env || fail "env file"
for unit in blockpulse-collect.service blockpulse-process.service blockpulse-process.timer blockpulse-deadline.service blockpulse-deadline.timer; do
    [[ -f /etc/systemd/system/$unit ]] || fail "missing unit $unit"
done
[[ -x /usr/local/bin/blockpulse-deadline-check ]] || fail "deadline script not executable"
[[ "$(stat -c %U /srv/blockpulse/data)" == blockpulse ]] || fail "data folder owner"
pass "install.sh: package, env file, units, deadline script, data folder"

# Re-running the installer must be harmless.
BLOCKPULSE_BUCKET=test-bucket BLOCKPULSE_DEADLINE=2030-01-01T00:00:00Z APP_DIR=/opt/blockpulse/app \
    BLOCKPULSE_SKIP_SYSTEMD=1 bash /opt/blockpulse/app/infra/aws/instance/install.sh >/dev/null
pass "install.sh is safe to run twice"

# A bad deadline is refused before anything is installed.
if BLOCKPULSE_BUCKET=b BLOCKPULSE_DEADLINE=not-a-date APP_DIR=/opt/blockpulse/app BLOCKPULSE_SKIP_SYSTEMD=1 \
    bash /opt/blockpulse/app/infra/aws/instance/install.sh >/dev/null 2>&1; then fail "bad deadline accepted"; fi
pass "install.sh rejects an invalid deadline"

# 2. Process a real saved capture as the service user.
PY=/opt/blockpulse/venv/bin/python
session="session-aws-20261015T000000Z"
install -d -o blockpulse -g blockpulse /srv/blockpulse/data/raw/$session
install -o blockpulse -g blockpulse -m 0644 /sample/messages.jsonl /sample/summary.json /srv/blockpulse/data/raw/$session/
chown -R blockpulse:blockpulse /srv/blockpulse/data  # install -d made the parent folders root-owned
runuser -u blockpulse -- $PY -m blockpulse process-pending --root /srv/blockpulse/data > /tmp/process.json
[[ -f /srv/blockpulse/data/screened/$session/screened.jsonl ]] || fail "screened output"
[[ -f /srv/blockpulse/data/processed/$session/events.jsonl.gz && ! -f /srv/blockpulse/data/processed/$session/events.jsonl ]] || fail "compaction"
rows=$(wc -l < /srv/blockpulse/data/screened/$session/screened.jsonl)
[[ "$rows" -gt 0 ]] || fail "no screened rows"
runuser -u blockpulse -- $PY -m blockpulse process-pending --root /srv/blockpulse/data | grep -q '"sessions": \[\]' || fail "second pass not idempotent"
lake=/srv/blockpulse/data/lake
[[ -f "$lake/_quality/$session.json" ]] || fail "lake quality report"
find "$lake/transactions" -name "$session.parquet" | grep -q . || fail "lake transactions partition"
runuser -u blockpulse -- $PY -c "import pyarrow.dataset as ds; t = ds.dataset('$lake/transactions', partitioning='hive').to_table(); assert t.num_rows > 0, t"     || fail "lake not readable with hive partitioning"
pass "process-pending as the service user: $rows screened rows, lake partitions + quality report, events compacted, second pass is a no-op"

# 3. Upload through a fake AWS CLI that records its arguments.
cat > /usr/local/bin/fake-aws <<'EOF'
#!/usr/bin/env bash
echo "$*" >> /tmp/aws-calls.log
EOF
chmod 0755 /usr/local/bin/fake-aws
runuser -u blockpulse -- env BLOCKPULSE_AWS_CLI=/usr/local/bin/fake-aws $PY -m blockpulse sync-pending \
    --root /srv/blockpulse/data --bucket test-bucket > /tmp/sync.json
for part in raw processed screened; do
    grep -q "s3://test-bucket/data/$part/$session/" /tmp/aws-calls.log || fail "upload of $part"
done
grep -q "s3://test-bucket/status/status.json" /tmp/aws-calls.log || fail "status upload"
grep -q "s3://test-bucket/data/lake/_quality/$session.json" /tmp/aws-calls.log || fail "lake upload"
[[ -f /srv/blockpulse/data/screened/$session/.uploaded ]] || fail "upload marker"
pass "sync-pending: raw, processed, screened and status uploaded once, marker written"

# 4. Deadline script: future deadline does nothing, past deadline powers off.
ENV_FILE=/etc/blockpulse/collector.env POWEROFF_COMMAND="echo POWEROFF" /usr/local/bin/blockpulse-deadline-check | grep -q "minutes" \
    || fail "future deadline"
sed 's/^BLOCKPULSE_DEADLINE=.*/BLOCKPULSE_DEADLINE=2020-01-01T00:00:00Z/' /etc/blockpulse/collector.env > /tmp/past.env
ENV_FILE=/tmp/past.env POWEROFF_COMMAND="echo POWEROFF" /usr/local/bin/blockpulse-deadline-check 2>/dev/null | grep -q POWEROFF \
    || fail "past deadline"
pass "deadline check: waits before the deadline, powers off after it"

# 5. SIGINT (what systemd sends on stop, via KillSignal=SIGINT) ends a capture cleanly with a summary and exit 130.
# Background jobs in a non-interactive shell start with SIGINT ignored, unlike systemd services, so restore the
# default disposition before exec. setpriv switches to the service user without forking, so the signal reaches Python.
python3 -c 'import os, signal, sys; signal.signal(signal.SIGINT, signal.SIG_DFL); os.execvp(sys.argv[1], sys.argv[1:])'     setpriv --reuid=blockpulse --regid=blockpulse --init-groups     $PY -m blockpulse collect-loop --root /srv/blockpulse/data --prefix session-aws     --session-seconds 60 --url wss://127.0.0.1:9/ws --max-sessions 1 --min-free-gib 0 > /tmp/loop.json 2>&1 &
pid=$!
sleep 4
kill -INT "$pid"
set +e; wait "$pid"; code=$?; set -e
[[ $code -eq 130 ]] || { tail -5 /tmp/loop.json >&2; fail "collect-loop exit code after SIGINT was $code"; }
latest=$(find /srv/blockpulse/data/raw -maxdepth 1 -name "session-aws-*" | sort | tail -1)
grep -q '"stop_reason": "interrupted"' "$latest/summary.json" || fail "interrupted capture has no summary"
pass "SIGINT stops a capture cleanly (exit 130, summary written, as systemd's stop expects)"

echo "ALL INSTANCE SMOKE TESTS PASSED"
