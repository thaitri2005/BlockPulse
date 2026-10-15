#!/usr/bin/env bash
# Install the BlockPulse collector on Amazon Linux 2023. Run as root.
# The instance's user data downloads the app bundle from S3, unpacks it, and runs this script. It is safe to run
# again by hand (for example after replacing the bundle); existing data under /srv/blockpulse is never touched.
#
# Required environment:  BLOCKPULSE_BUCKET, BLOCKPULSE_DEADLINE (UTC, ISO 8601, e.g. 2026-10-19T04:00:00Z)
# Optional environment:  BLOCKPULSE_URL, BLOCKPULSE_SESSION_SECONDS, BLOCKPULSE_MIN_FREE_GIB, AWS_REGION,
#                        APP_DIR (the unpacked bundle), PYTHON (default python3.11),
#                        BLOCKPULSE_SKIP_SYSTEMD=1 (install files but don't start services; used by tests)
set -euo pipefail

: "${BLOCKPULSE_BUCKET:?set BLOCKPULSE_BUCKET}"
: "${BLOCKPULSE_DEADLINE:?set BLOCKPULSE_DEADLINE}"
APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
PYTHON="${PYTHON:-python3.11}"
URL="${BLOCKPULSE_URL:-wss://node203.sv1.mempool.space/api/v1/ws}"
SESSION_SECONDS="${BLOCKPULSE_SESSION_SECONDS:-3600}"
MIN_FREE_GIB="${BLOCKPULSE_MIN_FREE_GIB:-5}"
REGION="${AWS_REGION:-}"
PREFIX=/opt/blockpulse
HOME_DIR=/srv/blockpulse
ENV_FILE=/etc/blockpulse/collector.env

log() { printf '[blockpulse-install] %s\n' "$*"; }

# Fail early on a deadline the collector would not understand.
if ! date -u -d "$BLOCKPULSE_DEADLINE" +%s >/dev/null 2>&1; then
    log "BLOCKPULSE_DEADLINE is not a valid date: $BLOCKPULSE_DEADLINE" >&2
    exit 2
fi
if [[ ! -f "$APP_DIR/pyproject.toml" ]]; then
    log "APP_DIR ($APP_DIR) does not contain the BlockPulse bundle" >&2
    exit 2
fi

if ! command -v "$PYTHON" >/dev/null 2>&1; then
    log "installing $PYTHON"
    dnf install -y --setopt=install_weak_deps=False "$PYTHON" "${PYTHON}-pip" >/dev/null
fi

if ! id blockpulse >/dev/null 2>&1; then
    useradd --system --home-dir "$HOME_DIR" --shell /sbin/nologin blockpulse
fi
install -d -o blockpulse -g blockpulse -m 0750 "$HOME_DIR" "$HOME_DIR/data"
install -d -m 0755 "$PREFIX" /etc/blockpulse

log "installing the Python package into $PREFIX/venv"
"$PYTHON" -m venv "$PREFIX/venv"
"$PREFIX/venv/bin/pip" install --quiet --no-cache-dir --disable-pip-version-check "$APP_DIR"
# The version number doesn't change between code updates, so force the app itself (not its dependencies) to
# reinstall; otherwise a re-run after uploading a new bundle could keep the old code.
"$PREFIX/venv/bin/pip" install --quiet --no-cache-dir --disable-pip-version-check --force-reinstall --no-deps "$APP_DIR"

log "writing $ENV_FILE"
umask 022
cat > "$ENV_FILE" <<EOF
# Written by install.sh. Read by the blockpulse systemd units.
BLOCKPULSE_BUCKET=$BLOCKPULSE_BUCKET
BLOCKPULSE_DEADLINE=$BLOCKPULSE_DEADLINE
BLOCKPULSE_URL=$URL
BLOCKPULSE_SESSION_SECONDS=$SESSION_SECONDS
BLOCKPULSE_MIN_FREE_GIB=$MIN_FREE_GIB
AWS_DEFAULT_REGION=$REGION
EOF

install -d -m 0755 /etc/systemd/system
install -m 0644 "$APP_DIR"/infra/aws/instance/systemd/blockpulse-*.service "$APP_DIR"/infra/aws/instance/systemd/blockpulse-*.timer \
    /etc/systemd/system/
install -m 0755 "$APP_DIR/infra/aws/instance/deadline-check.sh" /usr/local/bin/blockpulse-deadline-check

# Smoke test as the service user: the package imports and the CLI starts.
runuser -u blockpulse -- "$PREFIX/venv/bin/python" -m blockpulse --help >/dev/null
log "package OK: $("$PREFIX/venv/bin/python" -c 'import blockpulse, sys; print(sys.version.split()[0])')"

if [[ "${BLOCKPULSE_SKIP_SYSTEMD:-0}" == 1 ]]; then
    log "BLOCKPULSE_SKIP_SYSTEMD=1: files installed, services not started"
    exit 0
fi
systemctl daemon-reload
systemctl enable --now blockpulse-collect.service blockpulse-process.timer blockpulse-deadline.timer
log "collector running; deadline $BLOCKPULSE_DEADLINE"
