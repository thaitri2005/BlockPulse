#!/usr/bin/env bash
# Run both instance rehearsals in Amazon Linux 2023 containers. No AWS account needed; needs Docker.
#   bash infra/aws/tests/run_container_tests.sh          (from the repository root; also used by CI)
# 1. instance_smoke.sh: install, process, upload, deadline logic and SIGINT in a plain container.
# 2. systemd_e2e.sh: the real systemd units in a container booted with systemd, ending in the deadline poweroff.
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash on Windows: don't rewrite container paths
root=$(pwd)
image=public.ecr.aws/amazonlinux/amazonlinux:2023
mounts=(-v "$root/infra/aws/build:/bundle:ro" -v "$root/infra/aws/tests:/tests:ro" -v "$root/infra/aws/tests/fixtures/session:/sample:ro")

python3 infra/aws/build_bundle.py 2>/dev/null || python infra/aws/build_bundle.py

echo "== instance smoke test"
docker run --rm "${mounts[@]}" "$image" bash /tests/instance_smoke.sh

echo "== systemd end-to-end test"
docker build -q -t blockpulse-al2023-systemd -f infra/aws/tests/systemd.Dockerfile infra/aws/tests >/dev/null
docker rm -f bp-systemd >/dev/null 2>&1 || true
docker run -d --name bp-systemd --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw "${mounts[@]}" \
    blockpulse-al2023-systemd >/dev/null
trap 'docker rm -f bp-systemd >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 30); do
    state=$(docker exec bp-systemd systemctl is-system-running 2>/dev/null || true)
    [[ "$state" == running || "$state" == degraded ]] && break
    sleep 1
done
docker exec bp-systemd bash /tests/systemd_e2e.sh
for _ in $(seq 1 60); do
    if [[ "$(docker inspect -f '{{.State.Status}}' bp-systemd)" == exited ]]; then
        echo "PASS deadline check powered the instance off"
        echo "ALL CONTAINER TESTS PASSED"
        exit 0
    fi
    sleep 1
done
echo "FAIL the deadline check did not power the container off" >&2
exit 1
