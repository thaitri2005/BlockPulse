# DE7 · Kubernetes: session log and measurements

Started 2026-11-24, after a five-week pause (last commit 2026-10-20). This file is the running record of DE7: what was done, what was measured, what broke. The how-to is [guide 14](../guides/14-kubernetes.md).

## Where this work runs

DE7 is being built in a cloud development session, not on the laptop: a Linux container with 4 CPUs, 15.7 GiB of memory, no swap, 61 GB of free disk and Docker. The container is ephemeral, so:

- Only what is committed and pushed survives. Work is pushed after every step to the branch `claude/gallant-ptolemy-866nd6`.
- The 2.65 M-transaction dataset (`data/`, git-ignored, on the laptop) is not here. Everything in DE7 runs on generated or fixture data, and the numbers below say so.
- The cluster, images and tools are rebuilt at the start of each session (`bash infra/k8s/cluster-up.sh`).

## Baseline check before starting (2026-11-24)

| Check | Result |
| --- | --- |
| Venv from `requirements.lock` with hashes, Python 3.12, Linux | Installed without changes to the lock |
| `pytest -q` with a Postgres 17 test database | 155 passed, 1 skipped (the loopback-socket test, opt-in) |
| `ruff check src tests infra` | Clean |

The count is one more passed and one fewer skipped than the Windows figure in `CLAUDE.md` (154 and 2), because the Linux-only test runs here. Nothing had rotted in five weeks.

## DE7a · A cluster that can run pods

Tools installed in the session: k3d 5.8.3, kubectl 1.31.5, Helm 3.17.3 (copied out of the `alpine/helm` image, because `get.helm.sh` is not reachable from the session), k3s image v1.31.5-k3s1.

| Attempt | Result |
| --- | --- |
| `k3d cluster create` | Node `Ready`, but no pod could start: image pulls failed with `x509: certificate signed by unknown authority` (the session's outbound proxy re-signs TLS) |
| Mount the proxy CA over the node's CA bundle | Pulls work. Every pod then failed with `runc create failed: ... can't get final child's PID from pipe: EOF` |
| Replay the failing container config by hand, removing one setting at a time (sysctls, seccomp, cgroup path, network namespace, user, masked paths, `oomScoreAdj`) | Only removing `oomScoreAdj: -998` let the container start. `echo -5 > /proc/self/oom_score_adj` in the node is refused with `Permission denied` |
| Shim `runc` in the node to clamp negative `oomScoreAdj` to the node's value (0) | All system pods `Running`; a Postgres 17 pod pulled, started, and accepted `kubectl exec` |

Both fixes are in `infra/k8s/cluster-up.sh` and activate only when needed (the CA through `K3D_CA_BUNDLE`, the shim by detecting that the node cannot lower its own value).

| Measure | Value |
| --- | --- |
| `cluster-up.sh` from nothing to CoreDNS running, images cached | 34 s |
| First run, including pulling the k3s and helper images | about 5.5 min |
| Idle node memory (`docker stats`, CoreDNS, provisioner and metrics-server running) | 416 MiB |

In this session the command is:

```bash
K3D_CA_BUNDLE=/root/.ccr/ca-bundle.crt bash infra/k8s/cluster-up.sh
```

## DE7b · Postgres and Kafka from a Helm chart

Data: none from the project; a 1,000-row probe table and a 500-message probe topic, created and removed by hand.

| Step | Result |
| --- | --- |
| First install | Postgres ready; Kafka in `CrashLoopBackOff`: `Invalid value 8.388608e+06 for configuration message.max.bytes` (Helm rendered a large integer as a float) |
| Fix the template (`int64`), `helm upgrade --wait` | Timed out after 5 min with the pod still crash-looping on the old spec: the StatefulSet waits for a ready pod before updating it |
| Delete the pod by hand | Recreated from the new spec, ready in 18 s |
| Uninstall, delete the claims, install again with `--wait` | 12.4 s |
| Force-delete `bp-postgres-0` holding 1,000 rows | Ready again in 2.8 s, 1,000 rows |
| Force-delete `bp-kafka-0` holding 500 messages | Ready again in 6.4 s, end offset 500 |
| `kubectl top pods` 45 s after install | Kafka 288 Mi (311 m CPU while starting), Postgres 30 Mi |

Recovery times are from the delete command to the pod reporting Ready, on an idle cluster with the image already on the node; they are not a measure of anything under load.

Tests: `tests/test_helm_chart.py` adds 5 tests (lint, StatefulSet properties for both workloads, Kafka settings, the Secret). Full suite here: 160 passed, 1 skipped.

Not verified: the CI run. The shellcheck step now includes `infra/k8s/*.sh`, and the chart tests run in the Python job if the runner has `helm` (GitHub's Ubuntu images normally do), but nothing could be pushed during this step (see below).

## Open issue: pushes are refused

`git push` to `thaitri2005/BlockPulse` returns 403 ("Claude doesn't have GitHub access"). The commits exist only in the session until the owner installs the Claude GitHub App on the repository or reconnects GitHub. Remove this section once a push succeeds.
