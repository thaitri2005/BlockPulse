# DE7 · Kubernetes: session log and measurements

Status: DE7a–d done on 2026-11-24. Started 2026-11-24, after a five-week pause (last commit 2026-10-20). This file is the running record of DE7: what was done, what was measured, what broke. The how-to is [guide 14](../guides/14-kubernetes.md).

## Where this work runs

DE7 is being built in a cloud development session, not on the laptop: a Linux container with 4 CPUs, 15.7 GiB of memory, no swap, 61 GB of free disk and Docker. The container is ephemeral, so:

- Only what is committed and pushed survives. Work is pushed to `main` after every step, authored as the owner.
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

Tests after DE7b: `tests/test_helm_chart.py` with 5 tests (lint, StatefulSet properties for both workloads, Kafka settings, the Secret); full suite 160 passed, 1 skipped.

## DE7c · Image, setup Jobs, API, worker, Grafana

Data: none from the project. The database is empty apart from the migrations; the worker was pointed at an unreachable address (`http://127.0.0.1:9/api`), as the drills do, so nothing called the public API.

| Step | Result |
| --- | --- |
| First image build | Failed: a dependency (`dbt-core-experimental-parser`) downloads a wheel with `urllib` during its build and didn't trust the proxy CA; pip's `PIP_CERT` doesn't cover it |
| Set `SSL_CERT_FILE` as well | Built in 93 s; 1.41 GB; `docker run blockpulse:dev lake-schema` works |
| `k3d image import` | 19 s |
| Add Secret, two hook Jobs, API Deployment and Service; `helm upgrade --wait --wait-for-jobs` | 11 s; migrations `001`, `002` applied; three topics created; 2 API pods ready |
| API through `port-forward` | `/readyz`, `/pipeline/status`, `/alerts?limit=2`, `/models/current`: 200 |
| Add worker and Grafana; upgrade | 23 s (Grafana image pull included); dashboard listed, datasource "Database Connection OK" |
| `--set worker.replicas=1` | Pod running, `kafka_enrichment_started` logged; `worker.replicas=2` refused by the template |
| `kubectl top`: idle Kafka at 309–315 m CPU | Cause: the readiness probe ran a Kafka CLI command (a JVM start) every 10 s. Changed to a TCP check: 28–38 m |
| Uninstall, delete claims, full install with `--wait --wait-for-jobs` | 19.5 s |
| Idle memory, full stack | Pods 499 Mi (Kafka 297, Grafana 91, API 38 + 39, Postgres 34); node total 1,380 Mi |
| `image-load.sh` with everything cached | 13 s |

The 1.4 GiB total is well under the 3–4 GB estimated before starting; the estimate assumed each container would sit near its limit.

Tests after DE7c: `tests/test_helm_chart.py` has 11 tests; full suite 166 passed, 1 skipped; `helm lint --strict`, ruff and shellcheck clean.

Not done: a build of the image without the proxy settings, which is the path the laptop will take.

## First push and CI

Pushes were refused (HTTP 403) until the owner installed the Claude GitHub App on the repository; DE7a–c then went to `main` in one push, authored as the owner. A separate session branch, `claude/gallant-ptolemy-866nd6`, was pushed first by mistake and could not be deleted from the session; it holds nothing that isn't on `main` and can be deleted on GitHub.

CI run 13 on `main` (commit `d34c9ce`) passed all three jobs: Python tests with Postgres, Terraform, and the instance rehearsal, whose shellcheck step now covers `infra/k8s/*.sh`. Not confirmed: whether the 11 Helm chart tests ran or skipped there; the job passes either way, and the log tail did not show the pytest summary.

## DE7d · Pod-kill drills: Kafka

Data: synthetic, generated in the cluster by `blockpulse synth-capture` (3 sessions of 10,000 messages, 4 transactions each, seeds 1–3). Check on the generator first: 2,000 messages processed by `blockpulse process` gave 8,000 features, 0 issues, 0 incomplete transactions; two runs with the same seed were byte-identical (5.65 MB).

| Measure | `k8s-enrich-kill` | `k8s-broker-kill` |
| --- | --- | --- |
| Result | Passed, 7 of 7 checks, first run | Passed, 8 of 8 checks, first run |
| Chart install into a fresh namespace | 19.3 s | 19.0 s |
| Group offsets when the pod was deleted | 9,001 committed of 30,000 | 9,001 committed of 30,000 |
| Recovery | Replacement worker pod running after 2.3 s | Broker ready after 6.5 s |
| Lag 0 after the kill | 8.8 s | 13.4 s |
| Worker pod | Replaced (new name) | Same pod, 0 restarts |
| Raw / enriched records | 30,000 / 30,413 | 30,000 / 30,000 |
| Duplicates | 413 | 0 |
| Parquet files in the sink | 30,000 | 30,000 |
| Whole drill | 199.5 s | 201.9 s |

Most of each drill's 200 s is not the failure: it is generating and publishing the traffic, and the verify pod reading both topics and writing 30,000 Parquet files.

The committed offset at the kill was 9,001 in both; see the second run below, where it differs.

Tests after this step: 176 passed, 1 skipped (5 for the generator, 5 for the drill logic with `kubectl` and `helm` faked).

## DE7d · Pod-kill drills: Postgres

Data: synthetic, 50,000 messages with 4 transactions each (seed 11), giving 200,000 screened rows per run. Timing of the stages outside the cluster first, to size the kill window: generate 5.0 s, process 18.8 s, screen 9.1 s, load 11.5 s.

`k8s-postgres-kill`: passed, 8 of 8 checks, first run, 268.4 s. Install 19.0 s.

| Check | Detail |
| --- | --- |
| Loader pod killed inside its load transaction | `load-a-v5nxp` force-deleted when `pg_stat_activity` showed an open transaction |
| Nothing from the killed load visible | 0 rows for `drill-kill` |
| Job retried in a new pod | `load-a-cjphf` succeeded: 200,000 inserted, 200,000 in the table |
| Second Job, same run | 0 inserted, 200,000 duplicates |
| Postgres pod killed under a second load | Ready after 3.3 s; attempt `load-b-2grmg` failed; 0 rows for `drill-restart` |
| Job retried after Postgres returned | 200,000 rows, 2 attempts |
| First run after the Postgres kill | 200,000 rows |

Tests after this step: 179 passed, 1 skipped (8 for the drill logic).

## Second run of all three drills

Same day, after the session's container had been restarted (Docker daemon restarted, cluster reused, image reloaded). Run at the owner's request so each drill has more than one logged run.

| Measure | `k8s-enrich-kill` | `k8s-broker-kill` | `k8s-postgres-kill` |
| --- | --- | --- | --- |
| Result | Passed, 7 of 7 | Passed, 8 of 8 | Passed, 8 of 8 |
| Install | 21.3 s | 19.2 s | 19.6 s |
| Offsets committed at the kill | 4,501 of 30,000 | 7,501 of 30,000 | n/a |
| Recovery | New worker pod after 1.2 s | Broker ready after 4.5 s; worker same pod, 0 restarts | Postgres ready after 3.1 s |
| Lag 0 after the kill | 12.9 s | 16.6 s | n/a |
| Enriched records / duplicates | 30,117 / 117 | 30,000 / 0 | n/a |
| Rows after each retry | n/a | n/a | 200,000, exactly once |
| Whole drill | 199.5 s | 198.1 s | 291.4 s |

The committed offset at the kill differs from the first run (9,001), so the earlier note that it would always land at the same place was wrong: it depends on when the poll happens to see the first commit.

Restarting the cluster showed one bug: `cluster-up.sh` failed its first `kubectl wait` with `ServiceUnavailable` while the restarted API server warmed up. It now retries.

Raw output: [`reports/de7-run1/`](reports/de7-run1/) and [`reports/de7-run2/`](reports/de7-run2/) hold the JSON report of every run; run 2 also has the console output.

## What DE7 did not cover

- Each drill ran twice, in the cloud session, on synthetic traffic. Nothing here was run on the laptop or on recorded sessions.
- CI does not create a cluster, so the drills and a real chart install are not part of CI.
- The image was built only behind the session's proxy.
- Prometheus, the pipeline dashboard and Airflow are still Compose-only.
