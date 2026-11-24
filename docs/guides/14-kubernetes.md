# 14 · Kubernetes: the same pipeline as pods

The whole pipeline downstream of capture (Kafka, the enrichment worker, the stream sink, Postgres, the API, Prometheus and Grafana), run on Kubernetes from a Helm chart, with the failure drills of [guide 10](10-failure-drills.md) rerun by killing pods. Every number here comes from generated traffic in a single-node k3d cluster; the full record is in the [DE7 report](../experiments/2026-11-24-de7-kubernetes.md).

## Quick start

Tools: Docker, [k3d](https://k3d.io) 5.8, `kubectl` 1.31, Helm 3.17. On Windows, run the scripts from Git Bash or WSL.

```bash
bash infra/k8s/cluster-up.sh        # 1. create or reuse the cluster "blockpulse" (34 s with images cached)
bash infra/k8s/image-load.sh        # 2. build blockpulse:dev and load it into the node (about 1.5 min the first time)
helm upgrade --install bp infra/k8s/chart/blockpulse -n blockpulse --create-namespace --wait --wait-for-jobs   # 3. install or update (19 s)

kubectl -n blockpulse get pods,pvc,jobs
kubectl -n blockpulse port-forward svc/bp-api 8000:8000          # API at http://127.0.0.1:8000/readyz
kubectl -n blockpulse port-forward svc/bp-grafana 3000:3000      # Grafana at http://127.0.0.1:3000 (admin / blockpulse_dev)
kubectl -n blockpulse port-forward svc/bp-postgres 5432:5432     # psql on 127.0.0.1
kubectl -n blockpulse port-forward svc/bp-prometheus 9090:9090   # Prometheus at http://127.0.0.1:9090

python -m blockpulse drill k8s-enrich-kill      # the drills: about 3.5 min each, 5 min for Postgres
python -m blockpulse drill k8s-broker-kill
python -m blockpulse drill k8s-postgres-kill

helm uninstall bp -n blockpulse            # remove the pods; the volumes (PVCs) and their data stay
kubectl -n blockpulse delete pvc --all     # remove the data too
k3d cluster stop blockpulse                # free the memory, keep the cluster
k3d cluster delete blockpulse              # remove everything
```

The whole stack uses about 1.4 GiB when idle and 1.8 GiB after streaming 20,000 messages (1.1 GiB of that in the pods).

## Concepts

| Term | Meaning here |
| --- | --- |
| Cluster, node | An API server plus machines that run containers. You declare what should exist; controllers keep making it true. Compose runs what you start, Kubernetes restores what you declared |
| k3s, k3d | k3s is a small complete Kubernetes; k3d runs a k3s node as a Docker container. For learning and tests only |
| Pod | The unit that is scheduled: one or more containers sharing an address. Disposable |
| Namespace, context | A namespace groups one application's objects; a context tells `kubectl` which cluster to talk to |
| Helm chart, release, values | Templated manifests, one installed instance of them (`bp`), and the settings they read (`values.yaml`) |
| StatefulSet | For pods with identity: a fixed name (`bp-postgres-0`) and its own volume, both kept across replacements |
| Deployment | For interchangeable pods with no state: any number, any order |
| PersistentVolumeClaim | Storage that exists independently of a pod. Deleting the pod keeps the data |
| Service | A stable name in front of pods. A *headless* Service gives each pod its own DNS name, which Kafka needs |
| Job, CronJob | A Job runs a pod until it succeeds, retrying on failure; a CronJob creates a Job on a schedule |
| Secret, ConfigMap | Settings kept outside the image: passwords and connection strings; ordinary files and values |
| Probes | *Startup* waits out a slow boot; *readiness* removes a pod from its Service without restarting it; *liveness* restarts a container that stopped responding |
| Requests, limits | What the scheduler reserves; the ceiling. Over a memory limit the container is killed |
| Helm hook | A manifest Helm runs at a point in the release's life, here after each install and upgrade |

Exercises 25–28 in the [learning path](../learning/README.md) walk through these. One more term appears in the chart: an *init container* runs to completion before a pod's main container starts, and is retried until it succeeds.

## What the chart installs (`infra/k8s/chart/blockpulse/`)

Defaults in `values.yaml` mirror `compose.yaml` (same images, same memory ceilings). The layout is drawn in the [architecture document](../ARCHITECTURE.md#where-it-runs).

| Workload | Kind | Notes |
| --- | --- | --- |
| `bp-postgres` | StatefulSet, 2 Gi volume | Postgres 17; credentials in a Secret |
| `bp-kafka` | StatefulSet, 2 Gi volume | Kafka 4.3.1, one KRaft broker; a plain StatefulSet, not an operator (D29) |
| `bp-db-init`, `bp-kafka-init` | Jobs, Helm hooks | Migrations and topics after every install and upgrade; both apply only what is missing |
| `bp-api` | Deployment, 2 pods, Service | `blockpulse serve` |
| `bp-worker` | Deployment, 0 or 1 pod | `blockpulse kafka-enrich`; off by default, `--set worker.replicas=1` to start. Like the sink, it creates the topics in an init container before starting |
| `bp-sink` | StatefulSet, 2 Gi volume | `blockpulse kafka-lake`: the enriched topic into lake tables on its volume; metrics on port 9108 |
| `bp-maintenance` | CronJob, hourly | `lake-compact` on the sink's volume; with `--set maintenance.serveStreamedLake=true` it also rebuilds Postgres from the streamed lake |
| `bp-prometheus` | Deployment, Service | Scrapes the sink every 5 s; the four alert rules from `observability/prometheus/alerts.yml` |
| `bp-grafana` | Deployment, Service | Postgres and Prometheus datasources; the overview and pipeline dashboards from ConfigMaps |

One image (`infra/k8s/Dockerfile`, 1.41 GB, the full hashed lock, unprivileged user) runs the API, the worker, the sink and the Jobs. k3d has no registry here, so `image-load.sh` copies the image into the node.

Choices that differ from Compose:

- **Kafka advertises its pod's DNS name**, not `127.0.0.1`, because clients are other pods and a pod's IP changes on restart. It is reachable only inside the cluster.
- **The API's readiness and liveness ask different questions.** Ready means it can query Postgres (`/readyz`), so it leaves the Service during a database outage and returns by itself. Alive means the process answers (`/healthz`); restarting the API cannot repair Postgres.
- **The worker cannot be scaled past one.** It calls a public API within a request budget (D08), so the chart refuses more replicas and uses the `Recreate` strategy.
- **A changed connection string restarts the API.** The Deployment carries a checksum of the Secret; pods read their environment only at start.
- **The sink is a StatefulSet.** It owns files: its lake lives on a volume that must follow the pod. Like the worker it is limited to one replica.
- **Dashboards and alert rules are copies** of the files under `observability/`, because Helm reads only files inside the chart. Tests fail if a copy differs.

## The streaming path, end to end

With the chart installed, traffic can go from a topic to the API without leaving the cluster. The worker is started with its REST fallback pointed at an unreachable address so nothing calls the public API:

```bash
N="kubectl -n blockpulse"
helm upgrade bp infra/k8s/chart/blockpulse -n blockpulse --reuse-values --set worker.apiBase=http://127.0.0.1:9/api --set worker.maxRequests=1
$N run publish --image=blockpulse:dev --image-pull-policy=IfNotPresent --restart=Never --command -- sh -c \
  "python -m blockpulse synth-capture --output /tmp/s --messages 20000 --seed 1 && python -m blockpulse kafka-replay /tmp/s --bootstrap-servers bp-kafka-0.bp-kafka:9092"
$N scale deployment bp-worker --replicas=1                                   # enrich: raw topic -> enriched topic
$N exec bp-sink-0 -- python -m blockpulse lake-compact --lake /data/lake-stream                     # merge closed hours
$N exec bp-sink-0 -- python -m blockpulse serving-rebuild --root /data --lake /data/lake-stream    # lake -> Postgres
$N scale deployment bp-worker --replicas=0
```

Measured on 2026-11-24 with 20,000 generated messages (80,000 transactions):

| Stage | Result |
| --- | --- |
| Worker and sink | Both consumer groups at lag 0 within a minute; the sink wrote 84 Parquet files over two hours of event time, 0 quarantined |
| Prometheus | Target up; 20,000 messages and 80,000 rows per table counted; lag 0. `DataStale` goes pending, correctly: the generated events are dated January 2026 |
| Grafana | Both dashboards provisioned; both datasources healthy |
| `lake-compact` | The closed hour went from 30 files to 1 per table (57,596 rows each); the open hour was left alone |
| `serving-rebuild --lake` | 80,000 scores, events and transactions in Postgres in 8.1 s |
| API | `/pipeline/status` reports 80,000 transactions, 12,714 flagged; `/alerts` returns them |

The two maintenance commands run inside the sink's pod because that is where the lake volume is mounted. The `bp-maintenance` CronJob runs the compaction every hour on the same volume (`kubectl -n blockpulse create job --from=cronjob/bp-maintenance now` triggers it by hand).

## The drills as pod kills (`src/blockpulse/k8s_drills.py`)

Same invariant as guide 10: after a crash, nothing is lost and every input is represented exactly once where it matters. What changes is who recovers. In Compose the drill restarts what it killed. Here it only deletes a pod (`--grace-period=0 --force`, so SIGKILL and no clean shutdown); a controller replaces it, and the drill waits and checks.

Each drill installs the chart into its own namespace, `blockpulse-drill`, so topics and tables start empty, and deletes the namespace afterwards (`--keep` leaves it). Traffic is generated inside the cluster with `blockpulse synth-capture`, a deterministic generator of capture sessions labelled `source_mode: synthetic`. The worker's REST fallback points at an unreachable address, so nothing calls the public API. Kills are triggered by a condition, not a timer: the consumer group has committed some offsets but not all, or Postgres reports an open transaction from the loader.

### Results (2026-11-24, two runs of each)

Kafka drills, 30,000 messages (120,000 transactions) per run:

| Check | Worker pod killed | Broker pod killed |
| --- | --- | --- |
| Recovery, with nobody restarting anything | New worker pod running after 2.3 s and 1.2 s | Broker ready on the same volume after 6.5 s and 4.5 s |
| Worker survived | Not applicable | Yes both times: same pod, 0 restarts |
| No message lost | 0 of 30,000 missing | 0 of 30,000 missing |
| Duplicates, absorbed by the sink | 413 and 117 | 0 and 0 |
| Idempotent sink | 30,000 Parquet files | 30,000 Parquet files |

Postgres drill, 200,000 rows per load:

| Check | Result, both runs |
| --- | --- |
| Loader pod killed inside its transaction | 0 rows visible afterwards |
| Job retried in a new pod | 200,000 inserted, exactly once |
| Same run loaded again | 0 inserted, 200,000 recognized as duplicates |
| Postgres pod killed under a second load | Ready on its volume after 3.3 s and 3.1 s; that attempt failed; 0 rows visible |
| Job retried after Postgres returned | 200,000 loaded exactly once |
| The first run's rows after the Postgres kill | Still 200,000 |

All six runs passed every check, as did a seventh run of the worker drill after the chart gained the sink and Prometheus (install 21.7 s). Duplicates are the messages a killed worker had processed since its last checkpoint; they vary by run and are bounded, not zero. The raw reports and console output are in [`experiments/reports/`](../experiments/reports/).

## Restricted hosts

Inside a sandboxed container (a cloud development session, some CI runners) three things fail; the scripts handle them. On a normal laptop none applies.

| Symptom | Cause | Fix |
| --- | --- | --- |
| Pods stuck in `ContainerCreating` with `x509: certificate signed by unknown authority` | Image pulls go through a TLS-inspecting proxy | `K3D_CA_BUNDLE=/path/ca.crt bash infra/k8s/cluster-up.sh` |
| Pods stuck with `can't get final child's PID from pipe: EOF` | The host forbids lowering `oom_score_adj`, which the kubelet sets on every pod | Detected automatically; the script installs `runc-oom-shim.sh` in the node |
| `docker build` fails on a certificate error | The same proxy, inside the build | `K3D_CA_BUNDLE=... bash infra/k8s/image-load.sh` |

## Troubleshooting

- **A fresh install hangs until the timeout with one pod crash-looping.** Check whether that pod needs something a post-install hook creates. With `--wait`, hooks run only after every pod is ready, so a pod that needs the hook's result can never become ready. The sink and the worker need the Kafka topics; each creates them in an init container for that reason.
- **A fixed StatefulSet still crash-loops.** A StatefulSet won't replace a pod that never became ready. Delete the pod; it is recreated from the new spec.
- **A large number in `values.yaml` renders as `8.388608e+06`.** Helm reads big integers as floats. Convert with `int64` in the template.
- **An idle pod uses a lot of CPU.** Check its probes: an `exec` probe that starts a JVM every few seconds cost Kafka 0.3 CPU. Use a TCP or HTTP probe.

## Limits

- One node and one broker: self-healing of pods, not high availability. The drills don't test a node failure, a full disk, a network partition or an out-of-memory kill.
- `local-path` volumes live on the node; deleting the cluster deletes the data.
- Passwords in `values.yaml` are development defaults, as in Compose.
- Airflow is not in the chart, and there is no ingress.
- The maintenance CronJob mounts the sink's ReadWriteOnce volume, which works because it is scheduled onto the sink's node. The scheduled serving rebuild is off by default because it replaces what Postgres serves with the streamed data only.
- Prometheus keeps its data in the pod; replacing the pod loses the history.
- Run only on generated traffic, in a cloud session and in CI (the worker drill on every push, on a plain Linux runner with none of the restricted-host fixes): not yet on Windows with Docker Desktop, or on recorded sessions.
