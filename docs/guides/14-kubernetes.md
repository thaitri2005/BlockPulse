# 14 · Kubernetes: the same pipeline as pods

DE7 moves the serving stack from Docker Compose to Kubernetes, then reruns the failure drills by killing pods. The running log with every measurement is [the DE7 experiment report](../experiments/2026-11-24-de7-kubernetes.md).

| Step | What | Status |
| --- | --- | --- |
| DE7a | A local cluster that can run pods (`infra/k8s/`) | Done 2026-11-24 |
| DE7b | Helm chart: Postgres and Kafka as StatefulSets with volumes, probes, requests and limits | Done 2026-11-24 |
| DE7c | BlockPulse image; migrations and topics as Jobs; API, enrichment worker and Grafana as Deployments | Done 2026-11-24 |
| DE7d | Drills rerun as pod kills, same invariants as [guide 10](10-failure-drills.md) | Kafka drills done 2026-11-24; the Postgres drill is next |

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

helm uninstall bp -n blockpulse            # remove the pods; the volumes (PVCs) and their data stay
kubectl -n blockpulse delete pvc --all     # remove the data too
k3d cluster stop blockpulse                # free the memory, keep the cluster
k3d cluster delete blockpulse              # remove everything
```

The whole stack uses about 1.4 GiB: 0.5 GiB in the pods and the rest in the node itself (measured idle, 2026-11-24).

## Concepts, in the order they appear

- **Cluster and node.** A Kubernetes *cluster* is an API server plus one or more *nodes* (machines that run containers). You tell the API what should exist ("one Postgres with this volume"); controllers keep making it true. Compose runs what you start; Kubernetes keeps restoring what you declared. That difference is the reason the drills change shape in DE7d.
- **k3s and k3d.** *k3s* is a small, complete Kubernetes distribution in one binary. *k3d* runs a k3s node as a Docker container, so a cluster is created in under a minute and deleted without a trace. It is for learning and tests, not for production.
- **Pod.** The unit Kubernetes schedules: one or more containers sharing a network address. Pods are disposable; anything that must survive a pod lives in a volume or another system.
- **kubectl and context.** `kubectl` is the CLI for the API. A *context* names which cluster it talks to; `cluster-up.sh` selects `k3d-blockpulse`. A *namespace* (`blockpulse`) groups one application's objects.
- **Helm chart, release, values.** A *chart* is a folder of templated Kubernetes manifests; `values.yaml` holds the settings the templates read. Installing a chart creates a *release* (here `bp`), which Helm can upgrade or uninstall as one unit.
- **StatefulSet.** A controller for pods that need an identity: each pod gets a fixed name (`bp-postgres-0`) and its own volume, and a replacement pod gets the same name and the same volume.
- **PersistentVolumeClaim (PVC).** A request for storage that exists independently of any pod. Deleting the pod keeps the claim and its data; only deleting the claim removes the data.
- **Service.** A stable name in front of pods. A normal Service has one virtual IP and spreads connections over the ready pods; a *headless* Service gives each pod its own DNS name (`bp-kafka-0.bp-kafka`), which Kafka needs because a broker tells clients its own address.
- **Probes.** Checks the kubelet runs against a container. *Startup* holds the others off during a slow first boot; *readiness* takes the pod out of its Service while it can't serve (no restart); *liveness* restarts a container that has stopped responding.
- **Requests and limits.** A *request* is what the scheduler reserves for a container; a *limit* is the ceiling. Exceeding a memory limit gets the container killed (`OOMKilled`); CPU above a limit is throttled.
- **Deployment.** A controller for interchangeable pods with no state of their own: random names, no volumes, any number of replicas, replaced in any order.
- **Job.** Runs a pod until it succeeds, retrying on failure. Used for one-off work such as migrations.
- **Secret and ConfigMap.** Settings kept outside the image. A *Secret* is for passwords and connection strings; a *ConfigMap* for ordinary files and values. Pods read either as environment variables or mounted files.
- **Helm hook.** A manifest Helm runs at a point in the release's life instead of with everything else, here after each install and upgrade.

## The cluster (`infra/k8s/k3d.yaml`, `cluster-up.sh`)

One node, k3s v1.31.5, with the ingress controller (Traefik) and the service load balancer switched off. Services are reached with `kubectl port-forward`, which binds to 127.0.0.1 like the Compose ports do. What stays on: CoreDNS (service names), the local-path provisioner (volumes backed by a folder on the node), and metrics-server (`kubectl top`). The idle node uses 416 MiB.

### Running inside a restricted container

The scripts have fixes for hosts that are themselves sandboxed (a cloud development session, some CI runners). On a normal laptop none is needed and none activates.

| Symptom | Cause | Fix |
| --- | --- | --- |
| Pods stay in `ContainerCreating`; events say `x509: certificate signed by unknown authority` | Image pulls pass through a TLS-inspecting proxy whose CA the node doesn't trust | `K3D_CA_BUNDLE=/path/to/ca.crt bash infra/k8s/cluster-up.sh` mounts the CA into the node |
| Pods stay in `ContainerCreating`; events say `can't get final child's PID from pipe: EOF` | The kubelet gives pods a negative `oom_score_adj` (protection from the kernel's out-of-memory killer); the host forbids lowering that value, so the container runtime (runc) fails | Detected automatically: the script installs `infra/k8s/runc-oom-shim.sh` in the node, which rewrites negative values to the node's own |
| `docker build` fails on `pip install` with a certificate error | The same proxy, inside the build | `K3D_CA_BUNDLE=... bash infra/k8s/image-load.sh` passes the CA as a build secret and builds on the host network |

How the second one was found is worth knowing as a method: the kubelet's error named no cause, so the node's `runc` was wrapped to save the container config it was given, and that config was replayed by hand with one setting removed at a time until it started. Removing `oomScoreAdj` was the only change that fixed it. The shim costs pods their relative OOM protection, which is acceptable in a throwaway cluster and never on a real one.

## The image (`infra/k8s/Dockerfile`)

One image, `blockpulse:dev`, runs the API, the worker and the setup jobs; only the arguments differ. It installs exactly `requirements.lock` with hashes, then the package, and runs as an unprivileged user. It is 1.41 GB because the lock includes every extra (dbt, scikit-learn, DuckDB); a serving-only lock would be smaller and is not worth a second lock file yet.

k3d has no image registry here, so `image-load.sh` copies the built image into the node (`k3d image import`), and the chart uses `imagePullPolicy: IfNotPresent` so the kubelet uses that copy.

## The chart (`infra/k8s/chart/blockpulse/`)

The defaults in `values.yaml` mirror `compose.yaml`: the same images and memory ceilings, so the two stacks can be compared.

| Workload | Kind | Template | Notes |
| --- | --- | --- | --- |
| `bp-postgres` | StatefulSet, 2 Gi volume | `postgres.yaml` | Postgres 17; credentials in `Secret/bp-postgres`; memory 256 Mi requested, 512 Mi limit |
| `bp-kafka` | StatefulSet, 2 Gi volume | `kafka.yaml` | Kafka 4.3.1, one KRaft broker-controller; memory 768 Mi requested, 1 Gi limit |
| `bp-db-init`, `bp-kafka-init` | Jobs (Helm hooks) | `setup-jobs.yaml` | `blockpulse db-init` and `blockpulse kafka-init` after every install and upgrade; both apply only what is missing |
| `bp-api` | Deployment, 2 replicas, Service | `api.yaml` | `blockpulse serve`; readiness on `/readyz` (runs a query), liveness on `/healthz` (doesn't) |
| `bp-worker` | Deployment, 0 or 1 replica | `worker.yaml` | `blockpulse kafka-enrich`; off by default |
| `bp-grafana` | Deployment, Service | `grafana.yaml` | Grafana 12.2 with the Postgres datasource and the overview dashboard from ConfigMaps |

Design choices that differ from Compose, and why:

- **Kafka advertises a DNS name.** `bp-kafka-0.bp-kafka.blockpulse.svc.cluster.local:9092` instead of `127.0.0.1:9092`, because clients are other pods and a pod's IP changes on every restart. It is reachable only inside the cluster. Kafka is a plain StatefulSet with the Compose image, not an operator (decision D29).
- **Readiness and liveness ask different questions.** The API is *ready* only if it can query Postgres, so it leaves the Service during a database outage and returns by itself. It is *alive* as long as the process answers; restarting the API cannot repair Postgres, so liveness must not depend on it.
- **The worker is off by default and can't be scaled.** It calls a public REST API within a request budget, so the chart refuses `worker.replicas` above 1 and uses the `Recreate` strategy, which stops the old pod before starting a new one (decision D08). Turn it on with `--set worker.replicas=1`. It has no probes because it serves no port: an exit is restarted, and a hang would show as consumer lag.
- **The connection string is a Secret, and changing it restarts the API.** Pods read environment variables only at start, so the Deployment carries a checksum of the Secret as an annotation; a new password changes the checksum, which rolls the pods.
- **The dashboard is a copy.** Helm reads only files inside the chart, so `files/dashboards/blockpulse-overview.json` duplicates the one in `observability/`. A test fails if they differ. The pipeline dashboard needs Prometheus, which is not in the chart yet.

### Checked on 2026-11-24

All on generated probe data (the recorded dataset is not in the cloud session).

| Check | Result |
| --- | --- |
| Full install from nothing (no volumes), images on the node, `--wait --wait-for-jobs` | 19 s; 5 pods running, both Jobs complete |
| Migrations Job | Applied `001_initial` and `002_lifecycle_events`; on the next upgrade it applied nothing |
| API through `port-forward` | `/readyz`, `/pipeline/status`, `/alerts`, `/models/current` all 200 (empty database) |
| Grafana | Dashboard "BlockPulse overview" provisioned; datasource health "Database Connection OK" |
| Worker with `--set worker.replicas=1` | Running, joined group `blockpulse-enrich`; `worker.replicas=2` is refused at render time |
| Postgres pod force-deleted with 1,000 rows in a table | Same pod name back and ready in 2.8 s; 1,000 rows |
| Kafka pod force-deleted with 500 messages in a topic | Back and ready in 6.4 s; end offset still 500 |
| Memory in use, idle (`kubectl top`) | Kafka 297 Mi, Grafana 91 Mi, API 38 Mi each, Postgres 34 Mi: 499 Mi in pods, 1,380 Mi for the whole node |

Nobody restarted the killed pods: the StatefulSet controller saw a missing pod and created it again, attached to the same claim. In the Compose drills the drill itself ran `docker restart`.

`tests/test_helm_chart.py` (11 tests) renders the chart and asserts these properties without a cluster; the tests skip when `helm` isn't installed. `tests/test_k8s_drills.py` and `tests/test_synth.py` cover the drill logic (with `kubectl` and `helm` faked) and the generator.

### Things that went wrong

- **A number rendered as `8.388608e+06`.** Helm parses large YAML integers as floats, so `{{ .Values.kafka.messageMaxBytes | quote }}` produced scientific notation and the broker exited with `Not a number of type INT`. The template converts with `int64` first, and a test pins the rendered value.
- **`helm upgrade` didn't replace the crashing pod.** A StatefulSet updates pods in order and waits for each to be ready before touching it, so a pod crash-looping on the old spec is never replaced by the fixed one. Deleting the pod by hand lets the controller recreate it from the new spec. After fixing a broken StatefulSet, check the pod's age.
- **A health check that cost a third of a CPU.** Kafka's readiness probe first ran a Kafka CLI command, as the Compose healthcheck does. Every call starts a JVM, and the idle broker showed about 310 m CPU. A TCP check on the listener brought it to about 30 m. Compose hid the cost because nobody looks at `docker stats` for an idle container; `kubectl top` showed it at once.
- **A build-time download ignored pip's CA setting.** One dependency fetches a file with Python's `urllib` while building, which reads `SSL_CERT_FILE`, not `PIP_CERT`. The Dockerfile sets both when a CA is supplied.

## The drills as pod kills (`src/blockpulse/k8s_drills.py`)

The same invariant as [guide 10](10-failure-drills.md): after a crash, nothing is lost and every input is represented exactly once where it matters. What changes is who recovers. In Compose the drill killed a process and then started it again itself. Here the drill only deletes a pod, with `--grace-period=0 --force` so the container gets SIGKILL and no clean shutdown; a controller replaces it, and the drill waits and checks.

```bash
python -m blockpulse drill k8s-enrich-kill      # about 3.5 min each, cluster and image must exist (steps 1-2 above)
python -m blockpulse drill k8s-broker-kill
```

Each drill:

1. Installs the chart into its own namespace, `blockpulse-drill`, so the topics start empty, and deletes the namespace at the end (`--keep` leaves it for inspection). Installing from nothing on every run is also the "chart installs from scratch" check.
2. Generates traffic inside the cluster: a pod runs `blockpulse synth-capture` for three sessions and replays them into the raw topic. One session maps to one partition, because the record key is the session.
3. Scales the worker to one pod and waits until the consumer group has committed some offsets but not all. The kill is triggered by that condition, not by a timer, so it always lands mid-stream.
4. Deletes the pod, waits for the controller to recover and for lag to reach 0.
5. Runs the Compose drills' Kafka checks in a pod (`blockpulse drill kafka-verify`): every raw message has an enriched record, duplicates are bounded, and the Parquet sink holds each message exactly once.

The worker's REST fallback points at an unreachable address, as in the Compose drills, so nothing calls the public API.

**Synthetic traffic (`src/blockpulse/synth.py`).** `blockpulse synth-capture --output DIR --messages N --seed S` writes a capture session with the same envelope and payload layout as a recorded one, labelled `source_mode: synthetic`. Amounts balance and every input value is present, so the normal pipeline accepts it with no issues. The same seed gives byte-identical files. It exists because drills need thousands of messages and the recorded sessions are not in the repository; results on it describe pipeline behaviour only.

### Results (2026-11-24, 30,000 synthetic messages, 120,000 transactions)

| Check | Worker pod killed | Broker pod killed |
| --- | --- | --- |
| Kill landed mid-stream | 9,001 of 30,000 offsets committed | 9,001 of 30,000 committed |
| Recovery, with nobody restarting anything | Deployment had a new worker pod running 2.3 s after the kill | StatefulSet had the broker ready on the same volume after 6.5 s |
| Lag back to 0 | 8.8 s after the kill | 13.4 s after the kill |
| Worker survived | Not applicable (it was the one killed) | Yes: same pod, 0 container restarts |
| No message lost | 0 of 30,000 missing from the enriched topic | 0 of 30,000 missing |
| Duplicates | 413 enriched twice | 0 |
| Idempotent sink | 30,000 Parquet files from 30,413 records | 30,000 files from 30,000 records |
| Whole drill, install to teardown | 200 s | 202 s |

Both passed on the first run. The 413 duplicates are the messages the killed worker had processed after its last checkpoint; the replacement redelivered them and the sink absorbed them, as in the Compose drill (44 there, on a smaller and slower run). The broker kill produced none in this run: the worker happened to have nothing uncommitted when its commit was next accepted. Duplicates there are possible and bounded, not expected to be zero.

Compared with [guide 10](10-failure-drills.md): the invariants and the worker code are the same; the traffic is synthetic instead of recorded (30,000 messages instead of 4,641), so the counts are not comparable, only the pass or fail of each check.

## Limits

- One node and one broker: this shows scheduling, self-healing and configuration, not high availability.
- `local-path` volumes live on the node. Deleting the cluster deletes the data.
- Passwords in `values.yaml` are development defaults, as in Compose.
- No Prometheus or Airflow in the chart yet, and no ingress.
