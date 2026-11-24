# 14 · Kubernetes: the same pipeline as pods

DE7 moves the serving stack from Docker Compose to Kubernetes, then reruns the failure drills by killing pods. This guide grows one step at a time; the running log with measurements is [the DE7 experiment report](../experiments/2026-11-24-de7-kubernetes.md).

| Step | What | Status |
| --- | --- | --- |
| DE7a | A local cluster that can run pods (`infra/k8s/`) | Done 2026-11-24 |
| DE7b | Helm chart: Postgres and Kafka as StatefulSets with volumes, probes, requests and limits | Done 2026-11-24 |
| DE7c | BlockPulse image; migrations as a Job; API, enrichment worker and Grafana as Deployments | Next |
| DE7d | Drills rerun as pod kills, same invariants as [guide 10](10-failure-drills.md) | Staged |

## Concepts, in the order they appear

- **Cluster and node.** A Kubernetes *cluster* is an API server plus one or more *nodes* (machines that run containers). You tell the API what should exist ("one Postgres with this volume"); controllers keep making it true. Compose runs what you start; Kubernetes keeps restoring what you declared. That difference is the whole reason the drills change shape in DE7d.
- **k3s and k3d.** *k3s* is a small, complete Kubernetes distribution in one binary. *k3d* runs a k3s node as a Docker container, so a cluster is created in under a minute and deleted without a trace. It is for learning and tests, not for production.
- **Pod.** The unit Kubernetes schedules: one or more containers sharing a network address. Pods are disposable; anything that must survive a pod lives in a volume or another system.
- **Helm chart, release, values.** A *chart* is a folder of templated Kubernetes manifests; `values.yaml` holds the settings the templates read. Installing a chart creates a *release* (here `bp`), which Helm can upgrade or uninstall as one unit.
- **StatefulSet.** A controller for pods that need an identity: each pod gets a fixed name (`bp-postgres-0`) and its own volume, and a replacement pod gets the same name and the same volume. A *Deployment* (DE7c) is for interchangeable pods with no state of their own.
- **PersistentVolumeClaim (PVC).** A request for storage that exists independently of any pod. Deleting the pod keeps the claim and its data; only deleting the claim removes the data.
- **Headless Service.** A Service with no virtual IP. It gives each pod a stable DNS name (`bp-kafka-0.bp-kafka`), which Kafka needs because a broker tells clients its own address.
- **Probes.** Checks the kubelet runs against a container. *Startup* holds the others off during a slow first boot; *readiness* takes the pod out of its Service while it can't serve (no restart); *liveness* restarts a container that has stopped responding.
- **Requests and limits.** A *request* is what the scheduler reserves for a container; a *limit* is the ceiling. Exceeding a memory limit gets the container killed (`OOMKilled`); CPU above the limit is throttled.
- **kubectl and context.** `kubectl` is the CLI for the API. A *context* names which cluster it talks to; `cluster-up.sh` selects `k3d-blockpulse`.

## DE7a: create the cluster

Tools: Docker, [k3d](https://k3d.io) 5.8, `kubectl` 1.31, and Helm 3.17 (from DE7b on).

```bash
bash infra/k8s/cluster-up.sh        # create or reuse the cluster "blockpulse", wait until it can run pods
kubectl get pods -A                 # CoreDNS, the volume provisioner and metrics-server should be Running
k3d cluster stop blockpulse         # free the memory, keep the state
k3d cluster delete blockpulse       # remove everything
```

On Windows, run the script from Git Bash or WSL, or run the one command it wraps: `k3d cluster create --config infra/k8s/k3d.yaml`.

The cluster is defined in `infra/k8s/k3d.yaml`: one node, k3s v1.31.5, with the ingress controller (Traefik) and the service load balancer switched off. Services are reached with `kubectl port-forward`, which binds to 127.0.0.1 like the Compose ports do. What stays on: CoreDNS (service names), the local-path provisioner (volumes backed by a folder on the node), and metrics-server (`kubectl top`).

Measured on 2026-11-24 (4 CPUs, 15.7 GiB): the cluster is ready in 34 s with images cached, and the idle node uses 416 MiB.

### Running inside a restricted container

The script has two fixes for hosts that are themselves sandboxed (a cloud development session, some CI runners). On a normal laptop neither is needed and neither activates.

| Symptom | Cause | Fix |
| --- | --- | --- |
| Pods stay in `ContainerCreating`; events say `x509: certificate signed by unknown authority` | Image pulls pass through a TLS-inspecting proxy whose CA the node doesn't trust | `K3D_CA_BUNDLE=/path/to/ca.crt bash infra/k8s/cluster-up.sh` mounts the CA into the node |
| Pods stay in `ContainerCreating`; events say `can't get final child's PID from pipe: EOF` | The kubelet gives pods a negative `oom_score_adj` (protection from the kernel's out-of-memory killer); the host forbids lowering that value, so the container runtime (runc) fails | Detected automatically: the script installs `infra/k8s/runc-oom-shim.sh` in the node, which rewrites negative values to the node's own |

How the second one was found is worth knowing as a method: the kubelet's error named no cause, so the node's `runc` was wrapped to save the container config it was given, and that config was replayed by hand with one setting removed at a time until it started. Removing `oomScoreAdj` was the only change that fixed it. The shim costs pods their relative OOM protection, which is acceptable in a throwaway cluster and never on a real one.

## DE7b: Postgres and Kafka from a Helm chart

The chart is `infra/k8s/chart/blockpulse/`. Its defaults (`values.yaml`) mirror `compose.yaml`: the same images and the same memory ceilings, so the two stacks can be compared.

```bash
helm lint --strict infra/k8s/chart/blockpulse
helm upgrade --install bp infra/k8s/chart/blockpulse -n blockpulse --create-namespace --wait   # install or update
kubectl -n blockpulse get pods,pvc
kubectl -n blockpulse port-forward svc/bp-postgres 5432:5432                                    # psql on 127.0.0.1
helm uninstall bp -n blockpulse                      # removes the pods; the volumes (PVCs) stay
kubectl -n blockpulse delete pvc --all               # removes the data too
```

| Object | Template | What it is |
| --- | --- | --- |
| `Secret/bp-postgres` | `postgres.yaml` | User, password and database name; the pod reads them as environment variables |
| `Service/bp-postgres`, `Service/bp-kafka` | both | Headless Services: stable DNS names for the pods |
| `StatefulSet/bp-postgres` | `postgres.yaml` | Postgres 17, a 2 Gi volume, memory 256 Mi requested and 512 Mi limit |
| `StatefulSet/bp-kafka` | `kafka.yaml` | Kafka 4.3.1, one KRaft broker-controller, a 2 Gi volume, memory 768 Mi requested and 1 Gi limit |

Kafka is a plain StatefulSet running the same image as Compose, not an operator (decision D29). The one setting that differs from Compose is the advertised listener: `bp-kafka-0.bp-kafka.blockpulse.svc.cluster.local:9092` instead of `127.0.0.1:9092`, because clients are other pods. It is reachable only inside the cluster.

### Checked on 2026-11-24

| Check | Result |
| --- | --- |
| Install from nothing, images cached, `--wait` until both pods are ready | 12 s |
| Postgres pod force-deleted (`--grace-period=0 --force`) with 1,000 rows in a table | Same pod name back and ready in 2.8 s; 1,000 rows |
| Kafka pod force-deleted with 500 messages in a topic | Back and ready in 6.4 s; end offset still 500 |
| Memory in use after start (`kubectl top`) | Kafka 288 Mi, Postgres 30 Mi |

Nobody restarted anything: the StatefulSet controller saw a missing pod and created it again, attached to the same claim. That is the difference from the Compose drills, where the drill itself ran `docker restart`.

The offline tests in `tests/test_helm_chart.py` render the chart and assert these properties (claim templates, three probes, requests and limits, the stable advertised name); they skip when `helm` isn't installed.

### Two things that went wrong

- **A number rendered as `8.388608e+06`.** Helm parses large YAML integers as floats, so `{{ .Values.kafka.messageMaxBytes | quote }}` produced scientific notation and the broker exited with `Not a number of type INT`. The template now converts with `int64` first, and a test pins the rendered value.
- **`helm upgrade` didn't replace the crashing pod.** A StatefulSet updates pods in order and waits for each to be ready before touching it, so a pod that is crash-looping on the old spec is never replaced by the fixed one. Deleting the pod by hand (`kubectl delete pod bp-kafka-0`) lets the controller recreate it from the new spec. Worth remembering: after fixing a broken StatefulSet, check the pod's age.
