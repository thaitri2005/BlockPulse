# 14 · Kubernetes: the same pipeline as pods

DE7 moves the serving stack from Docker Compose to Kubernetes, then reruns the failure drills by killing pods. This guide grows one step at a time; the running log with measurements is [the DE7 experiment report](../experiments/2026-11-24-de7-kubernetes.md).

| Step | What | Status |
| --- | --- | --- |
| DE7a | A local cluster that can run pods (`infra/k8s/`) | Done 2026-11-24 |
| DE7b | Helm chart: Postgres and Kafka as StatefulSets with volumes, probes, requests and limits | Next |
| DE7c | BlockPulse image; migrations as a Job; API, enrichment worker and Grafana as Deployments | Staged |
| DE7d | Drills rerun as pod kills, same invariants as [guide 10](10-failure-drills.md) | Staged |

## Concepts, in the order they appear

- **Cluster and node.** A Kubernetes *cluster* is an API server plus one or more *nodes* (machines that run containers). You tell the API what should exist ("one Postgres with this volume"); controllers keep making it true. Compose runs what you start; Kubernetes keeps restoring what you declared. That difference is the whole reason the drills change shape in DE7d.
- **k3s and k3d.** *k3s* is a small, complete Kubernetes distribution in one binary. *k3d* runs a k3s node as a Docker container, so a cluster is created in under a minute and deleted without a trace. It is for learning and tests, not for production.
- **Pod.** The unit Kubernetes schedules: one or more containers sharing a network address. Pods are disposable; anything that must survive a pod lives in a volume or another system.
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
