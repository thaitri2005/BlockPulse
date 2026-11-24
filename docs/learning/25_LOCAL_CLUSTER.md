# Exercise 25: a cluster on your own machine

The goal is to get a Kubernetes cluster running on the laptop, look at what it runs before any BlockPulse code is in it, and learn the first debugging habit: reading a pod's events. Nothing from the pipeline is installed in this exercise. One caution up front: everything in the [Kubernetes guide](../guides/14-kubernetes.md) was run on 2026-11-24 in a cloud development session, not yet on the laptop, so you are the first to try it there. Write down anything that differs.

## Words you need first

- A **cluster** is an API server plus one or more machines that run containers for it. You tell the API server what should exist; it keeps making that true.
- A **node** is one of those machines. Ours has exactly one.
- A **pod** is the smallest thing Kubernetes runs: one or more containers that share a network address. Pods are disposable.
- **k3s** is a small, complete Kubernetes; **k3d** runs a k3s node as a Docker container, so the "machine" is a container on your laptop.
- `kubectl` is the command-line client. A **context** is its saved answer to "which cluster am I talking to?".
- A **namespace** is a named group of objects inside a cluster.

## Run it

Tools: Docker, k3d 5.8, `kubectl` 1.31. The script is bash, so use Git Bash or WSL, from the repository root:

```bash
bash infra/k8s/cluster-up.sh
kubectl get pods -A
```

`-A` means all namespaces. The script ends by printing the node list and `cluster blockpulse is ready (context k3d-blockpulse)`. In the cloud session it took 34 s with images cached and about 5.5 min the first time, most of it image pulls ([DE7 report](../experiments/2026-11-24-de7-kubernetes.md), 2026-11-24).

Now start a throwaway pod, read its story, and delete it:

```bash
kubectl run probe --image=postgres:17 --env=POSTGRES_PASSWORD=x
kubectl describe pod probe
kubectl delete pod probe
kubectl get pods
```

When you are done for the day, `k3d cluster stop blockpulse` frees the memory and keeps the cluster.

## Open the output

`kubectl get pods -A` lists pods in the `kube-system` namespace that you did not start. The DE7 report names three running on the idle node: CoreDNS, a provisioner and metrics-server. For each, write one line on what would stop working without it:

- **CoreDNS** answers name lookups inside the cluster, so a pod can find `bp-postgres` by name.
- The **local-path provisioner** creates storage on the node when something asks for a volume (Exercise 26).
- **metrics-server** collects CPU and memory use, which `kubectl top pods` reads.

The idle node used 416 MiB in the cloud session (same report). Compare with `docker stats` on your machine.

In `kubectl describe pod probe`, scroll to **Events** at the bottom. Events are the cluster's own log of what it did to an object. Read them in order: the pod was assigned to the node, the image was pulled, the container was created, then started. When a pod will not start, the reason is almost always in this list. The DE7 session found both of its cluster problems there: `x509: certificate signed by unknown authority` (image pulls through a proxy) and `can't get final child's PID from pipe: EOF` (a host restriction). Neither should appear on a normal laptop.

## Read one file

Open `infra/k8s/k3d.yaml` (21 lines) and then `infra/k8s/cluster-up.sh`. Trace one decision: how does the script know the cluster is usable?

```bash
if kubectl wait --for=condition=Ready node --all --timeout=60s; then ready="yes"; break; fi
...
# The node reports Ready before its first pods run; CoreDNS running proves image pulls and the runtime both work.
if kubectl -n kube-system rollout status deployment/coredns --timeout=10s >/dev/null 2>&1; then ok="yes"; break; fi
```

Two checks, because the first is not enough. In the first attempt recorded in the DE7 report the node was `Ready` and yet no pod could start. So the script waits for a real pod, CoreDNS, to run. Both checks sit in retry loops; the first was added after a restarted cluster answered `ServiceUnavailable` for a few seconds.

Also find, earlier in the script, the branch that prints `already exists; reusing it`. Running the script twice is safe: it describes the state it wants and does nothing if that state is already there. That is the same idea Kubernetes itself is built on, **declared state**: you say what should exist, and the system works out the steps. Compose is closer to "run what I start".

In `k3d.yaml`, the two `--disable` arguments switch off the ingress controller and load balancer. That is why services in later exercises are reached with `kubectl port-forward`.

## Change one small thing

Start a pod that cannot start, and diagnose it from its events alone:

```bash
kubectl run broken --image=postgres:no-such-tag
kubectl get pod broken
kubectl describe pod broken
kubectl delete pod broken
```

Before reading the events, predict which step will fail: assignment to the node, the image pull, or the container start. Then find the event that names the cause.

This changes no files, so there are no tests to rerun. If you edit `cluster-up.sh` itself, run the check the project uses for shell scripts:

```powershell
.\.venv\Scripts\shellcheck.exe infra/k8s/*.sh
```

## Question to carry forward

You deleted a pod started with `kubectl run` and it stayed gone. Why will a pod created by a StatefulSet come back?

## Verified

Nothing in this exercise was run against a cluster here; the session that wrote it was not allowed to use Docker, k3d or `kubectl`. `infra/k8s/k3d.yaml` and `infra/k8s/cluster-up.sh` were read and the quoted lines are from the script. Timings (34 s, about 5.5 min), the 416 MiB figure, the list of system pods and the two error messages are quoted from `docs/experiments/2026-11-24-de7-kubernetes.md` (2026-11-24, cloud session). The `kubectl run`, `describe` and `delete` commands come from the outline in `docs/learning/README.md`; the `broken` pod experiment is new and has not been run, so its events are for you to record. Run here: `python -m blockpulse --help` only.
