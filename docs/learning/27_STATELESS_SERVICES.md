# Exercise 27: stateless services, jobs and configuration

The goal is to see how the parts of the stack that hold no data are run, set up and configured, and why "is it ready?" and "is it alive?" must be different questions. You will take Postgres away from the API on purpose and watch what Kubernetes does and does not do about it. This needs the chart installed as in Exercise 26, and like the [Kubernetes guide](../guides/14-kubernetes.md) it has been run only in a cloud session so far.

## Run it

With the release `bp` installed in the namespace `blockpulse`, open two terminals (Git Bash or WSL).

Terminal 1, watch the pods:

```bash
kubectl -n blockpulse get pods -w
```

Terminal 2, remove Postgres, wait half a minute, bring it back:

```bash
kubectl -n blockpulse scale statefulset bp-postgres --replicas=0
kubectl -n blockpulse scale statefulset bp-postgres --replicas=1
```

To see the API from outside the cluster, in a third terminal:

```bash
kubectl -n blockpulse port-forward svc/bp-api 8000:8000      # then open http://127.0.0.1:8000/readyz and /healthz
```

## Open the output

In terminal 1, follow the two `bp-api-...` pods. By design they stay `Running` and their READY column goes from `1/1` to `0/1`; the RESTARTS column should not move. After Postgres returns they go back to `1/1` without anyone touching them. That is the behaviour the chart is written for (the outline in the [learning path](README.md) and guide 14 describe it); no timing of this particular experiment is on record, so note yours.

Also list what set the stack up:

```bash
kubectl -n blockpulse get jobs
kubectl -n blockpulse logs job/bp-db-init
```

In the recorded install (DE7 report, section DE7c, 2026-11-24) the two Jobs applied migrations `001` and `002` and created three topics, and `/readyz`, `/pipeline/status`, `/alerts?limit=2` and `/models/current` all answered 200 through the port-forward.

## Read one template

Open `infra/k8s/chart/blockpulse/templates/api.yaml`. The terms first:

- A **Deployment** keeps a number of interchangeable pods running. They have random names and no storage, so any of them can be replaced in any order. Compare the StatefulSet of Exercise 26, whose pod has a fixed name and its own volume.
- A **Service** is a stable name and address in front of a changing set of pods. It sends connections only to pods that are ready.
- A **Secret** holds settings that must not be in the image, such as a password; a **ConfigMap** does the same for ordinary files and values (Grafana's dashboard is one).

Now trace one decision, the two probes. Rendered here with `helm template`:

```yaml
readinessProbe:
  httpGet: {path: /readyz, port: http}
  periodSeconds: 5
  timeoutSeconds: 3
livenessProbe:
  httpGet: {path: /healthz, port: http}
  periodSeconds: 10
  timeoutSeconds: 3
```

Follow each path into `src/blockpulse/api.py`. `readyz()` runs `SELECT 1` on a database connection; `healthz()` returns `{"status": "ok"}` and touches nothing. So **readiness** asks "can this pod do useful work right now?", and a failing answer takes it out of the Service. **Liveness** asks "is this process responding at all?", and a failing answer restarts the container. With Postgres gone the first fails and the second passes, which is what you watched.

Then two more pieces of the same file and its neighbours:

- **Configuration.** The container has `envFrom: secretRef: bp-app`. Open `templates/app-secret.yaml`: the Secret holds one value, `BLOCKPULSE_DSN`, built from the Postgres values. Rendered with defaults it is `postgresql://blockpulse:blockpulse_dev@bp-postgres:5432/blockpulse`. The host is just `bp-postgres`, the Service name, resolved by CoreDNS.
- **Setup.** Open `templates/setup-jobs.yaml`. A **Job** runs a pod until it succeeds, retrying on failure. A **Helm hook** is a manifest Helm applies at a set point in a release's life; the annotation `helm.sh/hook: post-install,post-upgrade` means after every install and upgrade. Both commands (`db-init`, `kafka-init`) apply only what is missing, so running them again is harmless.

`infra/k8s/Dockerfile` builds the one image all of these use. Its last lines set `USER 10001` and `ENTRYPOINT ["python", "-m", "blockpulse"]`, which is why each container's `args` is just a subcommand such as `serve` or `db-init`.

## Change one small thing

A pod reads its environment only when it starts. So how does a changed password reach a running API? Render the chart twice and compare one annotation on the API Deployment:

```powershell
helm template bp infra/k8s/chart/blockpulse | Select-String "checksum/secret|BLOCKPULSE_DSN"
helm template bp infra/k8s/chart/blockpulse --set postgres.password=other | Select-String "checksum/secret|BLOCKPULSE_DSN"
```

Rendered here, the checksum starts `2b7289ac...` with the default password and `9058c515...` with `other`, and the connection string changes with it. The checksum sits in the pod template, so a different value is a different pod template, and the Deployment replaces its pods. Find the line in `api.yaml` that computes it.

Then change something in the template itself, for example `periodSeconds` of the readiness probe from 5 to 2, and run:

```powershell
helm lint --strict infra/k8s/chart/blockpulse
.\.venv\Scripts\python.exe -m pytest -q tests/test_helm_chart.py
```

Read `test_api_is_a_stateless_deployment_behind_a_service_with_split_probes` to see which properties are pinned and which are free to tune. Restore the file unless you mean to keep the change.

## Question to carry forward

What would happen to the API pods during that outage if liveness also used `/readyz`?

## Verified

Run here on 2026-11-24 (Linux, Helm v3.17.3, no cluster): `helm lint --strict infra/k8s/chart/blockpulse` (0 failed); `helm template bp infra/k8s/chart/blockpulse` with defaults and with `--set postgres.password=other` (the probe excerpt, the connection string and both checksum prefixes are from that output); `python -m pytest -q tests/test_helm_chart.py` (11 passed). `api.yaml`, `app-secret.yaml`, `setup-jobs.yaml`, the Dockerfile and `api.py` were read. Not run here: every `kubectl` command, the scale-down experiment, the port-forward, the `Select-String` form of the pipeline, and the `periodSeconds` edit. The Job results and the 200 responses are quoted from `docs/experiments/2026-11-24-de7-kubernetes.md` (section DE7c). The `0/1` behaviour is the chart's stated design, not a recorded measurement.
