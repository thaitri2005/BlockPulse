# Exercise 26: state that survives its pod

The goal is to see why deleting a database pod does not delete the database. You will install the BlockPulse chart, put rows into Postgres, delete its pod, and find the rows still there. Then you will find the few lines of YAML that made that true. This needs the cluster from Exercise 25. Like the rest of the [Kubernetes guide](../guides/14-kubernetes.md), it has so far been run only in a cloud session (2026-11-24), not on the laptop.

## Run it

Tools: Helm 3.17 in addition to Exercise 25. From Git Bash or WSL, in the repository root:

```bash
bash infra/k8s/cluster-up.sh
bash infra/k8s/image-load.sh
helm upgrade --install bp infra/k8s/chart/blockpulse -n blockpulse --create-namespace --wait --wait-for-jobs
kubectl -n blockpulse get pods,pvc
```

A **Helm chart** is a folder of templated Kubernetes manifests; a **release** is one installed copy of it, here named `bp`; **values** are the settings the templates read, from `values.yaml` or `--set`. In the cloud session the image build took about 1.5 min the first time and the install 19 s ([DE7 report](../experiments/2026-11-24-de7-kubernetes.md)).

Create a table, delete the pod, watch, and look again:

```bash
kubectl -n blockpulse exec bp-postgres-0 -- psql -U blockpulse -d blockpulse -c "CREATE TABLE probe AS SELECT generate_series(1, 1000) AS n"
kubectl -n blockpulse delete pod bp-postgres-0
kubectl -n blockpulse get pods -w          # Ctrl+C when bp-postgres-0 is 1/1 Running again
kubectl -n blockpulse exec bp-postgres-0 -- psql -U blockpulse -d blockpulse -c "SELECT count(*) FROM probe"
kubectl -n blockpulse exec bp-postgres-0 -- psql -U blockpulse -d blockpulse -c "DROP TABLE probe"
```

## Open the output

Two things to notice while watching. The replacement pod has the *same name*, `bp-postgres-0`, where the `probe` pod of Exercise 25 simply vanished. And `kubectl -n blockpulse get pvc` shows `data-bp-postgres-0` unchanged through the whole thing.

The same test is on record: on 2026-11-24, `bp-postgres-0` was force-deleted while holding a 1,000-row probe table, was ready again in 2.8 s, and still had 1,000 rows (DE7 report, section DE7b, idle cluster, hand-made data).

Now look at what Helm sends to the cluster, without a cluster. This runs anywhere Helm is installed:

```powershell
helm template bp infra/k8s/chart/blockpulse --show-only templates/postgres.yaml
```

The end of the output, as rendered here:

```yaml
  # One PersistentVolumeClaim per pod, named data-<pod>. It outlives the pod, which is what the drills rely on.
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 2Gi
```

## Read one template

Open `infra/k8s/chart/blockpulse/templates/postgres.yaml` next to `values.yaml`. Trace one setting, the volume size:

1. `values.yaml` has `postgres.storage: 2Gi`.
2. The template's last line is `storage: {{ .Values.postgres.storage }}`.
3. The rendered manifest above says `storage: 2Gi`, inside `volumeClaimTemplates`.
4. In the cluster that becomes a claim named `data-bp-postgres-0`: the template's name, then the pod's name.

The terms, one sentence each:

- A **StatefulSet** runs pods that have an identity: a fixed name and their own storage, both kept when the pod is replaced.
- A **PersistentVolumeClaim** (PVC) is a request for storage that exists as its own object, separate from any pod.
- A **headless Service** (`clusterIP: None`, the second document in the file) gives each pod its own DNS name instead of one shared address.
- A **probe** is a check the cluster runs against a container. The file has three: *startup* waits out a slow first boot, *readiness* decides whether the pod receives connections, *liveness* decides whether the container is restarted.
- **Requests** are what the scheduler reserves for the container; **limits** are the ceiling, and a container over its memory limit is killed. Find both under `resources` in `values.yaml`: 256Mi requested, 512Mi limit.

Then find where the data directory is mounted: `volumeMounts` attaches the claim `data` at `/var/lib/postgresql/data`. The pod is replaceable because nothing it needs lives in the pod.

`tests/test_helm_chart.py` holds these properties in place. Read `test_stateful_workloads_keep_their_data_and_declare_probes_and_resources`: it renders the chart and asserts the claim template, the headless Service and all three probes exist, for Postgres and Kafka both.

## Change one small thing

Override the value at render time and watch it arrive:

```powershell
helm template bp infra/k8s/chart/blockpulse --show-only templates/postgres.yaml --set postgres.storage=5Gi
```

The last line becomes `storage: 5Gi`; nothing else changes. Then make a change the tests should catch: in `postgres.yaml`, temporarily delete the `livenessProbe` block and run:

```powershell
helm lint --strict infra/k8s/chart/blockpulse
.\.venv\Scripts\python.exe -m pytest -q tests/test_helm_chart.py
```

Note which check objects, restore the file (`git checkout infra/k8s/chart/blockpulse/templates/postgres.yaml`), and rerun until all pass. The tests skip if `helm` is not on your PATH, so check the summary line says passed, not skipped.

Do not try the 5Gi value on the installed release as a quick test. Kubernetes treats a StatefulSet's claim template as fixed after creation, so expect the upgrade to be rejected; that was not tried here.

## Question to carry forward

`helm uninstall` leaves the claims behind. Why is that the safe default, and what would you have to do to lose the data?

## Verified

Run here on 2026-11-24 (Linux, Helm v3.17.3, no cluster): `helm lint --strict infra/k8s/chart/blockpulse` (0 failed); `helm template bp infra/k8s/chart/blockpulse`, with `--show-only templates/postgres.yaml` and with `--set postgres.storage=5Gi` (the excerpt and the `5Gi` line are from that output); `python -m pytest -q tests/test_helm_chart.py` (11 passed). Not run here: `cluster-up.sh`, `image-load.sh`, `helm upgrade --install`, every `kubectl` command including the `psql` lines, and the deleted-probe experiment. The `psql` invocation follows `_psql()` in `src/blockpulse/k8s_drills.py`. Timings and the 1,000-row result are quoted from `docs/experiments/2026-11-24-de7-kubernetes.md`; note the recorded run used a forced delete, and the commands above use a normal one.
