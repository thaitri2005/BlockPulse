"""The Helm chart renders, and every workload keeps the properties the pod-kill drills rely on.

Needs the `helm` binary; skipped when it isn't installed. Nothing here talks to a cluster.
"""
import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "infra" / "k8s" / "chart" / "blockpulse"
pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")


def _render(*args: str) -> list[dict]:
    out = subprocess.run(["helm", "template", "bp", str(CHART), "--namespace", "blockpulse", *args],
                         capture_output=True, text=True, check=True).stdout
    return [doc for doc in yaml.safe_load_all(out) if doc]


def _one(docs: list[dict], kind: str, name: str) -> dict:
    found = [d for d in docs if d["kind"] == kind and d["metadata"]["name"] == name]
    assert len(found) == 1, f"expected one {kind}/{name}, found {len(found)}"
    return found[0]


def _env(container: dict) -> dict:
    return {e["name"]: e.get("value") for e in container.get("env", [])}


def test_chart_lints_cleanly():
    result = subprocess.run(["helm", "lint", "--strict", str(CHART)], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("component", ["postgres", "kafka"])
def test_stateful_workloads_keep_their_data_and_declare_probes_and_resources(component):
    docs = _render()
    workload = _one(docs, "StatefulSet", f"bp-{component}")
    service = _one(docs, "Service", f"bp-{component}")

    # A stable DNS name needs a headless Service that the StatefulSet names, selecting the same pods.
    assert service["spec"]["clusterIP"] == "None"
    assert workload["spec"]["serviceName"] == service["metadata"]["name"]
    assert service["spec"]["selector"] == workload["spec"]["selector"]["matchLabels"]
    assert workload["spec"]["selector"]["matchLabels"].items() <= workload["spec"]["template"]["metadata"]["labels"].items()

    # The volume must come from a claim template, so it outlives the pod.
    claims = {c["metadata"]["name"] for c in workload["spec"]["volumeClaimTemplates"]}
    (container,) = workload["spec"]["template"]["spec"]["containers"]
    assert {m["name"] for m in container["volumeMounts"]} == claims == {"data"}

    for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
        assert probe in container, f"{component} has no {probe}"
    assert container["resources"]["requests"]["memory"]
    assert container["resources"]["limits"]["memory"]


def test_kafka_advertises_its_stable_dns_name_and_integer_sizes():
    (container,) = _one(_render(), "StatefulSet", "bp-kafka")["spec"]["template"]["spec"]["containers"]
    env = _env(container)
    assert env["KAFKA_ADVERTISED_LISTENERS"] == "PLAINTEXT://bp-kafka-0.bp-kafka.blockpulse.svc.cluster.local:9092"
    # Helm reads large numbers as floats; an unconverted one renders as 8.388608e+06 and the broker won't start.
    assert env["KAFKA_MESSAGE_MAX_BYTES"] == "8388608"
    assert env["KAFKA_REPLICA_FETCH_MAX_BYTES"] == "9437184"
    assert env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"


def test_postgres_password_comes_from_a_secret_and_can_be_overridden():
    docs = _render("--set", "postgres.password=s3cret")
    assert _one(docs, "Secret", "bp-postgres")["stringData"]["POSTGRES_PASSWORD"] == "s3cret"
    (container,) = _one(docs, "StatefulSet", "bp-postgres")["spec"]["template"]["spec"]["containers"]
    assert container["envFrom"] == [{"secretRef": {"name": "bp-postgres"}}]
    assert "s3cret" not in yaml.safe_dump(_one(docs, "StatefulSet", "bp-postgres"))


def _pod_spec(doc: dict) -> dict:
    return doc["spec"]["template"]["spec"]


def test_api_is_a_stateless_deployment_behind_a_service_with_split_probes():
    docs = _render()
    api = _one(docs, "Deployment", "bp-api")
    service = _one(docs, "Service", "bp-api")
    assert service["spec"].get("clusterIP") != "None"
    assert service["spec"]["selector"] == api["spec"]["selector"]["matchLabels"]
    assert api["spec"]["replicas"] == 2
    assert "volumes" not in _pod_spec(api)

    (container,) = _pod_spec(api)["containers"]
    # Readiness depends on the database, liveness must not: a restart cannot repair a Postgres outage.
    assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert container["livenessProbe"]["httpGet"]["path"] == "/healthz"
    assert container["args"][:3] == ["serve", "--host", "0.0.0.0"]
    assert container["envFrom"] == [{"secretRef": {"name": "bp-app"}}]
    assert container["resources"]["requests"]["memory"] and container["resources"]["limits"]["memory"]


def test_blockpulse_containers_run_unprivileged():
    docs = _render()
    workloads = [_one(docs, "Deployment", "bp-api"), _one(docs, "Job", "bp-db-init"), _one(docs, "Job", "bp-kafka-init")]
    for workload in workloads:
        for container in _pod_spec(workload)["containers"]:
            context = container["securityContext"]
            assert context["runAsNonRoot"] is True and context["allowPrivilegeEscalation"] is False
            assert container["image"] == "blockpulse:dev"


def test_setup_jobs_are_hooks_that_run_idempotent_commands():
    docs = _render()
    expected = {"bp-db-init": ["db-init"], "bp-kafka-init": ["kafka-init", "--bootstrap-servers", "bp-kafka-0.bp-kafka:9092"]}
    for name, args in expected.items():
        job = _one(docs, "Job", name)
        assert job["metadata"]["annotations"]["helm.sh/hook"] == "post-install,post-upgrade"
        assert _pod_spec(job)["restartPolicy"] == "OnFailure"
        assert _pod_spec(job)["containers"][0]["args"] == args


def test_connection_string_is_a_secret_and_a_new_password_restarts_the_api():
    def dsn_and_checksum(*args):
        docs = _render(*args)
        annotations = _one(docs, "Deployment", "bp-api")["spec"]["template"]["metadata"]["annotations"]
        return _one(docs, "Secret", "bp-app")["stringData"]["BLOCKPULSE_DSN"], annotations["checksum/secret"]

    dsn, checksum = dsn_and_checksum()
    assert dsn == "postgresql://blockpulse:blockpulse_dev@bp-postgres:5432/blockpulse"
    new_dsn, new_checksum = dsn_and_checksum("--set", "postgres.password=p@ss/word")
    assert new_dsn == "postgresql://blockpulse:p%40ss%2Fword@bp-postgres:5432/blockpulse"  # special characters are escaped
    assert new_checksum != checksum


def test_worker_is_off_by_default_and_never_runs_twice():
    worker = _one(_render(), "Deployment", "bp-worker")
    assert worker["spec"]["replicas"] == 0
    assert worker["spec"]["strategy"] == {"type": "Recreate"}  # an upgrade must not overlap two workers

    enabled = _one(_render("--set", "worker.replicas=1", "--set", "worker.apiBase=http://127.0.0.1:9/api"), "Deployment", "bp-worker")
    args = _pod_spec(enabled)["containers"][0]["args"]
    assert args[0] == "kafka-enrich"
    assert {"--group-id=blockpulse-enrich", "--bootstrap-servers=bp-kafka-0.bp-kafka:9092", "--api-base=http://127.0.0.1:9/api",
            "--max-requests=100", "--idle-timeout=3600"} <= set(args)

    refused = subprocess.run(["helm", "template", "bp", str(CHART), "--set", "worker.replicas=2"], capture_output=True, text=True)
    assert refused.returncode != 0 and "worker.replicas must be 0 or 1" in refused.stderr


def test_grafana_reads_postgres_through_the_secret_and_ships_the_repository_dashboard():
    docs = _render()
    (container,) = _pod_spec(_one(docs, "Deployment", "bp-grafana"))["containers"]
    password = next(e for e in container["env"] if e["name"] == "POSTGRES_PASSWORD")
    assert password["valueFrom"]["secretKeyRef"] == {"name": "bp-postgres", "key": "POSTGRES_PASSWORD"}
    datasources = yaml.safe_load(_one(docs, "ConfigMap", "bp-grafana-provisioning")["data"]["datasources.yaml"])
    assert datasources["datasources"][0]["url"] == "bp-postgres:5432"
    assert datasources["datasources"][0]["secureJsonData"]["password"] == "${POSTGRES_PASSWORD}"

    # Helm can only read files inside the chart, so the dashboard is a copy. It must stay identical to the one
    # Compose mounts, and what the ConfigMap carries must be that file.
    source = CHART.parents[3] / "observability" / "grafana" / "dashboards" / "blockpulse-overview.json"
    copy = CHART / "files" / "dashboards" / "blockpulse-overview.json"
    assert copy.read_bytes() == source.read_bytes(), "copy observability/grafana/dashboards/blockpulse-overview.json into the chart"
    rendered = _one(docs, "ConfigMap", "bp-grafana-dashboards")["data"]
    assert list(rendered) == ["blockpulse-overview.json"]
    assert json.loads(rendered["blockpulse-overview.json"]) == json.loads(source.read_text(encoding="utf-8"))
