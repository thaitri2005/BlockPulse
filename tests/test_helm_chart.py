"""The Helm chart renders, and every workload keeps the properties the pod-kill drills rely on.

Needs the `helm` binary; skipped when it isn't installed. Nothing here talks to a cluster.
"""
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
