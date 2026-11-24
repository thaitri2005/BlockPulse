"""The Kubernetes drills' logic with kubectl and helm replaced by fakes. No cluster is contacted."""
import json
from types import SimpleNamespace

from blockpulse import drills, k8s_drills

DESCRIBE = """
GROUP             TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID  HOST  CLIENT-ID
blockpulse-enrich blockpulse.raw.v1 0          4000            10000           6000  c-1          /10.1 blockpulse
blockpulse-enrich blockpulse.raw.v1 1          -               10000           -     c-1          /10.1 blockpulse
blockpulse-enrich blockpulse.raw.v1 2          10000           10000           0     c-1          /10.1 blockpulse
"""


def test_group_lag_totals_and_uncommitted_partitions_count_from_zero():
    assert k8s_drills.parse_group_lag(DESCRIBE) == {"partitions": 3, "committed": 14000, "end": 30000, "lag": 16000}
    assert k8s_drills.parse_group_lag("Consumer group 'x' has no active members.\n") == {"partitions": 0, "committed": 0, "end": 0, "lag": 0}


def test_an_empty_group_is_not_mistaken_for_a_drained_one(monkeypatch):
    monkeypatch.setattr(k8s_drills, "group_lag", lambda: {"partitions": 0, "committed": 0, "end": 0, "lag": 0})
    assert k8s_drills._drained() is None
    monkeypatch.setattr(k8s_drills, "group_lag", lambda: {"partitions": 3, "committed": 30, "end": 30, "lag": 0})
    assert k8s_drills._drained()["lag"] == 0
    monkeypatch.setattr(k8s_drills, "group_lag", lambda: None)  # the broker is down
    assert k8s_drills._drained() is None


def test_install_points_the_worker_away_from_the_public_api(monkeypatch):
    commands = []
    monkeypatch.setattr(k8s_drills, "_run", lambda *command, **_: commands.append(command) or SimpleNamespace(returncode=0, stdout="", stderr=""))
    k8s_drills.install()
    helm = next(c for c in commands if c[0] == "helm")
    assert f"worker.apiBase={drills.UNREACHABLE_API}" in helm and "worker.maxRequests=1" in helm
    assert helm[helm.index("-n") + 1] == k8s_drills.NAMESPACE == "blockpulse-drill"  # never the normal namespace
    assert commands[0][:3] == ("kubectl", "delete", "namespace")  # a fresh namespace, so topics start empty


def test_publish_generates_in_cluster_and_spreads_sessions(monkeypatch):
    shells = []
    monkeypatch.setattr(k8s_drills, "run_pod", lambda name, shell, **_: shells.append(shell) or "")
    assert k8s_drills._publish(30000, 3) == 30000
    assert shells[0].count("synth-capture") == 3 and shells[0].count("kafka-replay") == 3
    assert "--messages 10000 --seed 2" in shells[0]


def test_verify_merges_the_kafka_invariants_into_the_report(monkeypatch):
    inner = {"checks": [{"check": "no message lost", "passed": True, "detail": {}}], "raw_records": 5, "enriched_records": 6, "duplicate_records": 1}
    monkeypatch.setattr(k8s_drills, "run_pod", lambda name, shell, **_: "warning line\n" + json.dumps(inner, indent=2) + "\n")
    checks = [{"check": "earlier", "passed": True, "detail": {}}]
    assert k8s_drills._verify(checks, 5) == {"raw_records": 5, "enriched_records": 6, "duplicate_records": 1}
    assert [c["check"] for c in checks] == ["earlier", "no message lost"]


def test_load_job_retries_in_new_pods_and_reads_the_connection_secret():
    job = k8s_drills.load_job_manifest("load-a", "drill-kill", 50000)
    spec = job["spec"]["template"]["spec"]
    assert job["metadata"]["namespace"] == "blockpulse-drill"
    assert spec["restartPolicy"] == "Never" and job["spec"]["backoffLimit"] >= 1  # each retry is a fresh pod
    (container,) = spec["containers"]
    assert container["envFrom"] == [{"secretRef": {"name": "bp-app"}}]
    assert container["securityContext"]["runAsNonRoot"] is True
    shell = container["command"][-1]
    # The same seed on every attempt, so a retry loads identical rows; the load is the last step.
    assert "--messages 50000 --seed 11" in shell and shell.endswith("load-results /tmp/d --run-id drill-kill")


def test_a_load_counts_as_in_progress_only_while_another_transaction_is_open(monkeypatch):
    for answer, expected in (("1", True), ("2", True), ("0", False), ("", False), (None, False)):
        monkeypatch.setattr(k8s_drills, "_psql", lambda sql, answer=answer: answer)
        assert k8s_drills._load_in_progress() is expected


def test_score_count_is_unknown_while_postgres_is_down(monkeypatch):
    monkeypatch.setattr(k8s_drills, "_psql", lambda sql: None)
    assert k8s_drills._scores("drill-kill") is None
    monkeypatch.setattr(k8s_drills, "_psql", lambda sql: "200000")
    assert k8s_drills._scores("drill-kill") == 200000
