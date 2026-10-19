import json

from blockpulse import drills


def test_report_passes_only_when_every_check_passes(tmp_path):
    checks = []
    drills._check(checks, "first", True, {"n": 1})
    drills._check(checks, "second", False, {"n": 2})
    report = drills._report("demo", 0.0, checks, extra=1)
    assert report["passed"] is False and report["extra"] == 1
    assert [c["check"] for c in report["checks"]] == ["first", "second"]
    path = drills.write_report(report, tmp_path)
    assert json.loads(path.read_text(encoding="utf-8"))["drill"] == "demo"
    assert drills._report("demo", 0.0, checks[:1])["passed"] is True


def test_drills_point_enrichment_away_from_the_public_api():
    assert drills.UNREACHABLE_API.startswith("http://127.0.0.1:")
