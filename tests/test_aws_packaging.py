"""Checks on what gets shipped to the AWS instance: the app bundle and the rendered start-up script."""

import importlib.util
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "infra" / "aws" / "terraform" / "templates" / "user-data.sh.tftpl"
TEMPLATE_VARS = {"bucket": "blockpulse-data-test", "bundle_key": "artifacts/blockpulse-app.zip", "region": "us-west-2",
                 "deadline": "2026-10-19T04:00:00Z", "source_url": "wss://node203.sv1.mempool.space/api/v1/ws",
                 "session_seconds": "3600"}


def load_builder():
    spec = importlib.util.spec_from_file_location("build_bundle", ROOT / "infra" / "aws" / "build_bundle.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bundle_is_reproducible_complete_and_linux_safe(tmp_path):
    builder = load_builder()
    first = builder.build(tmp_path / "a.zip")
    second = builder.build(tmp_path / "b.zip")
    assert first["sha256"] == second["sha256"]  # same inputs, byte-identical bundle

    with zipfile.ZipFile(tmp_path / "a.zip") as bundle:
        names = set(bundle.namelist())
        required = {"pyproject.toml", "README.md", "src/blockpulse/cli.py", "src/blockpulse/collector.py",
                    "src/blockpulse/migrations/001_initial.sql", "infra/aws/instance/install.sh",
                    "infra/aws/instance/deadline-check.sh", "infra/aws/instance/systemd/blockpulse-collect.service",
                    "constraints.txt"}
        assert required <= names
        constraints = bundle.read("constraints.txt").decode("utf-8")
        assert "--hash" not in constraints and "pyarrow==" in constraints and "numpy==" in constraints
        assert not any("__pycache__" in n or n.endswith(".pyc") or n.startswith("data/") for n in names)
        for name in names:
            assert b"\r\n" not in bundle.read(name), f"{name} has Windows line endings"
        assert (bundle.getinfo("infra/aws/instance/install.sh").external_attr >> 16) & 0o111


def test_installer_references_only_units_that_exist():
    install = (ROOT / "infra/aws/instance/install.sh").read_text(encoding="utf-8")
    enabled = re.search(r"systemctl enable --now (.+)", install).group(1).split()
    units = {p.name for p in (ROOT / "infra/aws/instance/systemd").iterdir()}
    assert set(enabled) <= units
    for unit in units:  # every service runs a command the package or installer actually provides
        text = (ROOT / "infra/aws/instance/systemd" / unit).read_text(encoding="utf-8")
        for command in re.findall(r"-m blockpulse (\S+)", text):
            assert command in {"collect-loop", "process-pending", "sync-pending"}


def render(template: str, values: dict) -> str:
    """Terraform templatefile() substitutes ${name}; bash variables in the template use $name without braces."""
    rendered = re.sub(r"\$\{(\w+)\}", lambda m: values[m.group(1)], template)
    assert "${" not in rendered, "unrendered template placeholder"
    return rendered


def test_user_data_template_renders_every_placeholder():
    rendered = render(TEMPLATE.read_text(encoding="utf-8"), TEMPLATE_VARS)
    assert 'BLOCKPULSE_DEADLINE="2026-10-19T04:00:00Z"' in rendered
    assert "s3://blockpulse-data-test/artifacts/blockpulse-app.zip" in rendered
    assert set(re.findall(r"\$\{(\w+)\}", TEMPLATE.read_text(encoding="utf-8"))) == set(TEMPLATE_VARS)


# The dev extra installs shellcheck next to the interpreter, which may not be on PATH if the venv isn't activated.
SHELLCHECK = shutil.which("shellcheck") or shutil.which("shellcheck", path=str(Path(sys.executable).parent))


@pytest.mark.skipif(SHELLCHECK is None, reason="shellcheck not installed")
def test_rendered_user_data_passes_shellcheck(tmp_path):
    script = tmp_path / "user-data.sh"
    script.write_text(render(TEMPLATE.read_text(encoding="utf-8"), TEMPLATE_VARS), encoding="utf-8", newline="\n")
    result = subprocess.run([SHELLCHECK, str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout
