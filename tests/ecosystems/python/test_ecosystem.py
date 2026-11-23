"""
Copyright (C) 2026 Lightwell

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from import_orchestrator.cli import make_parser
from import_orchestrator.ecosystems.base import Ecosystem
from import_orchestrator.ecosystems.python import config
from import_orchestrator.ecosystems.python.ecosystem import PythonEcosystem
from import_orchestrator.engine.errors import TriggerError


def test_python_ecosystem_satisfies_protocol():
    assert isinstance(PythonEcosystem(), Ecosystem)


def test_python_attributes():
    eco = PythonEcosystem()
    assert eco.name == "python"
    assert eco.namespace == "lightwell-python-tenant"
    assert eco.pipelinerun_prefix == "python-build-"


def test_build_pipelinerun_from_ref(monkeypatch, tmp_path):
    pipeline_file = tmp_path / "tekton" / "pipelines" / "python-build" / "python-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace())

    assert manifest["metadata"]["namespace"] == "lightwell-python-tenant"
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["PACKAGE"] == "foolib"
    assert params["VERSION"] == "0.4.0"


def test_build_pipelinerun_uses_default_target_labels(monkeypatch, tmp_path):
    pipeline_file = tmp_path / "tekton" / "pipelines" / "python-build" / "python-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    # No target on args -> DEFAULT_TARGET (REMEDIATED) supplies the labels.
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace())

    labels = manifest["metadata"]["labels"]
    assert labels["appstudio.openshift.io/application"] == "remediated-build"
    assert labels["appstudio.openshift.io/component"] == "remediated-build"


def test_build_pipelinerun_threads_builds_tag(monkeypatch, tmp_path):
    pipeline_file = tmp_path / "tekton" / "pipelines" / "python-build" / "python-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    args = argparse.Namespace(builds_tag="CVE-2025-1234/0.4.0/pipeline-9")
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", args)

    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["LIGHTWELL_BUILDS_TAG"] == "CVE-2025-1234/0.4.0/pipeline-9"


def test_build_pipelinerun_applies_overrides(monkeypatch, tmp_path):
    pipeline_file = tmp_path / "tekton" / "pipelines" / "python-build" / "python-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")

    overrides_file = tmp_path / "tekton" / "pipelines" / "python-build" / "overrides.yaml"
    overrides_file.write_text("timeouts:\n  pipeline: 1h\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    manifest = PythonEcosystem().build_pipelinerun("ntplib==0.4.0", argparse.Namespace())

    assert manifest["spec"]["timeouts"] == {"pipeline": "1h"}


def test_build_pipelinerun_rejects_malformed_ref(tmp_path, monkeypatch):
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    with pytest.raises(TriggerError):
        PythonEcosystem().build_pipelinerun("foolib-0.4.0", argparse.Namespace())


def _pipeline_file(tmp_path, monkeypatch):
    f = tmp_path / "tekton" / "pipelines" / "python-build" / "python-build.yaml"
    f.parent.mkdir(parents=True)
    f.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))


def test_build_pipelinerun_scratch_target_labels(monkeypatch, tmp_path):
    _pipeline_file(tmp_path, monkeypatch)
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace(target="SCRATCH"))
    labels = manifest["metadata"]["labels"]
    assert labels["appstudio.openshift.io/application"] == "scratch-build"
    assert labels["appstudio.openshift.io/component"] == "scratch-build"


def test_build_pipelinerun_scratch_service_account(monkeypatch, tmp_path):
    _pipeline_file(tmp_path, monkeypatch)
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace(target="SCRATCH"))
    assert manifest["spec"]["taskRunTemplate"]["serviceAccountName"] == "build-pipeline-scratch-build"


def test_build_pipelinerun_remediated_service_account(monkeypatch, tmp_path):
    _pipeline_file(tmp_path, monkeypatch)
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace(target="REMEDIATED"))
    assert manifest["spec"]["taskRunTemplate"]["serviceAccountName"] == "build-pipeline-remediated-build"


def test_build_pipelinerun_validated_target_defaults_build_type(monkeypatch, tmp_path):
    _pipeline_file(tmp_path, monkeypatch)
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace(target="VALIDATED"))
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["BUILD_TYPE"] == "Validated"
    assert params["WHEEL_SERVER_URL"] == config.WHEEL_SERVER_URL


@pytest.mark.parametrize("command", ["trigger", "run", "orchestrate"])
@pytest.mark.parametrize("target", ["VALIDATED", "VALIDATED-STAGE"])
@pytest.mark.parametrize("url", ["https://custom.repo/simple/", ""])
def test_validated_cli_wheel_server_url(monkeypatch, tmp_path, command, target, url):
    _pipeline_file(tmp_path, monkeypatch)
    argv = ["python", command, "--target", target, "--wheel-server-url", url]
    if command != "orchestrate":
        argv.append("foolib==0.4.0")
    args = make_parser().parse_args(argv)
    manifest = args.ecosystem.build_pipelinerun("foolib==0.4.0", args)
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["BUILD_TYPE"] == "Validated"
    assert params["WHEEL_SERVER_URL"] == url


def test_build_pipelinerun_remediated_target_defaults_build_type(monkeypatch, tmp_path):
    _pipeline_file(tmp_path, monkeypatch)
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace(target="REMEDIATED"))
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["BUILD_TYPE"] == "Backport"


def test_build_pipelinerun_scratch_target_defaults_build_type(monkeypatch, tmp_path):
    _pipeline_file(tmp_path, monkeypatch)
    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace(target="SCRATCH"))
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["BUILD_TYPE"] == "Scratch"


def test_build_pipelinerun_explicit_build_type_overrides_target_pin(monkeypatch, tmp_path):
    _pipeline_file(tmp_path, monkeypatch)
    manifest = PythonEcosystem().build_pipelinerun(
        "foolib==0.4.0", argparse.Namespace(target="SCRATCH", build_type="Backport")
    )
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["BUILD_TYPE"] == "Backport"


class TestTargetSkipRelease:
    def test_remediated_does_not_skip_release(self):
        assert PythonEcosystem().target_skip_release("REMEDIATED") is False

    def test_scratch_skips_release(self):
        assert PythonEcosystem().target_skip_release("SCRATCH") is True

    def test_unknown_target_defaults_to_not_skipping(self):
        assert PythonEcosystem().target_skip_release("UNKNOWN") is False


def test_request_identity_defaults_to_remediated_target():
    identity = PythonEcosystem().request_identity("foolib==0.4.0", argparse.Namespace())
    assert identity == {
        "namespace": "lightwell-python-tenant",
        "application": "remediated-build",
        "component": "remediated-build",
        "pipeline": "python-build",
        "release_plan": "",
        "target": "REMEDIATED",
        "artifact": "foolib==0.4.0",
        "source_repo": f"{config.LIGHTWELL_BUILDS_REPO_BASE}/pypi.org-foolib",
    }


def test_request_identity_honours_target():
    identity = PythonEcosystem().request_identity("foolib==0.4.0", argparse.Namespace(target="SCRATCH"))
    assert identity["application"] == "scratch-build"
    assert identity["target"] == "SCRATCH"
    assert identity["artifact"] == "foolib==0.4.0"


@pytest.mark.parametrize("target", ["VALIDATED", "VALIDATED-STAGE", "REMEDIATED", "SCRATCH"])
def test_real_pipeline_preserves_service_account_auth_for_each_target(monkeypatch, target):
    project_root = Path(__file__).resolve().parents[3]
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(project_root / "tekton"))
    monkeypatch.setenv("BUILD_CONFIGS_REPO_URL", "https://example.test/build-configs.git")

    manifest = PythonEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace(target=target))
    spec = manifest["spec"]
    bindings = {workspace["name"]: workspace["secret"]["secretName"] for workspace in spec["workspaces"]}
    required = {workspace["name"] for workspace in spec["pipelineSpec"]["workspaces"]}
    assert bindings.keys() == required
    assert set(bindings) == {"git-auth"}
    assert spec["taskRunTemplate"]["serviceAccountName"] == config.TARGET_CONFIGS[target]["service_account"]

    supplied = {param["name"] for param in spec["params"]}
    required_params = {param["name"] for param in spec["pipelineSpec"]["params"] if "default" not in param}
    assert required_params <= supplied


@pytest.mark.parametrize("command", ["trigger", "run", "orchestrate"])
@pytest.mark.parametrize("image", [None, "quay.io/example/builder@sha256:" + "a" * 64])
def test_cli_builder_image_preserves_pipeline_default_unless_overridden(monkeypatch, tmp_path, command, image):
    _pipeline_file(tmp_path, monkeypatch)
    pipeline_path = tmp_path / "tekton/pipelines/python-build/python-build.yaml"
    default_image = "quay.io/example/default-builder@sha256:" + "b" * 64
    pipeline_path.write_text(
        f"spec:\n  tasks: []\n  params:\n    - name: PYTHON_BUILDER_IMAGE\n      default: {default_image}\n"
    )
    argv = ["python", command, "--target", "VALIDATED-STAGE"]
    if command != "orchestrate":
        argv.append("foolib==0.4.0")
    if image is not None:
        argv.extend(["--python-builder-image", image])
    args = make_parser().parse_args(argv)
    manifest = args.ecosystem.build_pipelinerun("foolib==0.4.0", args)
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    defaults = {p["name"]: p["default"] for p in manifest["spec"]["pipelineSpec"]["params"]}
    assert defaults["PYTHON_BUILDER_IMAGE"] == default_image
    if image is None:
        assert "PYTHON_BUILDER_IMAGE" not in params
    else:
        assert params["PYTHON_BUILDER_IMAGE"] == image
