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

import pytest

from import_orchestrator.ecosystems.base import Ecosystem
from import_orchestrator.ecosystems.javascript import config
from import_orchestrator.ecosystems.javascript.ecosystem import JavaScriptEcosystem
from import_orchestrator.engine.errors import TriggerError


def test_javascript_ecosystem_satisfies_protocol():
    """JavaScriptEcosystem implements the Ecosystem protocol."""
    assert isinstance(JavaScriptEcosystem(), Ecosystem)


def test_javascript_attributes():
    """JavaScriptEcosystem exposes expected name/namespace/prefix."""
    eco = JavaScriptEcosystem()
    assert eco.name == "javascript"
    assert eco.namespace == "lightwell-javascript-tenant"
    assert eco.pipelinerun_prefix == "javascript-validated-build-"


def test_build_pipelinerun_from_ref(monkeypatch, tmp_path):
    """build_pipelinerun produces a VALIDATED manifest for package==version."""
    pipeline_file = tmp_path / "tekton" / "pipelines" / "javascript-validated-build" / "javascript-validated-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    manifest = JavaScriptEcosystem().build_pipelinerun("bluebird==3.7.2", argparse.Namespace())

    assert manifest["metadata"]["namespace"] == "lightwell-javascript-tenant"
    assert manifest["metadata"]["generateName"] == "javascript-validated-build-"
    labels = manifest["metadata"]["labels"]
    assert labels["appstudio.openshift.io/application"] == "javascript-validated"
    assert labels["appstudio.openshift.io/component"] == "javascript-validated-build"
    assert manifest["spec"]["taskRunTemplate"]["serviceAccountName"] == ("build-pipeline-javascript-validated-build")
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["packages"] == ["packages/bluebird/3.7.2"]
    assert params["output-image"].endswith(":bluebird-3.7.2")
    assert params["tl-npm-registry-url"] == config.TL_NPM_REGISTRY_URL_PROD
    assert labels["lightwell.redhat.com/package"] == "bluebird"


def test_build_pipelinerun_validated_stage_target(monkeypatch, tmp_path):
    """VALIDATED_STAGE uses the stage app/component/SA and stage Pulp registry."""
    pipeline_file = tmp_path / "tekton" / "pipelines" / "javascript-validated-build" / "javascript-validated-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    manifest = JavaScriptEcosystem().build_pipelinerun(
        "bluebird==3.7.2",
        argparse.Namespace(target="VALIDATED_STAGE"),
    )

    labels = manifest["metadata"]["labels"]
    assert labels["appstudio.openshift.io/application"] == "javascript-validated-stage"
    assert labels["appstudio.openshift.io/component"] == "javascript-validated-stage-build"
    assert manifest["spec"]["taskRunTemplate"]["serviceAccountName"] == (
        "build-pipeline-javascript-validated-stage-build"
    )
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["tl-npm-registry-url"] == config.TL_NPM_REGISTRY_URL_STAGE
    assert params["output-image"].endswith(
        "/javascript-validated-stage/javascript-validated-stage-build:bluebird-3.7.2"
    )


def test_build_pipelinerun_tl_registry_cli_override(monkeypatch, tmp_path):
    """Explicit --tl-npm-registry-url wins over the target default."""
    pipeline_file = tmp_path / "tekton" / "pipelines" / "javascript-validated-build" / "javascript-validated-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    manifest = JavaScriptEcosystem().build_pipelinerun(
        "bluebird==3.7.2",
        argparse.Namespace(
            target="VALIDATED_STAGE",
            tl_npm_registry_url="https://example.test/javascript",
        ),
    )
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["tl-npm-registry-url"] == "https://example.test/javascript"


def test_build_pipelinerun_scoped_package(monkeypatch, tmp_path):
    """Scoped refs keep packages/@scope/name path; tag/label use a slug."""
    pipeline_file = tmp_path / "tekton" / "pipelines" / "javascript-validated-build" / "javascript-validated-build.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))

    manifest = JavaScriptEcosystem().build_pipelinerun("@babel/core==7.3.2", argparse.Namespace())

    labels = manifest["metadata"]["labels"]
    assert labels["lightwell.redhat.com/package"] == "babel-core"
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["packages"] == ["packages/@babel/core/7.3.2"]
    assert params["output-image"].endswith(":babel-core-7.3.2")


def test_build_pipelinerun_rejects_malformed_ref(tmp_path, monkeypatch):
    """build_pipelinerun rejects refs that are not package==version."""
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    with pytest.raises(TriggerError):
        JavaScriptEcosystem().build_pipelinerun("bluebird-3.7.2", argparse.Namespace())


def test_validated_does_not_skip_release():
    """VALIDATED waits on Release; no SCRATCH target until dedicated wiring exists."""
    assert JavaScriptEcosystem().target_skip_release("VALIDATED") is False
    assert JavaScriptEcosystem().target_skip_release("VALIDATED_STAGE") is False


def test_request_identity_defaults_to_validated_target():
    """request_identity fingerprints VALIDATED when no target is set."""
    identity = JavaScriptEcosystem().request_identity("bluebird==3.7.2", argparse.Namespace())
    assert identity == {
        "namespace": "lightwell-javascript-tenant",
        "application": "javascript-validated",
        "component": "javascript-validated-build",
        "pipeline": "javascript-validated-build",
        "release_plan": "",
        "target": "VALIDATED",
        "artifact": "bluebird==3.7.2",
        "source_repo": config.DEFAULT_SOURCE_GIT_URL,
    }


def test_request_identity_validated_stage_target():
    """request_identity fingerprints VALIDATED_STAGE app/component when selected."""
    identity = JavaScriptEcosystem().request_identity(
        "bluebird==3.7.2",
        argparse.Namespace(target="VALIDATED_STAGE"),
    )
    assert identity["application"] == "javascript-validated-stage"
    assert identity["component"] == "javascript-validated-stage-build"
    assert identity["target"] == "VALIDATED_STAGE"
