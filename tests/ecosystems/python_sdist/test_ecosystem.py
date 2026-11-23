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
from import_orchestrator.ecosystems.python_sdist.ecosystem import PythonSdistEcosystem
from import_orchestrator.engine.errors import TriggerError


def test_python_sdist_ecosystem_satisfies_protocol():
    assert isinstance(PythonSdistEcosystem(), Ecosystem)


def test_python_sdist_attributes():
    eco = PythonSdistEcosystem()
    assert eco.name == "python-sdist"
    assert eco.namespace == "lightwell-python-tenant"
    assert eco.pipelinerun_prefix == "python-sdist-ingest-"


def test_build_pipelinerun_from_ref(monkeypatch, tmp_path):
    pipeline_file = tmp_path / "tekton" / "pipelines" / "python-sdist-ingest" / "python-sdist-ingest.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))
    monkeypatch.setattr(
        "import_orchestrator.ecosystems.python_sdist.ecosystem.resolve_canonical_source",
        lambda package, version: {"type": "pypi-sdist"},
    )

    manifest = PythonSdistEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace())

    assert manifest["metadata"]["namespace"] == "lightwell-python-tenant"
    assert manifest["metadata"]["generateName"] == "python-sdist-ingest-"
    labels = manifest["metadata"]["labels"]
    assert labels["appstudio.openshift.io/application"] == "python-sdist-mirror"
    assert labels["appstudio.openshift.io/component"] == "python-sdist-mirror"
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["PACKAGE"] == "foolib"
    assert params["VERSION"] == "0.4.0"
    assert params["SOURCE_REGISTRIES"] == "rhtl"


def test_build_pipelinerun_rejects_malformed_ref(tmp_path, monkeypatch):
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    with pytest.raises(TriggerError):
        PythonSdistEcosystem().build_pipelinerun("foolib-0.4.0", argparse.Namespace())


def test_build_pipelinerun_scratch_target_uses_scratch_app(monkeypatch, tmp_path):
    pipeline_file = tmp_path / "tekton" / "pipelines" / "python-sdist-ingest" / "python-sdist-ingest.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))
    monkeypatch.setattr(
        "import_orchestrator.ecosystems.python_sdist.ecosystem.resolve_canonical_source",
        lambda package, version: {"type": "pypi-sdist"},
    )

    args = argparse.Namespace(target="SCRATCH")
    manifest = PythonSdistEcosystem().build_pipelinerun("foolib==0.4.0", args)

    labels = manifest["metadata"]["labels"]
    assert labels["appstudio.openshift.io/application"] == "python-sdist-mirror-scratch"
    assert labels["appstudio.openshift.io/component"] == "python-sdist-mirror-scratch"


def test_build_pipelinerun_default_target_uses_mirror_app(monkeypatch, tmp_path):
    pipeline_file = tmp_path / "tekton" / "pipelines" / "python-sdist-ingest" / "python-sdist-ingest.yaml"
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))
    monkeypatch.setattr(
        "import_orchestrator.ecosystems.python_sdist.ecosystem.resolve_canonical_source",
        lambda package, version: {"type": "pypi-sdist"},
    )

    manifest = PythonSdistEcosystem().build_pipelinerun("foolib==0.4.0", argparse.Namespace())

    labels = manifest["metadata"]["labels"]
    assert labels["appstudio.openshift.io/application"] == "python-sdist-mirror"


def test_build_pipelinerun_routes_git_source_to_git_ingest_pipeline(monkeypatch, tmp_path):
    git_pipeline_file = tmp_path / "tekton" / "pipelines" / "python-git-ingest" / "python-git-ingest.yaml"
    git_pipeline_file.parent.mkdir(parents=True)
    git_pipeline_file.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path / "tekton"))
    monkeypatch.setattr(
        "import_orchestrator.ecosystems.python_sdist.ecosystem.resolve_canonical_source",
        lambda package, version: {
            "type": "git",
            "upstream_url": "https://github.com/mkdocstrings/griffe",
            "ref": "2.3.2",
        },
    )

    manifest = PythonSdistEcosystem().build_pipelinerun("griffe==2.3.2", argparse.Namespace())

    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["UPSTREAM_URL"] == "https://github.com/mkdocstrings/griffe"
    assert params["UPSTREAM_REF"] == "2.3.2"
    assert manifest["metadata"]["generateName"] == "python-sdist-ingest-"  # prefix unchanged — see Step 3 note


def test_request_identity_defaults_to_mirror_target():
    from import_orchestrator.ecosystems.python_sdist.ecosystem import PythonSdistEcosystem

    identity = PythonSdistEcosystem().request_identity("foolib==0.4.0", argparse.Namespace())
    assert identity == {
        "namespace": "lightwell-python-tenant",
        "application": "python-sdist-mirror",
        "component": "python-sdist-mirror",
        "pipeline": "python-sdist-ingest",
        "release_plan": "",
        "target": "MIRROR",
        "artifact": "foolib==0.4.0",
    }
