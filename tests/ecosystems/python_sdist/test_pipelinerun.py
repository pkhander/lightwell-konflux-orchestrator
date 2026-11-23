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

import pytest

from import_orchestrator.ecosystems.python_sdist.pipelinerun import (
    TriggerError,
    build_pipelinerun_manifest,
    parse_ref,
)


class TestParseRef:
    def test_splits_package_and_version(self):
        assert parse_ref("foolib==0.4.0") == ("foolib", "0.4.0")

    def test_rejects_missing_separator(self):
        with pytest.raises(TriggerError):
            parse_ref("foolib-0.4.0")

    def test_rejects_empty_package(self):
        with pytest.raises(TriggerError):
            parse_ref("==0.4.0")

    def test_rejects_empty_version(self):
        with pytest.raises(TriggerError):
            parse_ref("foolib==")


def _manifest(**overrides):
    kwargs = dict(
        package="foolib",
        version="0.4.0",
        pipeline_spec={"tasks": []},
        namespace="lightwell-python-tenant",
        application="python-sdist-mirror",
        component="python-sdist-mirror",
        prefix="python-sdist-ingest-",
        image_repo_base="quay.io/redhat-user-workloads/lightwell-python-tenant",
        service_account="build-pipeline-python-sdist-mirror",
        source_registries="rhtl",
    )
    kwargs.update(overrides)
    return build_pipelinerun_manifest(**kwargs)


class TestBuildManifest:
    def test_kind_and_generate_name(self):
        manifest = _manifest()
        assert manifest["kind"] == "PipelineRun"
        assert manifest["metadata"]["generateName"] == "python-sdist-ingest-"

    def test_namespace(self):
        assert _manifest()["metadata"]["namespace"] == "lightwell-python-tenant"

    def test_labels(self):
        labels = _manifest()["metadata"]["labels"]
        assert labels["appstudio.openshift.io/application"] == "python-sdist-mirror"
        assert labels["appstudio.openshift.io/component"] == "python-sdist-mirror"
        assert labels["pipelines.appstudio.openshift.io/type"] == "build"
        assert labels["lightwell.redhat.com/package"] == "foolib"
        assert labels["lightwell.redhat.com/version"] == "0.4.0"

    def test_service_account(self):
        manifest = _manifest(service_account="test-sa")
        assert manifest["spec"]["taskRunTemplate"]["serviceAccountName"] == "test-sa"

    def test_params(self):
        manifest = _manifest(source_registries="pypi.org")
        params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
        assert params["PACKAGE"] == "foolib"
        assert params["VERSION"] == "0.4.0"
        assert params["SOURCE_REGISTRIES"] == "pypi.org"
        expected_image = (
            "quay.io/redhat-user-workloads/lightwell-python-tenant/python-sdist-mirror/python-sdist-mirror:foolib-0.4.0"
        )
        assert params["IMAGE"] == expected_image
        assert params["ociStorage"] == f"{expected_image}.src"

    def test_pipeline_source_identity_params(self):
        manifest = _manifest(
            pipeline_git_url="https://github.com/konflux-lightwell/lightwell-konflux-orchestrator",
            pipeline_revision="a" * 40,
        )
        params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
        assert params["git-url"] == "https://github.com/konflux-lightwell/lightwell-konflux-orchestrator"
        assert params["revision"] == "a" * 40

    def test_rejects_partial_pipeline_source_identity(self):
        with pytest.raises(TriggerError):
            _manifest(pipeline_git_url="https://github.com/konflux-lightwell/lightwell-konflux-orchestrator")


def test_build_pipelinerun_manifest_for_git_source():
    manifest = build_pipelinerun_manifest(
        package="griffe",
        version="2.3.2",
        pipeline_spec={"tasks": []},
        namespace="lightwell-python-tenant",
        application="python-sdist-mirror",
        component="python-sdist-mirror",
        prefix="python-git-ingest-",
        image_repo_base="quay.io/redhat-user-workloads/lightwell-python-tenant",
        source_type="git",
        upstream_url="https://github.com/mkdocstrings/griffe",
        upstream_ref="2.3.2",
    )
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["UPSTREAM_URL"] == "https://github.com/mkdocstrings/griffe"
    assert params["UPSTREAM_REF"] == "2.3.2"
    assert params["SOURCE_TYPE"] == "git"
    assert "SOURCE_REGISTRIES" not in params


def test_build_pipelinerun_manifest_for_git_source_requires_upstream_url_and_ref():
    with pytest.raises(TriggerError):
        build_pipelinerun_manifest(
            package="griffe",
            version="2.3.2",
            pipeline_spec={"tasks": []},
            namespace="lightwell-python-tenant",
            application="python-sdist-mirror",
            component="python-sdist-mirror",
            prefix="python-git-ingest-",
            image_repo_base="quay.io/redhat-user-workloads/lightwell-python-tenant",
            source_type="git",
            upstream_url="https://github.com/mkdocstrings/griffe",
        )
