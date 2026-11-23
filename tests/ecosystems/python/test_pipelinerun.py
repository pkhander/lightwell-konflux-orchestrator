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

import os

import pytest

os.environ.setdefault("BUILD_CONFIGS_REPO_URL", "https://example.test/build-configs.git")

from import_orchestrator.ecosystems.python.pipelinerun import (
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
        application="remediated-build",
        component="remediated-build",
        prefix="python-remediated-build-",
        repo_base="https://gitlab.cee.redhat.com/lightwell/lightwell-builds",
        image_repo_base="quay.io/redhat-user-workloads/lightwell-python-tenant",
        git_auth_secret="lightwell-builds-git-auth",
    )
    kwargs.update(overrides)
    return build_pipelinerun_manifest(**kwargs)


class TestAiAttributionParams:
    """AI attribution params that Tekton Chains records in SLSA provenance."""

    def test_manifest_includes_ai_params_from_env(self, monkeypatch):
        """The Python manifest carries the shared AI_ASSISTED and AI_MODELS params."""
        monkeypatch.setenv("AI_ASSISTED", "true")
        monkeypatch.setenv("AI_MODELS", "model-alpha")
        params = {p["name"]: p["value"] for p in _manifest()["spec"]["params"]}
        assert (params["AI_ASSISTED"], params["AI_MODELS"]) == ("true", "model-alpha")


class TestBuildManifest:
    def test_kind_and_generate_name(self):
        manifest = _manifest()
        assert manifest["kind"] == "PipelineRun"
        assert manifest["metadata"]["generateName"] == "python-remediated-build-"

    def test_namespace(self):
        assert _manifest()["metadata"]["namespace"] == "lightwell-python-tenant"

    def test_service_account_omitted_by_default(self):
        # No service account -> no taskRunTemplate, so the cluster default applies.
        assert "taskRunTemplate" not in _manifest()["spec"]

    def test_service_account_included_when_provided(self):
        manifest = _manifest(service_account="build-pipeline-python-remediated-build")
        assert manifest["spec"]["taskRunTemplate"]["serviceAccountName"] == "build-pipeline-python-remediated-build"

    def test_application_and_component_labels(self):
        labels = _manifest()["metadata"]["labels"]
        assert labels["appstudio.openshift.io/application"] == "remediated-build"
        assert labels["appstudio.openshift.io/component"] == "remediated-build"
        assert labels["pipelines.appstudio.openshift.io/type"] == "build"

    def test_lightwell_package_labels(self):
        # Package identity is carried in Lightwell-namespaced labels so builds
        # can be queried/filtered by package and version.
        labels = _manifest()["metadata"]["labels"]
        assert labels["lightwell.redhat.com/package"] == "foolib"
        assert labels["lightwell.redhat.com/version"] == "0.4.0"

    def test_params_are_derived(self):
        params = {p["name"]: p["value"] for p in _manifest()["spec"]["params"]}
        assert params["PACKAGE"] == "foolib"
        assert params["VERSION"] == "0.4.0"
        assert params["BUILD_TYPE"] == "Backport"
        assert params["LIGHTWELL_BUILDS_TAG"] == "foolib/0.4.0"
        assert params["WHEEL_SERVER_URL"] == "https://packages.redhat.com/lightwell/python/validated/simple/"
        assert params["BUILD_CONFIGS_REPO_URL"] == "https://example.test/build-configs.git"
        assert params["BUILD_CONFIGS_REVISION"] == "main"

    def test_build_config_revision_is_configurable(self, monkeypatch):
        monkeypatch.setenv("BUILD_CONFIGS_REVISION", "review-ref")
        params = {p["name"]: p["value"] for p in _manifest()["spec"]["params"]}
        assert params["BUILD_CONFIGS_REVISION"] == "review-ref"

    def test_rejects_invalid_build_config_revision(self, monkeypatch):
        monkeypatch.setenv("BUILD_CONFIGS_REVISION", "--option")
        with pytest.raises(TriggerError, match="BUILD_CONFIGS_REVISION"):
            _manifest()

    @pytest.mark.parametrize("build_type", ["Backport", "Novel", "Validated", "Scratch"])
    def test_wheel_server_url_override(self, build_type):
        params = {
            p["name"]: p["value"]
            for p in _manifest(build_type=build_type, wheel_server_url="https://custom.repo/simple/")["spec"]["params"]
        }
        assert params["WHEEL_SERVER_URL"] == "https://custom.repo/simple/"

    def test_build_type_is_passed_to_pipeline(self):
        params = {p["name"]: p["value"] for p in _manifest(build_type="Novel")["spec"]["params"]}
        assert params["BUILD_TYPE"] == "Novel"

    def test_validated_build_type_uses_default_wheel_server(self):
        params = {p["name"]: p["value"] for p in _manifest(build_type="Validated")["spec"]["params"]}
        assert params["BUILD_TYPE"] == "Validated"
        assert params["WHEEL_SERVER_URL"] == "https://packages.redhat.com/lightwell/python/validated/simple/"

    @pytest.mark.parametrize("build_type", ["Backport", "Novel", "Validated", "Scratch"])
    def test_empty_wheel_server_url_disables_server(self, build_type):
        params = {
            p["name"]: p["value"] for p in _manifest(build_type=build_type, wheel_server_url="")["spec"]["params"]
        }
        assert params["WHEEL_SERVER_URL"] == ""

    def test_builds_tag_defaults_to_validated_version_tag(self):
        # With no builds_tag, the tag falls back to the validated <package>/<version>.
        params = {p["name"]: p["value"] for p in _manifest(builds_tag=None)["spec"]["params"]}
        assert params["LIGHTWELL_BUILDS_TAG"] == "foolib/0.4.0"

    def test_builds_tag_override(self):
        # An explicit builds_tag (the remediation branch balor-fianna pushed)
        # overrides the validated version tag; repo URL is unaffected.
        params = {
            p["name"]: p["value"] for p in _manifest(builds_tag="CVE-2025-1234/0.4.0/pipeline-9")["spec"]["params"]
        }
        assert params["LIGHTWELL_BUILDS_TAG"] == "CVE-2025-1234/0.4.0/pipeline-9"
        assert params["LIGHTWELL_BUILDS_REPO_URL"] == (
            "https://gitlab.cee.redhat.com/lightwell/lightwell-builds/pypi.org-foolib"
        )
        assert params["LIGHTWELL_BUILDS_REPO_URL"] == (
            "https://gitlab.cee.redhat.com/lightwell/lightwell-builds/pypi.org-foolib"
        )
        # Built wheels push to the Konflux component repo (<app>/<component>),
        # which is what the build service account has push rights to; the
        # package/version is encoded in the tag.
        assert (
            params["IMAGE"]
            == "quay.io/redhat-user-workloads/lightwell-python-tenant/remediated-build/remediated-build:foolib-0.4.0"
        )
        assert params["ociStorage"] == (
            "quay.io/redhat-user-workloads/lightwell-python-tenant/remediated-build/remediated-build:foolib-0.4.0.src"
        )

    def test_builds_tag_accepts_commit_sha(self):
        # A commit SHA passes through as the revision unchanged, exactly like a
        # branch or tag -- the clone task's revision param accepts any of them.
        sha = "deadbeefcafe1234567890abcdef1234567890ab"
        params = {p["name"]: p["value"] for p in _manifest(builds_tag=sha)["spec"]["params"]}
        assert params["LIGHTWELL_BUILDS_TAG"] == sha

    def test_pipeline_spec_embedded(self):
        assert _manifest()["spec"]["pipelineSpec"] == {"tasks": []}

    def test_git_auth_workspace_bound_to_secret(self):
        # The clone task authenticates via the git-auth workspace, backed by a secret.
        workspaces = _manifest()["spec"]["workspaces"]
        assert workspaces == [
            {"name": "git-auth", "secret": {"secretName": "lightwell-builds-git-auth"}},
        ]
