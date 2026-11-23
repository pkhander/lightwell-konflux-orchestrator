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

from import_orchestrator.ecosystems.javascript.pipelinerun import (
    TriggerError,
    build_pipelinerun_manifest,
    parse_ref,
    sanitize_package_for_resource,
)


class TestParseRef:
    def test_splits_package_and_version(self):
        """parse_ref splits a valid package==version reference."""
        assert parse_ref("bluebird==3.7.2") == ("bluebird", "3.7.2")

    def test_splits_scoped_package(self):
        """parse_ref keeps the scope in the package name."""
        assert parse_ref("@babel/core==7.3.2") == ("@babel/core", "7.3.2")

    def test_rejects_missing_separator(self):
        """parse_ref rejects refs without ==."""
        with pytest.raises(TriggerError):
            parse_ref("bluebird-3.7.2")

    def test_rejects_empty_package(self):
        """parse_ref rejects an empty package name."""
        with pytest.raises(TriggerError):
            parse_ref("==3.7.2")

    def test_rejects_empty_version(self):
        """parse_ref rejects an empty version."""
        with pytest.raises(TriggerError):
            parse_ref("bluebird==")


class TestSanitizePackageForResource:
    def test_unscoped_unchanged(self):
        """Unscoped npm names need no encoding for tags/labels."""
        assert sanitize_package_for_resource("bluebird") == "bluebird"

    def test_scoped_strips_at_and_slashes(self):
        """Scoped names become OCI/label-safe slugs."""
        assert sanitize_package_for_resource("@babel/core") == "babel-core"
        assert sanitize_package_for_resource("@types/node") == "types-node"


def _manifest(**overrides):
    kwargs = dict(
        package="bluebird",
        version="3.7.2",
        pipeline_spec={"tasks": []},
        namespace="lightwell-javascript-tenant",
        application="javascript-validated",
        component="javascript-validated-build",
        prefix="javascript-validated-build-",
        image_repo_base="quay.io/redhat-user-workloads/lightwell-javascript-tenant",
        source_git_url="https://gitlab.cee.redhat.com/lightwell/lightwell-builds/build-configs/calunga-javascript",
        source_revision="main",
        builder_image="quay.io/example/npm-builder@sha256:abc",
        task_build_npm_package_bundle="quay.io/example/task-build@sha256:def",
        task_assess_npm_compliance_bundle="quay.io/example/task-assess@sha256:ghi",
        service_account="build-pipeline-javascript-validated-build",
    )
    kwargs.update(overrides)
    return build_pipelinerun_manifest(**kwargs)


class TestBuildManifest:
    def test_kind_and_generate_name(self):
        """Manifest is a PipelineRun with the javascript generateName prefix."""
        manifest = _manifest()
        assert manifest["kind"] == "PipelineRun"
        assert manifest["metadata"]["generateName"] == "javascript-validated-build-"

    def test_namespace(self):
        """Manifest targets the lightwell-javascript tenant."""
        assert _manifest()["metadata"]["namespace"] == "lightwell-javascript-tenant"

    def test_labels(self):
        """Manifest carries application, component, and package labels."""
        labels = _manifest()["metadata"]["labels"]
        assert labels["appstudio.openshift.io/application"] == "javascript-validated"
        assert labels["appstudio.openshift.io/component"] == "javascript-validated-build"
        assert labels["pipelines.appstudio.openshift.io/type"] == "build"
        assert labels["lightwell.redhat.com/package"] == "bluebird"
        assert labels["lightwell.redhat.com/version"] == "3.7.2"

    def test_service_account(self):
        """Manifest sets the build service account."""
        manifest = _manifest(service_account="test-sa")
        assert manifest["spec"]["taskRunTemplate"]["serviceAccountName"] == "test-sa"

    def test_params(self):
        """Manifest params target a single packages/<name>/<version> path."""
        params = {p["name"]: p["value"] for p in _manifest()["spec"]["params"]}
        assert params["git-url"] == (
            "https://gitlab.cee.redhat.com/lightwell/lightwell-builds/build-configs/calunga-javascript"
        )
        assert params["revision"] == "main"
        assert params["packages"] == ["packages/bluebird/3.7.2"]
        expected_image = (
            "quay.io/redhat-user-workloads/lightwell-javascript-tenant/"
            "javascript-validated/javascript-validated-build:bluebird-3.7.2"
        )
        assert params["output-image"] == expected_image
        assert params["builder-image"].startswith("quay.io/example/npm-builder")
        assert params["tl-npm-registry-url"] == (
            "https://packages.redhat.com/api/pulp-content/lightwell/javascript/validated"
        )
        assert params["pulp-read-secret-name"] == "lightwell-pulp-read-secret"

    def test_git_auth_workspace_bound_to_secret(self):
        """The clone task authenticates via the git-auth workspace, backed by a secret."""
        workspaces = _manifest()["spec"]["workspaces"]
        assert workspaces == [
            {"name": "git-auth", "secret": {"secretName": "lightwell-builds-git-auth"}},
        ]

    def test_git_auth_secret_override(self):
        """Manifest accepts an override for the git-auth secret name."""
        workspaces = _manifest(git_auth_secret="custom-git-auth")["spec"]["workspaces"]
        assert workspaces == [
            {"name": "git-auth", "secret": {"secretName": "custom-git-auth"}},
        ]

    def test_tl_registry_and_read_secret_overrides(self):
        """Manifest accepts overrides for TL registry URL and read secret name."""
        params = {
            p["name"]: p["value"]
            for p in _manifest(
                tl_npm_registry_url="https://example.test/javascript",
                pulp_read_secret_name="custom-pulp-read",
            )["spec"]["params"]
        }
        assert params["tl-npm-registry-url"] == "https://example.test/javascript"
        assert params["pulp-read-secret-name"] == "custom-pulp-read"

    def test_scoped_package_sanitizes_tag_and_label(self):
        """Scoped npm names keep the recipe path but sanitize tag and label."""
        manifest = _manifest(package="@babel/core", version="7.3.2")
        labels = manifest["metadata"]["labels"]
        assert labels["lightwell.redhat.com/package"] == "babel-core"
        assert labels["lightwell.redhat.com/version"] == "7.3.2"
        params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
        assert params["packages"] == ["packages/@babel/core/7.3.2"]
        expected_image = (
            "quay.io/redhat-user-workloads/lightwell-javascript-tenant/"
            "javascript-validated/javascript-validated-build:babel-core-7.3.2"
        )
        assert params["output-image"] == expected_image
