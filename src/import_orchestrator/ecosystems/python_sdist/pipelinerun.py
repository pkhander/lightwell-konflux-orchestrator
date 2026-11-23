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

from typing import Any

from import_orchestrator.ecosystems.base import DEFAULT_PIPELINERUN_ANNOTATIONS
from import_orchestrator.ecosystems.python.identifiers import image_tag, version_label
from import_orchestrator.engine.errors import TriggerError

__all__ = ["TriggerError", "build_pipelinerun_manifest", "parse_ref"]


def parse_ref(ref: str) -> tuple[str, str]:
    """Split a ``package==version`` reference into its parts.

    Raises TriggerError if the reference is not in the expected form.
    """
    package, sep, version = ref.partition("==")
    if not sep or not package or not version:
        raise TriggerError(f"python reference must be in the form package==version: {ref}")
    return package, version


def build_pipelinerun_manifest(
    *,
    package: str,
    version: str,
    pipeline_spec: dict[str, Any],
    namespace: str,
    application: str,
    component: str,
    prefix: str,
    image_repo_base: str,
    service_account: str | None = None,
    source_registries: str = "rhtl",
    pipeline_git_url: str | None = None,
    pipeline_revision: str | None = None,
    source_type: str = "pypi-sdist",
    upstream_url: str | None = None,
    upstream_ref: str | None = None,
) -> dict[str, Any]:
    """Build a python-sdist-ingest or python-git-ingest PipelineRun manifest.

    ``pipeline_git_url`` and ``pipeline_revision`` identify the repository and
    immutable commit that supplied this inline pipeline definition to Chains.

    ``source_type`` selects the param set: the default ``"pypi-sdist"`` builds
    params for the sdist-mirror pipeline; ``"git"`` builds params for the
    git-ingest pipeline instead, requiring ``upstream_url``/``upstream_ref``.
    """
    if bool(pipeline_git_url) != bool(pipeline_revision):
        raise TriggerError("inline pipeline Git URL and revision must be supplied together")

    image = f"{image_repo_base}/{application}/{component}:{image_tag(package, version)}"
    if source_type == "git":
        if not (upstream_url and upstream_ref):
            raise TriggerError("upstream_url and upstream_ref must both be supplied for git source_type")
        params = [
            {"name": "PACKAGE", "value": package},
            {"name": "VERSION", "value": version},
            {"name": "IMAGE", "value": image},
            {"name": "UPSTREAM_URL", "value": upstream_url},
            {"name": "UPSTREAM_REF", "value": upstream_ref},
            {"name": "SOURCE_TYPE", "value": source_type},
            {"name": "ociStorage", "value": f"{image}.src"},
        ]
    else:
        params = [
            {"name": "PACKAGE", "value": package},
            {"name": "VERSION", "value": version},
            {"name": "IMAGE", "value": image},
            {"name": "SOURCE_REGISTRIES", "value": source_registries},
            {"name": "ociStorage", "value": f"{image}.src"},
        ]
    if pipeline_git_url and pipeline_revision:
        params.extend(
            [
                {"name": "git-url", "value": pipeline_git_url},
                {"name": "revision", "value": pipeline_revision},
            ]
        )

    spec: dict[str, Any] = {
        "pipelineSpec": pipeline_spec,
        "params": params,
    }
    if service_account:
        spec["taskRunTemplate"] = {"serviceAccountName": service_account}

    return {
        "apiVersion": "tekton.dev/v1",
        "kind": "PipelineRun",
        "metadata": {
            "generateName": prefix,
            "namespace": namespace,
            "annotations": DEFAULT_PIPELINERUN_ANNOTATIONS,
            "labels": {
                "appstudio.openshift.io/application": application,
                "appstudio.openshift.io/component": component,
                "pipelines.appstudio.openshift.io/type": "build",
                "lightwell.redhat.com/package": package,
                "lightwell.redhat.com/version": version_label(version),
            },
        },
        "spec": spec,
    }
