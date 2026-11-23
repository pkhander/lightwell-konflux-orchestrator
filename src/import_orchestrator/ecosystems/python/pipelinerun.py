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

from import_orchestrator.ecosystems.base import DEFAULT_PIPELINERUN_ANNOTATIONS, ai_attribution_params
from import_orchestrator.ecosystems.python import config
from import_orchestrator.ecosystems.python.build_config import get_build_config_revision, get_build_config_source
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


def resolve_repo_url(package: str, repo_base: str, repo_url: str | None = None) -> str:
    """Resolve the source repository used by both the manifest and request identity."""
    return repo_url or f"{repo_base.rstrip('/')}/pypi.org-{package}"


def build_pipelinerun_manifest(
    *,
    package: str,
    version: str,
    pipeline_spec: dict[str, Any],
    namespace: str,
    application: str,
    component: str,
    prefix: str,
    repo_base: str,
    image_repo_base: str,
    git_auth_secret: str,
    service_account: str | None = None,
    builds_tag: str | None = None,
    build_type: str = "Backport",
    wheel_server_url: str | None = None,
    repo_url: str | None = None,
    python_builder_image: str | None = None,
) -> dict[str, Any]:
    """Build a python-build PipelineRun manifest for one package/version.

    ``repo_url`` overrides the exact clone URL. Otherwise the URL is derived
    from ``repo_base`` and the package name.

    ``git_auth_secret`` names the secret bound to the ``git-auth`` workspace,
    which the clone task uses to authenticate against the lightwell-builds repo.

    When ``service_account`` is None, no ``taskRunTemplate`` is emitted and the
    cluster's default service account applies.

    ``builds_tag`` is the git revision -- branch, tag, or commit SHA -- cloned
    from the lightwell-builds repo (it is passed straight through as the clone
    task's ``revision``, which accepts any of the three). When None, it defaults
    to the validated ``<package>/<version>`` tag; a caller passes the remediation
    branch or a pinned commit to build patched source instead.

    ``build_type`` controls the pipeline build mode: ``Backport`` or ``Novel``
    for remediated builds (commit-distance version suffix applied), or
    ``Validated`` for an upstream rebuild (no version suffix).

    ``wheel_server_url`` overrides the configured wheel server for every build
    type. None uses the configured default; an empty URL disables the server.

    ``python_builder_image`` overrides PYTHON_BUILDER_IMAGE only when supplied.
    When None, omit the PipelineRun parameter so the pipeline default applies.

    The AI_ASSISTED and AI_MODELS params come from the AI_ASSISTED/AI_MODELS env
    vars, and Tekton Chains records them in provenance.
    """
    # Push to the Konflux component repository (<tenant>/<application>/<component>),
    # which is the only repo the build service account can push to. The package and
    # version are encoded in the tag rather than a per-package repo.
    image = f"{image_repo_base}/{application}/{component}:{image_tag(package, version)}"
    tag = builds_tag or f"{package}/{version}"
    effective_wheel_server = config.WHEEL_SERVER_URL if wheel_server_url is None else wheel_server_url
    spec: dict[str, Any] = {
        "pipelineSpec": pipeline_spec,
        "params": [
            {"name": "PACKAGE", "value": package},
            {"name": "VERSION", "value": version},
            {"name": "BUILD_TYPE", "value": build_type},
            {"name": "LIGHTWELL_BUILDS_REPO_URL", "value": resolve_repo_url(package, repo_base, repo_url)},
            {"name": "LIGHTWELL_BUILDS_TAG", "value": tag},
            {"name": "IMAGE", "value": image},
            {"name": "ociStorage", "value": f"{image}.src"},
            {"name": "WHEEL_SERVER_URL", "value": effective_wheel_server},
            {"name": "BUILD_CONFIGS_REPO_URL", "value": get_build_config_source()},
            {"name": "BUILD_CONFIGS_REVISION", "value": get_build_config_revision()},
            *ai_attribution_params(),
        ],
        "workspaces": [
            {"name": "git-auth", "secret": {"secretName": git_auth_secret}},
        ],
    }
    if python_builder_image is not None:
        spec["params"].append({"name": "PYTHON_BUILDER_IMAGE", "value": python_builder_image})
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
                # Package identity, so builds can be queried/filtered by package.
                "lightwell.redhat.com/package": package,
                "lightwell.redhat.com/version": version_label(version),
            },
        },
        "spec": spec,
    }
