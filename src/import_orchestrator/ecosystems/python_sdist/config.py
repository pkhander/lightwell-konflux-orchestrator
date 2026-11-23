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
import re
from pathlib import Path

from import_orchestrator.clients.git import GitClient, GitError
from import_orchestrator.engine.errors import TriggerError

_GIT_SHA1_RE = re.compile(r"[0-9a-f]{40}")

PYTHON_SDIST_DEFAULT_DB_PATH = "./python_sdist_import_state.db"
NAMESPACE = "lightwell-python-tenant"
PIPELINERUN_PREFIX = "python-sdist-ingest-"

# Build targets, keyed by target name. Each supplies the Konflux application and
# component labels applied to the PipelineRun, and whether the target produces a
# Release CR (`skip_release`). Selected via `--target`.
TARGET_CONFIGS: dict[str, dict] = {
    "MIRROR": {
        "app": "python-sdist-mirror",
        "component": "python-sdist-mirror",
        "service_account": "build-pipeline-python-sdist-mirror",
    },
    "MIRROR-STAGE": {
        "app": "python-sdist-mirror-scratch",
        "component": "python-sdist-mirror-scratch",
        "service_account": "build-pipeline-python-sdist-mirror-scratch",
    },
    "SCRATCH": {
        "app": "python-sdist-mirror-scratch",
        "component": "python-sdist-mirror-scratch",
        "service_account": "build-pipeline-python-sdist-mirror-scratch",
        "skip_release": True,
    },
}
DEFAULT_TARGET = "MIRROR"

# Base of the destination image repository. sdist artifacts are pushed to
# "<image_repo_base>/<app>/<component>:<package>-<version>".
IMAGE_REPO_BASE = "quay.io/redhat-user-workloads/lightwell-python-tenant"


def pipeline_source_identity() -> tuple[str, str]:
    """Return the immutable Git source identity of this inline pipeline definition.

    The sdist CLI embeds the pipeline YAML from this repository in each
    PipelineRun. Tekton Chains therefore needs this checkout's origin URL and
    exact commit SHA, rather than a branch name or the upstream package source.

    In the shipped image the wheel has no ``.git`` to query, so both values are
    baked in at build time via ``PIPELINE_GIT_URL`` and
    ``PIPELINE_GIT_REVISION``. When either is unset (e.g. a local
    checkout) they fall back to querying git directly.
    """
    remote_url = os.environ.get("PIPELINE_GIT_URL")
    revision = os.environ.get("PIPELINE_GIT_REVISION")
    if not (remote_url and revision):
        remote_url, revision = _git_source_identity()

    if remote_url.startswith("git@github.com:"):
        remote_url = f"https://github.com/{remote_url.removeprefix('git@github.com:')}"
    if remote_url.endswith(".git"):
        remote_url = remote_url.removesuffix(".git")
    if not _GIT_SHA1_RE.fullmatch(revision):
        raise TriggerError(f"inline sdist pipeline Git revision is not a full SHA-1: {revision!r}")

    return remote_url, revision


def _git_source_identity() -> tuple[str, str]:
    """Query the local checkout for its origin URL and HEAD commit SHA."""
    project_root = Path(__file__).resolve().parents[4]
    git = GitClient(project_root)
    try:
        return git.remote_url("origin"), git.head_revision()
    except GitError as err:
        raise TriggerError(f"unable to determine Git source identity to create the PipelineRun: {err}") from err


def pipeline_definition_path() -> Path:
    """Return the path to the python-sdist-ingest pipeline definition.

    Configurable via TEKTON_PIPELINE_DIR; otherwise falls back to the repo layout.
    """
    pipeline_subpath = Path("pipelines") / "python-sdist-ingest" / "python-sdist-ingest.yaml"
    if pipeline_dir := os.environ.get("TEKTON_PIPELINE_DIR"):
        return Path(pipeline_dir) / pipeline_subpath
    project_root = Path(__file__).resolve().parents[4]
    return project_root / "tekton" / pipeline_subpath


def git_pipeline_definition_path() -> Path:
    """Return the path to the python-git-ingest pipeline definition.

    Configurable via TEKTON_PIPELINE_DIR; otherwise falls back to the repo layout.
    """
    pipeline_subpath = Path("pipelines") / "python-git-ingest" / "python-git-ingest.yaml"
    if pipeline_dir := os.environ.get("TEKTON_PIPELINE_DIR"):
        return Path(pipeline_dir) / pipeline_subpath
    project_root = Path(__file__).resolve().parents[4]
    return project_root / "tekton" / pipeline_subpath
