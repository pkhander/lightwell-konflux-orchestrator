"""Build-config repository source supplied by orchestrator runtime configuration."""

from __future__ import annotations

import os
from urllib.parse import urlsplit

from import_orchestrator.engine.errors import TriggerError

DEFAULT_BUILD_CONFIGS_REVISION = "main"


def get_build_config_revision() -> str:
    """Return a valid build-config Git revision supplied by deployment config."""
    revision = os.environ.get("BUILD_CONFIGS_REVISION", DEFAULT_BUILD_CONFIGS_REVISION).strip()
    if not revision or revision.startswith("-"):
        raise TriggerError("BUILD_CONFIGS_REVISION must be a non-empty Git revision")
    return revision


def get_build_config_source() -> str:
    """Read and validate the configured build-config repository URL."""
    repo_url = os.environ.get("BUILD_CONFIGS_REPO_URL", "").strip()
    if not repo_url:
        raise TriggerError("BUILD_CONFIGS_REPO_URL must be configured in the orchestrator environment")
    parsed = urlsplit(repo_url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise TriggerError("BUILD_CONFIGS_REPO_URL must be an HTTPS URL without embedded credentials")
    return repo_url


def get_build_config_local_path() -> str | None:
    """Return a configured local path to an already-checked-out build-config repo, if any.

    The orchestrator commonly runs from *within* a checkout of the build-config
    repository itself (e.g. calunga-python's own CI triggers it directly), in which
    case cloning BUILD_CONFIGS_REPO_URL again is redundant. When set, callers should
    read the build-config repo directly from this path instead of cloning. Returns
    None when unset, in which case callers fall back to BUILD_CONFIGS_REPO_URL/
    BUILD_CONFIGS_REVISION.
    """
    local_path = os.environ.get("BUILD_CONFIGS_LOCAL_PATH", "").strip()
    return local_path or None
