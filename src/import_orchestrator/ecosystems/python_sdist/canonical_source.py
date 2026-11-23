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

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from import_orchestrator.ecosystems.python.build_config import (
    get_build_config_local_path,
    get_build_config_revision,
    get_build_config_source,
)
from import_orchestrator.engine.errors import TriggerError

# Trust note: the build-config repository and revision cloned below
# (BUILD_CONFIGS_REPO_URL/BUILD_CONFIGS_REVISION) are operator-configured, not
# user/caller-supplied, so the ref executed here is trusted the same way any other
# deployment-pinned dependency is. Even so, the cloned `get-canonical-source.py` is run
# with a minimal environment (PATH/HOME/LANG only, see `_minimal_subprocess_env`) as
# defense-in-depth against it incidentally observing the operator's cluster
# credentials (KONFLUX_TOKEN/KUBECONFIG/etc.) -- this is a precaution, not a sandbox.

# Confirmed against calunga-python's scripts/get-canonical-source.py: its
# DEFAULT_PACKAGES_DIR resolves to a top-level "packages/" directory at the repo
# root, i.e. `<clone_dir>/packages` -- matching this constant.
_PACKAGES_SUBDIR = "packages"

# Confirmed against calunga-python: the script lives at this fixed path.
_SCRIPT_RELATIVE_PATH = Path("scripts") / "get-canonical-source.py"

# Clamp how long we'll wait on network/subprocess operations against the
# (operator-configured, but still external) build-config repository, so a hung
# `git clone` or `get-canonical-source.py` invocation -- e.g. an unconfigured auth
# prompt blocking on /dev/tty, or a stalled network -- can't wedge the trigger loop
# forever.
_CLONE_TIMEOUT_SECONDS = 60
_SCRIPT_TIMEOUT_SECONDS = 30


def _no_prompt_env() -> dict[str, str]:
    """Full process environment with GIT_TERMINAL_PROMPT disabled.

    Prevents `git` from blocking indefinitely on an interactive credential
    prompt when the configured build-config repo requires auth that isn't
    available non-interactively.
    """
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def _minimal_subprocess_env() -> dict[str, str]:
    """A minimal environment for running untrusted-ish cloned script code.

    Deliberately excludes the orchestrator's full environment (cluster
    credentials such as KONFLUX_TOKEN/KUBECONFIG, etc.) as defense-in-depth --
    see the module-level trust note above. Also disables git prompts, since the
    script itself may shell out to git.
    """
    env = {"GIT_TERMINAL_PROMPT": "0"}
    for key in ("PATH", "HOME", "LANG"):
        value = os.environ.get(key)
        if value is not None:
            env[key] = value
    return env


# The only two vocabulary values canonical_source.type may take. Anything else -- a
# missing/None type, or an unrecognized string like "wheel" -- is treated as a
# malformed/drifted calunga-python result and fails closed via TriggerError, rather
# than silently passing an invented value through to callers (Task 9's ecosystem.py
# branches on type == "git" and would otherwise silently route anything else, e.g.
# "wheel", down the sdist path as if it were ordinary pypi-sdist).
_VALID_SOURCE_TYPES = ("git", "pypi-sdist")


def resolve_canonical_source(package: str, version: str) -> dict:
    """Resolve calunga-python's canonical-source verdict for a package/version.

    When ``BUILD_CONFIGS_LOCAL_PATH`` is configured (the common case: the
    orchestrator is already running from within a checkout of the build-config
    repository itself, e.g. calunga-python's own CI), reads directly from that
    path. Otherwise shallow-clones the configured build-config repository
    (``BUILD_CONFIGS_REPO_URL``/``BUILD_CONFIGS_REVISION``). Either way, runs the
    repo's ``get-canonical-source.py`` script to determine whether this
    package/version should be ingested from PyPI sdist or from an upstream Git
    checkout.

    Returns:
        ``{"type": "pypi-sdist"}`` or ``{"type": "git", "upstream_url": str, "ref": str}``.
        Never includes ``internal_url`` or any other field calunga-python's script
        may emit, even if present in its output.

    Raises:
        TriggerError: If ``BUILD_CONFIGS_LOCAL_PATH`` is set but doesn't exist, or
            (when it's unset) ``BUILD_CONFIGS_REPO_URL``/``BUILD_CONFIGS_REVISION``
            are misconfigured or the clone fails; or if the script invocation
            fails, its output is not valid JSON, it reports a ``type`` other than
            ``"git"``/``"pypi-sdist"`` (including a missing/``None`` type), or it
            reports ``type: "git"`` without both ``upstream_url`` and ``ref``.
            Fails closed rather than silently falling back to sdist or passing an
            unrecognized type through.
    """
    local_path = get_build_config_local_path()
    if local_path is not None:
        repo_dir = Path(local_path)
        if not repo_dir.is_dir():
            raise TriggerError(f"BUILD_CONFIGS_LOCAL_PATH does not exist or is not a directory: {local_path}")
        result = _run_get_canonical_source(repo_dir, package, version)
    else:
        repo_url = get_build_config_source()
        revision = get_build_config_revision()

        clone_dir = _clone_build_configs(repo_url, revision)
        try:
            result = _run_get_canonical_source(clone_dir, package, version)
        finally:
            shutil.rmtree(clone_dir, ignore_errors=True)

    source_type = result.get("type")
    if source_type not in _VALID_SOURCE_TYPES:
        raise TriggerError(
            f"calunga-python build-config for {package}=={version} reported an "
            f"unrecognized canonical_source type {source_type!r} (expected one of "
            f"{_VALID_SOURCE_TYPES}): {result!r}"
        )

    if source_type == "git":
        upstream_url = result.get("upstream_url")
        ref = result.get("ref")
        if not upstream_url or not ref:
            raise TriggerError(
                f"calunga-python build-config for {package}=={version} declares "
                f"type 'git' but is missing upstream_url and/or ref: {result!r}"
            )
        return {"type": "git", "upstream_url": upstream_url, "ref": ref}

    return {"type": source_type}


def _clone_build_configs(repo_url: str, revision: str) -> Path:
    """Shallow-clone the build-config repository at ``repo_url``@``revision``.

    Returns the path to a fresh temporary directory containing the clone. Callers
    are responsible for removing it once done.

    Note: `import_orchestrator.clients.git.clone()`/`GitClient` don't support
    cloning a specific ref, only the remote's default branch, so this clones
    directly via `git clone --branch <revision>` instead of reusing them.

    Caveat: `--branch` resolves branch and tag names reliably, but will not
    fetch an arbitrary bare commit SHA that isn't itself the tip of some ref,
    even though `get_build_config_revision()` is documented elsewhere as
    accepting "a valid Git revision" (which could be a SHA). In practice
    `BUILD_CONFIGS_REVISION` defaults to "main" and is expected to stay
    branch/tag-shaped; revisit this if it's ever pinned to a bare SHA.

    Raises:
        TriggerError: If git is unavailable, the clone fails (e.g. unknown
            revision, unreachable repo), or it doesn't complete within
            `_CLONE_TIMEOUT_SECONDS`.
    """
    dest = Path(tempfile.mkdtemp(prefix="calunga-python-"))
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", revision, repo_url, str(dest)],
            check=True,
            capture_output=True,
            text=True,
            env=_no_prompt_env(),
            timeout=_CLONE_TIMEOUT_SECONDS,
        )
    except OSError as exc:
        shutil.rmtree(dest, ignore_errors=True)
        raise TriggerError(f"unable to run git clone for build-config repo {repo_url}: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        shutil.rmtree(dest, ignore_errors=True)
        raise TriggerError(
            f"timed out cloning build-config repo {repo_url}@{revision} after {_CLONE_TIMEOUT_SECONDS}s"
        ) from exc
    except subprocess.CalledProcessError as exc:
        shutil.rmtree(dest, ignore_errors=True)
        raise TriggerError(f"failed to clone build-config repo {repo_url}@{revision}: {exc.stderr.strip()}") from exc
    return dest


def _run_get_canonical_source(clone_dir: Path, package: str, version: str) -> dict:
    """Run calunga-python's get-canonical-source.py and parse its JSON output.

    Raises:
        TriggerError: If the script is unavailable, exits non-zero, its stdout
            is not valid JSON, or it doesn't complete within
            `_SCRIPT_TIMEOUT_SECONDS`.
    """
    script = clone_dir / _SCRIPT_RELATIVE_PATH
    packages_dir = clone_dir / _PACKAGES_SUBDIR
    try:
        result = subprocess.run(
            [sys.executable, str(script), package, version, "--packages-dir", str(packages_dir)],
            check=True,
            capture_output=True,
            text=True,
            env=_minimal_subprocess_env(),
            timeout=_SCRIPT_TIMEOUT_SECONDS,
        )
    except OSError as exc:
        raise TriggerError(f"unable to run calunga-python's get-canonical-source.py: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise TriggerError(
            f"timed out running calunga-python's get-canonical-source.py for "
            f"{package}=={version} after {_SCRIPT_TIMEOUT_SECONDS}s"
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise TriggerError(
            f"calunga-python get-canonical-source.py failed for {package}=={version}: {exc.stderr.strip()}"
        ) from exc

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TriggerError(
            f"calunga-python get-canonical-source.py produced invalid JSON for {package}=={version}: {exc}"
        ) from exc
