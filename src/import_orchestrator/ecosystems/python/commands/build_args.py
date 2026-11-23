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

from import_orchestrator.ecosystems.python import config


def add_build_repo_args(parser: argparse.ArgumentParser) -> None:
    """Add source repository, Git revision, wheel server, and builder image options."""
    parser.add_argument(
        "--wheel-server-url",
        default=None,
        metavar="URL",
        help=(
            "Wheel server URL for all build types, including Validated "
            f"(default: {config.WHEEL_SERVER_URL}, or WHEEL_SERVER_URL env var). "
            "Pass an empty URL to disable the wheel server."
        ),
    )
    parser.add_argument(
        "--python-builder-image",
        default=None,
        metavar="IMAGE",
        help="Override PYTHON_BUILDER_IMAGE for this invocation; omitted uses the pipeline parameter default.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--lightwell-builds-repo-url",
        default=None,
        metavar="URL",
        help="Exact source Git URL passed as LIGHTWELL_BUILDS_REPO_URL for every package in this invocation.",
    )
    group.add_argument(
        "--lightwell-builds-repo-base",
        default=config.LIGHTWELL_BUILDS_REPO_BASE,
        metavar="URL",
        help=(
            "Source Git repository base; appends /pypi.org-<package> for each package "
            f"(default: {config.LIGHTWELL_BUILDS_REPO_BASE})."
        ),
    )
    parser.add_argument(
        "--builds-tag",
        "--builds-ref",
        dest="builds_tag",
        default=None,
        help=(
            "Git revision -- branch, tag, or commit SHA -- in the lightwell-builds "
            "repo to build from (--builds-ref is a synonym). When omitted, defaults "
            "to the '<package>/<version>' tag derived from each package reference -- by "
            "convention the tag the validated build publishes. The revision's "
            "existence is not checked here; a missing one fails later when the "
            "pipeline clones it."
        ),
    )
