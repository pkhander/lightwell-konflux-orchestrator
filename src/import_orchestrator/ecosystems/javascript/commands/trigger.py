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
import os

from import_orchestrator.commands.trigger import run_trigger
from import_orchestrator.ecosystems.base import Ecosystem
from import_orchestrator.ecosystems.javascript import config


def add_build_args(parser: argparse.ArgumentParser) -> None:
    """Attach shared javascript build CLI flags."""
    parser.add_argument(
        "--target",
        choices=list(config.TARGET_CONFIGS),
        default=os.environ.get("LIGHTWELL_JAVASCRIPT_TARGET", config.DEFAULT_TARGET),
        help=f"Build target (default: {config.DEFAULT_TARGET}, or LIGHTWELL_JAVASCRIPT_TARGET)",
    )
    parser.add_argument(
        "--source-git-url",
        default=os.environ.get("LIGHTWELL_JAVASCRIPT_SOURCE_GIT_URL", config.DEFAULT_SOURCE_GIT_URL),
        help="Recipe monorepo URL (default: calunga-javascript build-configs)",
    )
    parser.add_argument(
        "--source-revision",
        default=os.environ.get("LIGHTWELL_JAVASCRIPT_SOURCE_REVISION", config.DEFAULT_SOURCE_REVISION),
        help="Recipe monorepo git revision (default: main)",
    )
    parser.add_argument(
        "--builder-image",
        default=os.environ.get("LIGHTWELL_JAVASCRIPT_BUILDER_IMAGE", config.DEFAULT_BUILDER_IMAGE),
        help="Pinned npm-builder image reference",
    )
    parser.add_argument(
        "--task-build-npm-package-bundle",
        default=os.environ.get(
            "LIGHTWELL_JAVASCRIPT_TASK_BUILD_BUNDLE",
            config.DEFAULT_TASK_BUILD_NPM_PACKAGE_BUNDLE,
        ),
        help="Tekton bundle for build-npm-package-oci-ta",
    )
    parser.add_argument(
        "--task-assess-npm-compliance-bundle",
        default=os.environ.get(
            "LIGHTWELL_JAVASCRIPT_TASK_ASSESS_BUNDLE",
            config.DEFAULT_TASK_ASSESS_NPM_COMPLIANCE_BUNDLE,
        ),
        help="Tekton bundle for assess-npm-compliance-oci-ta",
    )
    parser.add_argument(
        "--tl-npm-registry-url",
        default=os.environ.get("LIGHTWELL_JAVASCRIPT_TL_NPM_REGISTRY_URL") or None,
        help=(
            "TL npm registry URL for assess-npm-compliance "
            "(default: from --target; or LIGHTWELL_JAVASCRIPT_TL_NPM_REGISTRY_URL)"
        ),
    )
    parser.add_argument(
        "--pulp-read-secret-name",
        default=os.environ.get(
            "LIGHTWELL_JAVASCRIPT_PULP_READ_SECRET_NAME",
            config.DEFAULT_PULP_READ_SECRET_NAME,
        ),
        help="K8s Secret name with Pulp read basic-auth for TL registry",
    )


def register(subparsers: argparse._SubParsersAction, ecosystem: Ecosystem) -> None:
    """Register the 'trigger' subcommand with the given subparsers."""
    parser = subparsers.add_parser(
        "trigger",
        help="Trigger a javascript-validated-build PipelineRun",
        description=(
            "Trigger a javascript-validated-build PipelineRun on the Konflux "
            "cluster for a single package==version reference."
        ),
    )
    parser.add_argument(
        "ref",
        help="Package reference to build, in the form package==version (e.g. bluebird==3.7.2)",
    )
    add_build_args(parser)
    parser.set_defaults(func=run, ecosystem=ecosystem)


def run(args: argparse.Namespace) -> int:
    """Execute the trigger subcommand."""
    return run_trigger(args, args.ref)
