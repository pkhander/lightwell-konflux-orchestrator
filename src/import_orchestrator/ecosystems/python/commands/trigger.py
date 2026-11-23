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
from import_orchestrator.ecosystems.python import config
from import_orchestrator.ecosystems.python.commands.build_args import add_build_repo_args


def register(subparsers: argparse._SubParsersAction, ecosystem: Ecosystem) -> None:
    """Register the 'trigger' subcommand with the given subparsers."""
    parser = subparsers.add_parser(
        "trigger",
        help="Trigger a python-build PipelineRun",
        description="Trigger a python-build PipelineRun on the Konflux cluster for a single package==version reference.",  # noqa: E501
    )

    parser.add_argument(
        "ref",
        help="Package reference to build, in the form package==version (e.g. ntplib==0.4.0)",
    )
    parser.add_argument(
        "--target",
        choices=list(config.TARGET_CONFIGS),
        default=os.environ.get("LIGHTWELL_PYTHON_TARGET", config.DEFAULT_TARGET),
        help=f"Build target (default: {config.DEFAULT_TARGET}, or LIGHTWELL_PYTHON_TARGET env var)",
    )
    parser.add_argument(
        "--build-type",
        choices=("Backport", "Novel", "Validated", "Scratch"),
        default=None,
        help=(
            "Build type passed to the Pipeline (Backport, Novel, Validated, or Scratch). "
            "When omitted, defaults to the target's pinned build type (Scratch for the "
            "SCRATCH target), else Validated for VALIDATED/VALIDATED-STAGE targets and "
            "Backport for all others."
        ),
    )

    add_build_repo_args(parser)
    parser.set_defaults(func=run, ecosystem=ecosystem)


def run(args: argparse.Namespace) -> int:
    """Execute the trigger subcommand."""
    return run_trigger(args, args.ref)
