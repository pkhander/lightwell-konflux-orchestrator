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

from import_orchestrator.commands.orchestrate import add_common_orchestrate_args, run_orchestrate
from import_orchestrator.ecosystems.base import Ecosystem
from import_orchestrator.ecosystems.python import config
from import_orchestrator.ecosystems.python.commands.build_args import add_build_repo_args

_EMPTY_DB_WARNING = "No package references in database. Run 'import-orchestrator python import-file' first."


def register(subparsers: argparse._SubParsersAction, ecosystem: Ecosystem) -> None:
    """Register the 'orchestrate' subcommand with the given subparsers."""
    parser = subparsers.add_parser(
        "orchestrate",
        help="Orchestrate batch python-remediated-build PipelineRuns",
        description="Orchestrate batch python-remediated-build PipelineRuns",
    )

    add_common_orchestrate_args(parser)
    add_build_repo_args(parser)

    parser.add_argument(
        "--target",
        choices=list(config.TARGET_CONFIGS),
        default=os.environ.get("LIGHTWELL_PYTHON_TARGET", config.DEFAULT_TARGET),
        help=f"Build target (default: {config.DEFAULT_TARGET}, or LIGHTWELL_PYTHON_TARGET env var)",
    )

    parser.set_defaults(func=run, ecosystem=ecosystem)


def run(args: argparse.Namespace) -> int:
    """Execute the orchestrate subcommand."""
    return run_orchestrate(args, _EMPTY_DB_WARNING)
