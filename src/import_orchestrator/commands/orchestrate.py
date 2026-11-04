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
import sys

from import_orchestrator.clients import KubeClient
from import_orchestrator.constants import (
    CLUSTER_API,
    DEFAULT_MAX_PARALLEL,
    DEFAULT_MAX_RETRIES,
    DEFAULT_POLL_INTERVAL,
    KUBEARCHIVE_API,
)
from import_orchestrator.database import WorkflowDatabase, warn_if_legacy_database
from import_orchestrator.engine import (
    ImportOrchestrator,
    ImportTrigger,
    PipelineMonitor,
    Reconciler,
    ReleaseMonitor,
    RequestView,
)
from import_orchestrator.engine.errors import TriggerError
from import_orchestrator.release_data import ReleaseDataError, ReleaseDataResolver, application_of


def add_common_orchestrate_args(parser: argparse.ArgumentParser) -> None:
    """Add the flags shared by every ecosystem's ``orchestrate`` subcommand.

    Ecosystems add their own selectors (``--artifact-type``, ``--target``, ...)
    on top of these.
    """
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=DEFAULT_MAX_PARALLEL,
        help=f"Maximum parallel PipelineRuns (default: {DEFAULT_MAX_PARALLEL})",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Seconds between status checks (default: {DEFAULT_POLL_INTERVAL})",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Max retry attempts for failed imports (default: {DEFAULT_MAX_RETRIES})",
    )
    parser.add_argument(
        "--print-resources",
        action="store_true",
        help="Preview the release chain (PipelineRun YAML + resolved ReleasePlan/RPA) "
        "for each pending reference and exit, without contacting the cluster.",
    )
    parser.add_argument(
        "--release-data-repo",
        default=None,
        help="Path to a konflux-release-data checkout for --print-resources "
        "(default: reference/konflux-release-data, cloned on demand).",
    )
    parser.add_argument(
        "--release-plan",
        default=None,
        metavar="NAME",
        help=(
            "Create every Release against this specific ReleasePlan instead of "
            "resolving one based on the Konflux application."
        ),
    )


def _is_database_empty(db: WorkflowDatabase) -> bool:
    """Check whether the database has any requests at all."""
    return db.count_requests() == 0


def run_orchestrate(args: argparse.Namespace, empty_db_warning: str) -> int:
    """Run the batch import orchestration loop for the given ecosystem.

    Generic across ecosystems: the ecosystem supplies its own namespace,
    PipelineRun prefix and manifest builder. ``empty_db_warning`` is the
    ecosystem-specific hint shown when the database has no references yet.
    """
    with WorkflowDatabase(args.db) as db:
        warn_if_legacy_database(db)

        if _is_database_empty(db):
            print(f"WARNING: {empty_db_warning}", file=sys.stderr)

        eco = args.ecosystem
        target = getattr(args, "target", None)
        skip_release = target is not None and getattr(eco, "target_skip_release", lambda t: False)(target)
        view = RequestView(db, skip_release)

        if getattr(args, "print_resources", False):
            return _run_print_resources(args, view)

        kube = KubeClient(eco.namespace, CLUSTER_API, KUBEARCHIVE_API)

        trigger = ImportTrigger(
            view=view,
            kube=kube,
            build_pipelinerun=lambda ref: eco.build_pipelinerun(ref, args),
            max_parallel=args.max_parallel,
            max_retries=args.max_retries,
        )
        pipeline_monitor = PipelineMonitor(view=view, kube=kube)
        release_monitor = ReleaseMonitor(
            view=view,
            kube=kube,
            max_parallel=args.max_parallel,
            prefix=eco.pipelinerun_prefix,
            release_plan=getattr(args, "release_plan", None),
        )
        reconciler = Reconciler(view=view, kube=kube, release_plan=getattr(args, "release_plan", None))

        orchestrator = ImportOrchestrator(
            view=view,
            trigger=trigger,
            pipeline_monitor=pipeline_monitor,
            release_monitor=release_monitor,
            poll_interval=args.poll_interval,
            max_retries=args.max_retries,
            reconciler=reconciler,
        )

        return orchestrator.run_until_complete()


def _run_print_resources(args: argparse.Namespace, view: RequestView) -> int:
    """Preview the release chain for every pending reference, without the cluster.

    For each pending request this builds the PipelineRun manifest the trigger would
    submit, then resolves its ReleasePlan and ReleasePlanAdmission from a local
    konflux-release-data checkout. Nothing is created and no KubeClient is opened.
    """
    eco = args.ecosystem
    pending = view.pending()
    if not pending:
        print("No pending references to preview.", file=sys.stderr)
        return 0

    try:
        resolver = ReleaseDataResolver.ensure(args.release_data_repo)
    except ReleaseDataError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    for request in pending:
        print(f"\n=== {request.artifact} ===")
        try:
            manifest = eco.build_pipelinerun(request.artifact, args)
        except TriggerError as exc:
            print(f"  ERROR building PipelineRun: {exc}", file=sys.stderr)
            continue

        app = application_of(manifest)
        namespace = (manifest.get("metadata", {}) or {}).get("namespace", eco.namespace)
        print(f"Application:          {app or '(no application label)'}")

        if app:
            _print_release_chain(resolver, app, namespace)

        print("Snapshot / Release:  created at build time")

    sha = resolver.head_revision()
    suffix = f" @ {sha[:12]}" if sha else ""
    print(f"\nResolved from konflux-release-data{suffix} (desired state; may lag the cluster).", file=sys.stderr)
    return 0


def _print_release_chain(resolver: ReleaseDataResolver, app: str, namespace: str) -> None:
    """Print the resolved ReleasePlan and RPA for an application, or why not."""
    try:
        rp = resolver.find_release_plan(app, namespace)
    except ReleaseDataError as exc:
        print(f"ReleasePlan:          UNRESOLVED ({exc})")
        return

    print(f"ReleasePlan:          {rp.name}")
    try:
        rpa = resolver.find_rpa(rp)
    except ReleaseDataError as exc:
        print(f"ReleasePlanAdmission: UNRESOLVED ({exc})")
        return

    print(f"ReleasePlanAdmission: {rpa.name}")
    if rpa.policy:
        print(f"  EC policy:          {rpa.policy}")
    if rpa.pipeline:
        print(f"  Release pipeline:   {rpa.pipeline}")
    if rpa.service_account:
        print(f"  Service account:    {rpa.service_account}")
    if rpa.pulp:
        pulp = rpa.pulp
        target = "/".join(str(pulp.get(k)) for k in ("domain", "pulpDomain", "repository") if pulp.get(k))
        if target:
            print(f"  Pulp target:        {target}")
    if rpa.intention:
        print(f"  Intention:          {rpa.intention}")
