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
import json
import shutil
import sys
import tempfile
from pathlib import Path

from import_orchestrator.clients import KubeClient
from import_orchestrator.constants import CLUSTER_API, KUBEARCHIVE_API
from import_orchestrator.database import WorkflowDatabase, warn_if_legacy_database
from import_orchestrator.engine import (
    ImportOrchestrator,
    ImportTrigger,
    Ingest,
    PipelineMonitor,
    Reconciler,
    ReleaseMonitor,
    RequestView,
)
from import_orchestrator.models import Phase, PhaseStatus, Request, RequestState, derive_state


def _resolve_db(args: argparse.Namespace) -> tuple[Path, str | None]:
    """Resolve the state database path for a single-ref orchestration run.

    When the user supplied ``--db`` explicitly, that database is used as-is and
    persists across the run (enabling resume/inspection). Otherwise an ephemeral
    database is created in a fresh temporary directory, isolated from the shared
    per-ecosystem database so the monitoring loop only ever sees the one ref.

    Returns:
        A ``(db_path, cleanup_dir)`` tuple. ``cleanup_dir`` is the temporary
        directory to remove after the run, or ``None`` for a persistent database.
    """
    if getattr(args, "db_explicit", False):
        return args.db, None

    tmp_dir = tempfile.mkdtemp(prefix="import-run-")
    return Path(tmp_dir) / "state.db", tmp_dir


def _result_payload(ref: str, view: RequestView, request: Request | None, rc: int) -> dict:
    """Build the structured result payload for a completed single-ref run.

    Reconstructs the final state from the Request's latest Attempt phases so
    callers can record build outputs on success (``pipelinerun_name``,
    ``snapshot_name``, ``release_name``) and diagnose failures
    (``error_message``). The payload keys match the pre-cutover flat model so
    existing consumers are unaffected. If the Request lookup somehow returns
    nothing, fall back to a minimal payload whose status is derived from the exit
    code.
    """
    if request is None or request.id is None:
        return {
            "ref": ref,
            "status": RequestState.SUCCESS.value if rc == 0 else RequestState.FAILED.value,
            "pipelinerun_name": None,
            "snapshot_name": None,
            "release_name": None,
            "error_message": None,
            "retry_count": 0,
        }

    phases = view.db.latest_phase_map(request.id)
    intake = phases.get(Phase.INTAKE)
    snapshot = phases.get(Phase.SNAPSHOT)
    release = phases.get(Phase.RELEASE)
    error = next(
        (p.error_message for p in phases.values() if p.status == PhaseStatus.FAILED and p.error_message),
        None,
    )
    return {
        "ref": request.artifact,
        "status": derive_state(phases, view.skip_release).value,
        "pipelinerun_name": intake.reference_id if intake else None,
        "snapshot_name": snapshot.reference_id if snapshot else None,
        "release_name": release.reference_id if release else None,
        "error_message": error,
        "retry_count": max(0, view.db.attempt_count(request.id) - 1),
    }


def _emit_result(payload: dict, output_json: str | None) -> None:
    """Emit the result payload as JSON to stdout, and optionally to a file.

    Human-readable progress goes to stderr, so stdout carries only this single
    JSON line, keeping it easy to capture or pipe. When ``output_json`` is set,
    the same payload is also written to that path for CI robustness.
    """
    serialized = json.dumps(payload)
    print(serialized)

    if output_json is not None:
        Path(output_json).write_text(serialized + "\n")


def run_single(args: argparse.Namespace, ref: str) -> int:
    """Orchestrate a single ref end-to-end: seed, trigger, monitor, retry.

    Unlike ``trigger`` (fire-and-forget), this seeds ``ref`` into a state
    database and runs the full ``ImportOrchestrator`` loop until the import
    reaches a terminal state, applying retries on failure. Generic across
    ecosystems: the ecosystem supplies its own namespace, PipelineRun prefix
    and manifest builder.

    On completion it emits a structured JSON result payload (see
    ``_result_payload``) to stdout, and to ``--output-json`` if given, before the
    ephemeral database is torn down -- so callers keep the final state.

    Returns:
        Exit code: 0 if the import succeeded, 1 if it failed.
    """
    db_path, cleanup_dir = _resolve_db(args)
    try:
        with WorkflowDatabase(db_path) as db:
            warn_if_legacy_database(db)

            eco = args.ecosystem
            target = getattr(args, "target", None)
            skip_release = target is not None and getattr(eco, "target_skip_release", lambda t: False)(target)

            Ingest(db, build_identity=lambda r: eco.request_identity(r, args)).from_lines([ref])

            kube = KubeClient(eco.namespace, CLUSTER_API, KUBEARCHIVE_API)
            view = RequestView(db, skip_release)

            trigger = ImportTrigger(
                view=view,
                kube=kube,
                build_pipelinerun=lambda r: eco.build_pipelinerun(r, args),
                max_parallel=1,
                max_retries=args.max_retries,
            )
            pipeline_monitor = PipelineMonitor(view=view, kube=kube)
            release_monitor = ReleaseMonitor(
                view=view,
                kube=kube,
                max_parallel=1,
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

            rc = orchestrator.run_until_complete()

            # Capture final state while the DB is still open, before cleanup.
            request = next((r for r in db.list_requests() if r.artifact == ref), None)
            payload = _result_payload(ref, view, request, rc)
            _emit_result(payload, getattr(args, "output_json", None))

            return rc
    finally:
        if cleanup_dir is not None:
            shutil.rmtree(cleanup_dir, ignore_errors=True)
            print(f"Removed ephemeral database: {cleanup_dir}", file=sys.stderr)
