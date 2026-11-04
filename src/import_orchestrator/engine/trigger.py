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

import sys
from collections.abc import Callable

from import_orchestrator.clients.kube import KubeClient
from import_orchestrator.constants import FINGERPRINT_LABEL
from import_orchestrator.engine.errors import TriggerError
from import_orchestrator.engine.request_view import RequestView
from import_orchestrator.models import Attempt, ClusterResource, Fingerprint, Phase, PhaseStatus, Request
from import_orchestrator.utils import extract_tag


class ImportTrigger:
    """Starts Attempts and submits intake PipelineRuns.

    For a Request with no prior Attempt this opens a fresh Attempt; for a failed
    Request being retried it opens a new Attempt that carries forward the phases
    that already succeeded (see :meth:`_open_attempt`). If that carried-over
    state already includes a successful intake, no new PipelineRun is submitted --
    the Request resumes from where it failed and the release monitor takes over.
    Otherwise an intake PipelineRun is built (by the injected ``build_pipelinerun``
    callable) and the intake phase is recorded RUNNING with the PipelineRun as its
    reference.
    """

    def __init__(
        self,
        view: RequestView,
        kube: KubeClient,
        build_pipelinerun: Callable[[str], dict],
        max_parallel: int,
        max_retries: int,
    ):
        self.view = view
        self.db = view.db
        self.kube = kube
        self.build_pipelinerun = build_pipelinerun
        self.max_parallel = max_parallel
        self.max_retries = max_retries
        # Requests the reconciler could not read from the cluster this run; never
        # triggered here, so a failed reconcile cannot duplicate in-flight work.
        self._skipped: set[int] = set()

    def skip_requests(self, request_ids: set[int]) -> None:
        """Exclude these Requests from triggering this run (their reconcile failed)."""
        self._skipped = set(request_ids)

    def trigger_next_batch(self) -> int:
        """Trigger imports up to the max_parallel limit, counting all in-flight stages.

        Returns:
            The number of imports successfully triggered.
        """
        available_slots = max(0, self.max_parallel - self.view.in_flight_count())
        if available_slots == 0:
            return 0

        candidates = [
            r for r in (self.view.pending() + self.view.retry_candidates(self.max_retries)) if r.id not in self._skipped
        ][:available_slots]

        triggered = 0
        for request in candidates:
            if request.id is None:
                continue
            triggered += self._trigger_single(request)

        return triggered

    def _trigger_single(self, request: Request) -> int:
        """Attempt to trigger (or resume) a single Request. Returns 1 if an intake
        PipelineRun was submitted, 0 otherwise (resume or failure).

        The intake phase is recorded (RUNNING or FAILED) before this returns on
        the expected paths, so an Attempt is not left phase-less: an expected
        build/submit failure (``TriggerError``) is caught and recorded as a
        failed intake, which keeps one such failure from aborting the whole batch
        and keeps the Request retriable rather than wedged in a phase-less PENDING
        state. Any other (unexpected) exception propagates so genuine programming
        errors surface immediately instead of being retried every poll.
        """
        assert request.id is not None
        tag = extract_tag(request.artifact)  # used only for log messages

        attempt = self._open_attempt(request.id)
        assert attempt.id is not None

        # A retry may have carried forward a successful intake; if so, resume from
        # the later phase rather than re-running the build.
        intake = self.db.get_phase(attempt.id, Phase.INTAKE)
        if intake is not None and intake.status == PhaseStatus.SUCCESS:
            print(f"  Resuming past intake: {tag}", file=sys.stderr)
            return 0

        try:
            manifest = self.build_pipelinerun(request.artifact)
            # Stamp the Request fingerprint so a later run can recognise this
            # PipelineRun (and, via the custom.appstudio.openshift.io/ prefix,
            # the Snapshot and integration-test PipelineRuns Konflux derives from
            # it) by label rather than by the local cache (ADR-0001). Done here,
            # where the full Request is in scope, rather than in the ecosystem
            # builders, which only receive the artifact.
            labels = manifest.setdefault("metadata", {}).setdefault("labels", {})
            labels[FINGERPRINT_LABEL] = Fingerprint.from_request(request).hash()
            pr_name = self.kube.create_pipelinerun(manifest)
            if pr_name is None:
                raise TriggerError("PipelineRun creation failed (API returned no name)")
        except TriggerError as e:  # an expected build/submit failure leaves a recorded, retriable FAILED intake
            self.db.upsert_phase(
                attempt.id,
                Phase.INTAKE,
                PhaseStatus.FAILED,
                error_message=f"PipelineRun trigger failed: {e}",
            )
            print(f"  ERROR: Failed to trigger {tag}: {e}", file=sys.stderr)
            return 0

        self.db.upsert_phase(
            attempt.id,
            Phase.INTAKE,
            PhaseStatus.RUNNING,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id=pr_name,
        )
        attempts = self.db.attempt_count(request.id)
        retry_indicator = f" (attempt {attempts})" if attempts > 1 else ""
        print(f"  Triggered: {tag}{retry_indicator}", file=sys.stderr)
        return 1

    def _open_attempt(self, request_id: int) -> Attempt:
        """Open the Attempt to record this trigger against.

        - No Attempt yet: start a fresh one.
        - A latest Attempt exists but has no intake phase (e.g. a crash between
          creating the Attempt and recording its intake): reuse it, so recovery
          re-submits into the same Attempt instead of stacking another phase-less
          one.
        - Otherwise (the latest Attempt's intake already settled): start a retry
          Attempt carrying forward the settled phases. Deciding *which* phases
          carry forward is the caller's job (the store only persists them): the
          retry seeds the new Attempt with the previous attempt's phases that
          carry forward (see :attr:`PhaseStatus.carries_forward`), so a completed
          phase is not re-run and history is never overwritten.
        """
        latest = self.db.latest_attempt(request_id)
        if latest is None or latest.id is None:
            return self.db.create_attempt(request_id)
        if self.db.get_phase(latest.id, Phase.INTAKE) is None:
            return latest
        carried = [phase for phase in self.db.list_phases(latest.id) if phase.status.carries_forward]
        return self.db.create_attempt(request_id, initial_phases=carried)
