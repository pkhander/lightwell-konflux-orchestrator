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

from import_orchestrator.clients import KubeClient
from import_orchestrator.engine.request_view import RequestView
from import_orchestrator.models import Phase, PhaseStatus
from import_orchestrator.utils import extract_tag


class PipelineMonitor:
    """Monitors intake PipelineRun status and advances the intake phase.

    Polls Kubernetes for the intake PipelineRun of each Request's latest Attempt
    and advances its intake :class:`AttemptPhase` RUNNING -> SUCCESS/FAILED. When
    the run succeeds, the release monitor takes over (via the snapshot phase) to run
    the integration-tests gate and, unless ``skip_release`` is set, the release.
    """

    def __init__(self, view: RequestView, kube: KubeClient):
        self.view = view
        self.db = view.db
        self.kube = kube
        self.skip_release = view.skip_release

    def update_statuses(self) -> None:
        """Check the intake PipelineRun of every in-progress Request and update it."""
        for request in self.view.requests():
            assert request.id is not None
            intake = self.db.latest_phase_map(request.id).get(Phase.INTAKE)
            if intake is None or intake.attempt_id is None:
                continue
            if intake.status not in (PhaseStatus.PENDING, PhaseStatus.RUNNING):
                continue
            if intake.reference_id is None:
                continue

            pr_status = self.kube.get_pipelinerun_status(intake.reference_id)
            if pr_status is None:
                continue

            tag = extract_tag(request.artifact)

            if pr_status.is_running:
                continue  # still in flight; intake already recorded RUNNING
            elif pr_status.is_successful:
                self.db.upsert_phase(
                    intake.attempt_id,
                    Phase.INTAKE,
                    PhaseStatus.SUCCESS,
                    reference_type=intake.reference_type,
                    reference_id=intake.reference_id,
                )
                if self.skip_release:
                    print(f"  Build done (scratch), awaiting integration tests: {tag}", file=sys.stderr)
                else:
                    print(f"  Pipeline done, awaiting release: {tag}", file=sys.stderr)
            elif pr_status.is_failed:
                detail = self.kube.get_pipelinerun_failure_detail(intake.reference_id)
                self.db.upsert_phase(
                    intake.attempt_id,
                    Phase.INTAKE,
                    PhaseStatus.FAILED,
                    reference_type=intake.reference_type,
                    reference_id=intake.reference_id,
                    error_message=detail or "PipelineRun failed",
                )
                print(f"  ✗ Failed: {tag}", file=sys.stderr)
                if detail:
                    print(f"    {detail}", file=sys.stderr)
