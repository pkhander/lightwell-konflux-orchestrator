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
from import_orchestrator.models import ClusterResource, PruneOutcome, Request, RequestState
from import_orchestrator.utils import extract_tag


class PruneMonitor:
    """Deletes the cluster objects of terminal Requests to reclaim namespace quota.

    Deletes each Request's Release and Snapshot by name across all attempts. Completed
    PipelineRuns are left to the platform's Tekton pruner. Prunes a Request once it is
    terminal -- SUCCESS, or FAILED with its retries exhausted (never a still-retriable
    failure). Once pruned, ``pruned_at`` blocks a later retry, including one opened by a
    higher ``--max-retries``. Idempotent via ``pruned_at``; a transient failure retries
    next cycle; a 403 disables pruning for the run (KubeArchive retains everything
    deleted).
    """

    def __init__(self, view: RequestView, kube: KubeClient, max_retries: int):
        self.view = view
        self.db = view.db
        self.kube = kube
        self.max_retries = max_retries
        # Set once a delete is RBAC-denied; disables pruning for the rest of the run.
        self._forbidden = False

    def prune_completed(self) -> int:
        """Prune every prunable, not-yet-pruned Request; return the count pruned."""
        if self._forbidden:
            return 0
        pruned = 0
        for request in self.view.requests():
            assert request.id is not None
            if request.pruned_at is not None or not self._is_prunable(request):
                continue

            tag = extract_tag(request.artifact)
            releases, snapshots = self._resources(request.id)
            outcome = self.kube.delete_request_resources(releases=releases, snapshots=snapshots)
            if outcome is PruneOutcome.DELETED:
                self.db.mark_pruned(request.id)
                pruned += 1
                print(f"  Pruned cluster resources for {tag}", file=sys.stderr)
            elif outcome is PruneOutcome.FORBIDDEN:
                self._forbidden = True
                print(
                    "WARNING: pruning disabled for this run -- the service account lacks delete "
                    "permission on these resources (see errors above).",
                    file=sys.stderr,
                )
                break
            else:  # RETRY
                print(f"  Prune incomplete for {tag}; will retry on the next cycle or a later run", file=sys.stderr)
        return pruned

    def unpruned_count(self) -> int:
        """Count prunable Requests not yet pruned (for the end-of-run warning)."""
        return sum(1 for request in self.view.requests() if request.pruned_at is None and self._is_prunable(request))

    def _is_prunable(self, request: Request) -> bool:
        """Whether a Request's cluster resources may be pruned now.

        A terminal Request's objects are dead weight (KubeArchive retains them): SUCCESS
        always, and FAILED only once retries are exhausted -- a still-retriable failure
        starts a fresh Attempt, so pruning now would stamp pruned_at and suppress pruning
        that Attempt's objects.
        """
        state = self.view.state(request)
        if state is RequestState.SUCCESS:
            return True
        if state is RequestState.FAILED:
            return not self.view.is_retriable(request, self.max_retries)
        return False

    def _resources(self, request_id: int) -> tuple[set[str], set[str]]:
        """Collect Release and Snapshot names across all attempts.

        A retry creates fresh objects, so every attempt's resources are pruned, not just
        the latest's. PipelineRuns are left to the platform's Tekton pruner.
        """
        releases: set[str] = set()
        snapshots: set[str] = set()
        for attempt in self.db.list_attempts(request_id):
            assert attempt.id is not None
            for phase in self.db.list_phases(attempt.id):
                if phase.reference_id is None:
                    continue
                if phase.reference_type is ClusterResource.RELEASE:
                    releases.add(phase.reference_id)
                elif phase.reference_type is ClusterResource.SNAPSHOT:
                    snapshots.add(phase.reference_id)
        return releases, snapshots
