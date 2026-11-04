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
import time

from import_orchestrator.engine.pipeline import PipelineMonitor
from import_orchestrator.engine.reconcile import Reconciler
from import_orchestrator.engine.release import ReleaseMonitor
from import_orchestrator.engine.request_view import RequestView
from import_orchestrator.engine.trigger import ImportTrigger
from import_orchestrator.models import RequestState


class ImportOrchestrator:
    """Coordinates the import lifecycle by delegating to specialized components.

    This is a thin coordinator that manages the polling loop and delegates
    to ImportTrigger, PipelineMonitor, and ReleaseMonitor for the actual work.
    Completion and statistics are derived from each Request's latest Attempt via
    the shared :class:`RequestView`.
    """

    def __init__(
        self,
        view: RequestView,
        trigger: ImportTrigger,
        pipeline_monitor: PipelineMonitor,
        release_monitor: ReleaseMonitor,
        poll_interval: int,
        max_retries: int,
        reconciler: Reconciler | None = None,
    ):
        self.view = view
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self._trigger = trigger
        self._pipeline_monitor = pipeline_monitor
        self._release_monitor = release_monitor
        self._reconciler = reconciler
        # Requests the reconciler could not read this run: neither triggered nor allowed
        # to block loop completion, so one unreadable Request can neither duplicate work
        # nor hang the run -- it is simply deferred to the next invocation.
        self._skipped: set[int] = set()

    def update_pipelinerun_statuses(self) -> None:
        """Check status of all in-progress intake PipelineRuns and update phases."""
        self._pipeline_monitor.update_statuses()

    def update_release_statuses(self) -> None:
        """For Requests past intake, discover snapshots and track releases."""
        self._release_monitor.update_statuses()

    def trigger_next_batch(self) -> int:
        """Trigger imports up to the max_parallel limit, counting all in-flight stages.

        Returns:
            The number of imports successfully triggered.
        """
        return self._trigger.trigger_next_batch()

    def is_complete(self) -> bool:
        """Check if all requests are either successful or permanently failed."""
        return self.view.is_complete(self.max_retries, skip_ids=self._skipped)

    def _print_statistics(self, stats: dict[str, int]) -> None:
        """Print current statistics to stderr."""
        print(
            f"Status: pending={stats[RequestState.PENDING.value]}, "
            f"running={stats[RequestState.RUNNING.value]}, "
            f"releasing={stats[RequestState.AWAITING_RELEASE.value]}, "
            f"success={stats[RequestState.SUCCESS.value]}, "
            f"failed={stats[RequestState.FAILED.value]}",
            file=sys.stderr,
        )

    def run_until_complete(self) -> int:
        """Main loop: trigger batches and monitor until all imports complete.

        Returns:
            Exit code: 0 if all imports succeeded, 1 if any failed.
        """
        if self._reconciler is not None:
            print("\n=== Reconciling cached state from cluster ===", file=sys.stderr)
            # Requests whose reconcile could not reach the cluster are excluded from
            # triggering this run: their true state is unknown, so firing would risk a
            # duplicate build (a transient error is not "nothing exists").
            self._skipped = self._reconciler.reconcile_all()
            self._trigger.skip_requests(self._skipped)

        iteration = 0

        while not self.is_complete():
            iteration += 1
            print(f"\n=== Iteration {iteration} ===", file=sys.stderr)

            self.update_pipelinerun_statuses()
            self.update_release_statuses()

            triggered = self.trigger_next_batch()
            if triggered > 0:
                print(f"Triggered {triggered} new import(s)", file=sys.stderr)

            self._print_statistics(self.view.statistics())

            if not self.is_complete():
                print(f"Sleeping {self.poll_interval}s...", file=sys.stderr)
                time.sleep(self.poll_interval)

        final_stats = self.view.statistics()
        print("\n=== Complete ===", file=sys.stderr)
        self._print_statistics(final_stats)

        return 0 if final_stats.get(RequestState.FAILED.value, 0) == 0 else 1
