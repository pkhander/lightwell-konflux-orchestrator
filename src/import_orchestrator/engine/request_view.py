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

from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.models import Request, RequestState, derive_state


class RequestView:
    """Projects the per-phase Request/Attempt model onto the aggregate reads the
    orchestration loop needs: a single :class:`RequestState` per Request, plus
    the batching, retry and completion queries that used to read a flat status
    column.

    ``skip_release`` is a run-level flag (true for targets whose workflow ends at
    the integration tests, producing no Release CR); it is fed to
    :func:`derive_state` so a passed (or skipped) ITS phase counts as terminal
    success for those targets.

    Retry eligibility matches the legacy flat model: with N attempts recorded, a
    failed Request is retriable while ``attempt_count <= max_retries`` (i.e. up to
    ``max_retries`` retries after the first attempt).
    """

    def __init__(self, db: WorkflowDatabase, skip_release: bool):
        self.db = db
        self.skip_release = skip_release

    def state(self, request: Request) -> RequestState:
        """Return the overall :class:`RequestState` of a Request."""
        assert request.id is not None
        return derive_state(self.db.latest_phase_map(request.id), self.skip_release)

    def requests(self) -> list[Request]:
        """Return every Request, ordered by id."""
        return self.db.list_requests()

    def pending(self) -> list[Request]:
        """Requests awaiting their first (or a recovered) intake trigger.

        A Request is PENDING when it has no Attempt yet, or when its latest
        Attempt has no intake phase recorded (e.g. a crash between creating the
        Attempt and recording its intake). Both must be (re)triggered, so this is
        defined by the derived state rather than ``attempt_count``."""
        return [r for r in self.requests() if self.state(r) == RequestState.PENDING]

    def retry_candidates(self, max_retries: int) -> list[Request]:
        """Failed Requests still eligible for another attempt."""
        return [r for r in self.requests() if self._is_retriable(r, max_retries)]

    def in_flight_count(self) -> int:
        """Count Requests actively being processed (running or awaiting release)."""
        return sum(1 for r in self.requests() if self.state(r) in (RequestState.RUNNING, RequestState.AWAITING_RELEASE))

    def statistics(self) -> dict[str, int]:
        """Return counts grouped by :class:`RequestState` value, for reporting."""
        stats = {s.value: 0 for s in RequestState}
        for r in self.requests():
            stats[self.state(r).value] += 1
        return stats

    def is_complete(self, max_retries: int, skip_ids: set[int] | None = None) -> bool:
        """True when no Request is still in flight or retriable.

        ``skip_ids`` are Requests the reconciler could not read this run; they are
        neither triggered nor counted toward completion, so one unreadable Request does
        not hang the loop -- it is deferred to the next invocation.
        """
        skip = skip_ids or set()
        for r in self.requests():
            if r.id in skip:
                continue
            st = self.state(r)
            if st in (RequestState.PENDING, RequestState.RUNNING, RequestState.AWAITING_RELEASE):
                return False
            if st == RequestState.FAILED and self._is_retriable(r, max_retries):
                return False
        return True

    def _is_retriable(self, request: Request, max_retries: int) -> bool:
        if request.id is None:
            return False
        return self.state(request) == RequestState.FAILED and self.db.attempt_count(request.id) <= max_retries
