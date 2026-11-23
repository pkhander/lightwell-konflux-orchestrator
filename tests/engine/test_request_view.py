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

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.engine import PipelineMonitor, ReleaseMonitor, RequestView
from import_orchestrator.models import ClusterResource, Phase, PhaseStatus, Request, RequestState


def _identity(ref: str) -> dict[str, str]:
    return {
        "namespace": "ns",
        "application": "app",
        "component": "comp",
        "pipeline": "import-pnc",
        "release_plan": "rp",
        "target": "remediated",
        "artifact": ref,
    }


@pytest.fixture
def db(tmp_path: Path):
    with WorkflowDatabase(tmp_path / "test.db") as database:
        yield database


@pytest.fixture
def view(db: WorkflowDatabase):
    return RequestView(db, skip_release=False)


def _pending(db: WorkflowDatabase, ref: str) -> Request:
    request, _ = db.get_or_create_request(**_identity(ref))
    return request


def _running(db: WorkflowDatabase, ref: str) -> Request:
    request = _pending(db, ref)
    attempt = db.create_attempt(request.id)
    db.upsert_phase(
        attempt.id, Phase.INTAKE, PhaseStatus.RUNNING, reference_type=ClusterResource.PIPELINE_RUN, reference_id=ref
    )
    return request


def _failed(db: WorkflowDatabase, ref: str, attempts: int = 1) -> Request:
    request = _pending(db, ref)
    for _ in range(attempts):
        attempt = db.create_attempt(request.id)
        db.upsert_phase(attempt.id, Phase.INTAKE, PhaseStatus.FAILED, error_message="boom")
    return request


class TestState:
    def test_pending(self, db, view):
        assert view.state(_pending(db, "r1")) == RequestState.PENDING

    def test_running(self, db, view):
        assert view.state(_running(db, "r1")) == RequestState.RUNNING

    def test_failed(self, db, view):
        assert view.state(_failed(db, "r1")) == RequestState.FAILED


class TestSelectors:
    def test_scope_applies_to_all_selectors(self, db):
        selected = _pending(db, "selected")
        _running(db, "other-running")
        _failed(db, "other-failed")
        view = RequestView(db, skip_release=False, request_ids={selected.id})
        assert view.requests() == [selected]
        assert view.pending() == [selected]
        assert view.retry_candidates(3) == []
        assert view.in_flight_count() == 0
        assert view.statistics() == {"pending": 1, "running": 0, "releasing": 0, "success": 0, "failed": 0}
        assert not view.is_complete(3)

    def test_empty_scope_is_complete_even_with_pending_requests(self, db):
        _pending(db, "other")
        view = RequestView(db, skip_release=False, request_ids=set())
        assert view.requests() == []
        assert view.is_complete(3)

    @pytest.mark.parametrize("intake_status", [PhaseStatus.RUNNING, PhaseStatus.SUCCESS])
    def test_monitors_leave_requests_outside_scope_untouched(self, db, intake_status):
        selected = _pending(db, "selected")
        other = _running(db, "other")
        attempt = db.latest_attempt(other.id)
        db.upsert_phase(attempt.id, Phase.INTAKE, intake_status, ClusterResource.PIPELINE_RUN, "other")
        phases_before = db.latest_phase_map(other.id)
        view = RequestView(db, skip_release=True, request_ids={selected.id})
        kube = MagicMock()
        PipelineMonitor(view, kube).update_statuses()
        ReleaseMonitor(view, kube, max_parallel=5, prefix="python-build-").update_statuses()
        assert kube.mock_calls == []
        assert db.latest_phase_map(other.id) == phases_before

    def test_pending_lists_only_unattempted(self, db, view):
        _pending(db, "p1")
        _running(db, "r1")
        pending = view.pending()
        assert [r.artifact for r in pending] == ["p1"]

    def test_in_flight_count(self, db, view):
        _running(db, "r1")
        _running(db, "r2")
        _pending(db, "p1")
        assert view.in_flight_count() == 2

    def test_in_flight_count_excludes_skipped_ids(self, db, view):
        skipped = _running(db, "r1")
        _running(db, "r2")
        assert view.in_flight_count(skip_ids={skipped.id}) == 1

    def test_retry_candidates_respect_max_retries(self, db, view):
        _failed(db, "retriable", attempts=1)
        _failed(db, "exhausted", attempts=4)  # 4 > max_retries=3
        candidates = view.retry_candidates(max_retries=3)
        assert [r.artifact for r in candidates] == ["retriable"]

    def test_pruned_failure_is_never_retriable(self, db, view):
        # A pruned failure is finished: raising --max-retries must not revive it against
        # the Snapshot/Release prune already deleted (LWLP-5515).
        pruned = _failed(db, "pruned", attempts=4)
        db.mark_pruned(pruned.id)

        assert view.retry_candidates(max_retries=10) == []
        assert view.is_complete(max_retries=10) is True

    def test_unpruned_failure_within_retries_is_still_retriable(self, db, view):
        # Guard against the pruned_at gate over-reaching: an ordinary failure is unaffected.
        _failed(db, "live", attempts=1)
        assert [r.artifact for r in view.retry_candidates(max_retries=3)] == ["live"]

    def test_statistics(self, db, view):
        _pending(db, "p1")
        _running(db, "r1")
        _failed(db, "f1", attempts=4)
        stats = view.statistics()
        assert stats == {"pending": 1, "running": 1, "releasing": 0, "success": 0, "failed": 1}


class TestIsComplete:
    def test_empty_is_complete(self, view):
        assert view.is_complete(max_retries=3) is True

    def test_pending_not_complete(self, db, view):
        _pending(db, "p1")
        assert view.is_complete(max_retries=3) is False

    def test_retriable_failure_not_complete(self, db, view):
        _failed(db, "f1", attempts=1)
        assert view.is_complete(max_retries=3) is False

    def test_exhausted_failure_is_complete(self, db, view):
        _failed(db, "f1", attempts=4)
        assert view.is_complete(max_retries=3) is True


class TestSkipRelease:
    def _seed_intake_success(self, db: WorkflowDatabase, ref: str) -> tuple[Request, int]:
        request = _pending(db, ref)
        attempt = db.create_attempt(request.id)
        db.upsert_phase(
            attempt.id,
            Phase.INTAKE,
            PhaseStatus.SUCCESS,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id=f"pr-{ref}",
        )
        return request, attempt.id

    def test_intake_success_awaits_integration_tests(self, db):
        """A skip_release request is not terminal at intake -- ITS still has to run."""
        view = RequestView(db, skip_release=True)
        request, _ = self._seed_intake_success(db, "r1")
        assert view.state(request) == RequestState.RUNNING
        assert view.is_complete(max_retries=3) is False

    def test_integration_tests_pass_is_terminal_when_skip_release(self, db):
        view = RequestView(db, skip_release=True)
        request, attempt_id = self._seed_intake_success(db, "r1")
        db.upsert_phase(attempt_id, Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS)
        assert view.state(request) == RequestState.SUCCESS
        assert view.is_complete(max_retries=3) is True
