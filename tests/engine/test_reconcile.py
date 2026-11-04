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
import requests

from import_orchestrator.clients import KubeClient
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.engine import Reconciler, RequestView
from import_orchestrator.models import Phase, PhaseStatus, PipelineRunStatus, Request, RequestState


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


def _seed_request(db: WorkflowDatabase, ref: str) -> Request:
    request, _ = db.get_or_create_request(**_identity(ref))
    return request


def _pr(name: str, status: str) -> PipelineRunStatus:
    return PipelineRunStatus(name=name, status=status)


@pytest.fixture
def db(tmp_path: Path):
    with WorkflowDatabase(tmp_path / "test.db") as database:
        yield database


@pytest.fixture
def mock_kube():
    kube = MagicMock(spec=KubeClient)
    kube.find_intake_pipelineruns_by_fingerprint.return_value = []
    kube.get_pipelinerun_failure_detail.return_value = None
    kube.find_snapshot_by_pipelinerun.return_value = None
    kube.get_snapshot.return_value = {}
    kube.get_snapshot_test_status.return_value = "passed"
    kube.get_snapshot_test_failure_detail.return_value = None
    kube.find_release_for_snapshot.return_value = None
    kube.find_release_for_snapshot_and_plan.return_value = None
    kube.list_releases_for_snapshot_and_plan.return_value = []
    kube.get_release_status.return_value = "Unknown"
    return kube


def _reconciler(db, mock_kube, *, skip_release=False, release_plan=None) -> Reconciler:
    view = RequestView(db, skip_release=skip_release)
    return Reconciler(view=view, kube=mock_kube, release_plan=release_plan)


class TestReconcileIntakeOnly:
    def test_no_intake_pipelineruns_leaves_request_untouched(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = []

        _reconciler(db, mock_kube).reconcile_all()

        assert db.attempt_count(request.id) == 0
        mock_kube.find_snapshot_by_pipelinerun.assert_not_called()

    def test_running_intake_is_rebuilt_from_empty_cache(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pnc-import-run", "Unknown")]

        _reconciler(db, mock_kube).reconcile_all()

        phases = db.latest_phase_map(request.id)
        assert db.attempt_count(request.id) == 1
        assert phases[Phase.INTAKE].status == PhaseStatus.RUNNING
        assert phases[Phase.INTAKE].reference_id == "pnc-import-run"

    def test_failed_intake_records_failure_detail(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pnc-import-bad", "False")]
        mock_kube.get_pipelinerun_failure_detail.return_value = "compiler blew up"

        _reconciler(db, mock_kube).reconcile_all()

        intake = db.latest_phase_map(request.id)[Phase.INTAKE]
        assert intake.status == PhaseStatus.FAILED
        assert intake.error_message == "compiler blew up"


class TestReconcileFullHistory:
    def test_reconstructs_one_attempt_per_intake_oldest_first(self, db, mock_kube):
        request = _seed_request(db, "r1")
        # find_intake returns oldest-first; the newest (running) must become the latest attempt.
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [
            _pr("pr-1", "False"),
            _pr("pr-2", "False"),
            _pr("pr-3", "Unknown"),
        ]

        _reconciler(db, mock_kube).reconcile_all()

        assert db.attempt_count(request.id) == 3
        latest = db.latest_phase_map(request.id)[Phase.INTAKE]
        assert latest.reference_id == "pr-3"
        assert latest.status == PhaseStatus.RUNNING

    def test_is_idempotent_across_runs(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [
            _pr("pr-1", "False"),
            _pr("pr-2", "Unknown"),
        ]

        reconciler = _reconciler(db, mock_kube)
        reconciler.reconcile_all()
        reconciler.reconcile_all()

        assert db.attempt_count(request.id) == 2

    def test_new_cluster_attempt_appends_without_duplicating_cached_one(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-1", "False")]
        reconciler = _reconciler(db, mock_kube)
        reconciler.reconcile_all()

        # A retry PipelineRun appeared on the cluster since the last run.
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [
            _pr("pr-1", "False"),
            _pr("pr-2", "Unknown"),
        ]
        reconciler.reconcile_all()

        assert db.attempt_count(request.id) == 2
        assert db.latest_phase_map(request.id)[Phase.INTAKE].reference_id == "pr-2"


class TestReconcileChain:
    def test_rebuilds_full_chain_to_release(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "passed"
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [("rel-1", "True")]

        _reconciler(db, mock_kube, release_plan="rp").reconcile_all()

        phases = db.latest_phase_map(request.id)
        assert phases[Phase.INTAKE].status == PhaseStatus.SUCCESS
        assert phases[Phase.SNAPSHOT].reference_id == "snap-1"
        assert phases[Phase.INTEGRATION_TESTS].status == PhaseStatus.SUCCESS
        assert phases[Phase.RELEASE].status == PhaseStatus.SUCCESS
        assert phases[Phase.RELEASE].reference_id == "rel-1"

    def test_uses_unscoped_release_lookup_without_a_plan(self, db, mock_kube):
        _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "passed"
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [("rel-any", "Unknown")]

        _reconciler(db, mock_kube, release_plan=None).reconcile_all()

        mock_kube.list_releases_for_snapshot_and_plan.assert_called_once_with("snap-1", None)

    def test_stops_at_snapshot_when_not_yet_created(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = None

        _reconciler(db, mock_kube).reconcile_all()

        phases = db.latest_phase_map(request.id)
        assert phases[Phase.INTAKE].status == PhaseStatus.SUCCESS
        assert Phase.SNAPSHOT not in phases

    def test_integration_tests_pending_blocks_release(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "pending"

        _reconciler(db, mock_kube, release_plan="rp").reconcile_all()

        phases = db.latest_phase_map(request.id)
        assert phases[Phase.INTEGRATION_TESTS].status == PhaseStatus.RUNNING
        assert Phase.RELEASE not in phases
        mock_kube.list_releases_for_snapshot_and_plan.assert_not_called()

    def test_skip_release_stops_after_integration_tests(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "passed"

        _reconciler(db, mock_kube, skip_release=True).reconcile_all()

        phases = db.latest_phase_map(request.id)
        assert phases[Phase.INTEGRATION_TESTS].status == PhaseStatus.SUCCESS
        assert Phase.RELEASE not in phases
        mock_kube.list_releases_for_snapshot_and_plan.assert_not_called()

    def test_unreadable_snapshot_test_status_leaves_gate_unset(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot.return_value = None
        mock_kube.get_snapshot_test_status.return_value = None

        _reconciler(db, mock_kube, release_plan="rp").reconcile_all()

        phases = db.latest_phase_map(request.id)
        assert phases[Phase.SNAPSHOT].status == PhaseStatus.SUCCESS
        assert Phase.INTEGRATION_TESTS not in phases
        mock_kube.list_releases_for_snapshot_and_plan.assert_not_called()


class TestReconcileState:
    def test_running_intake_yields_running_state(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-run", "Unknown")]

        view = RequestView(db, skip_release=False)
        Reconciler(view=view, kube=mock_kube).reconcile_all()

        assert view.state(request) == RequestState.RUNNING


class TestReconcileReleaseRetries:
    """Release retries share one intake PipelineRun, so the cluster's Release objects --
    not the intake count -- are what reconstruct the attempt history and the retry budget."""

    def _passed_chain(self, mock_kube):
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "passed"

    def test_terminal_release_is_recorded_failed(self, db, mock_kube):
        request = _seed_request(db, "r1")
        self._passed_chain(mock_kube)
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [("rel-1", "False")]

        view = RequestView(db, skip_release=False)
        Reconciler(view=view, kube=mock_kube, release_plan="rp").reconcile_all()

        release = db.latest_phase_map(request.id)[Phase.RELEASE]
        assert release.status == PhaseStatus.FAILED
        assert release.reference_id == "rel-1"
        # FAILED (not AWAITING_RELEASE), so the monitor will not mint a new Release in
        # this same attempt and bypass max_retries.
        assert view.state(request) == RequestState.FAILED

    def test_each_release_object_is_one_attempt(self, db, mock_kube):
        request = _seed_request(db, "r1")
        self._passed_chain(mock_kube)
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [
            ("rel-3", "False"),
            ("rel-2", "False"),
            ("rel-1", "False"),
        ]

        Reconciler(view=RequestView(db, skip_release=False), kube=mock_kube, release_plan="rp").reconcile_all()

        assert db.attempt_count(request.id) == 3
        assert db.latest_phase_map(request.id)[Phase.RELEASE].reference_id == "rel-3"

    def test_exhausted_release_budget_is_not_retriable_after_cache_wipe(self, db, mock_kube):
        _seed_request(db, "r1")
        self._passed_chain(mock_kube)
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [
            ("rel-3", "False"),
            ("rel-2", "False"),
            ("rel-1", "False"),
        ]

        view = RequestView(db, skip_release=False)
        Reconciler(view=view, kube=mock_kube, release_plan="rp").reconcile_all()

        # Three spent release attempts reconstructed from an empty cache; with a budget
        # of two the Request must not come back eligible for another Release.
        assert view.retry_candidates(max_retries=2) == []

    def test_newest_successful_release_completes_the_request(self, db, mock_kube):
        request = _seed_request(db, "r1")
        self._passed_chain(mock_kube)
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [
            ("rel-2", "True"),
            ("rel-1", "False"),
        ]

        view = RequestView(db, skip_release=False)
        Reconciler(view=view, kube=mock_kube, release_plan="rp").reconcile_all()

        assert db.attempt_count(request.id) == 2
        assert view.state(request) == RequestState.SUCCESS

    def test_empty_release_list_does_not_downgrade_cached_success(self, db, mock_kube):
        request = _seed_request(db, "r1")
        self._passed_chain(mock_kube)
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [("rel-1", "True")]
        view = RequestView(db, skip_release=False)
        reconciler = Reconciler(view=view, kube=mock_kube, release_plan="rp")
        reconciler.reconcile_all()
        assert view.state(request) == RequestState.SUCCESS

        # A later reconcile whose release list momentarily returns nothing must leave the
        # settled SUCCESS in place, not downgrade it.
        mock_kube.list_releases_for_snapshot_and_plan.return_value = []
        reconciler.reconcile_all()

        assert db.latest_phase_map(request.id)[Phase.RELEASE].status == PhaseStatus.SUCCESS


class TestReconcileTransientFailure:
    """A live query failure is "unknown", never "nothing exists": the Request is skipped
    this run and reported so the trigger does not fire a duplicate."""

    def test_live_intake_query_failure_skips_request_and_leaves_cache(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.side_effect = requests.HTTPError("503")

        unreconciled = _reconciler(db, mock_kube).reconcile_all()

        assert unreconciled == {request.id}
        assert db.attempt_count(request.id) == 0

    def test_release_list_failure_skips_request_without_partial_write(self, db, mock_kube):
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-ok", "True")]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "passed"
        mock_kube.list_releases_for_snapshot_and_plan.side_effect = requests.HTTPError("503")

        unreconciled = _reconciler(db, mock_kube, release_plan="rp").reconcile_all()

        assert unreconciled == {request.id}
        assert db.attempt_count(request.id) == 0  # read-then-write: no intake/snapshot rows left behind


class TestReconcileOrdering:
    def test_rebuilds_in_timestamp_order_when_older_pr_appears_later(self, db, mock_kube):
        request = _seed_request(db, "r1")
        # First run sees only the newer (live) PipelineRun.
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-new", "Unknown")]
        reconciler = _reconciler(db, mock_kube)
        reconciler.reconcile_all()

        # Next run, KubeArchive returns an older pruned PipelineRun too (oldest-first).
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [
            _pr("pr-old", "False"),
            _pr("pr-new", "Unknown"),
        ]
        reconciler.reconcile_all()

        assert db.attempt_count(request.id) == 2
        latest = db.latest_phase_map(request.id)[Phase.INTAKE]
        assert latest.reference_id == "pr-new"  # latest_attempt is the newest PR, not the higher rowid
        assert latest.status == PhaseStatus.RUNNING

    def test_phaseless_attempt_is_not_duplicated(self, db, mock_kube):
        request = _seed_request(db, "r1")
        db.create_attempt(request.id)  # crash window: an Attempt with no intake phase
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [_pr("pr-1", "Unknown")]

        _reconciler(db, mock_kube).reconcile_all()

        assert db.attempt_count(request.id) == 1
        assert db.latest_phase_map(request.id)[Phase.INTAKE].reference_id == "pr-1"
