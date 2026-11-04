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
from import_orchestrator.ecosystems.java.pipelinerun import TriggerError
from import_orchestrator.engine import (
    ImportOrchestrator,
    ImportTrigger,
    PipelineMonitor,
    Reconciler,
    ReleaseMonitor,
    RequestView,
)
from import_orchestrator.models import ClusterResource, Phase, PhaseStatus, PipelineRunStatus, Request


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


def _seed_running(db: WorkflowDatabase, ref: str) -> Request:
    request = _seed_request(db, ref)
    attempt = db.create_attempt(request.id)
    db.upsert_phase(
        attempt.id,
        Phase.INTAKE,
        PhaseStatus.RUNNING,
        reference_type=ClusterResource.PIPELINE_RUN,
        reference_id=f"pr-{ref}",
    )
    return request


def _seed_success(db: WorkflowDatabase, ref: str) -> Request:
    request = _seed_request(db, ref)
    attempt = db.create_attempt(request.id)
    db.upsert_phase(
        attempt.id,
        Phase.INTAKE,
        PhaseStatus.SUCCESS,
        reference_type=ClusterResource.PIPELINE_RUN,
        reference_id=f"pr-{ref}",
    )
    db.upsert_phase(
        attempt.id,
        Phase.RELEASE,
        PhaseStatus.SUCCESS,
        reference_type=ClusterResource.RELEASE,
        reference_id=f"rel-{ref}",
    )
    return request


def _seed_exhausted_failure(db: WorkflowDatabase, ref: str, attempts: int) -> Request:
    """Seed a Request with ``attempts`` failed attempts (intake FAILED on each)."""
    request = _seed_request(db, ref)
    for _ in range(attempts):
        attempt = db.create_attempt(request.id)
        db.upsert_phase(attempt.id, Phase.INTAKE, PhaseStatus.FAILED, error_message="boom")
    return request


@pytest.fixture
def db(tmp_path: Path):
    db_path = tmp_path / "test.db"
    with WorkflowDatabase(db_path) as database:
        yield database


@pytest.fixture
def mock_kube():
    kube = MagicMock(spec=KubeClient)
    # Integration tests pass by default so the release path is exercised; tests that
    # care about the ITS gate override this explicitly.
    kube.get_snapshot_test_status.return_value = "passed"
    kube.get_snapshot_test_failure_detail.return_value = None
    # Reconciliation defaults: nothing on the cluster unless a test says otherwise.
    kube.find_intake_pipelineruns_by_fingerprint.return_value = []
    kube.list_releases_for_snapshot_and_plan.return_value = []
    return kube


@pytest.fixture
def orchestrator(db: WorkflowDatabase, mock_kube: MagicMock):
    mock_kube.create_pipelinerun.return_value = "pnc-import-xxx"
    view = RequestView(db, skip_release=False)
    trigger = ImportTrigger(
        view=view,
        kube=mock_kube,
        build_pipelinerun=MagicMock(return_value={"kind": "PipelineRun"}),
        max_parallel=5,
        max_retries=3,
    )
    pipeline_monitor = PipelineMonitor(view=view, kube=mock_kube)
    release_monitor = ReleaseMonitor(view=view, kube=mock_kube, max_parallel=5, prefix="pnc-import-")

    return ImportOrchestrator(
        view=view,
        trigger=trigger,
        pipeline_monitor=pipeline_monitor,
        release_monitor=release_monitor,
        poll_interval=0,
        max_retries=3,
    )


class TestUpdatePipelineRunStatuses:
    def test_advances_running_to_success(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase, mock_kube):
        request = _seed_running(db, "r1")
        mock_kube.get_pipelinerun_status.return_value = PipelineRunStatus(name="pr-r1", status="True")

        orchestrator.update_pipelinerun_statuses()

        assert db.latest_phase_map(request.id)[Phase.INTAKE].status == PhaseStatus.SUCCESS

    def test_advances_running_to_failed(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase, mock_kube):
        request = _seed_running(db, "r1")
        mock_kube.get_pipelinerun_status.return_value = PipelineRunStatus(name="pr-r1", status="False")
        mock_kube.get_pipelinerun_failure_detail.return_value = None

        orchestrator.update_pipelinerun_statuses()

        assert db.latest_phase_map(request.id)[Phase.INTAKE].status == PhaseStatus.FAILED

    def test_skips_requests_without_attempt(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase, mock_kube):
        _seed_request(db, "r1")
        orchestrator.update_pipelinerun_statuses()
        mock_kube.get_pipelinerun_status.assert_not_called()


class TestTriggerNextBatch:
    def test_triggers_up_to_available_slots(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        for i in range(3):
            _seed_running(db, f"inflight{i}")
        for i in range(5):
            _seed_request(db, f"tag{i}")

        triggered = orchestrator.trigger_next_batch()
        assert triggered == 2

    def test_returns_zero_when_no_slots(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        for i in range(5):
            _seed_running(db, f"inflight{i}")
        _seed_request(db, "tag")

        assert orchestrator.trigger_next_batch() == 0

    def test_handles_trigger_failure(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase, mock_kube):
        request = _seed_request(db, "tag")
        mock_kube.create_pipelinerun.side_effect = TriggerError("connection refused")

        triggered = orchestrator.trigger_next_batch()
        assert triggered == 0
        assert db.latest_phase_map(request.id)[Phase.INTAKE].status == PhaseStatus.FAILED


class TestIsComplete:
    def test_complete_when_all_success(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        _seed_success(db, "r1")
        assert orchestrator.is_complete() is True

    def test_not_complete_with_pending(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        _seed_request(db, "r1")
        assert orchestrator.is_complete() is False

    def test_not_complete_with_retryable_failure(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        _seed_exhausted_failure(db, "r1", attempts=1)
        assert orchestrator.is_complete() is False

    def test_complete_when_failure_exhausted_retries(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        _seed_exhausted_failure(db, "r1", attempts=4)  # max_retries=3
        assert orchestrator.is_complete() is True

    def test_complete_with_empty_database(self, orchestrator: ImportOrchestrator):
        assert orchestrator.is_complete() is True


class TestRunUntilComplete:
    def test_completes_with_success(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase, mock_kube):
        _seed_request(db, "r1")

        mock_kube.create_pipelinerun.return_value = "pnc-import-abc"
        mock_kube.get_pipelinerun_status.return_value = PipelineRunStatus(name="pnc-import-abc", status="True")
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snapshot-abc"
        mock_kube.find_release_for_snapshot.return_value = "release-abc"
        mock_kube.get_release_status.return_value = "True"

        exit_code = orchestrator.run_until_complete()
        assert exit_code == 0

    def test_returns_1_when_failures(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        _seed_exhausted_failure(db, "r1", attempts=4)  # permanently failed
        exit_code = orchestrator.run_until_complete()
        assert exit_code == 1


class TestStatistics:
    def test_statistics_has_no_triggered_key(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        """The statistics projection uses RequestState values (no 'triggered')."""
        _seed_running(db, "r1")
        stats = orchestrator.view.statistics()
        assert set(stats) == {"pending", "running", "releasing", "success", "failed"}
        assert stats["running"] == 1


class TestReconcileIntegration:
    def _orchestrator(self, db: WorkflowDatabase, mock_kube) -> ImportOrchestrator:
        view = RequestView(db, skip_release=False)
        trigger = ImportTrigger(
            view=view,
            kube=mock_kube,
            build_pipelinerun=MagicMock(return_value={"kind": "PipelineRun"}),
            max_parallel=5,
            max_retries=3,
        )
        return ImportOrchestrator(
            view=view,
            trigger=trigger,
            pipeline_monitor=PipelineMonitor(view=view, kube=mock_kube),
            release_monitor=ReleaseMonitor(view=view, kube=mock_kube, max_parallel=5, prefix="pnc-import-"),
            poll_interval=0,
            max_retries=3,
            reconciler=Reconciler(view=view, kube=mock_kube),
        )

    def test_reconcile_prevents_duplicate_intake(self, db: WorkflowDatabase, mock_kube):
        """Headline AC: an empty cache with a completed workflow already on the cluster is
        adopted by reconciliation, so the loop never triggers a duplicate intake."""
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.return_value = [
            PipelineRunStatus(name="pnc-import-existing", status="True")
        ]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "passed"
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [("rel-1", "True")]

        exit_code = self._orchestrator(db, mock_kube).run_until_complete()

        assert exit_code == 0
        mock_kube.create_pipelinerun.assert_not_called()
        # The completed cluster workflow was actually adopted (not just "no create").
        phases = db.latest_phase_map(request.id)
        assert phases[Phase.INTAKE].reference_id == "pnc-import-existing"
        assert phases[Phase.SNAPSHOT].reference_id == "snap-1"
        assert phases[Phase.INTEGRATION_TESTS].status == PhaseStatus.SUCCESS
        assert phases[Phase.RELEASE].reference_id == "rel-1"
        assert phases[Phase.RELEASE].status == PhaseStatus.SUCCESS

    def test_reconcile_failure_does_not_trigger_duplicate(self, db: WorkflowDatabase, mock_kube):
        """If the cluster cannot be read during reconcile, the Request is skipped this run:
        it must not be triggered, or a transient error would duplicate in-flight work."""
        _seed_request(db, "r1")
        mock_kube.find_intake_pipelineruns_by_fingerprint.side_effect = requests.HTTPError("503")

        exit_code = self._orchestrator(db, mock_kube).run_until_complete()

        assert exit_code == 0
        mock_kube.create_pipelinerun.assert_not_called()

    def test_reconciler_is_optional(self, orchestrator: ImportOrchestrator, mock_kube):
        """The default orchestrator fixture has no reconciler; it must still run."""
        assert orchestrator.run_until_complete() == 0
        mock_kube.find_intake_pipelineruns_by_fingerprint.assert_not_called()
