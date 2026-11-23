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
from unittest.mock import MagicMock, patch

import pytest
import requests

from import_orchestrator.clients import KubeClient
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.ecosystems.java.pipelinerun import TriggerError
from import_orchestrator.engine import (
    ImportOrchestrator,
    ImportTrigger,
    PipelineMonitor,
    PruneMonitor,
    Reconciler,
    ReleaseMonitor,
    RequestView,
)
from import_orchestrator.models import (
    ClusterResource,
    Fingerprint,
    Phase,
    PhaseStatus,
    PipelineRunStatus,
    PruneOutcome,
    Request,
)


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


def _seed_awaiting_release(db: WorkflowDatabase, ref: str, snapshot: str) -> Request:
    """Intake, snapshot, and integration tests succeeded; no Release recorded yet."""
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
        Phase.SNAPSHOT,
        PhaseStatus.SUCCESS,
        reference_type=ClusterResource.SNAPSHOT,
        reference_id=snapshot,
    )
    db.upsert_phase(attempt.id, Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS)
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
    kube.find_intake_pipelinerun_statuses_by_fingerprint.return_value = []
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

    def test_skipped_in_flight_request_does_not_consume_a_slot(self, db: WorkflowDatabase, mock_kube):
        """An unreadable in-flight Request must not fill max_parallel for the rest of the batch."""
        waiting = _seed_awaiting_release(db, "waiting", "snap-waiting")
        pending = _seed_request(db, "next")
        view = RequestView(db, skip_release=False)
        trigger = ImportTrigger(
            view=view,
            kube=mock_kube,
            build_pipelinerun=MagicMock(return_value={"kind": "PipelineRun"}),
            max_parallel=1,
            max_retries=3,
        )
        mock_kube.create_pipelinerun.return_value = "pr-next"
        trigger.skip_requests({waiting.id})

        assert trigger.trigger_next_batch() == 1
        assert db.latest_phase_map(pending.id)[Phase.INTAKE].reference_id == "pr-next"

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

    def test_unchanged_status_logged_once_in_loop(self, db: WorkflowDatabase, capsys):
        """A quiet run with no state change logs the status once, not per poll."""
        _seed_running(db, "r1")
        orchestrator = _quiet_orchestrator(db)

        # Three poll iterations, then complete; stats never change across them.
        with patch.object(orchestrator, "is_complete", side_effect=[False] * 6 + [True]):
            orchestrator.run_until_complete()

        err = capsys.readouterr().err
        assert "=== Iteration" not in err
        assert "Sleeping" not in err
        # One timestamped in-loop line + the final (untimestamped) summary line.
        assert err.count("Status: pending") == 2
        assert err.count("] Status: pending") == 1

    def test_heartbeat_logged_when_quiet_exceeds_interval(self, db: WorkflowDatabase, capsys):
        """When nothing changes but the heartbeat interval elapses, re-log the status."""
        _seed_running(db, "r1")
        orchestrator = _quiet_orchestrator(db)

        # monotonic() is called once per iteration: the 3rd poll crosses the 300s gap.
        with (
            patch.object(orchestrator, "is_complete", side_effect=[False] * 6 + [True]),
            patch("import_orchestrator.engine.orchestrator.time.monotonic", side_effect=[10.0, 20.0, 400.0]),
        ):
            orchestrator.run_until_complete()

        err = capsys.readouterr().err
        # Two timestamped in-loop lines (first change + one heartbeat) + final summary.
        assert err.count("] Status: pending") == 2
        assert err.count("Status: pending") == 3


class TestStatistics:
    def test_statistics_has_no_triggered_key(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        """The statistics projection uses RequestState values (no 'triggered')."""
        _seed_running(db, "r1")
        stats = orchestrator.view.statistics()
        assert set(stats) == {"pending", "running", "releasing", "success", "failed"}
        assert stats["running"] == 1


def _quiet_orchestrator(db: WorkflowDatabase) -> ImportOrchestrator:
    """An orchestrator whose monitors/trigger are no-ops, so statistics stay stable."""
    trigger = MagicMock()
    trigger.trigger_next_batch.return_value = 0
    return ImportOrchestrator(
        view=RequestView(db, skip_release=False),
        trigger=trigger,
        pipeline_monitor=MagicMock(),
        release_monitor=MagicMock(),
        poll_interval=0,
        max_retries=3,
    )


def _pruning_orchestrator(db: WorkflowDatabase, mock_kube: MagicMock) -> ImportOrchestrator:
    """An orchestrator wired with a real PruneMonitor over the mock cluster."""
    mock_kube.create_pipelinerun.return_value = "pnc-import-xxx"
    mock_kube.delete_request_resources.return_value = PruneOutcome.DELETED
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
        prune_monitor=PruneMonitor(view=view, kube=mock_kube, max_retries=3),
    )


class TestPruneWiring:
    def test_prune_completed_is_noop_without_monitor(self, orchestrator: ImportOrchestrator, db: WorkflowDatabase):
        """An orchestrator with no monitor wired does not prune."""
        request = _seed_success(db, "r1")
        orchestrator.prune_completed()
        assert db.get_request(request.id).pruned_at is None

    def test_run_until_complete_prunes_already_complete_request(self, db: WorkflowDatabase, mock_kube: MagicMock):
        """A resume where everything already succeeded still prunes via the final sweep."""
        request = _seed_success(db, "r1")
        orchestrator = _pruning_orchestrator(db, mock_kube)

        assert orchestrator.run_until_complete() == 0

        mock_kube.delete_request_resources.assert_called_once_with(releases={"rel-r1"}, snapshots=set())
        assert db.get_request(request.id).pruned_at is not None

    def test_run_until_complete_prunes_request_that_succeeds_mid_run(self, db: WorkflowDatabase, mock_kube: MagicMock):
        _seed_request(db, "r1")
        mock_kube.get_pipelinerun_status.return_value = PipelineRunStatus(name="pnc-import-xxx", status="True")
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snapshot-abc"
        mock_kube.find_release_for_snapshot.return_value = "release-abc"
        mock_kube.get_release_status.return_value = "True"

        orchestrator = _pruning_orchestrator(db, mock_kube)

        assert orchestrator.run_until_complete() == 0
        request = db.list_requests()[0]
        assert request.pruned_at is not None
        mock_kube.delete_request_resources.assert_called_with(releases={"release-abc"}, snapshots={"snapshot-abc"})

    def test_persistent_prune_failure_warns_and_still_exits_zero(self, db: WorkflowDatabase, mock_kube, capsys):
        _seed_success(db, "r1")
        orchestrator = _pruning_orchestrator(db, mock_kube)
        mock_kube.delete_request_resources.return_value = PruneOutcome.RETRY  # every prune attempt fails

        assert orchestrator.run_until_complete() == 0  # exit code tracks FAILED requests, not prune
        assert db.list_requests()[0].pruned_at is None
        assert "could not be pruned" in capsys.readouterr().err

    def test_prune_runs_after_release_update_before_trigger(self, db: WorkflowDatabase):
        view = RequestView(db, skip_release=False)
        trigger = MagicMock()
        trigger.trigger_next_batch.return_value = 0
        prune_monitor = MagicMock()
        orchestrator = ImportOrchestrator(
            view=view,
            trigger=trigger,
            pipeline_monitor=MagicMock(),
            release_monitor=MagicMock(),
            poll_interval=0,
            max_retries=3,
            prune_monitor=prune_monitor,
        )
        manager = MagicMock()
        manager.attach_mock(orchestrator._release_monitor.update_statuses, "release")
        manager.attach_mock(prune_monitor.prune_completed, "prune")
        manager.attach_mock(trigger.trigger_next_batch, "trigger")

        # One loop iteration, then complete: is_complete() is called at the top, after
        # the batch, and at the next loop check.
        with patch.object(orchestrator, "is_complete", side_effect=[False, True, True]):
            orchestrator.run_until_complete()

        order = [name for name, *_ in manager.mock_calls]
        assert order.index("release") < order.index("prune") < order.index("trigger")


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
        mock_kube.find_intake_pipelinerun_statuses_by_fingerprint.return_value = [
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

    def test_empty_release_list_does_not_rerelease_a_retried_success(self, db: WorkflowDatabase, mock_kube):
        """A success that took a release retry must survive an empty Release list.

        Rebuilding that history drops the Release phases, the Request looks
        AWAITING_RELEASE, and the monitor creates a duplicate.
        """
        request = _seed_request(db, "r1")
        mock_kube.find_intake_pipelinerun_statuses_by_fingerprint.return_value = [
            PipelineRunStatus(name="pr-ok", status="True")
        ]
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-1"
        mock_kube.get_snapshot_test_status.return_value = "passed"
        mock_kube.get_snapshot.return_value = {}
        mock_kube.list_releases_for_snapshot_and_plan.return_value = [
            ("rel-2", "True"),
            ("rel-1", "False"),
        ]
        Reconciler(view=RequestView(db, skip_release=False), kube=mock_kube).reconcile_all()
        assert db.attempt_count(request.id) == 2

        mock_kube.list_releases_for_snapshot_and_plan.return_value = []
        mock_kube.create_release.return_value = "rel-new"

        exit_code = self._orchestrator(db, mock_kube).run_until_complete()

        assert exit_code == 0
        assert db.attempt_count(request.id) == 2
        assert db.latest_phase_map(request.id)[Phase.RELEASE].reference_id == "rel-2"
        mock_kube.create_release.assert_not_called()
        mock_kube.create_pipelinerun.assert_not_called()

    def test_skipped_request_is_not_released_while_a_sibling_runs(self, db: WorkflowDatabase, mock_kube):
        """A batch-mate keeps the loop alive. The skipped Request must not get a new Release."""
        awaiting = _seed_awaiting_release(db, "stuck", "snap-a")
        running = _seed_running(db, "sibling")

        def find_intake(fingerprint: str):
            if fingerprint == Fingerprint.from_request(awaiting).hash():
                raise requests.HTTPError("503")
            return [PipelineRunStatus(name="pr-sibling", status="Unknown")]

        mock_kube.find_intake_pipelinerun_statuses_by_fingerprint.side_effect = find_intake
        mock_kube.get_pipelinerun_status.return_value = PipelineRunStatus(name="pr-sibling", status="True")
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snap-b"
        mock_kube.get_snapshot.return_value = {}
        mock_kube.get_snapshot_test_status.return_value = "passed"
        mock_kube.find_release_for_snapshot.return_value = None
        mock_kube.create_release.return_value = "rel-b"
        mock_kube.get_release_status.return_value = "True"

        exit_code = self._orchestrator(db, mock_kube).run_until_complete()

        assert exit_code == 0
        created = [call.args[0] for call in mock_kube.create_release.call_args_list]
        assert created == ["snap-b"]
        assert Phase.RELEASE not in db.latest_phase_map(awaiting.id)
        assert db.latest_phase_map(running.id)[Phase.RELEASE].status == PhaseStatus.SUCCESS

    def test_reconcile_failure_does_not_trigger_duplicate(self, db: WorkflowDatabase, mock_kube):
        """If the cluster cannot be read during reconcile, the Request is skipped this run:
        it must not be triggered, or a transient error would duplicate in-flight work."""
        _seed_request(db, "r1")
        mock_kube.find_intake_pipelinerun_statuses_by_fingerprint.side_effect = requests.HTTPError("503")

        exit_code = self._orchestrator(db, mock_kube).run_until_complete()

        assert exit_code == 0
        mock_kube.create_pipelinerun.assert_not_called()

    def test_reconciler_is_optional(self, orchestrator: ImportOrchestrator, mock_kube):
        """The default orchestrator fixture has no reconciler; it must still run."""
        assert orchestrator.run_until_complete() == 0
        mock_kube.find_intake_pipelinerun_statuses_by_fingerprint.assert_not_called()
