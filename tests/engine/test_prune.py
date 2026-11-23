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

from import_orchestrator.clients import KubeClient
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.engine import PruneMonitor, RequestView
from import_orchestrator.models import ClusterResource, Phase, PhaseStatus, PruneOutcome, Request

MAX_RETRIES = 3


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
        Phase.SNAPSHOT,
        PhaseStatus.SUCCESS,
        reference_type=ClusterResource.SNAPSHOT,
        reference_id=f"snap-{ref}",
    )
    db.upsert_phase(
        attempt.id,
        Phase.RELEASE,
        PhaseStatus.SUCCESS,
        reference_type=ClusterResource.RELEASE,
        reference_id=f"rel-{ref}",
    )
    return request


def _seed_failed(db: WorkflowDatabase, ref: str, *, attempts: int = 1) -> Request:
    """Seed a FAILED Request with ``attempts`` attempts; the latest has deletable refs."""
    request = _seed_request(db, ref)
    for _ in range(attempts):
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
            reference_id=f"snap-{ref}",
        )
        db.upsert_phase(
            attempt.id,
            Phase.RELEASE,
            PhaseStatus.FAILED,
            reference_type=ClusterResource.RELEASE,
            reference_id=f"rel-{ref}",
            error_message="boom",
        )
    return request


@pytest.fixture
def db(tmp_path: Path):
    with WorkflowDatabase(tmp_path / "test.db") as database:
        yield database


@pytest.fixture
def mock_kube():
    kube = MagicMock(spec=KubeClient)
    kube.delete_request_resources.return_value = PruneOutcome.DELETED
    return kube


def _monitor(db: WorkflowDatabase, kube: MagicMock, **kwargs) -> PruneMonitor:
    view = RequestView(db, skip_release=kwargs.pop("skip_release", False))
    return PruneMonitor(view=view, kube=kube, max_retries=MAX_RETRIES, **kwargs)


@pytest.fixture
def monitor(db: WorkflowDatabase, mock_kube: MagicMock):
    return _monitor(db, mock_kube)


class TestPruneCompleted:
    def test_prunes_successful_request_by_name(self, monitor, db, mock_kube):
        request = _seed_success(db, "r1")

        assert monitor.prune_completed() == 1

        # Release + Snapshot deleted by name; intake PipelineRun left to the platform pruner.
        mock_kube.delete_request_resources.assert_called_once_with(releases={"rel-r1"}, snapshots={"snap-r1"})
        assert db.get_request(request.id).pruned_at is not None

    def test_prunes_resources_across_all_attempts(self, monitor, db, mock_kube):
        # A retried Request that eventually succeeds: each attempt made its own Snapshot
        # and Release, and all of them must be pruned -- not just the latest attempt's.
        request = _seed_request(db, "retried")
        a1 = db.create_attempt(request.id)
        db.upsert_phase(
            a1.id, Phase.SNAPSHOT, PhaseStatus.SUCCESS, reference_type=ClusterResource.SNAPSHOT, reference_id="snap-a1"
        )
        db.upsert_phase(
            a1.id, Phase.RELEASE, PhaseStatus.FAILED, reference_type=ClusterResource.RELEASE, reference_id="rel-a1"
        )
        a2 = db.create_attempt(request.id)
        db.upsert_phase(
            a2.id, Phase.INTAKE, PhaseStatus.SUCCESS, reference_type=ClusterResource.PIPELINE_RUN, reference_id="pr-a2"
        )
        db.upsert_phase(
            a2.id, Phase.SNAPSHOT, PhaseStatus.SUCCESS, reference_type=ClusterResource.SNAPSHOT, reference_id="snap-a2"
        )
        db.upsert_phase(
            a2.id, Phase.RELEASE, PhaseStatus.SUCCESS, reference_type=ClusterResource.RELEASE, reference_id="rel-a2"
        )

        assert monitor.prune_completed() == 1
        mock_kube.delete_request_resources.assert_called_once_with(
            releases={"rel-a1", "rel-a2"}, snapshots={"snap-a1", "snap-a2"}
        )

    def test_skips_running_request(self, monitor, db, mock_kube):
        _seed_running(db, "r1")

        assert monitor.prune_completed() == 0
        mock_kube.delete_request_resources.assert_not_called()

    def test_skips_retriable_failed_request(self, monitor, db, mock_kube):
        _seed_failed(db, "r1")  # one attempt -> still retriable, so not pruned

        assert monitor.prune_completed() == 0
        mock_kube.delete_request_resources.assert_not_called()

    def test_does_not_reprune_already_pruned_request(self, monitor, db, mock_kube):
        _seed_success(db, "r1")

        assert monitor.prune_completed() == 1
        assert monitor.prune_completed() == 0
        assert mock_kube.delete_request_resources.call_count == 1

    def test_leaves_request_unmarked_on_transient_failure_then_retries(self, monitor, db, mock_kube):
        request = _seed_success(db, "r1")
        mock_kube.delete_request_resources.return_value = PruneOutcome.RETRY

        assert monitor.prune_completed() == 0
        assert db.get_request(request.id).pruned_at is None

        mock_kube.delete_request_resources.return_value = PruneOutcome.DELETED
        assert monitor.prune_completed() == 1
        assert db.get_request(request.id).pruned_at is not None

    def test_prunes_only_completed_requests_in_a_mixed_batch(self, monitor, db, mock_kube):
        _seed_success(db, "done")
        _seed_running(db, "busy")
        _seed_failed(db, "bad")

        assert monitor.prune_completed() == 1
        mock_kube.delete_request_resources.assert_called_once_with(releases={"rel-done"}, snapshots={"snap-done"})

    def test_respects_request_scope(self, db, mock_kube):
        in_scope = _seed_success(db, "in")
        out_of_scope = _seed_success(db, "out")
        view = RequestView(db, skip_release=False, request_ids={in_scope.id})
        monitor = PruneMonitor(view=view, kube=mock_kube, max_retries=MAX_RETRIES)

        assert monitor.prune_completed() == 1
        mock_kube.delete_request_resources.assert_called_once_with(releases={"rel-in"}, snapshots={"snap-in"})
        assert db.get_request(out_of_scope.id).pruned_at is None


class TestForbiddenDisablesPruning:
    def test_403_stops_pruning_for_the_run(self, db, mock_kube, capsys):
        _seed_success(db, "a")
        _seed_success(db, "b")
        mock_kube.delete_request_resources.return_value = PruneOutcome.FORBIDDEN
        monitor = _monitor(db, mock_kube)

        assert monitor.prune_completed() == 0
        # Stopped after the first forbidden delete rather than trying the second request.
        assert mock_kube.delete_request_resources.call_count == 1
        assert "pruning disabled for this run" in capsys.readouterr().err

        # Subsequent cycles are a no-op: no further API calls.
        assert monitor.prune_completed() == 0
        assert mock_kube.delete_request_resources.call_count == 1


class TestFailedRequestPruning:
    def test_prunes_exhausted_failed_request(self, db, mock_kube):
        # A terminal failure (retries exhausted) is pruned like a success -- KubeArchive
        # retains it, and a retry would start a fresh Attempt against new objects.
        request = _seed_failed(db, "dead", attempts=MAX_RETRIES + 1)  # retries exhausted
        monitor = _monitor(db, mock_kube)

        assert monitor.prune_completed() == 1
        mock_kube.delete_request_resources.assert_called_once_with(releases={"rel-dead"}, snapshots={"snap-dead"})
        assert db.get_request(request.id).pruned_at is not None


class TestSkipReleasePruning:
    def test_prunes_snapshot_after_integration_tests(self, db, mock_kube):
        """A skip-release target is terminal-success once its ITS phase passes; it has a
        Snapshot but no Release."""
        request = _seed_request(db, "scratch")
        attempt = db.create_attempt(request.id)
        db.upsert_phase(
            attempt.id,
            Phase.INTAKE,
            PhaseStatus.SUCCESS,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id="pr-scratch",
        )
        db.upsert_phase(
            attempt.id,
            Phase.SNAPSHOT,
            PhaseStatus.SUCCESS,
            reference_type=ClusterResource.SNAPSHOT,
            reference_id="snap-scratch",
        )
        db.upsert_phase(attempt.id, Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS)
        monitor = _monitor(db, mock_kube, skip_release=True)

        assert monitor.prune_completed() == 1
        mock_kube.delete_request_resources.assert_called_once_with(releases=set(), snapshots={"snap-scratch"})
        assert db.get_request(request.id).pruned_at is not None


class TestUnprunedCount:
    def test_counts_terminal_unpruned_requests(self, monitor, db, mock_kube):
        _seed_success(db, "a")
        _seed_running(db, "b")  # not terminal -> not counted
        mock_kube.delete_request_resources.return_value = PruneOutcome.RETRY

        monitor.prune_completed()  # fails to prune "a", leaving it unpruned
        assert monitor.unpruned_count() == 1
