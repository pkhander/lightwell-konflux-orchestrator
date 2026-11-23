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
from import_orchestrator.engine import ReleaseMonitor, RequestView
from import_orchestrator.engine.errors import ReleaseError
from import_orchestrator.models import ClusterResource, Fingerprint, Phase, PhaseStatus, Request, RequestState


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


def _seed(
    db: WorkflowDatabase,
    ref: str,
    *,
    snapshot: str | None = None,
    release: str | None = None,
) -> Request:
    """Seed a Request whose latest attempt has a successful intake, and optionally
    a discovered snapshot and/or a tracked (RUNNING) release."""
    request, _ = db.get_or_create_request(**_identity(ref))
    attempt = db.create_attempt(request.id)
    db.upsert_phase(
        attempt.id,
        Phase.INTAKE,
        PhaseStatus.SUCCESS,
        reference_type=ClusterResource.PIPELINE_RUN,
        reference_id=f"pr-{ref}",
    )
    if snapshot is not None:
        db.upsert_phase(
            attempt.id,
            Phase.SNAPSHOT,
            PhaseStatus.SUCCESS,
            reference_type=ClusterResource.SNAPSHOT,
            reference_id=snapshot,
        )
    if release is not None:
        db.upsert_phase(
            attempt.id,
            Phase.RELEASE,
            PhaseStatus.RUNNING,
            reference_type=ClusterResource.RELEASE,
            reference_id=release,
        )
    return request


def _phase(db: WorkflowDatabase, request: Request, name: Phase):
    return db.latest_phase_map(request.id).get(name)


@pytest.fixture
def db(tmp_path: Path):
    db_path = tmp_path / "test.db"
    with WorkflowDatabase(db_path) as database:
        yield database


@pytest.fixture
def mock_kube():
    m = MagicMock(spec=KubeClient)
    # Default to passing integration tests so release-path tests exercise the
    # release logic; the ITS gate itself is covered by TestIntegrationTestGate.
    m.get_snapshot_test_status.return_value = "passed"
    m.get_snapshot_test_failure_detail.return_value = None
    # Release-failure enrichment is opt-in per test: default to no extra detail and
    # no resolvable application so the generic fallback path is exercised unless a
    # test sets these explicitly.
    m.get_release_failure_detail.return_value = None
    m.get_snapshot_application.return_value = None
    m.release_url.side_effect = lambda app, name: (
        f"https://konflux-ui.example/ns/lightwell-poc-tenant/applications/{app}/releases/{name}"
    )
    return m


@pytest.fixture
def monitor(db: WorkflowDatabase, mock_kube: MagicMock):
    return ReleaseMonitor(RequestView(db, skip_release=False), mock_kube, max_parallel=5, prefix="pnc-import-")


class TestDiscoverSnapshot:
    """Test snapshot discovery phase."""

    def test_discovers_and_records_snapshot(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """Verify that the snapshot is discovered and recorded on the snapshot phase."""
        request = _seed(db, "r1")
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snapshot-123"

        monitor.update_statuses()

        snapshot = _phase(db, request, Phase.SNAPSHOT)
        assert snapshot.status == PhaseStatus.SUCCESS
        assert snapshot.reference_id == "snapshot-123"
        assert _phase(db, request, Phase.RELEASE) is None  # not yet created

    def test_waits_when_snapshot_not_found(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """Verify that monitor waits when snapshot is not yet available."""
        request = _seed(db, "r1")
        mock_kube.find_snapshot_by_pipelinerun.return_value = None

        monitor.update_statuses()

        assert _phase(db, request, Phase.SNAPSHOT) is None


class TestFindOrCreateRelease:
    """Test release finding/creation phase."""

    def test_records_integration_tests_as_skipped_when_no_scenario(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """When no scenario applies, the integration-tests phase is SKIPPED and the
        workflow still proceeds to release (preserving pre-gate behaviour)."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = "none"
        mock_kube.find_release_for_snapshot.return_value = "release-456"

        monitor.update_statuses()

        it = _phase(db, request, Phase.INTEGRATION_TESTS)
        assert it is not None
        assert it.status == PhaseStatus.SKIPPED
        assert _phase(db, request, Phase.RELEASE).reference_id == "release-456"

    def test_finds_existing_release(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """Verify that an existing release is adopted and tracked."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.find_release_for_snapshot.return_value = "release-456"

        monitor.update_statuses()

        release = _phase(db, request, Phase.RELEASE)
        assert release.status == PhaseStatus.RUNNING
        assert release.reference_id == "release-456"

    def test_creates_release_when_not_found(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """Verify that a new release is created when none exists."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.find_release_for_snapshot.return_value = None
        mock_kube.find_release_plan_for_snapshot.return_value = "release-plan-default"
        mock_kube.create_release.return_value = "release-789"

        monitor.update_statuses()

        release = _phase(db, request, Phase.RELEASE)
        assert release.reference_id == "release-789"
        mock_kube.create_release.assert_called_once_with(
            "snapshot-123",
            "release-plan-default",
            "pnc-import-",
            fingerprint=Fingerprint.from_request(request).hash(),
        )

    def test_defers_when_capacity_full(self, db: WorkflowDatabase, mock_kube: MagicMock):
        """Verify that release creation is deferred when max_parallel is reached."""
        monitor = ReleaseMonitor(RequestView(db, skip_release=False), mock_kube, max_parallel=5, prefix="pnc-import-")
        # 5 active releases already tracked.
        for i in range(5):
            _seed(db, f"active{i}", snapshot=f"snapshot-{i}", release=f"release-{i}")
        # One more needs a release.
        new_request = _seed(db, "new", snapshot="snapshot-new")

        mock_kube.find_release_for_snapshot.return_value = None
        mock_kube.get_release_status.return_value = "Unknown"

        monitor.update_statuses()

        mock_kube.create_release.assert_not_called()
        assert _phase(db, new_request, Phase.RELEASE) is None

    def test_waits_when_release_plan_not_found(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """Verify that monitor waits when ReleasePlan is not found."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.find_release_for_snapshot.return_value = None
        mock_kube.find_release_plan_for_snapshot.return_value = None

        monitor.update_statuses()

        mock_kube.create_release.assert_not_called()
        assert _phase(db, request, Phase.RELEASE) is None

    def test_waits_when_release_creation_fails(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """Verify that monitor waits when release creation fails."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.find_release_for_snapshot.return_value = None
        mock_kube.find_release_plan_for_snapshot.return_value = "release-plan-default"
        mock_kube.create_release.return_value = None  # Creation failed

        monitor.update_statuses()

        assert _phase(db, request, Phase.RELEASE) is None

    def test_records_failed_phase_when_release_creation_raises(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """A rejected create call (e.g. a permissions gap) records a bounded,
        retriable FAILED release phase instead of leaving the request stuck in
        AWAITING_RELEASE forever with no state change (LWLP-3937)."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.find_release_for_snapshot.return_value = None
        mock_kube.find_release_plan_for_snapshot.return_value = "release-plan-default"
        mock_kube.create_release.side_effect = ReleaseError("403 Forbidden")

        monitor.update_statuses()

        release = _phase(db, request, Phase.RELEASE)
        assert release is not None
        assert release.status == PhaseStatus.FAILED
        assert "403 Forbidden" in release.error_message


class TestReleasePlanOverride:
    """Test the explicit --release-plan override path."""

    @pytest.fixture
    def pinned_monitor(self, db: WorkflowDatabase, mock_kube: MagicMock):
        """A monitor pinned to the novel-stage ReleasePlan."""
        return ReleaseMonitor(
            RequestView(db, skip_release=False),
            mock_kube,
            max_parallel=5,
            prefix="pnc-import-",
            release_plan="pnc-import-java-pulp-novel-stage",
        )

    def test_creates_release_against_pinned_plan(
        self, pinned_monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """The override plan is used directly; the ambiguous lookup is skipped."""
        request = _seed(db, "r1", snapshot="snapshot-stage-123")

        mock_kube.find_release_for_snapshot_and_plan.return_value = None
        mock_kube.create_release.return_value = "release-novel-1"
        mock_kube.get_release_status.return_value = "Unknown"

        pinned_monitor.update_statuses()

        # Snapshot-based plan resolution must be bypassed entirely.
        mock_kube.find_release_plan_for_snapshot.assert_not_called()
        mock_kube.create_release.assert_called_once_with(
            "snapshot-stage-123",
            "pnc-import-java-pulp-novel-stage",
            "pnc-import-",
            fingerprint=Fingerprint.from_request(request).hash(),
        )
        assert _phase(db, request, Phase.RELEASE).reference_id == "release-novel-1"

    def test_existing_lookup_is_scoped_to_pinned_plan(
        self, pinned_monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """The 'already released?' check is scoped to the pinned plan.

        A Release created against a different stage plan (e.g. validated-stage) for
        the same snapshot must not be mistaken for the novel-stage release.
        """
        _seed(db, "r1", snapshot="snapshot-stage-123")

        # No novel-stage release exists yet (even though other plans may have one).
        mock_kube.find_release_for_snapshot_and_plan.return_value = None
        mock_kube.create_release.return_value = "release-novel-1"
        mock_kube.get_release_status.return_value = "Unknown"

        pinned_monitor.update_statuses()

        mock_kube.find_release_for_snapshot_and_plan.assert_called_once_with(
            "snapshot-stage-123", "pnc-import-java-pulp-novel-stage"
        )
        # The unscoped lookup must not be used when a plan is pinned.
        mock_kube.find_release_for_snapshot.assert_not_called()

    def test_adopts_existing_pinned_release(
        self, pinned_monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """An existing Release against the pinned plan is adopted, not recreated."""
        request = _seed(db, "r1", snapshot="snapshot-stage-123")

        mock_kube.find_release_for_snapshot_and_plan.return_value = "release-novel-existing"
        mock_kube.get_release_status.return_value = "Unknown"

        pinned_monitor.update_statuses()

        mock_kube.create_release.assert_not_called()
        assert _phase(db, request, Phase.RELEASE).reference_id == "release-novel-existing"


class TestCheckReleaseCompletion:
    """Test release completion checking phase."""

    def test_marks_success_when_release_succeeds(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """Verify the release phase is marked SUCCESS when the release succeeds."""
        request = _seed(db, "r1", snapshot="snapshot-123", release="release-456")
        mock_kube.get_release_status.return_value = "True"

        monitor.update_statuses()

        assert _phase(db, request, Phase.RELEASE).status == PhaseStatus.SUCCESS
        assert monitor.view.state(request) == RequestState.SUCCESS

    def test_marks_failed_when_release_fails(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """Verify the release phase is marked FAILED when the release fails."""
        request = _seed(db, "r1", snapshot="snapshot-123", release="release-456")
        mock_kube.get_release_status.return_value = "False"

        monitor.update_statuses()

        release = _phase(db, request, Phase.RELEASE)
        assert release.status == PhaseStatus.FAILED
        assert release.error_message == "Release release-456 failed"
        assert monitor.view.state(request) == RequestState.FAILED

    def test_failure_detail_is_recorded_and_logged(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock, capsys: pytest.CaptureFixture[str]
    ):
        """A Release failure detail replaces the opaque message and is printed."""
        request = _seed(db, "r1", snapshot="snapshot-123", release="release-456")
        mock_kube.get_release_status.return_value = "False"
        mock_kube.get_release_failure_detail.return_value = "Failed - managed pipeline failed"

        monitor.update_statuses()

        assert _phase(db, request, Phase.RELEASE).error_message == "Failed - managed pipeline failed"
        assert "Failed - managed pipeline failed" in capsys.readouterr().err

    def test_release_url_logged_when_application_known(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock, capsys: pytest.CaptureFixture[str]
    ):
        """A failed Release logs a Konflux UI link built from the snapshot's application."""
        _seed(db, "r1", snapshot="snapshot-123", release="release-456")
        mock_kube.get_release_status.return_value = "False"
        mock_kube.get_snapshot_application.return_value = "python-sdist-mirror"

        monitor.update_statuses()

        mock_kube.get_snapshot_application.assert_called_once_with("snapshot-123")
        mock_kube.release_url.assert_called_once_with("python-sdist-mirror", "release-456")
        assert "applications/python-sdist-mirror/releases/release-456" in capsys.readouterr().err

    def test_no_url_logged_when_application_unknown(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """When the application label can't be read, no link is built (failure still logged)."""
        _seed(db, "r1", snapshot="snapshot-123", release="release-456")
        mock_kube.get_release_status.return_value = "False"
        mock_kube.get_snapshot_application.return_value = None

        monitor.update_statuses()

        mock_kube.release_url.assert_not_called()

    def test_waits_when_release_still_running(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """Verify that monitor waits when release is still running."""
        request = _seed(db, "r1", snapshot="snapshot-123", release="release-456")
        mock_kube.get_release_status.return_value = "Unknown"

        monitor.update_statuses()

        assert _phase(db, request, Phase.RELEASE).status == PhaseStatus.RUNNING

    def test_skip_release_never_creates_release(self, db: WorkflowDatabase, mock_kube: MagicMock):
        """A skip_release monitor runs snapshot + ITS but never creates a Release."""
        monitor = ReleaseMonitor(RequestView(db, skip_release=True), mock_kube, max_parallel=5, prefix="pnc-import-")
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = "passed"

        monitor.update_statuses()

        assert _phase(db, request, Phase.INTEGRATION_TESTS).status == PhaseStatus.SUCCESS
        assert _phase(db, request, Phase.RELEASE) is None
        mock_kube.find_release_for_snapshot.assert_not_called()
        mock_kube.create_release.assert_not_called()


class TestIntegrationTestGate:
    """Test the integration-tests gate between snapshot and release."""

    def test_pending_tests_block_release(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """While tests are pending the ITS phase is RUNNING and no release is created."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = "pending"

        monitor.update_statuses()

        assert _phase(db, request, Phase.INTEGRATION_TESTS).status == PhaseStatus.RUNNING
        assert _phase(db, request, Phase.RELEASE) is None
        mock_kube.find_release_for_snapshot.assert_not_called()
        mock_kube.create_release.assert_not_called()

    def test_failed_tests_block_release(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """A test failure fails the ITS phase (and the request) and blocks release."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = "failed"

        monitor.update_statuses()

        its = _phase(db, request, Phase.INTEGRATION_TESTS)
        assert its.status == PhaseStatus.FAILED
        assert its.error_message
        assert _phase(db, request, Phase.RELEASE) is None
        assert monitor.view.state(request) == RequestState.FAILED
        mock_kube.create_release.assert_not_called()

    def test_failure_detail_is_recorded_on_the_phase(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """When the kube client can summarise the failure, it is stored as the phase error."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = "failed"
        mock_kube.get_snapshot_test_failure_detail.return_value = "ec: TestFail - boom (ec-plr-1)"

        monitor.update_statuses()

        assert _phase(db, request, Phase.INTEGRATION_TESTS).error_message == "ec: TestFail - boom (ec-plr-1)"

    def test_passed_tests_allow_release(self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock):
        """Passing tests record ITS SUCCESS and let the release be created."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = "passed"
        mock_kube.find_release_for_snapshot.return_value = "release-456"

        monitor.update_statuses()

        assert _phase(db, request, Phase.INTEGRATION_TESTS).status == PhaseStatus.SUCCESS
        assert _phase(db, request, Phase.RELEASE).reference_id == "release-456"

    def test_unreadable_status_blocks_and_does_not_record(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """If the Snapshot can't be read this cycle, nothing is recorded and we retry."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = None

        monitor.update_statuses()

        assert _phase(db, request, Phase.INTEGRATION_TESTS) is None
        assert _phase(db, request, Phase.RELEASE) is None
        mock_kube.create_release.assert_not_called()

    def test_scratch_flow_passes_and_is_terminal(self, db: WorkflowDatabase, mock_kube: MagicMock):
        """Scratch (skip_release) build: snapshot -> ITS pass -> terminal SUCCESS, no release."""
        monitor = ReleaseMonitor(RequestView(db, skip_release=True), mock_kube, max_parallel=5, prefix="pnc-import-")
        request = _seed(db, "r1")
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snapshot-123"
        mock_kube.get_snapshot_test_status.return_value = "passed"

        monitor.update_statuses()  # discover snapshot
        monitor.update_statuses()  # run ITS gate

        assert _phase(db, request, Phase.INTEGRATION_TESTS).status == PhaseStatus.SUCCESS
        assert _phase(db, request, Phase.RELEASE) is None
        assert monitor.view.state(request) == RequestState.SUCCESS

    def test_failed_its_is_terminal_for_releasing_target(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """A non-skip request whose ITS already FAILED is terminal: not re-polled, no release."""
        request = _seed(db, "r1", snapshot="snapshot-123")
        attempt = db.latest_attempt(request.id)
        db.upsert_phase(attempt.id, Phase.INTEGRATION_TESTS, PhaseStatus.FAILED, error_message="tests failed")

        monitor.update_statuses()

        # Terminal, so the gate never re-reads status and no release is attempted.
        mock_kube.get_snapshot_test_status.assert_not_called()
        mock_kube.create_release.assert_not_called()
        assert _phase(db, request, Phase.RELEASE) is None
        assert monitor.view.state(request) == RequestState.FAILED

    def test_scratch_flow_failure_is_terminal(self, db: WorkflowDatabase, mock_kube: MagicMock):
        """Scratch build whose ITS fails ends FAILED, with no release."""
        monitor = ReleaseMonitor(RequestView(db, skip_release=True), mock_kube, max_parallel=5, prefix="pnc-import-")
        request = _seed(db, "r1", snapshot="snapshot-123")
        mock_kube.get_snapshot_test_status.return_value = "failed"

        monitor.update_statuses()

        assert _phase(db, request, Phase.INTEGRATION_TESTS).status == PhaseStatus.FAILED
        assert _phase(db, request, Phase.RELEASE) is None
        assert monitor.view.state(request) == RequestState.FAILED


class TestIntegration:
    """Test complete flow through all phases."""

    def test_complete_flow_from_intake_to_success(
        self, monitor: ReleaseMonitor, db: WorkflowDatabase, mock_kube: MagicMock
    ):
        """Verify complete flow: discover snapshot, create release, mark success."""
        request = _seed(db, "r1")

        # Poll 1: Discover snapshot
        mock_kube.find_snapshot_by_pipelinerun.return_value = "snapshot-123"
        monitor.update_statuses()
        assert _phase(db, request, Phase.SNAPSHOT).reference_id == "snapshot-123"

        # Poll 2: Create release
        mock_kube.find_release_for_snapshot.return_value = None
        mock_kube.find_release_plan_for_snapshot.return_value = "release-plan-default"
        mock_kube.create_release.return_value = "release-456"
        monitor.update_statuses()
        assert _phase(db, request, Phase.RELEASE).reference_id == "release-456"

        # Poll 3: Release completes
        mock_kube.get_release_status.return_value = "True"
        monitor.update_statuses()
        assert monitor.view.state(request) == RequestState.SUCCESS
