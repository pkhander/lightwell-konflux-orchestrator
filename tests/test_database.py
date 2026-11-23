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

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from import_orchestrator import database as database_module
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.models import AttemptPhase, ClusterResource, Phase, PhaseStatus

REQUEST_FIELDS = {
    "namespace": "lightwell-poc-tenant",
    "application": "java-remediated",
    "component": "java-remediated",
    "pipeline": "import-pnc",
    "release_plan": "java-remediated-release-prod",
    "artifact": "quay.io/light-castle/java-secure@sha256:abc",
    "target": "remediated",
}


@pytest.fixture
def store(tmp_path: Path):
    """Provide a fresh store backed by a temp-file database for each test."""
    db_path = tmp_path / "test.db"
    with WorkflowDatabase(db_path) as workflow_store:
        yield workflow_store


def _make_request(store: WorkflowDatabase, **overrides):
    fields = {**REQUEST_FIELDS, **overrides}
    return store.get_or_create_request(**fields)


class TestRequests:
    def test_different_source_repository_creates_new_request(self, store):
        first, _ = _make_request(store, source_repo="https://example.com/original.git")
        second, inserted = _make_request(store, source_repo="https://example.com/other.git")
        assert inserted
        assert second.id != first.id
        assert store.get_request(second.id).source_repo == "https://example.com/other.git"
        again, inserted = _make_request(store, source_repo=first.source_repo)
        assert not inserted
        assert again.id == first.id

    def test_get_or_create_inserts_new(self, store: WorkflowDatabase):
        request, was_inserted = _make_request(store)
        assert was_inserted is True
        assert request.id is not None
        assert request.artifact == REQUEST_FIELDS["artifact"]
        assert request.target == "remediated"
        assert request.created_at is not None

    def test_get_or_create_duplicate_returns_existing(self, store: WorkflowDatabase):
        first, inserted1 = _make_request(store)
        second, inserted2 = _make_request(store)
        assert inserted1 is True
        assert inserted2 is False
        assert first.id == second.id

    def test_different_target_creates_new_request(self, store: WorkflowDatabase):
        remediated, _ = _make_request(store, target="remediated")
        novel, inserted = _make_request(store, target="novel")
        assert inserted is True
        assert novel.id != remediated.id

    @pytest.mark.parametrize("field", list(REQUEST_FIELDS))
    def test_varying_any_fingerprint_field_creates_new_request(self, store: WorkflowDatabase, field: str):
        base, inserted_base = _make_request(store)
        assert inserted_base is True
        # Changing any one of the seven fingerprint fields is a distinct Request.
        varied, inserted = _make_request(store, **{field: REQUEST_FIELDS[field] + "-x"})
        assert inserted is True
        assert varied.id != base.id
        # The original 7-tuple still dedupes to the original row.
        again, inserted_again = _make_request(store)
        assert inserted_again is False
        assert again.id == base.id

    def test_get_request(self, store: WorkflowDatabase):
        created, _ = _make_request(store)
        assert created.id is not None
        fetched = store.get_request(created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_get_request_not_found(self, store: WorkflowDatabase):
        assert store.get_request(999) is None

    def test_list_requests(self, store: WorkflowDatabase):
        _make_request(store, target="remediated")
        _make_request(store, target="novel")
        requests = store.list_requests()
        assert len(requests) == 2
        assert [r.target for r in requests] == ["remediated", "novel"]


class TestPruning:
    def test_new_request_is_not_pruned(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.pruned_at is None
        assert store.get_request(request.id).pruned_at is None

    def test_mark_pruned_stamps_timezone_aware_utc_timestamp(self, store: WorkflowDatabase):
        request, _ = _make_request(store)

        store.mark_pruned(request.id)

        pruned_at = store.get_request(request.id).pruned_at
        assert pruned_at is not None
        assert pruned_at.tzinfo is not None
        assert pruned_at.utcoffset() == timedelta(0)
        # list_requests surfaces the same value the prune monitor reads each cycle.
        assert store.list_requests()[0].pruned_at == pruned_at

    def test_mark_pruned_is_idempotent(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        store.mark_pruned(request.id)
        first = store.get_request(request.id).pruned_at
        store.mark_pruned(request.id)
        second = store.get_request(request.id).pruned_at
        assert first is not None and second is not None

    def test_created_at_is_timezone_aware_utc(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.created_at is not None
        # Timestamps are stored as tz-aware UTC, not naive local time.
        assert request.created_at.tzinfo is not None
        assert request.created_at.utcoffset() == timedelta(0)


class TestAttempts:
    def test_create_and_get_attempt(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        attempt = store.create_attempt(request.id)
        assert attempt.id is not None
        assert attempt.request_id == request.id
        assert attempt.created_at is not None
        assert store.get_attempt(attempt.id) is not None

    def test_get_attempt_not_found(self, store: WorkflowDatabase):
        assert store.get_attempt(999) is None

    def test_list_attempts_in_order(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        first = store.create_attempt(request.id)
        second = store.create_attempt(request.id)
        attempts = store.list_attempts(request.id)
        assert [a.id for a in attempts] == [first.id, second.id]

    def test_latest_attempt(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        store.create_attempt(request.id)
        second = store.create_attempt(request.id)
        latest = store.latest_attempt(request.id)
        assert latest is not None
        assert latest.id == second.id

    def test_latest_attempt_none_when_no_attempts(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        assert store.latest_attempt(request.id) is None


class TestAttemptPhases:
    def _attempt_id(self, store: WorkflowDatabase) -> int:
        request, _ = _make_request(store)
        assert request.id is not None
        attempt = store.create_attempt(request.id)
        assert attempt.id is not None
        return attempt.id

    def test_upsert_inserts_phase(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        phase = store.upsert_phase(
            attempt_id,
            Phase.INTAKE,
            PhaseStatus.RUNNING,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id="pnc-import-xyz",
        )
        assert phase.id is not None
        assert phase.name is Phase.INTAKE
        assert phase.status is PhaseStatus.RUNNING
        assert phase.reference_type is ClusterResource.PIPELINE_RUN
        assert phase.reference_id == "pnc-import-xyz"
        assert phase.created_at is not None
        assert phase.updated_at is not None

    def test_upsert_advances_existing_phase(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        created = store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.RUNNING)
        advanced = store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.SUCCESS)

        assert advanced.id == created.id  # same row, not a duplicate
        assert advanced.status is PhaseStatus.SUCCESS
        assert advanced.updated_at is not None
        assert created.created_at is not None
        assert advanced.updated_at >= created.created_at
        assert len(store.list_phases(attempt_id)) == 1

    def test_upsert_writes_omitted_reference_as_absent(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        store.upsert_phase(
            attempt_id,
            Phase.INTAKE,
            PhaseStatus.RUNNING,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id="pnc-import-xyz",
        )
        advanced = store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.SUCCESS)
        assert advanced.reference_type is None
        assert advanced.reference_id is None

    def test_upsert_updates_reference_and_error_on_existing_phase(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.PENDING)
        # Advancing an existing phase while supplying new reference/error values
        # overwrites those fields.
        advanced = store.upsert_phase(
            attempt_id,
            Phase.INTAKE,
            PhaseStatus.FAILED,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id="pnc-import-xyz",
            error_message="PipelineRun failed",
        )
        assert advanced.reference_type is ClusterResource.PIPELINE_RUN
        assert advanced.reference_id == "pnc-import-xyz"
        assert advanced.error_message == "PipelineRun failed"

    def test_upsert_captures_error_message(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        phase = store.upsert_phase(
            attempt_id,
            Phase.RELEASE,
            PhaseStatus.FAILED,
            error_message="Release release-abc failed",
        )
        assert phase.status is PhaseStatus.FAILED
        assert phase.error_message == "Release release-abc failed"

    def test_upsert_clears_stale_error_on_success(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.FAILED, error_message="boom")
        advanced = store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.SUCCESS)
        assert advanced.status is PhaseStatus.SUCCESS
        assert advanced.error_message is None

    def test_insert_success_ignores_error_message(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        # A non-failed phase carries no error even if one is mistakenly supplied.
        phase = store.upsert_phase(attempt_id, Phase.SNAPSHOT, PhaseStatus.SUCCESS, error_message="ignored")
        assert phase.error_message is None

    def test_upsert_writes_omitted_error_message_as_absent(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        store.upsert_phase(attempt_id, Phase.RELEASE, PhaseStatus.FAILED, error_message="first")
        advanced = store.upsert_phase(attempt_id, Phase.RELEASE, PhaseStatus.FAILED)
        assert advanced.error_message is None

    def test_upsert_skipped_status(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        phase = store.upsert_phase(attempt_id, Phase.INTEGRATION_TESTS, PhaseStatus.SKIPPED)
        assert phase.status is PhaseStatus.SKIPPED
        assert phase.error_message is None

    def test_updated_at_strictly_bumped_on_update(self, store: WorkflowDatabase, monkeypatch):
        # Freeze a strictly-increasing clock so the assertion is deterministic.
        class _Clock:
            base = datetime(2026, 1, 1, 0, 0, 0)
            calls = 0

            @classmethod
            def now(cls, tz=None):
                cls.calls += 1
                return cls.base + timedelta(seconds=cls.calls)

            @staticmethod
            def fromisoformat(value):
                return datetime.fromisoformat(value)

        monkeypatch.setattr(database_module, "datetime", _Clock)
        attempt_id = self._attempt_id(store)
        created = store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.RUNNING)
        advanced = store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.SUCCESS)
        assert created.created_at is not None and advanced.updated_at is not None
        assert advanced.updated_at > created.created_at
        assert advanced.created_at == created.created_at  # created_at unchanged by update

    def test_reference_type_without_id_is_rejected(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        with pytest.raises(ValueError, match="supplied together"):
            store.upsert_phase(
                attempt_id, Phase.INTAKE, PhaseStatus.RUNNING, reference_type=ClusterResource.PIPELINE_RUN
            )

    def test_reference_id_without_type_is_rejected(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        with pytest.raises(ValueError, match="supplied together"):
            store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.RUNNING, reference_id="pnc-import-xyz")

    def test_update_replaces_reference_id(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        store.upsert_phase(
            attempt_id,
            Phase.INTAKE,
            PhaseStatus.RUNNING,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id="pnc-import-xyz",
        )
        # Re-advancing the same phase with a new reference replaces both halves
        # together; no stale id survives.
        advanced = store.upsert_phase(
            attempt_id,
            Phase.INTAKE,
            PhaseStatus.SUCCESS,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id="pnc-import-abc",
        )
        assert advanced.reference_type is ClusterResource.PIPELINE_RUN
        assert advanced.reference_id == "pnc-import-abc"

    def test_reference_type_must_match_phase(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        # An intake phase may only point at a pipelineRun, never a snapshot.
        with pytest.raises(ValueError, match="intake phase must reference a pipelineRun"):
            store.upsert_phase(
                attempt_id,
                Phase.INTAKE,
                PhaseStatus.RUNNING,
                reference_type=ClusterResource.SNAPSHOT,
                reference_id="snap-abc",
            )

    def test_reference_type_matches_each_phase(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        # The allowed (phase, reference_type) pairings all succeed.
        store.upsert_phase(attempt_id, Phase.SNAPSHOT, PhaseStatus.SUCCESS, ClusterResource.SNAPSHOT, "snap-abc")
        store.upsert_phase(
            attempt_id, Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS, ClusterResource.PIPELINE_RUN, "it-xyz"
        )
        store.upsert_phase(attempt_id, Phase.RELEASE, PhaseStatus.RUNNING, ClusterResource.RELEASE, "rel-abc")
        names = {p.name: p.reference_type for p in store.list_phases(attempt_id)}
        assert names[Phase.SNAPSHOT] is ClusterResource.SNAPSHOT
        assert names[Phase.INTEGRATION_TESTS] is ClusterResource.PIPELINE_RUN
        assert names[Phase.RELEASE] is ClusterResource.RELEASE

    def test_get_phase_not_found(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        assert store.get_phase(attempt_id, Phase.SNAPSHOT) is None

    def test_list_phases_in_order(self, store: WorkflowDatabase):
        attempt_id = self._attempt_id(store)
        store.upsert_phase(attempt_id, Phase.INTAKE, PhaseStatus.SUCCESS)
        store.upsert_phase(attempt_id, Phase.SNAPSHOT, PhaseStatus.SUCCESS)
        store.upsert_phase(attempt_id, Phase.RELEASE, PhaseStatus.RUNNING)
        phases = store.list_phases(attempt_id)
        assert [p.name for p in phases] == [Phase.INTAKE, Phase.SNAPSHOT, Phase.RELEASE]


class TestCreateAttemptWithPhases:
    """The store's atomic seeding primitive. Deciding *which* phases to carry is
    the caller's job (the phases whose status.carries_forward is true); here we only
    verify the store writes whatever phases it is handed, verbatim and in one shot."""

    def _settled_intake(self, store: WorkflowDatabase, request_id: int) -> AttemptPhase:
        first = store.create_attempt(request_id)
        assert first.id is not None
        return store.upsert_phase(
            first.id,
            Phase.INTAKE,
            PhaseStatus.SUCCESS,
            reference_type=ClusterResource.PIPELINE_RUN,
            reference_id="pnc-import-xyz",
        )

    def test_without_phases_is_a_bare_attempt(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        attempt = store.create_attempt(request.id)
        assert attempt.id is not None
        assert store.list_phases(attempt.id) == []

    def test_seeds_given_phases_under_new_attempt(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        source = self._settled_intake(store, request.id)

        seeded = store.create_attempt(request.id, initial_phases=[source])

        assert seeded.id is not None
        phases = {p.name: p for p in store.list_phases(seeded.id)}
        assert set(phases) == {Phase.INTAKE}
        assert phases[Phase.INTAKE].status is PhaseStatus.SUCCESS
        assert phases[Phase.INTAKE].reference_id == "pnc-import-xyz"
        # The seeded row belongs to the new attempt, not the source one.
        assert phases[Phase.INTAKE].attempt_id == seeded.id

    def test_preserves_original_timestamps(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        source = self._settled_intake(store, request.id)

        seeded = store.create_attempt(request.id, initial_phases=[source])
        assert seeded.id is not None

        # Seeded phases keep their original timestamps rather than being restamped,
        # so history stays accurate across a retry.
        copied = store.get_phase(seeded.id, Phase.INTAKE)
        assert copied is not None
        assert copied.created_at == source.created_at
        assert copied.updated_at == source.updated_at

    def test_leaves_source_attempt_intact(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        source = self._settled_intake(store, request.id)

        store.create_attempt(request.id, initial_phases=[source])

        source_phases = store.list_phases(source.attempt_id)
        assert len(source_phases) == 1
        assert source_phases[0].status is PhaseStatus.SUCCESS

    def test_seeds_multiple_phases(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        first = store.create_attempt(request.id)
        assert first.id is not None
        intake = store.upsert_phase(first.id, Phase.INTAKE, PhaseStatus.SUCCESS, ClusterResource.PIPELINE_RUN, "pr-1")
        snapshot = store.upsert_phase(first.id, Phase.SNAPSHOT, PhaseStatus.SUCCESS, ClusterResource.SNAPSHOT, "snap-1")

        seeded = store.create_attempt(request.id, initial_phases=[intake, snapshot])

        assert seeded.id is not None
        names = {p.name for p in store.list_phases(seeded.id)}
        assert names == {Phase.INTAKE, Phase.SNAPSHOT}

    def test_rolls_back_when_a_phase_insert_fails(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert request.id is not None
        duplicate = AttemptPhase(id=None, attempt_id=1, name=Phase.INTAKE, status=PhaseStatus.SUCCESS)

        # Two phases with the same name violate UNIQUE(attempt_id, name) on the
        # second insert; the whole transaction (attempt included) must roll back.
        with pytest.raises(sqlite3.IntegrityError):
            store.create_attempt(request.id, initial_phases=[duplicate, duplicate])

        assert store.list_attempts(request.id) == []


class TestStoreInfrastructure:
    def test_creates_parent_directories(self, tmp_path: Path):
        nested_path = tmp_path / "deep" / "nested" / "dir" / "test.db"
        with WorkflowDatabase(nested_path) as store:
            _, was_inserted = _make_request(store)
            assert was_inserted is True

    def test_data_persists_across_reopen(self, tmp_path: Path):
        db_path = tmp_path / "persist.db"
        with WorkflowDatabase(db_path) as store:
            request, _ = _make_request(store)
            assert request.id is not None
            attempt = store.create_attempt(request.id)
            assert attempt.id is not None
            store.upsert_phase(attempt.id, Phase.INTAKE, PhaseStatus.SUCCESS)

        # Reopen the same file in a fresh connection: the graph is still there
        # and the request dedupes instead of being re-created.
        with WorkflowDatabase(db_path) as store:
            again, inserted = _make_request(store)
            assert inserted is False
            assert again.id == request.id
            latest = store.latest_attempt(request.id)
            assert latest is not None
            phase = store.get_phase(latest.id, Phase.INTAKE)
            assert phase is not None
            assert phase.status is PhaseStatus.SUCCESS

    def test_parse_timestamp_handles_none(self, store: WorkflowDatabase):
        assert store._parse_timestamp(None) is None

    def test_parse_timestamp_handles_valid_iso(self, store: WorkflowDatabase):
        result = store._parse_timestamp("2026-01-15T10:30:00")
        assert result is not None
        assert result.year == 2026

    def test_parse_timestamp_handles_invalid_string(self, store: WorkflowDatabase):
        assert store._parse_timestamp("not-a-date") is None


class TestDerivedViews:
    def test_latest_phase_map_empty_without_attempt(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        assert store.latest_phase_map(request.id) == {}

    def test_latest_phase_map_uses_latest_attempt(self, store: WorkflowDatabase):
        request, _ = _make_request(store)
        first = store.create_attempt(request.id)
        store.upsert_phase(first.id, Phase.INTAKE, PhaseStatus.FAILED, error_message="old")
        second = store.create_attempt(request.id)
        store.upsert_phase(second.id, Phase.INTAKE, PhaseStatus.RUNNING)

        phase_map = store.latest_phase_map(request.id)
        assert phase_map[Phase.INTAKE].status is PhaseStatus.RUNNING

    def test_count_requests(self, store: WorkflowDatabase):
        assert store.count_requests() == 0
        _make_request(store)
        _make_request(store, artifact="quay.io/light-castle/java-secure@sha256:def")
        assert store.count_requests() == 2
