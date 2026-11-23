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

import argparse
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from import_orchestrator.commands.run import run_single
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.engine import ImportOrchestrator, PipelineMonitor, PruneMonitor, Reconciler
from import_orchestrator.models import ClusterResource, Phase, PhaseStatus

_REF = "ntplib==0.4.0"


def _identity(ref: str, args) -> dict:
    return {
        "namespace": "lightwell-tenant",
        "application": "app",
        "component": "comp",
        "pipeline": "p",
        "release_plan": "",
        "target": "remediated",
        "artifact": ref,
    }


def _make_eco(**overrides):
    eco = MagicMock()
    eco.namespace = "lightwell-tenant"
    eco.pipelinerun_prefix = "prefix"
    eco.build_pipelinerun.return_value = {"kind": "PipelineRun"}
    eco.request_identity.side_effect = _identity
    for key, value in overrides.items():
        setattr(eco, key, value)
    return eco


def _args(**overrides):
    eco = overrides.pop("ecosystem", None) or _make_eco()
    base = dict(ecosystem=eco, max_retries=3, poll_interval=0, db_explicit=False, db=None, output_json=None)
    base.update(overrides)
    return argparse.Namespace(**base)


def _request_count(db_path: Path) -> int:
    with WorkflowDatabase(db_path) as db:
        return db.count_requests()


def _record_phases(
    db: WorkflowDatabase, status: PhaseStatus, *, pipelinerun=None, snapshot=None, release=None, error=None
):
    """Record the latest attempt's phases for the single seeded request."""
    request = db.list_requests()[0]
    attempt = db.latest_attempt(request.id) or db.create_attempt(request.id)
    if pipelinerun:
        db.upsert_phase(
            attempt.id, Phase.INTAKE, status, ClusterResource.PIPELINE_RUN, pipelinerun, error_message=error
        )
    else:
        db.upsert_phase(attempt.id, Phase.INTAKE, status, error_message=error)
    if snapshot:
        db.upsert_phase(attempt.id, Phase.SNAPSHOT, PhaseStatus.SUCCESS, ClusterResource.SNAPSHOT, snapshot)
        # The integration-tests gate runs after the snapshot; a completed run has it
        # SUCCESS. For skip_release targets this is what makes the request terminal.
        db.upsert_phase(attempt.id, Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS)
    if release:
        db.upsert_phase(attempt.id, Phase.RELEASE, PhaseStatus.SUCCESS, ClusterResource.RELEASE, release)


def _make_success(pipelinerun_name=None, snapshot_name=None, release_name=None):
    """Return a fake run_until_complete that marks the seeded ref's phases SUCCESS."""

    def fake_run(self):
        _record_phases(
            self.view.db,
            PhaseStatus.SUCCESS,
            pipelinerun=pipelinerun_name,
            snapshot=snapshot_name,
            release=release_name,
        )
        return 0

    return fake_run


def _make_failure(error_message):
    """Return a fake run_until_complete that marks the seeded ref's intake FAILED."""

    def fake_run(self):
        _record_phases(self.view.db, PhaseStatus.FAILED, pipelinerun="pr-x", error=error_message)
        return 1

    return fake_run


class TestPruneWiring:
    def test_run_wires_prune_monitor(self, tmp_path, monkeypatch):
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        captured: dict = {}

        def fake_run(self):
            captured["monitor"] = self._prune_monitor
            return 0

        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", fake_run),
        ):
            assert run_single(_args(), _REF) == 0

        monitor = captured["monitor"]
        assert isinstance(monitor, PruneMonitor)
        assert monitor.max_retries == 3


class TestEphemeralDatabase:
    def test_seeds_single_ref_then_cleans_up(self, tmp_path, monkeypatch):
        eph_dir = tmp_path / "eph"
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(eph_dir))

        captured: dict = {}

        def fake_run(self):
            # Capture DB state mid-run, before the ephemeral dir is removed.
            captured["requests"] = self.view.db.count_requests()
            return 0

        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", fake_run),
        ):
            rc = run_single(_args(), _REF)

        assert rc == 0
        assert captured["requests"] == 1  # exactly the one seeded ref
        assert not eph_dir.exists()  # ephemeral DB removed afterward

    def test_exit_code_propagates(self, tmp_path, monkeypatch):
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", lambda self: 1),
        ):
            rc = run_single(_args(), _REF)
        assert rc == 1


class TestPersistentDatabase:
    def test_result_uses_request_for_selected_repository(self, tmp_path, capsys):
        db_path = tmp_path / "shared.db"
        eco = _make_eco()
        eco.request_identity.side_effect = lambda ref, args: {
            **_identity(ref, args),
            "source_repo": args.lightwell_builds_repo_url,
        }

        def complete_selected_request(self):
            request = self.view.pending()[0]
            attempt = self.view.db.create_attempt(request.id)
            self.view.db.upsert_phase(
                attempt.id, Phase.INTAKE, PhaseStatus.SUCCESS, ClusterResource.PIPELINE_RUN, request.source_repo
            )
            self.view.db.upsert_phase(
                attempt.id, Phase.RELEASE, PhaseStatus.SUCCESS, ClusterResource.RELEASE, "release"
            )
            return 0

        for source in ["https://example.com/first.git", "https://example.com/second.git"]:
            with (
                patch("import_orchestrator.commands.run.KubeClient"),
                patch.object(ImportOrchestrator, "run_until_complete", complete_selected_request),
            ):
                run_single(_args(ecosystem=eco, db_explicit=True, db=db_path, lightwell_builds_repo_url=source), _REF)
            payload = json.loads(capsys.readouterr().out)
            assert payload["pipelinerun_name"] == source
        assert _request_count(db_path) == 2

    def test_explicit_db_seeded_and_retained(self, tmp_path):
        db_path = tmp_path / "ct-123.db"

        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", lambda self: 0),
        ):
            rc = run_single(_args(db_explicit=True, db=db_path), _REF)

        assert rc == 0
        assert db_path.exists()  # persistent DB is not deleted
        assert _request_count(db_path) == 1  # the single ref was seeded

    def test_resume_dedups_existing_ref(self, tmp_path):
        db_path = tmp_path / "ct-123.db"

        for _ in range(2):
            with (
                patch("import_orchestrator.commands.run.KubeClient"),
                patch.object(ImportOrchestrator, "run_until_complete", lambda self: 0),
            ):
                run_single(_args(db_explicit=True, db=db_path), _REF)

        assert _request_count(db_path) == 1  # second run does not duplicate the ref


class TestResultPayload:
    def test_success_payload_to_stdout(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        fake = _make_success(
            pipelinerun_name="python-remediated-build-abc",
            snapshot_name="snapshot-xyz",
            release_name="release-123",
        )
        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", fake),
        ):
            rc = run_single(_args(), _REF)

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload == {
            "ref": _REF,
            "status": "success",
            "pipelinerun_name": "python-remediated-build-abc",
            "snapshot_name": "snapshot-xyz",
            "release_name": "release-123",
            "error_message": None,
            "retry_count": 0,
        }

    def test_failure_payload_to_stdout(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", _make_failure("pipeline exploded")),
        ):
            rc = run_single(_args(), _REF)

        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["status"] == "failed"
        assert payload["error_message"] == "pipeline exploded"
        assert payload["ref"] == _REF

    def test_output_json_writes_matching_file(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        out_path = tmp_path / "result.json"
        fake = _make_success(pipelinerun_name="pr-1", snapshot_name="snap-1", release_name="rel-1")
        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", fake),
        ):
            run_single(_args(output_json=str(out_path)), _REF)

        stdout_payload = json.loads(capsys.readouterr().out)
        assert out_path.exists()
        assert json.loads(out_path.read_text()) == stdout_payload

    def test_payload_emitted_before_ephemeral_cleanup(self, tmp_path, monkeypatch, capsys):
        eph_dir = tmp_path / "eph"
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(eph_dir))
        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", _make_success(pipelinerun_name="pr-9")),
        ):
            run_single(_args(), _REF)

        # The payload reflects state that only existed in the now-removed DB.
        payload = json.loads(capsys.readouterr().out)
        assert payload["pipelinerun_name"] == "pr-9"
        assert not eph_dir.exists()

    def test_scratch_payload_has_no_release_name(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        eco = _make_eco()
        eco.target_skip_release.return_value = True
        fake = _make_success(pipelinerun_name="scratch-abc", snapshot_name="snap-xyz")
        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", fake),
        ):
            rc = run_single(_args(ecosystem=eco, target="SCRATCH"), _REF)

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["release_name"] is None
        assert payload["status"] == "success"


class TestSkipRelease:
    def _captured_skip_release(self, tmp_path, monkeypatch, args):
        """Run run_single and return the skip_release flag the PipelineMonitor saw."""
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        captured = {}

        real_init = PipelineMonitor.__init__

        def capturing_init(self, view, kube):
            captured["skip_release"] = view.skip_release
            real_init(self, view, kube)

        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", lambda self: 0),
            patch.object(PipelineMonitor, "__init__", capturing_init),
        ):
            run_single(args, _REF)

        return captured["skip_release"]

    def test_non_releasing_target_sets_skip_release(self, tmp_path, monkeypatch):
        eco = _make_eco()
        eco.target_skip_release.return_value = True
        assert self._captured_skip_release(tmp_path, monkeypatch, _args(ecosystem=eco, target="SCRATCH")) is True

    def test_releasing_target_does_not_skip_release(self, tmp_path, monkeypatch):
        eco = _make_eco()
        eco.target_skip_release.return_value = False
        assert self._captured_skip_release(tmp_path, monkeypatch, _args(ecosystem=eco, target="REMEDIATED")) is False

    def test_no_target_does_not_skip_release(self, tmp_path, monkeypatch):
        assert self._captured_skip_release(tmp_path, monkeypatch, _args()) is False

    def test_scopes_reconcile_to_the_single_request(self, tmp_path, monkeypatch):
        monkeypatch.setattr("tempfile.mkdtemp", lambda *a, **k: str(tmp_path / "eph"))
        captured: dict = {}
        real_init = Reconciler.__init__

        def capturing_init(self, view, kube, release_plan=None):
            captured["request_ids"] = view.request_ids
            real_init(self, view, kube, release_plan=release_plan)

        with (
            patch("import_orchestrator.commands.run.KubeClient"),
            patch.object(ImportOrchestrator, "run_until_complete", lambda self: 0),
            patch.object(Reconciler, "__init__", capturing_init),
        ):
            run_single(_args(), _REF)

        assert captured["request_ids"] is not None
        assert len(captured["request_ids"]) == 1
