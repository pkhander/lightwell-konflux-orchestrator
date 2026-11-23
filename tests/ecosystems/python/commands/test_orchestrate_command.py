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
from unittest.mock import patch

import pytest

from import_orchestrator.cli import main, make_parser
from import_orchestrator.constants import DEFAULT_MAX_PARALLEL, FINGERPRINT_LABEL
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.ecosystems.python import config
from import_orchestrator.engine import ImportOrchestrator
from import_orchestrator.models import ClusterResource, Fingerprint, Phase, PhaseStatus, RequestState


class TestOrchestrateArgParsing:
    def test_defaults(self):
        parser = make_parser()
        args = parser.parse_args(["python", "orchestrate"])
        assert args.command == "orchestrate"
        assert args.max_parallel == DEFAULT_MAX_PARALLEL

    def test_max_parallel_override(self):
        parser = make_parser()
        args = parser.parse_args(["python", "orchestrate", "--max-parallel", "5"])
        assert args.max_parallel == 5

    def test_no_artifact_type_flag(self):
        parser = make_parser()
        args = parser.parse_args(["python", "orchestrate"])
        assert not hasattr(args, "artifact_type")

    def test_target_default(self, monkeypatch):
        monkeypatch.delenv("LIGHTWELL_PYTHON_TARGET", raising=False)
        parser = make_parser()
        args = parser.parse_args(["python", "orchestrate"])
        assert args.target == "REMEDIATED"

    def test_release_plan_defaults_to_none(self):
        parser = make_parser()
        args = parser.parse_args(["python", "orchestrate"])
        assert args.release_plan is None

    def test_release_plan_override(self):
        parser = make_parser()
        args = parser.parse_args(["python", "orchestrate", "--release-plan", "remediated-build-prod"])
        assert args.release_plan == "remediated-build-prod"


class TestOrchestrateEmptyDbWarning:
    def test_empty_database_prints_python_warning(self, monkeypatch, tmp_path: Path, capsys):
        monkeypatch.setattr(
            "sys.argv",
            ["prog", "--db", str(tmp_path / "test.db"), "python", "orchestrate"],
        )

        orchestrator_mod = __import__("import_orchestrator.engine.orchestrator", fromlist=["ImportOrchestrator"])
        with (
            patch("import_orchestrator.commands.orchestrate.KubeClient"),
            patch.object(orchestrator_mod.ImportOrchestrator, "run_until_complete", return_value=0),
        ):
            exit_code = main()

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "No package references in database" in captured.err
        assert "python import-file" in captured.err


@pytest.mark.parametrize(
    ("first_options", "second_options"),
    [
        (
            ["--lightwell-builds-repo-base", "https://example.com/a"],
            ["--lightwell-builds-repo-base", "https://example.com/b"],
        ),
        (
            ["--lightwell-builds-repo-url", "https://example.com/a.git"],
            ["--lightwell-builds-repo-url", "https://example.com/b.git"],
        ),
        ([], ["--target", "VALIDATED-STAGE"]),
        ([], ["--target", "SCRATCH"]),
        ([], ["--release-plan", "another-plan"]),
    ],
)
def test_import_then_orchestrate_reconciles_identity(first_options, second_options, tmp_path, monkeypatch):
    monkeypatch.delenv("LIGHTWELL_PYTHON_TARGET", raising=False)
    monkeypatch.setenv("BUILD_CONFIGS_REPO_URL", "https://example.com/build-configs.git")
    pipeline = tmp_path / "pipelines" / "python-build" / "python-build.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    refs = ["ntplib==0.4.0", "pypdf==1.0"]
    packages = tmp_path / "packages.txt"
    packages.write_text("\n".join(refs))
    db_path = tmp_path / "batch.db"
    parser = make_parser()

    def invoke(command, options):
        if command == "orchestrate":
            options = ["--max-parallel", "5", *options]
        args = parser.parse_args(["--db", str(db_path), "python", command, *options])
        assert args.func(args) == 0
        return args

    invoke("import-file", [str(packages)])
    selected_ids = []

    def complete_batch(orchestrator):
        view = orchestrator.view
        requests = view.requests()
        assert [request.artifact for request in requests] == refs
        assert view.skip_release == (current_options == ["--target", "SCRATCH"])
        expected_args = parser.parse_args(["python", "orchestrate", *current_options])
        for request in requests:
            identity = expected_args.ecosystem.request_identity(request.artifact, expected_args)
            assert all(getattr(request, key) == value for key, value in identity.items())
        selected_ids.append([request.id for request in requests])
        kube.reset_mock()
        assert orchestrator.trigger_next_batch() == (len(refs) if expected_new else 0)
        assert kube.create_pipelinerun.call_count == (len(refs) if expected_new else 0)
        for request, call in zip(requests, kube.create_pipelinerun.call_args_list):
            manifest = call.args[0]
            params = {param["name"]: param["value"] for param in manifest["spec"]["params"]}
            assert params["LIGHTWELL_BUILDS_REPO_URL"] == request.source_repo
            assert manifest["metadata"]["labels"]["appstudio.openshift.io/application"] == request.application
            assert manifest["metadata"]["labels"][FINGERPRINT_LABEL] == Fingerprint.from_request(request).hash()
            attempt = view.db.latest_attempt(request.id)
            view.db.upsert_phase(attempt.id, Phase.INTAKE, PhaseStatus.SUCCESS, ClusterResource.PIPELINE_RUN, "built")
            view.db.upsert_phase(attempt.id, Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS)
            if not view.skip_release:
                view.db.upsert_phase(
                    attempt.id, Phase.RELEASE, PhaseStatus.SUCCESS, ClusterResource.RELEASE, "released"
                )
        assert view.is_complete(max_retries=3)
        assert view.statistics()[RequestState.SUCCESS.value] == len(refs)
        return 0

    with (
        patch("import_orchestrator.commands.orchestrate.KubeClient") as kube_cls,
        patch.object(ImportOrchestrator, "run_until_complete", autospec=True, side_effect=complete_batch),
    ):
        kube = kube_cls.return_value
        kube.create_pipelinerun.return_value = "new-build"
        for current_options, expected_new in [
            (first_options, True),
            (second_options, True),
            (second_options, False),
            (first_options, False),
        ]:
            invoke("orchestrate", current_options)

    assert set(selected_ids[0]).isdisjoint(selected_ids[1])
    assert selected_ids[1] == selected_ids[2]
    assert selected_ids[0] == selected_ids[3]
    with WorkflowDatabase(db_path) as db:
        requests = db.list_requests()
        expected_count = len(refs) * (3 if first_options else 2)
        assert len(requests) == expected_count
        for request in requests:
            expected_attempts = 1 if request.id in selected_ids[0] + selected_ids[1] else 0
            assert db.attempt_count(request.id) == expected_attempts
        if first_options:
            assert all(db.attempt_count(request.id) == 0 for request in requests[: len(refs)])
            assert all(request.target == config.DEFAULT_TARGET for request in requests[: len(refs)])


def test_print_resources_uses_reconciled_requests(tmp_path):
    packages = tmp_path / "packages.txt"
    packages.write_text("ntplib==0.4.0\n")
    parser = make_parser()
    prefix = ["--db", str(tmp_path / "batch.db"), "python"]
    args = parser.parse_args(prefix + ["import-file", str(packages)])
    assert args.func(args) == 0
    args = parser.parse_args(
        prefix + ["orchestrate", "--lightwell-builds-repo-base", "https://example.com/a", "--print-resources"]
    )
    with (
        patch("import_orchestrator.commands.orchestrate.KubeClient") as kube,
        patch("import_orchestrator.commands.orchestrate._run_print_resources", return_value=0) as preview,
    ):
        assert args.func(args) == 0
        view = preview.call_args.args[1]
        assert view.request_ids == {2}
        kube.assert_not_called()
