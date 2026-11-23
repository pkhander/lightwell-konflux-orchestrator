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

from unittest.mock import patch

import pytest
import yaml

from import_orchestrator.cli import make_parser
from import_orchestrator.constants import FINGERPRINT_LABEL
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.engine import ImportOrchestrator
from import_orchestrator.models import ClusterResource, Fingerprint, Phase, PhaseStatus, RequestState


@pytest.mark.parametrize(
    ("ecosystem", "import_command", "options"),
    [
        ("java", "import-file", ["--artifact-type", "REMEDIATED"]),
        ("java", "import-file", ["--release-plan", "custom-plan"]),
        ("java", "import-manifest", ["--artifact-type", "VALIDATED"]),
        ("java", "import-manifest", ["--release-plan", "custom-plan"]),
        ("python-sdist", "import-file", ["--target", "MIRROR-STAGE"]),
        ("python-sdist", "import-file", ["--target", "SCRATCH"]),
        ("python-sdist", "import-file", ["--release-plan", "custom-plan"]),
        ("javascript", "import-file", ["--release-plan", "custom-plan"]),
        ("javascript", "import-file", ["--source-git-url", "https://example.com/recipes.git"]),
    ],
)
def test_batch_identity_matches_invocation(ecosystem, import_command, options, tmp_path, monkeypatch):
    for name in ["LIGHTWELL_ARTIFACT_TYPE", "LIGHTWELL_PYTHON_SDIST_TARGET", "LIGHTWELL_JAVASCRIPT_TARGET"]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("LIGHTWELL_JAVASCRIPT_SOURCE_GIT_URL", raising=False)
    monkeypatch.setenv("PIPELINE_GIT_URL", "https://example.com/orchestrator")
    monkeypatch.setenv("PIPELINE_GIT_REVISION", "a" * 40)
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    for name in ["pnc-import", "python-sdist-ingest", "javascript-validated-build"]:
        pipeline = tmp_path / "pipelines" / name / f"{name}.yaml"
        pipeline.parent.mkdir(parents=True)
        pipeline.write_text("spec:\n  tasks: []\n")
    if ecosystem == "python-sdist":
        # build_pipelinerun resolves canonical_source.type against calunga-python at
        # trigger time (LWLP-4827); this test doesn't exercise that resolution, so
        # stub it to the sdist route rather than letting it attempt a real clone
        # against the fake BUILD_CONFIGS_REPO_URL another test module leaves set.
        monkeypatch.setattr(
            "import_orchestrator.ecosystems.python_sdist.ecosystem.resolve_canonical_source",
            lambda package, version: {"type": "pypi-sdist"},
        )

    refs = (
        [f"quay.io/example/pkg:build-{n}@sha256:{str(n) * 64}" for n in [1, 2]]
        if ecosystem == "java"
        else ["bluebird==3.7.2", "other==1.0"]
    )
    source = tmp_path / "input.txt"
    if import_command == "import-manifest":
        source.write_text(yaml.safe_dump({"libraries": [{"output": {"artifact": {"digest": ref}}} for ref in refs]}))
    else:
        source.write_text("\n".join(refs))
    parser = make_parser()
    db_path = tmp_path / "batch.db"
    prefix = ["--db", str(db_path), ecosystem]
    imported = parser.parse_args(prefix + [import_command, str(source)])
    assert imported.func(imported) == 0
    selected_ids = []

    def complete_batch(orchestrator):
        view = orchestrator.view
        requests = view.requests()
        assert [request.artifact for request in requests] == refs
        selected_ids.append({request.id for request in requests})
        assert view.skip_release == (current_options == ["--target", "SCRATCH"])
        for request in requests:
            identity = args.ecosystem.request_identity(request.artifact, args)
            assert all(getattr(request, key) == value for key, value in identity.items())
        assert orchestrator._release_monitor.release_plan == args.release_plan
        kube.reset_mock()
        assert orchestrator.trigger_next_batch() == (len(refs) if should_build else 0)
        assert kube.create_pipelinerun.call_count == (len(refs) if should_build else 0)
        for request, call in zip(requests, kube.create_pipelinerun.call_args_list):
            manifest = call.args[0]
            labels = manifest["metadata"]["labels"]
            assert labels["appstudio.openshift.io/application"] == request.application
            assert labels[FINGERPRINT_LABEL] == Fingerprint.from_request(request).hash()
            if ecosystem == "javascript":
                params = {param["name"]: param["value"] for param in manifest["spec"]["params"]}
                assert params["git-url"] == request.source_repo
            attempt = view.db.latest_attempt(request.id)
            view.db.upsert_phase(attempt.id, Phase.INTAKE, PhaseStatus.SUCCESS, ClusterResource.PIPELINE_RUN, "built")
            view.db.upsert_phase(attempt.id, Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS)
            if not view.skip_release:
                view.db.upsert_phase(
                    attempt.id, Phase.RELEASE, PhaseStatus.SUCCESS, ClusterResource.RELEASE, "released"
                )
        assert view.is_complete(3)
        assert view.statistics()[RequestState.SUCCESS.value] == len(refs)
        return 0

    with (
        patch("import_orchestrator.commands.orchestrate.KubeClient") as kube_cls,
        patch.object(ImportOrchestrator, "run_until_complete", autospec=True, side_effect=complete_batch),
    ):
        kube = kube_cls.return_value
        kube.create_pipelinerun.return_value = "new-build"
        for current_options, should_build in [(options, True), (options, False), ([], True), ([], False)]:
            args = parser.parse_args(prefix + ["orchestrate", "--max-parallel", "5", *current_options])
            assert args.func(args) == 0

    assert selected_ids[0] == selected_ids[1]
    assert selected_ids[2] == selected_ids[3] == {1, 2}
    assert selected_ids[0].isdisjoint(selected_ids[2])
    with WorkflowDatabase(db_path) as db:
        assert db.count_requests() == 2 * len(refs)
        assert all(db.attempt_count(request.id) == 1 for request in db.list_requests())
