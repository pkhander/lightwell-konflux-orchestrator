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

import pytest

from import_orchestrator.cli import make_parser
from import_orchestrator.database import WorkflowDatabase
from import_orchestrator.ecosystems.python import config
from import_orchestrator.engine.request_view import RequestView
from import_orchestrator.models import ClusterResource, Phase, PhaseStatus, RequestState


@pytest.fixture(params=["run", "trigger", "orchestrate"])
def command(request, monkeypatch):
    monkeypatch.setenv("BUILD_CONFIGS_REPO_URL", "https://example.com/build-configs.git")
    return ["python", request.param] + ([] if request.param == "orchestrate" else ["ntplib==0.4.0"])


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ([], f"{config.LIGHTWELL_BUILDS_REPO_BASE}/pypi.org-ntplib"),
        (["--lightwell-builds-repo-url", "https://example.com/custom.git"], "https://example.com/custom.git"),
        (["--lightwell-builds-repo-base", "https://example.com/test"], "https://example.com/test/pypi.org-ntplib"),
        (["--lightwell-builds-repo-base", "https://example.com/test/"], "https://example.com/test/pypi.org-ntplib"),
    ],
)
def test_repository_options_reach_manifest(command, options, expected, tmp_path, monkeypatch):
    pipeline = tmp_path / "pipelines" / "python-build" / "python-build.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    args = make_parser().parse_args(command + options)
    manifest = args.ecosystem.build_pipelinerun("ntplib==0.4.0", args)
    params = {param["name"]: param["value"] for param in manifest["spec"]["params"]}
    assert params["LIGHTWELL_BUILDS_REPO_URL"] == expected
    assert args.ecosystem.request_identity("ntplib==0.4.0", args)["source_repo"] == expected
    assert params["LIGHTWELL_BUILDS_TAG"] == "ntplib/0.4.0"
    assert args.builds_tag is None


def test_repository_options_are_mutually_exclusive(command):
    with pytest.raises(SystemExit):
        make_parser().parse_args(
            command
            + [
                "--lightwell-builds-repo-url",
                "https://example.com/custom.git",
                "--lightwell-builds-repo-base",
                "https://example.com/test",
            ]
        )


def test_batch_repository_base_selects_each_package(command, tmp_path, monkeypatch):
    pipeline = tmp_path / "pipelines" / "python-build" / "python-build.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    args = make_parser().parse_args(command + ["--lightwell-builds-repo-base", "https://example.com/test"])
    for package in ["ntplib", "pypdf"]:
        manifest = args.ecosystem.build_pipelinerun(f"{package}==0.4.0", args)
        params = {param["name"]: param["value"] for param in manifest["spec"]["params"]}
        assert params["LIGHTWELL_BUILDS_REPO_URL"] == f"https://example.com/test/pypi.org-{package}"
        assert (
            args.ecosystem.request_identity(f"{package}==0.4.0", args)["source_repo"]
            == params["LIGHTWELL_BUILDS_REPO_URL"]
        )


@pytest.mark.parametrize(
    "options",
    [
        ["--lightwell-builds-repo-url", "https://example.com/custom.git"],
        ["--lightwell-builds-repo-base", "https://example.com/test"],
    ],
)
def test_changed_source_does_not_reuse_completed_request(command, options, tmp_path):
    original_args = make_parser().parse_args(command)
    changed_args = make_parser().parse_args(command + options)
    ref = "ntplib==0.4.0"
    with WorkflowDatabase(tmp_path / "shared.db") as db:
        original, _ = db.get_or_create_request(**original_args.ecosystem.request_identity(ref, original_args))
        attempt = db.create_attempt(original.id)
        db.upsert_phase(attempt.id, Phase.INTAKE, PhaseStatus.SUCCESS, ClusterResource.PIPELINE_RUN, "old-build")
        db.upsert_phase(attempt.id, Phase.RELEASE, PhaseStatus.SUCCESS, ClusterResource.RELEASE, "old-release")

        identity = changed_args.ecosystem.request_identity(ref, changed_args)
        changed, inserted = db.get_or_create_request(**identity)
        assert inserted
        assert changed.id != original.id
        view = RequestView(db, skip_release=False)
        assert view.state(original) == RequestState.SUCCESS
        assert view.state(changed) == RequestState.PENDING
        repeated, inserted = db.get_or_create_request(**identity)
        assert not inserted
        assert repeated.id == changed.id


def test_equivalent_repository_options_reuse_request(command, tmp_path):
    options = [
        [],
        ["--lightwell-builds-repo-base", config.LIGHTWELL_BUILDS_REPO_BASE + "/"],
        ["--lightwell-builds-repo-url", f"{config.LIGHTWELL_BUILDS_REPO_BASE}/pypi.org-ntplib"],
    ]
    with WorkflowDatabase(tmp_path / "shared.db") as db:
        requests = []
        for option in options:
            args = make_parser().parse_args(command + option)
            request, inserted = db.get_or_create_request(**args.ecosystem.request_identity("ntplib==0.4.0", args))
            assert inserted == (not requests)
            requests.append(request)
        assert len({request.id for request in requests}) == 1


@pytest.mark.parametrize("flag", ["--builds-tag", "--builds-ref"])
@pytest.mark.parametrize("revision", ["test-branch", "release/1.0", "a" * 40])
def test_revision_aliases_reach_manifest(command, flag, revision, tmp_path, monkeypatch):
    pipeline = tmp_path / "pipelines" / "python-build" / "python-build.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text("spec:\n  tasks: []\n")
    monkeypatch.setenv("TEKTON_PIPELINE_DIR", str(tmp_path))
    args = make_parser().parse_args(
        command + ["--lightwell-builds-repo-base", "https://example.com/test", flag, revision]
    )
    assert args.builds_tag == revision
    manifest = args.ecosystem.build_pipelinerun("ntplib==0.4.0", args)
    params = {param["name"]: param["value"] for param in manifest["spec"]["params"]}
    assert params["LIGHTWELL_BUILDS_REPO_URL"] == "https://example.com/test/pypi.org-ntplib"
    assert params["LIGHTWELL_BUILDS_TAG"] == revision
