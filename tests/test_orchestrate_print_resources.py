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

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml

from import_orchestrator.commands.orchestrate import (
    _print_release_chain,
    _run_print_resources,
    run_orchestrate,
)
from import_orchestrator.engine.errors import TriggerError
from import_orchestrator.release_data import ReleaseDataError, ReleaseDataResolver

_TENANT = "tenants-config/cluster/stone-prod-p01/tenants"
_MANAGED = "config/stone-prod-p01.wcfb.p1"
_APP = "pnc-import-novel"
_NS = "lightwell-poc-tenant"


def _write(root: Path, rel: str, doc: dict) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(doc, sort_keys=False))


@pytest.fixture
def release_repo(tmp_path: Path) -> Path:
    """A minimal release-data tree that resolves a full RP -> RPA chain."""
    _write(
        tmp_path,
        f"{_TENANT}/lightwell/rp.yaml",
        {
            "kind": "ReleasePlan",
            "metadata": {
                "name": "rp1",
                "namespace": _NS,
                "labels": {"release.appstudio.openshift.io/releasePlanAdmission": "rpa1"},
            },
            "spec": {"application": _APP, "target": "rhtap-releng-tenant"},
        },
    )
    _write(
        tmp_path,
        f"{_MANAGED}/rhtap/rpa.yaml",
        {
            "kind": "ReleasePlanAdmission",
            "metadata": {"name": "rpa1", "namespace": "rhtap-releng-tenant"},
            "spec": {
                "applications": [_APP],
                "origin": _NS,
                "policy": "mrrc-lightwell-poc-novel-prod",
                "pipeline": {
                    "serviceAccountName": "release-pulp-lightwell-prod",
                    "pipelineRef": {"params": [{"name": "pathInRepo", "value": "release.yaml"}]},
                },
                "data": {
                    "intention": "production",
                    "pulp": {"domain": "lightwell", "repository": "novel"},
                },
            },
        },
    )
    return tmp_path


def _manifest(app: str | None = _APP) -> dict:
    labels = {"appstudio.openshift.io/application": app} if app else {}
    return {"metadata": {"namespace": _NS, "labels": labels}}


def _eco(manifest: dict | None = None, *, error: Exception | None = None):
    def build_pipelinerun(ref, args):
        if error is not None:
            raise error
        return manifest if manifest is not None else _manifest()

    return SimpleNamespace(namespace=_NS, build_pipelinerun=build_pipelinerun)


def _args(eco, repo: Path | None) -> argparse.Namespace:
    return argparse.Namespace(ecosystem=eco, release_data_repo=str(repo) if repo else None)


class _FakeView:
    """Minimal RequestView stand-in exposing just ``pending()`` for preview."""

    def __init__(self, pending):
        self._pending = pending

    def pending(self):
        return self._pending


class TestRunPrintResources:
    def test_no_pending_returns_zero(self, capsys):
        rc = _run_print_resources(_args(_eco(), None), _FakeView([]))
        assert rc == 0
        assert "No pending references" in capsys.readouterr().err

    def test_resolver_ensure_error_returns_one(self, capsys):
        view = _FakeView([SimpleNamespace(artifact="quay.io/x@sha256:abc")])
        with patch.object(ReleaseDataResolver, "ensure", side_effect=ReleaseDataError("no repo")):
            rc = _run_print_resources(_args(_eco(), None), view)
        assert rc == 1
        assert "no repo" in capsys.readouterr().err

    def test_happy_path_prints_full_chain(self, release_repo: Path, capsys):
        view = _FakeView([SimpleNamespace(artifact="quay.io/x@sha256:abc")])
        rc = _run_print_resources(_args(_eco(), release_repo), view)
        assert rc == 0
        out = capsys.readouterr().out
        assert "Application:          pnc-import-novel" in out
        assert "ReleasePlan:          rp1" in out
        assert "ReleasePlanAdmission: rpa1" in out
        assert "EC policy:          mrrc-lightwell-poc-novel-prod" in out
        assert "Release pipeline:   release.yaml" in out
        assert "Service account:    release-pulp-lightwell-prod" in out
        assert "Pulp target:        lightwell/novel" in out
        assert "Intention:          production" in out

    def test_build_pipelinerun_error_is_reported_and_skipped(self, release_repo: Path, capsys):
        view = _FakeView([SimpleNamespace(artifact="quay.io/x@sha256:abc")])
        args = _args(_eco(error=TriggerError("bad ref")), release_repo)
        rc = _run_print_resources(args, view)
        assert rc == 0
        assert "ERROR building PipelineRun: bad ref" in capsys.readouterr().err

    def test_missing_application_label_skips_chain(self, release_repo: Path, capsys):
        view = _FakeView([SimpleNamespace(artifact="quay.io/x@sha256:abc")])
        args = _args(_eco(_manifest(app=None)), release_repo)
        rc = _run_print_resources(args, view)
        assert rc == 0
        out = capsys.readouterr().out
        assert "(no application label)" in out
        assert "ReleasePlan:" not in out


class TestPrintReleaseChain:
    def test_unresolved_release_plan(self, tmp_path: Path, capsys):
        _print_release_chain(ReleaseDataResolver(tmp_path), _APP, _NS)
        assert "ReleasePlan:          UNRESOLVED" in capsys.readouterr().out

    def test_unresolved_rpa(self, tmp_path: Path, capsys):
        # RP resolves, but no RPA exists for it.
        _write(
            tmp_path,
            f"{_TENANT}/x/rp.yaml",
            {
                "kind": "ReleasePlan",
                "metadata": {"name": "rp1", "namespace": _NS},
                "spec": {"application": _APP, "target": "rhtap-releng-tenant"},
            },
        )
        _print_release_chain(ReleaseDataResolver(tmp_path), _APP, _NS)
        out = capsys.readouterr().out
        assert "ReleasePlan:          rp1" in out
        assert "ReleasePlanAdmission: UNRESOLVED" in out


class TestRunOrchestratePrintResourcesDispatch:
    def test_print_resources_flag_routes_to_preview(self, tmp_path: Path, capsys):
        args = argparse.Namespace(
            db=tmp_path / "state.db",
            print_resources=True,
            ecosystem=_eco(),
            release_data_repo=None,
        )
        rc = run_orchestrate(args, "empty db hint")
        assert rc == 0
        err = capsys.readouterr().err
        assert "empty db hint" in err
        assert "No pending references" in err


class TestPruneFlag:
    @pytest.mark.parametrize("flag", ["--prune", "--no-prune", "--prune-failed", "--prune-pipelineruns"])
    def test_no_prune_flags_are_accepted(self, flag):
        from import_orchestrator.commands.orchestrate import add_common_orchestrate_args

        parser = argparse.ArgumentParser()
        add_common_orchestrate_args(parser)
        # Pruning is always on and has no flags; any prune switch is rejected.
        with pytest.raises(SystemExit):
            parser.parse_args([flag])

    def test_run_orchestrate_always_wires_prune_monitor(self, tmp_path: Path):
        eco = SimpleNamespace(
            namespace=_NS,
            pipelinerun_prefix="pnc-import-",
            build_pipelinerun=lambda ref, args: _manifest(),
        )
        args = argparse.Namespace(
            db=tmp_path / "state.db",
            print_resources=False,
            ecosystem=eco,
            release_data_repo=None,
            max_parallel=1,
            max_retries=3,
            poll_interval=0,
            release_plan=None,
        )

        with (
            patch("import_orchestrator.commands.orchestrate.KubeClient"),
            patch("import_orchestrator.commands.orchestrate.ImportOrchestrator") as mock_orch,
        ):
            mock_orch.return_value.run_until_complete.return_value = 0
            rc = run_orchestrate(args, "empty db hint")

        assert rc == 0
        assert mock_orch.call_args.kwargs["prune_monitor"] is not None
