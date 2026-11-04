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

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from import_orchestrator.clients import KubeClient
from import_orchestrator.clients.kube_api import KubeAuth
from import_orchestrator.engine.errors import ReleaseError, TriggerError


def _make_kube_client(monkeypatch, token=None, kubearchive_api=""):
    """Create a KubeClient with mocked auth and API layer."""
    if token:
        monkeypatch.setenv("KONFLUX_TOKEN", token)
    else:
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

    mock_api = MagicMock()
    mock_ka_api = MagicMock() if kubearchive_api else None
    with patch("import_orchestrator.clients.kube.resolve_auth") as mock_resolve:
        mock_resolve.return_value = KubeAuth(
            server="https://api.example.com:6443", token=token or "test-token", ca_cert=None
        )
        with patch("import_orchestrator.clients.kube.KubeAPI", return_value=mock_api):
            client = KubeClient(
                namespace="test-ns",
                cluster_api="https://api.example.com:6443",
                kubearchive_api=kubearchive_api,
            )
    client._mock_api = mock_api
    if mock_ka_api is not None:
        client._ka_api = mock_ka_api
        client._mock_ka_api = mock_ka_api
    return client


@pytest.fixture
def kube(monkeypatch):
    """Create a KubeClient with no KONFLUX_TOKEN set."""
    return _make_kube_client(monkeypatch)


@pytest.fixture
def kube_with_ka(monkeypatch):
    """Create a KubeClient with KubeArchive API configured."""
    return _make_kube_client(monkeypatch, kubearchive_api="https://kubearchive.example.com")


class TestApiErrorLogging:
    @pytest.mark.parametrize("status_code", [403, 500])
    @pytest.mark.parametrize(
        "method,args,api_method,context,expected",
        [
            ("get_running_pipelineruns", (), "list", "list PipelineRuns", []),
            ("get_pipelinerun_status", ("pr-1",), "get", "get PipelineRun pr-1", None),
            ("_fetch_resource", ("/taskruns/tr-1",), "get", "get resource /taskruns/tr-1", None),
            ("_pod_log_tail", ("pod-1", "step-build"), "get_text", "pod pod-1, container step-build", None),
            ("find_snapshot_by_pipelinerun", ("pr-1",), "list", "find Snapshot for PipelineRun pr-1", None),
            ("find_release_plan_for_snapshot", ("snap-1",), "get", "get Snapshot snap-1", None),
            ("find_release_plan_for_snapshot", ("snap-1",), "list", "list ReleasePlans for Snapshot snap-1", None),
            ("find_release_for_snapshot", ("snap-1",), "list", "find Release for Snapshot snap-1", None),
            ("get_release_status", ("release-1",), "get", "get Release release-1 status", None),
        ],
    )
    def test_logs_api_message_with_operation_and_namespace(
        self, kube, capsys, status_code, method, args, api_method, context, expected
    ):
        response = requests.Response()
        response.status_code = status_code
        detail = "API rejected the operation\nAdditional server diagnostic"
        response._content = json.dumps({"kind": "Status", "message": detail}).encode()
        kube._mock_api.get.return_value = {"metadata": {"labels": {"appstudio.openshift.io/application": "app-1"}}}
        getattr(kube._mock_api, api_method).side_effect = requests.HTTPError(
            f"{status_code} API error", response=response
        )

        assert getattr(kube, method)(*args) == expected

        stderr = capsys.readouterr().err
        assert detail in stderr
        assert context in stderr
        assert "namespace test-ns" in stderr

    @pytest.mark.parametrize(
        "method,args,api_method",
        [
            ("get_pipelinerun_status", ("pr-1",), "get"),
            ("_fetch_resource", ("/taskruns/tr-1",), "get"),
            ("_pod_log_tail", ("pod-1", "step-build"), "get_text"),
            ("find_release_plan_for_snapshot", ("snap-1",), "get"),
            ("get_release_status", ("release-1",), "get"),
        ],
    )
    def test_expected_missing_resources_remain_quiet(self, kube, capsys, method, args, api_method):
        response = requests.Response()
        response.status_code = 404
        getattr(kube._mock_api, api_method).side_effect = requests.HTTPError("404 Not Found", response=response)

        assert getattr(kube, method)(*args) is None

        assert capsys.readouterr().err == ""

    @pytest.mark.parametrize("method", ["get_pipelinerun_status", "_fetch_resource"])
    def test_archive_failure_is_identified_after_quiet_cluster_404(self, kube_with_ka, capsys, method):
        missing = requests.Response()
        missing.status_code = 404
        kube_with_ka._mock_api.get.side_effect = requests.HTTPError("404 Not Found", response=missing)
        failure = requests.Response()
        failure.status_code = 503
        failure._content = b'{"message": "Archive temporarily unavailable"}'
        kube_with_ka._mock_ka_api.get.side_effect = requests.HTTPError("503 Unavailable", response=failure)

        assert getattr(kube_with_ka, method)("pr-1") is None

        stderr = capsys.readouterr().err
        assert "KubeArchive" in stderr
        assert "Archive temporarily unavailable" in stderr
        assert "404" not in stderr

    def test_release_creation_surfaces_non_json_response_body(self, kube):
        response = requests.Response()
        response.status_code = 502
        response._content = b"Upstream proxy unavailable"
        kube._mock_api.create.side_effect = requests.HTTPError("502 Bad Gateway", response=response)

        with pytest.raises(ReleaseError, match="Upstream proxy unavailable"):
            kube.create_release("snap-1", "plan-1", "release-")

    def test_release_status_logs_connection_error(self, kube, capsys):
        kube._mock_api.get.side_effect = requests.ConnectionError("Connection refused")

        assert kube.get_release_status("release-1") is None

        stderr = capsys.readouterr().err
        assert "release-1" in stderr
        assert "Connection refused" in stderr


class TestKubeClientInit:
    def test_ka_api_is_none_when_not_configured(self, kube: KubeClient):
        assert kube._ka_api is None

    def test_ka_api_is_set_when_configured(self, kube_with_ka: KubeClient):
        assert kube_with_ka._ka_api is not None


class TestGetRunningPipelineRuns:
    def test_returns_only_running(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "pr-1"}, "status": {"conditions": [{"status": "Unknown"}]}},
                {"metadata": {"name": "pr-2"}, "status": {"conditions": [{"status": "True"}]}},
                {"metadata": {"name": "pr-3"}, "status": {"conditions": [{"status": "False"}]}},
                {"metadata": {"name": "pr-4"}, "status": {"conditions": [{"status": "Unknown"}]}},
            ]
        }

        result = kube.get_running_pipelineruns()
        assert len(result) == 2
        assert result[0].name == "pr-1"
        assert result[1].name == "pr-4"

    def test_returns_empty_on_error(self, kube: KubeClient):
        kube._mock_api.list.side_effect = requests.ConnectionError("connection refused")

        result = kube.get_running_pipelineruns()
        assert result == []

    def test_handles_empty_items(self, kube: KubeClient):
        kube._mock_api.list.return_value = {"items": []}

        result = kube.get_running_pipelineruns()
        assert result == []

    def test_skips_items_without_conditions(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "pr-1"}, "status": {"conditions": [{"status": "Unknown"}]}},
                {"metadata": {"name": "pr-2"}, "status": {}},
                {"metadata": {"name": "pr-3"}, "status": {"conditions": [{"status": "Invalid"}]}},
            ]
        }

        result = kube.get_running_pipelineruns()
        assert len(result) == 1
        assert result[0].name == "pr-1"


class TestGetPipelineRunStatus:
    def test_returns_status_from_api(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"status": {"conditions": [{"status": "True"}]}}

        result = kube.get_pipelinerun_status("my-pr")
        assert result is not None
        assert result.name == "my-pr"
        assert result.is_successful is True

    def test_returns_running_status(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"status": {"conditions": [{"status": "Unknown"}]}}

        result = kube.get_pipelinerun_status("my-pr")
        assert result is not None
        assert result.is_running is True

    def test_returns_none_when_no_conditions(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"status": {}}

        result = kube.get_pipelinerun_status("my-pr")
        assert result is None

    def test_falls_back_to_kubearchive(self, kube_with_ka: KubeClient):
        kube_with_ka._mock_api.get.side_effect = requests.HTTPError("404")
        kube_with_ka._mock_ka_api.get.return_value = {
            "status": {"conditions": [{"type": "Succeeded", "status": "True"}]}
        }

        result = kube_with_ka.get_pipelinerun_status("archived-pr")
        assert result is not None
        assert result.name == "archived-pr"
        assert result.is_successful is True

    def test_returns_none_when_both_fail(self, kube_with_ka: KubeClient):
        kube_with_ka._mock_api.get.side_effect = requests.HTTPError("404")
        kube_with_ka._mock_ka_api.get.side_effect = requests.HTTPError("404")

        result = kube_with_ka.get_pipelinerun_status("missing-pr")
        assert result is None

    def test_skips_kubearchive_when_not_configured(self, kube: KubeClient):
        kube._mock_api.get.side_effect = requests.HTTPError("404")

        result = kube.get_pipelinerun_status("missing-pr")
        assert result is None


class TestGetPipelineRunFailureDetail:
    """Test the get_pipelinerun_failure_detail method."""

    def _pr(self, name, *, reason="Failed", message="Tasks Completed: 1 (Failed: 1)", children=()):
        return {
            "status": {
                "conditions": [{"type": "Succeeded", "status": "False", "reason": reason, "message": message}],
                "childReferences": [{"kind": "TaskRun", "name": c} for c in children],
            }
        }

    def _failed_tr(self, *, reason="Failed", message="build step failed", pod_name="pod-1", steps=None):
        return {
            "status": {
                "conditions": [{"type": "Succeeded", "status": "False", "reason": reason, "message": message}],
                "podName": pod_name,
                "steps": steps if steps is not None else [],
            }
        }

    def test_returns_none_when_pipelinerun_missing(self, kube: KubeClient):
        kube._mock_api.get.side_effect = requests.HTTPError("404")

        assert kube.get_pipelinerun_failure_detail("pnc-import-abc") is None

    def test_returns_none_when_no_diagnostics(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"status": {}}

        assert kube.get_pipelinerun_failure_detail("pnc-import-abc") is None

    def test_captures_condition_and_failed_taskrun_with_logs(self, kube: KubeClient):
        pr = self._pr(
            "pnc-import-abc",
            reason="Failed",
            message="Tasks Completed: 1 (Failed: 1)",
            children=["tr-build"],
        )
        tr = self._failed_tr(
            reason="Failed",
            message="TaskRun failed",
            pod_name="pod-abc",
            steps=[
                {"name": "prepare", "container": "step-prepare", "terminated": {"reason": "Completed", "exitCode": 0}},
                {"name": "build", "container": "step-build", "terminated": {"reason": "Error", "exitCode": 1}},
            ],
        )

        def get_side_effect(path):
            if path.endswith("/pipelineruns/pnc-import-abc"):
                return pr
            if path.endswith("/taskruns/tr-build"):
                return tr
            raise requests.HTTPError("404")

        kube._mock_api.get.side_effect = get_side_effect
        kube._mock_api.get_text.return_value = "compiling wheel...\nfatal error: Python.h: No such file\n"

        detail = kube.get_pipelinerun_failure_detail("pnc-import-abc")

        assert detail is not None
        assert "PipelineRun pnc-import-abc: Failed - Tasks Completed: 1 (Failed: 1)" in detail
        assert "TaskRun tr-build: Failed - TaskRun failed" in detail
        assert "step 'build' terminated: reason=Error exitCode=1" in detail
        # The completed step is not reported as a failure.
        assert "step 'prepare'" not in detail
        # The failing step's container logs are tailed in.
        assert "fatal error: Python.h" in detail
        kube._mock_api.get_text.assert_called_once_with(
            "/api/v1/namespaces/test-ns/pods/pod-abc/log",
            container="step-build",
            tailLines=20,
        )

    def test_degrades_when_logs_unavailable(self, kube: KubeClient):
        pr = self._pr("pnc-import-abc", children=["tr-build"])
        tr = self._failed_tr(
            pod_name="pod-abc",
            steps=[{"name": "build", "container": "step-build", "terminated": {"reason": "Error", "exitCode": 1}}],
        )

        def get_side_effect(path):
            if path.endswith("/pipelineruns/pnc-import-abc"):
                return pr
            if path.endswith("/taskruns/tr-build"):
                return tr
            raise requests.HTTPError("404")

        kube._mock_api.get.side_effect = get_side_effect
        kube._mock_api.get_text.side_effect = requests.HTTPError("pod gone")

        detail = kube.get_pipelinerun_failure_detail("pnc-import-abc")

        assert detail is not None
        assert "step 'build' terminated: reason=Error exitCode=1" in detail
        assert "logs (" not in detail

    def test_skips_successful_taskruns(self, kube: KubeClient):
        pr = self._pr("pnc-import-abc", children=["tr-ok"])
        tr_ok = {"status": {"conditions": [{"type": "Succeeded", "status": "True", "reason": "Succeeded"}]}}

        def get_side_effect(path):
            if path.endswith("/pipelineruns/pnc-import-abc"):
                return pr
            if path.endswith("/taskruns/tr-ok"):
                return tr_ok
            raise requests.HTTPError("404")

        kube._mock_api.get.side_effect = get_side_effect

        detail = kube.get_pipelinerun_failure_detail("pnc-import-abc")

        assert detail is not None
        assert "TaskRun tr-ok" not in detail
        assert "PipelineRun pnc-import-abc" in detail

    def test_falls_back_to_kubearchive(self, kube_with_ka: KubeClient):
        pr = self._pr("archived-pr", children=[])
        kube_with_ka._mock_api.get.side_effect = requests.HTTPError("404")
        kube_with_ka._mock_ka_api.get.return_value = pr

        detail = kube_with_ka.get_pipelinerun_failure_detail("archived-pr")

        assert detail is not None
        assert "PipelineRun archived-pr: Failed" in detail

    def test_truncates_to_max_chars(self, kube: KubeClient):
        pr = self._pr("pnc-import-abc", message="x" * 10000, children=[])
        kube._mock_api.get.return_value = pr

        detail = kube.get_pipelinerun_failure_detail("pnc-import-abc")

        assert detail is not None
        assert len(detail) <= KubeClient._MAX_DETAIL_CHARS


class TestCountRunningImports:
    def test_counts_only_pnc_import_prefix(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "pnc-import-abc"}, "status": {"conditions": [{"status": "Unknown"}]}},
                {"metadata": {"name": "pnc-import-def"}, "status": {"conditions": [{"status": "Unknown"}]}},
                {"metadata": {"name": "other-pr"}, "status": {"conditions": [{"status": "Unknown"}]}},
            ]
        }

        assert kube.count_running_imports("pnc-import-") == 2


class TestFindIntakePipelinerunsByFingerprint:
    _LABEL = "custom.appstudio.openshift.io/lightwell-fingerprint"

    def _pr(self, name, *, status="True", created="2026-01-01T00:00:00Z"):
        meta = {"name": name, "creationTimestamp": created}
        return {
            "metadata": meta,
            "status": {"conditions": [{"type": "Succeeded", "status": status}]},
        }

    def test_queries_with_fingerprint_and_build_type_selector(self, kube: KubeClient):
        kube._mock_api.list.return_value = {"items": [self._pr("pnc-import-a")]}

        kube.find_intake_pipelineruns_by_fingerprint("fp-abc")

        kube._mock_api.list.assert_called_once_with(
            "/apis/tekton.dev/v1/namespaces/test-ns/pipelineruns",
            labelSelector=f"{self._LABEL}=fp-abc,pipelines.appstudio.openshift.io/type=build",
        )

    def test_returns_statuses_oldest_first(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                self._pr("pr-new", status="Unknown", created="2026-03-01T00:00:00Z"),
                self._pr("pr-old", status="True", created="2026-01-01T00:00:00Z"),
                self._pr("pr-mid", status="False", created="2026-02-01T00:00:00Z"),
            ]
        }

        result = kube.find_intake_pipelineruns_by_fingerprint("fp-abc")

        assert [pr.name for pr in result] == ["pr-old", "pr-mid", "pr-new"]
        assert [pr.status for pr in result] == ["True", "False", "Unknown"]

    def test_pipelinerun_without_condition_is_running_not_dropped(self, kube: KubeClient):
        """A just-created intake has no Succeeded condition yet; it must be reported RUNNING
        so the caller does not re-trigger it and create a duplicate."""
        kube._mock_api.list.return_value = {
            "items": [{"metadata": {"name": "pr-fresh", "creationTimestamp": "2026-01-01T00:00:00Z"}, "status": {}}]
        }

        result = kube.find_intake_pipelineruns_by_fingerprint("fp-abc")

        assert len(result) == 1
        assert result[0].name == "pr-fresh"
        assert result[0].is_running is True

    def test_skips_unnamed_items(self, kube: KubeClient):
        kube._mock_api.list.return_value = {"items": [{"metadata": {}, "status": {}}]}

        assert kube.find_intake_pipelineruns_by_fingerprint("fp-abc") == []

    def test_raises_on_live_error_so_caller_can_tell_failure_from_empty(self, kube: KubeClient):
        """A failed live list must NOT look like "no PipelineRuns": the caller has to be
        able to skip the Request rather than treat it as pending and create a duplicate."""
        kube._mock_api.list.side_effect = requests.HTTPError("500")

        with pytest.raises(requests.HTTPError):
            kube.find_intake_pipelineruns_by_fingerprint("fp-abc")

    def test_falls_back_to_kubearchive_when_live_empty(self, kube_with_ka: KubeClient):
        kube_with_ka._mock_api.list.return_value = {"items": []}
        kube_with_ka._mock_ka_api.list.return_value = {"items": [self._pr("pr-archived")]}

        result = kube_with_ka.find_intake_pipelineruns_by_fingerprint("fp-abc")

        assert [pr.name for pr in result] == ["pr-archived"]

    def test_merges_live_and_archived_deduplicating_by_name(self, kube_with_ka: KubeClient):
        """History spanning the prune boundary: a live attempt plus older archived ones,
        with the overlap counted once (live copy preferred)."""
        kube_with_ka._mock_api.list.return_value = {
            "items": [
                self._pr("pr-live", status="Unknown", created="2026-03-01T00:00:00Z"),
                self._pr("pr-overlap", status="True", created="2026-02-01T00:00:00Z"),
            ]
        }
        kube_with_ka._mock_ka_api.list.return_value = {
            "items": [
                self._pr("pr-overlap", status="True", created="2026-02-01T00:00:00Z"),
                self._pr("pr-archived", status="False", created="2026-01-01T00:00:00Z"),
            ]
        }

        result = kube_with_ka.find_intake_pipelineruns_by_fingerprint("fp-abc")

        assert [pr.name for pr in result] == ["pr-archived", "pr-overlap", "pr-live"]

    def test_live_error_raises_even_when_kubearchive_has_results(self, kube_with_ka: KubeClient):
        """KubeArchive holds only pruned history; the in-flight intake is on the live API.
        A live-list failure must raise (not be masked by archived results), or the caller
        could adopt a stale archived PipelineRun and miss -- then duplicate -- the live one."""
        kube_with_ka._mock_api.list.side_effect = requests.HTTPError("503")
        kube_with_ka._mock_ka_api.list.return_value = {"items": [self._pr("pr-archived")]}

        with pytest.raises(requests.HTTPError):
            kube_with_ka.find_intake_pipelineruns_by_fingerprint("fp-abc")


class TestFindSnapshotByPipelinerun:
    def test_returns_snapshot_name(self, kube: KubeClient):
        kube._mock_api.list.return_value = {"items": [{"metadata": {"name": "snap-abc"}}]}

        result = kube.find_snapshot_by_pipelinerun("pr-1")
        assert result == "snap-abc"
        kube._mock_api.list.assert_called_once_with(
            "/apis/appstudio.redhat.com/v1alpha1/namespaces/test-ns/snapshots",
            labelSelector="appstudio.openshift.io/build-pipelinerun=pr-1",
        )

    def test_returns_none_when_no_items(self, kube: KubeClient):
        kube._mock_api.list.return_value = {"items": []}

        assert kube.find_snapshot_by_pipelinerun("pr-1") is None

    def test_returns_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.list.side_effect = requests.HTTPError("404")

        assert kube.find_snapshot_by_pipelinerun("pr-1") is None


class TestFindReleaseForSnapshot:
    def test_finds_active_release(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "release-1"},
                    "spec": {"snapshot": "snap-1"},
                    "status": {"conditions": [{"type": "Released", "status": "Unknown", "reason": "Progressing"}]},
                }
            ]
        }

        assert kube.find_release_for_snapshot("snap-1") == "release-1"

    def test_skips_terminally_failed_release(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "release-bad"},
                    "spec": {"snapshot": "snap-1"},
                    "status": {"conditions": [{"type": "Released", "status": "False", "reason": "Failed"}]},
                }
            ]
        }

        assert kube.find_release_for_snapshot("snap-1") is None

    def test_does_not_skip_progressing_false(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "release-prog"},
                    "spec": {"snapshot": "snap-1"},
                    "status": {"conditions": [{"type": "Released", "status": "False", "reason": "Progressing"}]},
                }
            ]
        }

        assert kube.find_release_for_snapshot("snap-1") == "release-prog"

    def test_returns_none_when_no_match(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "release-other"}, "spec": {"snapshot": "other-snap"}, "status": {}},
            ]
        }

        assert kube.find_release_for_snapshot("snap-1") is None

    def test_returns_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.list.side_effect = requests.HTTPError("500")

        assert kube.find_release_for_snapshot("snap-1") is None

    def test_returns_newest_across_any_plan(self, kube: KubeClient):
        """Matches a Release against any plan and, when several match, picks the newest."""
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "rel-new", "creationTimestamp": "2026-02-01T00:00:00Z"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build-prod"},
                    "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]},
                },
                {
                    "metadata": {"name": "rel-old", "creationTimestamp": "2026-01-01T00:00:00Z"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build"},
                    "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]},
                },
            ]
        }

        assert kube.find_release_for_snapshot("snap-1") == "rel-new"


class TestFindReleaseForSnapshotAndPlan:
    def test_ignores_release_against_a_different_plan(self, kube: KubeClient):
        """A stage release for this snapshot must not be mistaken for a prod release."""
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "rel-stage"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build"},
                    "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]},
                }
            ]
        }

        assert kube.find_release_for_snapshot_and_plan("snap-1", "remediated-build-prod") is None

    def test_finds_release_against_the_named_plan(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "rel-stage"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build"},
                    "status": {},
                },
                {
                    "metadata": {"name": "rel-prod"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build-prod"},
                    "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]},
                },
            ]
        }

        assert kube.find_release_for_snapshot_and_plan("snap-1", "remediated-build-prod") == "rel-prod"

    def test_ignores_other_snapshots(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "rel-other"},
                    "spec": {"snapshot": "snap-2", "releasePlan": "remediated-build-prod"},
                    "status": {},
                }
            ]
        }

        assert kube.find_release_for_snapshot_and_plan("snap-1", "remediated-build-prod") is None

    def test_skips_terminally_failed_release(self, kube: KubeClient):
        """A failed prod release must not be adopted — the operator needs a fresh attempt."""
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "rel-bad"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build-prod"},
                    "status": {"conditions": [{"type": "Released", "status": "False", "reason": "Failed"}]},
                }
            ]
        }

        assert kube.find_release_for_snapshot_and_plan("snap-1", "remediated-build-prod") is None

    def test_does_not_skip_progressing_false(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "rel-live"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build-prod"},
                    "status": {"conditions": [{"type": "Released", "status": "False", "reason": "Progressing"}]},
                }
            ]
        }

        assert kube.find_release_for_snapshot_and_plan("snap-1", "remediated-build-prod") == "rel-live"

    def test_returns_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.list.side_effect = requests.HTTPError("500")

        assert kube.find_release_for_snapshot_and_plan("snap-1", "remediated-build-prod") is None

    def test_returns_newest_when_multiple_match(self, kube: KubeClient):
        """Adoption must be deterministic: pick the newest by creationTimestamp, not API order."""
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {"name": "rel-old", "creationTimestamp": "2026-01-01T00:00:00Z"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build-prod"},
                    "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]},
                },
                {
                    "metadata": {"name": "rel-new", "creationTimestamp": "2026-02-01T00:00:00Z"},
                    "spec": {"snapshot": "snap-1", "releasePlan": "remediated-build-prod"},
                    "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]},
                },
            ]
        }

        assert kube.find_release_for_snapshot_and_plan("snap-1", "remediated-build-prod") == "rel-new"


class TestListReleasesForSnapshotAndPlan:
    def _rel(self, name, *, created, status="False", reason="Failed", plan="remediated-build-prod", snapshot="snap-1"):
        return {
            "metadata": {"name": name, "creationTimestamp": created},
            "spec": {"snapshot": snapshot, "releasePlan": plan},
            "status": {"conditions": [{"type": "Released", "status": status, "reason": reason}]},
        }

    def test_returns_all_releases_including_terminal_newest_first(self, kube: KubeClient):
        """Unlike the finder, this keeps terminally-failed Releases so reconcile can count
        retries; newest first so the caller's newest Release decides the latest attempt."""
        kube._mock_api.list.return_value = {
            "items": [
                self._rel("rel-1", created="2026-01-01T00:00:00Z", status="False"),
                self._rel("rel-3", created="2026-03-01T00:00:00Z", status="True", reason="Succeeded"),
                self._rel("rel-2", created="2026-02-01T00:00:00Z", status="False"),
            ]
        }

        result = kube.list_releases_for_snapshot_and_plan("snap-1", "remediated-build-prod")

        assert result == [("rel-3", "True"), ("rel-2", "False"), ("rel-1", "False")]

    def test_scopes_to_snapshot_and_plan(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                self._rel("rel-other-snap", created="2026-01-01T00:00:00Z", snapshot="snap-2"),
                self._rel("rel-other-plan", created="2026-01-01T00:00:00Z", plan="some-other-plan"),
                self._rel("rel-match", created="2026-01-01T00:00:00Z"),
            ]
        }

        result = kube.list_releases_for_snapshot_and_plan("snap-1", "remediated-build-prod")

        assert [name for name, _ in result] == ["rel-match"]

    def test_unscoped_matches_any_plan(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [
                self._rel("rel-a", created="2026-02-01T00:00:00Z", plan="plan-a"),
                self._rel("rel-b", created="2026-01-01T00:00:00Z", plan="plan-b"),
            ]
        }

        result = kube.list_releases_for_snapshot_and_plan("snap-1", None)

        assert [name for name, _ in result] == ["rel-a", "rel-b"]

    def test_progressing_release_reads_as_unknown(self, kube: KubeClient):
        kube._mock_api.list.return_value = {
            "items": [self._rel("rel-prog", created="2026-01-01T00:00:00Z", status="False", reason="Progressing")]
        }

        assert kube.list_releases_for_snapshot_and_plan("snap-1", "remediated-build-prod") == [("rel-prog", "Unknown")]

    def test_raises_on_list_error(self, kube: KubeClient):
        """A failed list must raise so reconcile can skip the Request rather than
        undercount the retry budget."""
        kube._mock_api.list.side_effect = requests.HTTPError("503")

        with pytest.raises(requests.HTTPError):
            kube.list_releases_for_snapshot_and_plan("snap-1", "remediated-build-prod")


class TestGetReleaseStatus:
    def test_returns_true_on_success(self, kube: KubeClient):
        kube._mock_api.get.return_value = {
            "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]}
        }

        assert kube.get_release_status("rel-1") == "True"

    def test_returns_false_on_terminal_failure(self, kube: KubeClient):
        kube._mock_api.get.return_value = {
            "status": {"conditions": [{"type": "Released", "status": "False", "reason": "Failed"}]}
        }

        assert kube.get_release_status("rel-1") == "False"

    def test_returns_unknown_when_progressing(self, kube: KubeClient):
        kube._mock_api.get.return_value = {
            "status": {"conditions": [{"type": "Released", "status": "False", "reason": "Progressing"}]}
        }

        assert kube.get_release_status("rel-1") == "Unknown"

    def test_returns_unknown_when_no_released_condition(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"status": {"conditions": [{"type": "Other", "status": "True"}]}}

        assert kube.get_release_status("rel-1") == "Unknown"

    def test_returns_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.get.side_effect = requests.HTTPError("404")

        assert kube.get_release_status("rel-1") is None


class TestGetSnapshotTestStatus:
    _ANNOTATION = "test.appstudio.openshift.io/status"
    _CONDITION = "AppStudioTestSucceeded"

    def _snap(self, *, condition=None, results=None) -> dict:
        annotations = {} if results is None else {self._ANNOTATION: json.dumps(results)}
        status = {} if condition is None else {"conditions": [{"type": self._CONDITION, "status": condition}]}
        return {"metadata": {"name": "snap-1", "annotations": annotations}, "status": status}

    def test_pending_when_condition_absent(self, kube: KubeClient):
        """No AppStudioTestSucceeded condition yet means tests are still running."""
        kube._mock_api.get.return_value = self._snap(results=[{"scenario": "a", "status": "InProgress"}])
        assert kube.get_snapshot_test_status("snap-1") == "pending"

    def test_pending_when_condition_unknown(self, kube: KubeClient):
        kube._mock_api.get.return_value = self._snap(condition="Unknown")
        assert kube.get_snapshot_test_status("snap-1") == "pending"

    def test_failed_when_condition_false(self, kube: KubeClient):
        kube._mock_api.get.return_value = self._snap(
            condition="False", results=[{"scenario": "a", "status": "TestFail"}]
        )
        assert kube.get_snapshot_test_status("snap-1") == "failed"

    def test_passed_when_condition_true_and_scenarios_ran(self, kube: KubeClient):
        kube._mock_api.get.return_value = self._snap(
            condition="True", results=[{"scenario": "a", "status": "TestPassed"}]
        )
        assert kube.get_snapshot_test_status("snap-1") == "passed"

    def test_passed_when_condition_true_with_terminal_warning(self, kube: KubeClient):
        """A TestWarning is a pass upstream; the condition being True reflects that."""
        kube._mock_api.get.return_value = self._snap(
            condition="True", results=[{"scenario": "a", "status": "TestWarning"}]
        )
        assert kube.get_snapshot_test_status("snap-1") == "passed"

    def test_none_when_condition_true_but_no_annotation(self, kube: KubeClient):
        """Condition True with no results = no required scenario ran (skipped testing)."""
        kube._mock_api.get.return_value = self._snap(condition="True")
        assert kube.get_snapshot_test_status("snap-1") == "none"

    def test_none_when_condition_true_but_empty_results(self, kube: KubeClient):
        kube._mock_api.get.return_value = self._snap(condition="True", results=[])
        assert kube.get_snapshot_test_status("snap-1") == "none"

    def test_none_when_condition_true_but_annotation_unparseable(self, kube: KubeClient):
        snap = self._snap(condition="True")
        snap["metadata"]["annotations"] = {self._ANNOTATION: "not-json"}
        kube._mock_api.get.return_value = snap
        assert kube.get_snapshot_test_status("snap-1") == "none"

    def test_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.get.side_effect = requests.HTTPError("404")
        assert kube.get_snapshot_test_status("snap-1") is None


class TestGetSnapshotFingerprint:
    _LABEL = "custom.appstudio.openshift.io/lightwell-fingerprint"

    def test_returns_propagated_label(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"metadata": {"labels": {self._LABEL: "fp-abc123"}}}
        assert kube.get_snapshot_fingerprint("snap-1") == "fp-abc123"

    def test_none_when_label_absent(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"metadata": {"labels": {"other": "x"}}}
        assert kube.get_snapshot_fingerprint("snap-1") is None

    def test_none_when_no_labels(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"metadata": {}}
        assert kube.get_snapshot_fingerprint("snap-1") is None

    def test_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.get.side_effect = requests.HTTPError("404")
        assert kube.get_snapshot_fingerprint("snap-1") is None


class TestGetSnapshotTestFailureDetail:
    _ANNOTATION = "test.appstudio.openshift.io/status"

    def _snap(self, results=None) -> dict:
        annotations = {} if results is None else {self._ANNOTATION: json.dumps(results)}
        return {"metadata": {"name": "snap-1", "annotations": annotations}}

    def test_summarises_failing_required_scenario(self, kube: KubeClient):
        kube._mock_api.get.return_value = self._snap(
            [
                {"scenario": "pass-one", "status": "TestPassed"},
                {
                    "scenario": "ec",
                    "status": "TestFail",
                    "details": "boom",
                    "testPipelineRunName": "ec-plr-1",
                },
            ]
        )
        detail = kube.get_snapshot_test_failure_detail("snap-1")
        assert detail == "ec: TestFail - boom (ec-plr-1)"

    def test_skips_optional_passing_and_pending_scenarios(self, kube: KubeClient):
        kube._mock_api.get.return_value = self._snap(
            [
                {"scenario": "opt", "status": "TestFail", "isOptionalScenario": True},
                {"scenario": "passing", "status": "TestPassed"},
                {"scenario": "running", "status": "InProgress"},
            ]
        )
        assert kube.get_snapshot_test_failure_detail("snap-1") is None

    def test_none_when_annotation_absent(self, kube: KubeClient):
        kube._mock_api.get.return_value = self._snap()
        assert kube.get_snapshot_test_failure_detail("snap-1") is None

    def test_none_when_unparseable(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"metadata": {"annotations": {self._ANNOTATION: "not-json"}}}
        assert kube.get_snapshot_test_failure_detail("snap-1") is None

    def test_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.get.side_effect = requests.HTTPError("404")
        assert kube.get_snapshot_test_failure_detail("snap-1") is None


class TestFindReleasePlanForSnapshot:
    def test_finds_matching_plan(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"metadata": {"labels": {"appstudio.openshift.io/application": "my-app"}}}
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "plan-other"}, "spec": {"application": "other-app"}},
                {"metadata": {"name": "plan-mine"}, "spec": {"application": "my-app"}},
            ]
        }

        assert kube.find_release_plan_for_snapshot("snap-1") == "plan-mine"

    def test_returns_none_when_no_application_label(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"metadata": {"labels": {}}}

        assert kube.find_release_plan_for_snapshot("snap-1") is None
        kube._mock_api.list.assert_not_called()

    def test_returns_none_when_no_matching_plan(self, kube: KubeClient):
        kube._mock_api.get.return_value = {"metadata": {"labels": {"appstudio.openshift.io/application": "my-app"}}}
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "plan-other"}, "spec": {"application": "other-app"}},
            ]
        }

        assert kube.find_release_plan_for_snapshot("snap-1") is None

    def test_returns_none_on_http_error(self, kube: KubeClient):
        kube._mock_api.get.side_effect = requests.HTTPError("404")

        assert kube.find_release_plan_for_snapshot("snap-1") is None

    def test_excludes_manual_only_plan(self, kube: KubeClient):
        """A plan labelled auto-release=false is operator-driven and never auto-selected."""
        kube._mock_api.get.return_value = {"metadata": {"labels": {"appstudio.openshift.io/application": "my-app"}}}
        kube._mock_api.list.return_value = {
            "items": [
                {
                    "metadata": {
                        "name": "plan-prod",
                        "labels": {"release.appstudio.openshift.io/auto-release": "false"},
                    },
                    "spec": {"application": "my-app"},
                },
                {
                    "metadata": {
                        "name": "plan-stage",
                        "labels": {"release.appstudio.openshift.io/auto-release": "true"},
                    },
                    "spec": {"application": "my-app"},
                },
            ]
        }

        assert kube.find_release_plan_for_snapshot("snap-1") == "plan-stage"

    def test_keeps_plan_without_auto_release_label(self, kube: KubeClient):
        """Only an explicit 'false' excludes a plan — a missing label is not a signal."""
        kube._mock_api.get.return_value = {"metadata": {"labels": {"appstudio.openshift.io/application": "my-app"}}}
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "plan-unlabelled"}, "spec": {"application": "my-app"}},
            ]
        }

        assert kube.find_release_plan_for_snapshot("snap-1") == "plan-unlabelled"

    def test_handles_null_labels_mapping(self, kube: KubeClient):
        """A manifest with `labels: null` must not raise AttributeError — treat as no labels."""
        kube._mock_api.get.return_value = {"metadata": {"labels": {"appstudio.openshift.io/application": "my-app"}}}
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "plan-null-labels", "labels": None}, "spec": {"application": "my-app"}},
            ]
        }

        assert kube.find_release_plan_for_snapshot("snap-1") == "plan-null-labels"

    def test_returns_none_when_ambiguous(self, kube: KubeClient, capsys: pytest.CaptureFixture[str]):
        """Two auto-releasing plans for one application is unresolvable — refuse rather than guess."""
        kube._mock_api.get.return_value = {"metadata": {"labels": {"appstudio.openshift.io/application": "my-app"}}}
        kube._mock_api.list.return_value = {
            "items": [
                {"metadata": {"name": "plan-a"}, "spec": {"application": "my-app"}},
                {"metadata": {"name": "plan-b"}, "spec": {"application": "my-app"}},
            ]
        }

        assert kube.find_release_plan_for_snapshot("snap-1") is None

        stderr = capsys.readouterr().err
        assert "plan-a" in stderr
        assert "plan-b" in stderr
        assert "my-app" in stderr


class TestCreatePipelinerun:
    """Test the create_pipelinerun method."""

    def test_returns_generated_name(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-abcde"}}

        result = kube.create_pipelinerun({"apiVersion": "tekton.dev/v1", "kind": "PipelineRun"})
        assert result == "pnc-import-abcde"

    def test_passes_manifest_as_body(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-xyz"}}

        manifest = {"apiVersion": "tekton.dev/v1", "kind": "PipelineRun"}
        kube.create_pipelinerun(manifest)

        kube._mock_api.create.assert_called_once_with(
            "/apis/tekton.dev/v1/namespaces/test-ns/pipelineruns",
            manifest,
        )

    def test_raises_trigger_error_on_http_error(self, kube: KubeClient):
        kube._mock_api.create.side_effect = requests.HTTPError("403 Forbidden")

        with pytest.raises(TriggerError):
            kube.create_pipelinerun({"kind": "PipelineRun"})

    def test_http_error_surfaces_api_message(self, kube: KubeClient):
        response = MagicMock()
        response.json.return_value = {"kind": "Status", "message": "non-existent variable in value"}
        kube._mock_api.create.side_effect = requests.HTTPError("400 Bad Request", response=response)

        with pytest.raises(TriggerError, match="non-existent variable in value"):
            kube.create_pipelinerun({"kind": "PipelineRun"})

    def test_returns_none_when_name_missing(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {}}

        result = kube.create_pipelinerun({"kind": "PipelineRun"})
        assert result is None

    def test_uses_correct_namespace_in_path(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-ns"}}

        kube.create_pipelinerun({"kind": "PipelineRun"})

        api_path = kube._mock_api.create.call_args[0][0]
        assert "/namespaces/test-ns/" in api_path


class TestCreateRelease:
    def test_returns_generated_name(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-abc123"}}

        result = kube.create_release("my-snapshot", "my-release-plan", "pnc-import-")
        assert result == "pnc-import-abc123"

    def test_passes_correct_manifest(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-xyz"}}

        kube.create_release("snap-1", "plan-1", "pnc-import-")

        kube._mock_api.create.assert_called_once_with(
            "/apis/appstudio.redhat.com/v1alpha1/namespaces/test-ns/releases",
            {
                "apiVersion": "appstudio.redhat.com/v1alpha1",
                "kind": "Release",
                "metadata": {"generateName": "pnc-import-", "namespace": "test-ns"},
                "spec": {"releasePlan": "plan-1", "snapshot": "snap-1"},
            },
        )

    def test_stamps_fingerprint_label_when_given(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-fp"}}

        kube.create_release("snap-1", "plan-1", "pnc-import-", fingerprint="abc123")

        manifest = kube._mock_api.create.call_args.args[1]
        assert manifest["metadata"]["labels"] == {"custom.appstudio.openshift.io/lightwell-fingerprint": "abc123"}

    def test_omits_labels_when_no_fingerprint(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-nofp"}}

        kube.create_release("snap-1", "plan-1", "pnc-import-")

        manifest = kube._mock_api.create.call_args.args[1]
        assert "labels" not in manifest["metadata"]

    def test_raises_release_error_on_http_error(self, kube: KubeClient):
        kube._mock_api.create.side_effect = requests.HTTPError("403 Forbidden")

        with pytest.raises(ReleaseError):
            kube.create_release("snap-1", "plan-1", "pnc-import-")

    def test_http_error_surfaces_api_message(self, kube: KubeClient):
        response = MagicMock()
        response.json.return_value = {"kind": "Status", "message": "releaseplans.appstudio.redhat.com is forbidden"}
        kube._mock_api.create.side_effect = requests.HTTPError("403 Forbidden", response=response)

        with pytest.raises(ReleaseError, match="releaseplans.appstudio.redhat.com is forbidden"):
            kube.create_release("snap-1", "plan-1", "pnc-import-")

    def test_returns_none_when_name_missing(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {}}

        result = kube.create_release("snap-1", "plan-1", "pnc-import-")
        assert result is None

    def test_uses_correct_namespace_in_path(self, kube: KubeClient):
        kube._mock_api.create.return_value = {"metadata": {"name": "pnc-import-ns"}}

        kube.create_release("snap-1", "plan-1", "pnc-import-")

        api_path = kube._mock_api.create.call_args[0][0]
        assert "/namespaces/test-ns/" in api_path
