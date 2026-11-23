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

from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml

from import_orchestrator.clients.kube_api import KubeAPI, KubeAuth, resolve_auth


class TestResolveAuth:
    """Test the resolve_auth function."""

    def test_ci_mode_uses_konflux_token(self, monkeypatch):
        monkeypatch.setenv("KONFLUX_TOKEN", "ci-token")
        auth = resolve_auth("https://api.example.com:6443")
        assert auth.token == "ci-token"
        assert auth.server == "https://api.example.com:6443"
        assert auth.ca_cert is None

    def test_local_dev_parses_kubeconfig(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "test-ctx",
            "contexts": [{"name": "test-ctx", "context": {"cluster": "test-cluster", "user": "test-user"}}],
            "clusters": [{"name": "test-cluster", "cluster": {"server": "https://api.example.com:6443"}}],
            "users": [{"name": "test-user", "user": {"token": "my-oauth-token"}}],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        auth = resolve_auth("unused-in-local-mode")
        assert auth.token == "my-oauth-token"
        assert auth.server == "https://api.example.com:6443"
        assert auth.ca_cert is None

    def test_reads_certificate_authority(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "ctx",
            "contexts": [{"name": "ctx", "context": {"cluster": "c", "user": "u"}}],
            "clusters": [{"name": "c", "cluster": {"server": "https://x", "certificate-authority": "/path/to/ca.crt"}}],
            "users": [{"name": "u", "user": {"token": "tok"}}],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        auth = resolve_auth("unused")
        assert auth.ca_cert == "/path/to/ca.crt"

    def test_raises_when_no_token_in_kubeconfig(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "ctx",
            "contexts": [{"name": "ctx", "context": {"cluster": "c", "user": "u"}}],
            "clusters": [{"name": "c", "cluster": {"server": "https://x"}}],
            "users": [{"name": "u", "user": {}}],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="has no 'token' field in"):
            resolve_auth("unused")

    def test_raises_when_kubeconfig_missing(self, tmp_path, monkeypatch):
        kc_path = tmp_path / "nonexistent"
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="Kubeconfig file not found"):
            resolve_auth("unused")

    def test_raises_when_kubeconfig_invalid_yaml(self, tmp_path, monkeypatch):
        kc_path = tmp_path / "invalid.yaml"
        kc_path.write_text(":\n invalid")
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="Failed to parse kubeconfig YAML"):
            resolve_auth("unused")

    def test_raises_when_no_current_context(self, tmp_path, monkeypatch):
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump({"contexts": []}))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="No 'current-context' set"):
            resolve_auth("unused")

    def test_raises_when_context_not_found(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "ctx-x",
            "contexts": [{"name": "ctx-y", "context": {}}],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="Context 'ctx-x' not found"):
            resolve_auth("unused")

    def test_raises_when_cluster_not_found(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "ctx",
            "contexts": [{"name": "ctx", "context": {"cluster": "missing-cluster", "user": "u"}}],
            "clusters": [],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="Cluster 'missing-cluster' not found"):
            resolve_auth("unused")

    def test_raises_when_user_not_found(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "ctx",
            "contexts": [{"name": "ctx", "context": {"cluster": "c", "user": "missing-user"}}],
            "clusters": [{"name": "c", "cluster": {"server": "https://x"}}],
            "users": [],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="User 'missing-user' not found"):
            resolve_auth("unused")

    def test_raises_when_cluster_has_no_server(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "ctx",
            "contexts": [{"name": "ctx", "context": {"cluster": "c", "user": "u"}}],
            "clusters": [{"name": "c", "cluster": {}}],
            "users": [{"name": "u", "user": {"token": "t"}}],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="has no 'server' URL"):
            resolve_auth("unused")

    def test_custom_kubeconfig_path(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "my-ctx",
            "contexts": [{"name": "my-ctx", "context": {"cluster": "cl", "user": "usr"}}],
            "clusters": [{"name": "cl", "cluster": {"server": "https://custom.example.com"}}],
            "users": [{"name": "usr", "user": {"token": "custom-token"}}],
        }
        custom_path = tmp_path / "custom" / "config"
        custom_path.parent.mkdir()
        custom_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(custom_path))
        monkeypatch.delenv("KONFLUX_TOKEN", raising=False)

        auth = resolve_auth("unused")
        assert auth.token == "custom-token"
        assert auth.server == "https://custom.example.com"

    def test_ci_mode_takes_precedence_over_kubeconfig(self, tmp_path, monkeypatch):
        kubeconfig = {
            "current-context": "ctx",
            "contexts": [{"name": "ctx", "context": {"cluster": "c", "user": "u"}}],
            "clusters": [{"name": "c", "cluster": {"server": "https://kubeconfig-server"}}],
            "users": [{"name": "u", "user": {"token": "kubeconfig-token"}}],
        }
        kc_path = tmp_path / "kubeconfig"
        kc_path.write_text(yaml.dump(kubeconfig))
        monkeypatch.setenv("KUBECONFIG", str(kc_path))
        monkeypatch.setenv("KONFLUX_TOKEN", "ci-token")

        auth = resolve_auth("https://ci-server:6443")
        assert auth.token == "ci-token"
        assert auth.server == "https://ci-server:6443"


class TestKubeAPI:
    """Test the KubeAPI HTTP transport layer."""

    @pytest.fixture
    def auth(self):
        return KubeAuth(server="https://api.example.com:6443", token="test-token", ca_cert=None)

    @pytest.fixture
    def api(self, auth):
        with patch("import_orchestrator.clients.kube_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            api = KubeAPI(auth)
            api._mock_session = mock_session
        return api

    def test_sets_auth_header(self, auth):
        with patch("import_orchestrator.clients.kube_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.headers = {}
            mock_session_cls.return_value = mock_session
            KubeAPI(auth)
            assert mock_session.headers["Authorization"] == "Bearer test-token"
            assert mock_session.headers["Accept"] == "application/json"

    def test_ca_cert_sets_verify(self):
        auth = KubeAuth(server="https://x", token="t", ca_cert="/path/to/ca.crt")
        with patch("import_orchestrator.clients.kube_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.headers = {}
            mock_session_cls.return_value = mock_session
            KubeAPI(auth)
            assert mock_session.verify == "/path/to/ca.crt"

    def test_get_constructs_url(self, api):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"metadata": {"name": "pr-1"}}
        api._mock_session.get.return_value = mock_resp

        result = api.get("/apis/tekton.dev/v1/namespaces/ns/pipelineruns/pr-1")

        api._mock_session.get.assert_called_once_with(
            "https://api.example.com:6443/apis/tekton.dev/v1/namespaces/ns/pipelineruns/pr-1",
            timeout=30,
        )
        mock_resp.raise_for_status.assert_called_once()
        assert result == {"metadata": {"name": "pr-1"}}

    def test_get_text_returns_body(self, api):
        mock_resp = MagicMock()
        mock_resp.text = "line 1\nline 2\n"
        api._mock_session.get.return_value = mock_resp

        result = api.get_text(
            "/api/v1/namespaces/ns/pods/pod-1/log",
            container="step-build",
            tailLines=20,
        )

        api._mock_session.get.assert_called_once_with(
            "https://api.example.com:6443/api/v1/namespaces/ns/pods/pod-1/log",
            params={"container": "step-build", "tailLines": 20},
            timeout=30,
        )
        mock_resp.raise_for_status.assert_called_once()
        assert result == "line 1\nline 2\n"

    def test_get_text_raises_on_http_error(self, api):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        api._mock_session.get.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            api.get_text("/api/v1/namespaces/ns/pods/missing/log")

    def test_list_passes_query_params(self, api):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"items": []}
        api._mock_session.get.return_value = mock_resp

        result = api.list(
            "/apis/appstudio.redhat.com/v1alpha1/namespaces/ns/snapshots",
            labelSelector="app=test",
        )

        api._mock_session.get.assert_called_once_with(
            "https://api.example.com:6443/apis/appstudio.redhat.com/v1alpha1/namespaces/ns/snapshots",
            params={"labelSelector": "app=test"},
            timeout=30,
        )
        assert result == {"items": []}

    def test_create_posts_json_body(self, api):
        body = {"apiVersion": "tekton.dev/v1", "kind": "PipelineRun"}
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"metadata": {"name": "pnc-import-abcde"}}
        api._mock_session.post.return_value = mock_resp

        result = api.create("/apis/tekton.dev/v1/namespaces/ns/pipelineruns", body)

        api._mock_session.post.assert_called_once_with(
            "https://api.example.com:6443/apis/tekton.dev/v1/namespaces/ns/pipelineruns",
            json=body,
            timeout=30,
        )
        mock_resp.raise_for_status.assert_called_once()
        assert result == {"metadata": {"name": "pnc-import-abcde"}}

    def test_get_raises_on_http_error(self, api):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        api._mock_session.get.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            api.get("/apis/tekton.dev/v1/namespaces/ns/pipelineruns/missing")

    def test_create_raises_on_http_error(self, api):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
        api._mock_session.post.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            api.create("/apis/tekton.dev/v1/namespaces/ns/pipelineruns", {})

    def test_delete_constructs_url(self, api):
        mock_resp = MagicMock()
        api._mock_session.delete.return_value = mock_resp

        result = api.delete("/apis/appstudio.redhat.com/v1alpha1/namespaces/ns/releases/rel-1")

        api._mock_session.delete.assert_called_once_with(
            "https://api.example.com:6443/apis/appstudio.redhat.com/v1alpha1/namespaces/ns/releases/rel-1",
            timeout=30,
        )
        mock_resp.raise_for_status.assert_called_once()
        mock_resp.json.assert_not_called()
        assert result is None

    def test_delete_raises_on_http_error(self, api):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
        api._mock_session.delete.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            api.delete("/apis/tekton.dev/v1/namespaces/ns/pipelineruns/pr-1")
