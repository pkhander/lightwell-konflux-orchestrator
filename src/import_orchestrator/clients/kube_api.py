"""Thin HTTP transport for Kubernetes API calls using requests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml


@dataclass(frozen=True)
class KubeAuth:
    """Resolved cluster credentials."""

    server: str
    token: str
    ca_cert: str | None


def _find_entry(items: Any, expected_name: str, item_type: str, path: Path) -> dict[str, Any]:
    """Find a named entry in a kubeconfig list and return its inner dictionary."""
    if not isinstance(items, list):
        raise RuntimeError(f"No '{item_type}s' list found in kubeconfig '{path}'.")
    entry = next(
        (
            item
            for item in items
            if isinstance(item, dict) and bool(item.get("name")) and item.get("name") == expected_name
        ),
        None,
    )
    payload = entry.get(item_type) if entry else None
    if not isinstance(payload, dict):
        raise RuntimeError(f"{item_type.capitalize()} '{expected_name}' not found in kubeconfig '{path}'.")
    return payload


def resolve_auth(cluster_api: str) -> KubeAuth:
    """Resolve auth credentials from env vars or kubeconfig.

    Priority:
      1. KONFLUX_TOKEN env var + cluster_api arg  (CI mode)
      2. KUBECONFIG / ~/.kube/config              (local dev, OAuth token only)
    """
    if token := os.getenv("KONFLUX_TOKEN"):
        return KubeAuth(server=cluster_api, token=token, ca_cert=None)

    raw_path = os.getenv("KUBECONFIG") or "~/.kube/config"
    kubeconfig_path = Path(raw_path).expanduser()
    if not kubeconfig_path.is_file():
        raise RuntimeError(
            f"Kubeconfig file not found at '{kubeconfig_path}'. Please run 'oc login' or set $KONFLUX_TOKEN."
        )

    try:
        with open(kubeconfig_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except OSError as e:
        raise RuntimeError(f"Cannot read kubeconfig at '{kubeconfig_path}': {e}") from e
    except yaml.YAMLError as e:
        raise RuntimeError(f"Failed to parse kubeconfig YAML from '{kubeconfig_path}': {e}") from e

    if not isinstance(config, dict):
        raise RuntimeError(f"Kubeconfig at '{kubeconfig_path}' is empty or invalid.")

    ctx_name = config.get("current-context")
    if not ctx_name or not isinstance(ctx_name, str):
        raise RuntimeError(
            f"No 'current-context' set in kubeconfig '{kubeconfig_path}'. Please run 'oc login' or select a context."
        )

    ctx = _find_entry(config.get("contexts"), ctx_name, "context", kubeconfig_path)

    cluster_name = ctx.get("cluster")
    if not cluster_name or not isinstance(cluster_name, str):
        raise RuntimeError(f"Context '{ctx_name}' in '{kubeconfig_path}' does not specify a 'cluster'.")

    cluster = _find_entry(config.get("clusters"), cluster_name, "cluster", kubeconfig_path)

    server = cluster.get("server")
    if not server or not isinstance(server, str):
        raise RuntimeError(f"Cluster '{cluster_name}' in '{kubeconfig_path}' has no 'server' URL.")

    user_name = ctx.get("user")
    if not user_name or not isinstance(user_name, str):
        raise RuntimeError(f"Context '{ctx_name}' in '{kubeconfig_path}' does not specify a 'user'.")

    user = _find_entry(config.get("users"), user_name, "user", kubeconfig_path)

    token = user.get("token", "")
    if not token or not isinstance(token, str):
        raise RuntimeError(
            f"Kubeconfig user '{user_name}' has no 'token' field in '{kubeconfig_path}'. "
            "Only OAuth token auth is supported (run 'oc login' first) or set $KONFLUX_TOKEN."
        )

    ca_cert = cluster.get("certificate-authority")
    if ca_cert and isinstance(ca_cert, str):
        ca_path = Path(ca_cert)
        if not ca_path.is_absolute():
            ca_cert = str((kubeconfig_path.parent / ca_path).resolve())

    return KubeAuth(
        server=server,
        token=token,
        ca_cert=ca_cert,
    )


class KubeAPI:
    """Low-level HTTP client for Kubernetes API calls."""

    _DEFAULT_TIMEOUT = 30

    def __init__(self, auth: KubeAuth, timeout: int = _DEFAULT_TIMEOUT):
        self._auth = auth
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {auth.token}"
        self._session.headers["Accept"] = "application/json"
        if auth.ca_cert:
            self._session.verify = auth.ca_cert

    def _url(self, api_path: str) -> str:
        return f"{self._auth.server}{api_path}"

    def get(self, api_path: str) -> dict:
        resp = self._session.get(self._url(api_path), timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def get_text(self, api_path: str, **params) -> str:
        """GET an endpoint that returns a plain-text body (e.g. pod logs)."""
        resp = self._session.get(self._url(api_path), params=params, timeout=self._timeout)
        resp.raise_for_status()
        return resp.text

    def list(self, api_path: str, **params) -> dict:
        resp = self._session.get(self._url(api_path), params=params, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def create(self, api_path: str, body: dict) -> dict:
        resp = self._session.post(self._url(api_path), json=body, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def delete(self, api_path: str) -> None:
        """DELETE a single object by its resource path.

        The response body (a Kubernetes ``Status``) is not parsed: callers only
        care that the delete was accepted, and a non-JSON body would otherwise
        raise ``JSONDecodeError``.
        """
        resp = self._session.delete(self._url(api_path), timeout=self._timeout)
        resp.raise_for_status()
