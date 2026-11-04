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

import json
import sys
from typing import Literal

import requests

from import_orchestrator.clients.kube_api import KubeAPI, KubeAuth, resolve_auth
from import_orchestrator.constants import FINGERPRINT_LABEL
from import_orchestrator.models import PipelineRunStatus


def _api_error_detail(exc: requests.RequestException) -> str:
    """Extract a human-readable message from a failed Kubernetes API response.

    Kubernetes returns a Status object whose ``message`` field explains why a
    request was rejected (e.g. pipeline validation errors). Surface it so a 400
    is actionable instead of an opaque "Bad Request".
    """
    response = getattr(exc, "response", None)
    if response is None:
        return str(exc)
    try:
        body = response.json()
    except (ValueError, AttributeError):
        body = None
    if isinstance(body, dict) and body.get("message"):
        return f"{exc}: {body['message']}"
    text = (getattr(response, "text", "") or "").strip()
    return f"{exc}: {text}" if text else str(exc)


class KubeClient:
    """Client for Kubernetes API operations against a cluster.

    Authenticates using either a KUBECONFIG file or the KONFLUX_TOKEN environment variable.
    """

    # Diagnostics captured on failure are bounded so a runaway build log cannot
    # bloat the phase's error_message (and any Jira comment derived from it).
    _MAX_LOG_LINES = 20
    _MAX_DETAIL_CHARS = 4000

    # Konflux's Integration Service records the *overall* test outcome of a Snapshot in
    # this status condition (set once the required, context-matching scenarios finish --
    # or immediately, True, when none are required). It is the authoritative gate signal.
    # The per-scenario annotation below is only used to describe a failure.
    _KONFLUX_TEST_SUCCEEDED_CONDITION = "AppStudioTestSucceeded"
    # The per-scenario results annotation (a JSON array, one entry per scenario).
    _KONFLUX_ITS_RESULTS_ANNOTATION = "test.appstudio.openshift.io/status"
    # Per-scenario statuses that are terminal-and-passing / still-running, used only when
    # summarising a failure so passing or in-flight scenarios are left out of the message.
    _KONFLUX_ITS_PASSING_STATUSES = frozenset({"TestPassed", "TestWarning"})
    _KONFLUX_ITS_PENDING_STATUSES = frozenset({"Pending", "InProgress"})

    def __init__(self, namespace: str, cluster_api: str, kubearchive_api: str = ""):
        self.namespace = namespace
        self.cluster_api = cluster_api
        auth = resolve_auth(cluster_api)
        self._api = KubeAPI(auth)
        self._ka_api: KubeAPI | None = None
        if kubearchive_api:
            ka_auth = KubeAuth(server=kubearchive_api, token=auth.token, ca_cert=auth.ca_cert)
            self._ka_api = KubeAPI(ka_auth)

    def _log_api_error(
        self,
        operation: str,
        exc: requests.RequestException,
        *,
        source: str = "cluster",
        ignore_not_found: bool = False,
    ) -> None:
        """Log response details, keeping expected missing-resource lookups quiet."""
        response = getattr(exc, "response", None)
        if ignore_not_found and response is not None and response.status_code == 404:
            return
        print(
            f"ERROR: Failed to {operation} ({source}, namespace {self.namespace}): {_api_error_detail(exc)}",
            file=sys.stderr,
        )

    def get_running_pipelineruns(self) -> list[PipelineRunStatus]:
        """Get all PipelineRuns with status 'Unknown' (i.e. still running)."""
        try:
            result = self._api.list(
                f"/apis/tekton.dev/v1/namespaces/{self.namespace}/pipelineruns",
            )
            pipelineruns = []
            for item in result.get("items", []):
                name = item.get("metadata", {}).get("name", "")
                conditions = item.get("status", {}).get("conditions", [])
                status = conditions[0].get("status", "") if conditions else ""
                if pr_status := PipelineRunStatus.from_str(name, status):
                    if pr_status.is_running:
                        pipelineruns.append(pr_status)
            return pipelineruns
        except requests.RequestException as e:
            self._log_api_error("list PipelineRuns", e)
            return []

    def get_pipelinerun_status(self, name: str) -> PipelineRunStatus | None:
        """Get the status of a specific PipelineRun by name.

        Checks the live cluster first, then falls back to the KubeArchive API.
        Returns None if not found in either place.
        """
        try:
            data = self._api.get(
                f"/apis/tekton.dev/v1/namespaces/{self.namespace}/pipelineruns/{name}",
            )
            conditions = data.get("status", {}).get("conditions", [])
            status = conditions[0].get("status", "") if conditions else ""
            if pr_status := PipelineRunStatus.from_str(name, status):
                return pr_status
        except requests.RequestException as e:
            self._log_api_error(f"get PipelineRun {name}", e, ignore_not_found=True)

        if self._ka_api is not None:
            try:
                data = self._ka_api.get(
                    f"/apis/tekton.dev/v1/namespaces/{self.namespace}/pipelineruns/{name}",
                )
                for cond in data.get("status", {}).get("conditions", []):
                    if cond.get("type") == "Succeeded":
                        status = cond.get("status", "")
                        if pr_status := PipelineRunStatus.from_str(name, status):
                            return pr_status
            except requests.RequestException as e:
                self._log_api_error(f"get PipelineRun {name}", e, source="KubeArchive", ignore_not_found=True)

        return None

    def get_pipelinerun_failure_detail(self, name: str) -> str | None:
        """Best-effort human-readable diagnostics for a failed PipelineRun.

        Walks the PipelineRun's ``Succeeded`` condition and each failed child
        TaskRun (via ``status.childReferences``) to capture the failure reason,
        the failing step's termination info, and a tail of that step's
        container logs. Each source is optional: whatever can be gathered is
        returned, degrading to ``None`` only when no PipelineRun data is
        available at all. Callers (e.g. balor-fianna) surface this as the
        phase's ``error_message`` instead of the opaque "PipelineRun
        failed".
        """
        pr = self._fetch_resource(
            f"/apis/tekton.dev/v1/namespaces/{self.namespace}/pipelineruns/{name}",
        )
        if pr is None:
            return None

        parts: list[str] = []

        cond = self._succeeded_condition(pr)
        if cond is not None:
            header = " - ".join(p for p in (cond.get("reason", ""), cond.get("message", "")) if p)
            if header:
                parts.append(f"PipelineRun {name}: {header}")

        for child in pr.get("status", {}).get("childReferences", []):
            if child.get("kind") != "TaskRun":
                continue
            tr_name = child.get("name")
            if not tr_name:
                continue
            detail = self._taskrun_failure_detail(tr_name)
            if detail:
                parts.append(detail)

        if not parts:
            return None
        return "\n".join(parts)[: self._MAX_DETAIL_CHARS]

    def _taskrun_failure_detail(self, tr_name: str) -> str | None:
        """Return failure diagnostics for a single TaskRun, or None if it did not fail."""
        tr = self._fetch_resource(
            f"/apis/tekton.dev/v1/namespaces/{self.namespace}/taskruns/{tr_name}",
        )
        if tr is None:
            return None

        cond = self._succeeded_condition(tr)
        if cond is None or cond.get("status") != "False":
            return None

        header = " - ".join(p for p in (cond.get("reason", ""), cond.get("message", "")) if p)
        lines = [f"TaskRun {tr_name}: {header}" if header else f"TaskRun {tr_name}: failed"]

        status = tr.get("status", {})
        pod_name = status.get("podName", "")
        for step in status.get("steps", []):
            term = step.get("terminated")
            if not term or term.get("reason") == "Completed":
                continue
            step_name = step.get("name", "")
            exit_code = term.get("exitCode")
            step_line = f"  step '{step_name}' terminated: reason={term.get('reason')} exitCode={exit_code}"
            if term.get("message"):
                step_line += f" message={term['message']}"
            lines.append(step_line)

            container = step.get("container") or (f"step-{step_name}" if step_name else "")
            if pod_name and container:
                log_tail = self._pod_log_tail(pod_name, container)
                if log_tail:
                    lines.append(f"  logs ({container}):\n{log_tail}")

        return "\n".join(lines)

    def _pod_log_tail(self, pod_name: str, container: str) -> str | None:
        """Best-effort tail of a container's logs; None if unavailable."""
        try:
            text = self._api.get_text(
                f"/api/v1/namespaces/{self.namespace}/pods/{pod_name}/log",
                container=container,
                tailLines=self._MAX_LOG_LINES,
            )
        except requests.RequestException as e:
            self._log_api_error(f"get logs for pod {pod_name}, container {container}", e, ignore_not_found=True)
            return None
        stripped = text.strip()
        return stripped or None

    def _fetch_resource(self, api_path: str) -> dict | None:
        """GET a resource from the live cluster, falling back to KubeArchive."""
        try:
            return self._api.get(api_path)
        except requests.RequestException as e:
            self._log_api_error(f"get resource {api_path}", e, ignore_not_found=True)
        if self._ka_api is not None:
            try:
                return self._ka_api.get(api_path)
            except requests.RequestException as e:
                self._log_api_error(f"get resource {api_path}", e, source="KubeArchive", ignore_not_found=True)
        return None

    @staticmethod
    def _succeeded_condition(resource: dict) -> dict | None:
        """Return the ``Succeeded`` condition, falling back to the first condition."""
        conditions = resource.get("status", {}).get("conditions", [])
        for cond in conditions:
            if cond.get("type") == "Succeeded":
                return cond
        return conditions[0] if conditions else None

    def count_running_imports(self, prefix: str) -> int:
        """Count running PipelineRuns whose names start with the given prefix."""
        running_prs = self.get_running_pipelineruns()
        return sum(1 for pr in running_prs if pr.name.startswith(prefix))

    # Intake PipelineRuns carry type=build. The fingerprint label is also propagated onto
    # the Snapshot and integration-test PipelineRuns, so scope the query to type=build to
    # return only intakes.
    _PIPELINERUN_TYPE_LABEL = "pipelines.appstudio.openshift.io/type"

    def find_intake_pipelineruns_by_fingerprint(self, fingerprint: str) -> list[PipelineRunStatus]:
        """Find intake PipelineRuns carrying ``fingerprint``, oldest first.

        Reconciliation entry point (ADR-0001): a run finds the PipelineRuns it stamped
        with the Request fingerprint instead of trusting the cache. Each Attempt stamps
        the same (per-Request) fingerprint, so several may match; oldest-first lets the
        caller rebuild one Attempt per PipelineRun. Live and KubeArchive are merged so a
        history spanning pruned runs is complete. A PipelineRun with no condition yet is
        reported RUNNING, not dropped, so a fresh intake is never re-triggered.

        Raises :class:`requests.RequestException` if the *live* list fails: the caller
        must be able to tell "query failed" from "no PipelineRuns match", because
        treating a transient error as "nothing exists" would let it create a duplicate
        intake. A KubeArchive outage only loses pruned history and degrades to live-only.
        """
        selector = f"{FINGERPRINT_LABEL}={fingerprint},{self._PIPELINERUN_TYPE_LABEL}=build"
        items = self._list_pipelineruns_by_label(selector)
        items.sort(key=lambda item: (item.get("metadata", {}) or {}).get("creationTimestamp", ""))
        statuses = []
        for item in items:
            if pr_status := self._pipelinerun_status_from_item(item):
                statuses.append(pr_status)
        return statuses

    def _list_pipelineruns_by_label(self, selector: str) -> list[dict]:
        """List PipelineRuns matching a label selector, merging live and KubeArchive.

        The live list is authoritative for in-flight work and is not error-masked: a
        failed live query re-raises so the caller can distinguish it from an empty
        result (treating a transient error as "nothing exists" would let the caller
        create a duplicate). KubeArchive only supplies pruned history, so its outage
        degrades to [] and simply merges nothing. Live items first, then
        KubeArchive-only items (deduplicated by name).
        """
        path = f"/apis/tekton.dev/v1/namespaces/{self.namespace}/pipelineruns"
        items = self._list_by_label(self._api, path, selector, source="cluster", raise_on_error=True)
        if self._ka_api is not None:
            archived = self._list_by_label(self._ka_api, path, selector, source="KubeArchive", raise_on_error=False)
            seen = {(item.get("metadata", {}) or {}).get("name") for item in items}
            items = items + [i for i in archived if (i.get("metadata", {}) or {}).get("name") not in seen]
        return items

    def _list_by_label(
        self, api: KubeAPI, path: str, selector: str, *, source: str, raise_on_error: bool
    ) -> list[dict]:
        """GET a label-filtered list from one API.

        The error is always logged. With ``raise_on_error`` it then re-raises (the
        caller must tell a failed query from an empty one); otherwise it degrades to [].
        """
        try:
            result = api.list(path, labelSelector=selector)
            return result.get("items", [])
        except requests.RequestException as e:
            self._log_api_error(f"list resources by label {selector}", e, source=source, ignore_not_found=True)
            if raise_on_error:
                raise
            return []

    @staticmethod
    def _pipelinerun_status_from_item(item: dict) -> PipelineRunStatus | None:
        """Map a PipelineRun list item to a status, or None if unnamed.

        No recognised condition means the run just started: report RUNNING so the caller
        does not re-trigger it.
        """
        name = (item.get("metadata", {}) or {}).get("name", "")
        if not name:
            return None
        cond = KubeClient._succeeded_condition(item)
        status = cond.get("status", "") if cond else ""
        if status not in ("True", "False", "Unknown"):
            status = "Unknown"
        return PipelineRunStatus(name, status)

    def find_snapshot_by_pipelinerun(self, pr_name: str) -> str | None:
        """Find the Snapshot created by a specific PipelineRun via its label."""
        try:
            result = self._api.list(
                f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/snapshots",
                labelSelector=f"appstudio.openshift.io/build-pipelinerun={pr_name}",
            )
            items = result.get("items", [])
            return items[0]["metadata"]["name"] if items else None
        except requests.RequestException as e:
            self._log_api_error(f"find Snapshot for PipelineRun {pr_name}", e)
            return None
        except (KeyError, IndexError):
            return None

    def find_release_plan_for_snapshot(self, snapshot_name: str) -> str | None:
        """Find the single auto-releasing ReleasePlan for the snapshot's application.

        Plans labelled ``auto-release: 'false'`` are operator-driven (e.g. a production
        promotion gated on ticket state) and are never selected automatically. Only an
        explicit 'false' excludes a plan; a missing label is not a signal either way.

        Returns None if no plan matches, and also if more than one does — an application
        with two auto-releasing plans is unresolvable from a Snapshot alone, and guessing
        would mean guessing where content gets published. Callers that know which plan
        they want should pass it explicitly rather than relying on this lookup.
        """
        operation = f"get Snapshot {snapshot_name} for ReleasePlan lookup"
        fetching_snapshot = True
        try:
            snap = self._api.get(
                f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/snapshots/{snapshot_name}",
            )
            fetching_snapshot = False
            application = (snap.get("metadata", {}).get("labels") or {}).get("appstudio.openshift.io/application", "")
            if not application:
                return None

            operation = f"list ReleasePlans for Snapshot {snapshot_name}, application {application}"
            plans = self._api.list(
                f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/releaseplans",
            )
            candidates = [
                item["metadata"]["name"]
                for item in plans.get("items", [])
                if item.get("spec", {}).get("application") == application
                and (item.get("metadata", {}).get("labels") or {}).get("release.appstudio.openshift.io/auto-release")
                != "false"
            ]

            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) > 1:
                print(
                    f"ERROR: {len(candidates)} auto-releasing ReleasePlans match application "
                    f"'{application}': {', '.join(sorted(candidates))}. Refusing to guess.",
                    file=sys.stderr,
                )
            return None
        except requests.RequestException as e:
            self._log_api_error(operation, e, ignore_not_found=fetching_snapshot)
            return None
        except KeyError:
            return None

    def create_release(
        self, snapshot_name: str, release_plan: str, prefix: str, fingerprint: str | None = None
    ) -> str | None:
        """Create a Release for the given snapshot and return its name.

        When ``fingerprint`` is given it is stamped under the
        :data:`~import_orchestrator.constants.FINGERPRINT_LABEL` label so a later
        run can recognise this Release by querying on the label rather than
        trusting the local cache (ADR-0001).

        Raises ReleaseError if the API rejects the request (e.g. a permissions
        gap or an invalid releasePlan reference), mirroring create_pipelinerun's
        TriggerError -- a rejected create call will not resolve itself by
        retrying the identical request on the next poll, so the caller should
        record a failed release phase instead of looping forever with no state
        change (LWLP-3937). Returns None only if the Release was created but
        its name is absent from the response.
        """
        metadata: dict[str, object] = {
            "generateName": prefix,
            "namespace": self.namespace,
        }
        if fingerprint is not None:
            metadata["labels"] = {FINGERPRINT_LABEL: fingerprint}
        manifest = {
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "Release",
            "metadata": metadata,
            "spec": {
                "releasePlan": release_plan,
                "snapshot": snapshot_name,
            },
        }
        try:
            result = self._api.create(
                f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/releases",
                manifest,
            )
        except requests.RequestException as e:
            # Imported lazily: engine.errors pulls in the engine package, which
            # imports clients, so a module-level import would be circular.
            from import_orchestrator.engine.errors import ReleaseError

            raise ReleaseError(f"failed to create Release for Snapshot {snapshot_name}: {_api_error_detail(e)}") from e

        try:
            return result["metadata"]["name"]
        except KeyError as e:
            print(f"ERROR: Invalid create Release response for Snapshot {snapshot_name}: missing {e}", file=sys.stderr)
            return None

    def create_pipelinerun(self, manifest: dict) -> str | None:
        """Create a PipelineRun from a manifest dict and return its generated name.

        Raises TriggerError if the API rejects the request (e.g. an invalid
        pipeline definition), including the cluster's error message. Returns
        None only if the PipelineRun was created but its name is absent from
        the response.
        """
        try:
            result = self._api.create(
                f"/apis/tekton.dev/v1/namespaces/{self.namespace}/pipelineruns",
                manifest,
            )
        except requests.RequestException as e:
            # Imported lazily: engine.errors pulls in the engine package, which
            # imports clients, so a module-level import would be circular.
            from import_orchestrator.engine.errors import TriggerError

            raise TriggerError(f"failed to create PipelineRun: {_api_error_detail(e)}") from e

        try:
            return result["metadata"]["name"]
        except KeyError:
            return None

    def find_release_for_snapshot(self, snapshot_name: str) -> str | None:
        """Find an active (non-terminally-failed) Release for the given snapshot.

        Matches a Release created against any ReleasePlan. When several match, the
        newest by creationTimestamp is returned (see find_release_for_snapshot_and_plan).
        """
        return self.find_release_for_snapshot_and_plan(snapshot_name, None)

    def find_release_for_snapshot_and_plan(self, snapshot_name: str, release_plan: str | None = None) -> str | None:
        """Find an active Release for a snapshot, optionally scoped to one ReleasePlan.

        When `release_plan` is given, a Release created against a *different* plan for the
        same snapshot is not matched. Promotion deliberately creates a second Release for
        content that already has a successful stage Release; treating that stage Release as
        "already done" would silently skip the promotion. When `release_plan` is None, a
        Release against any plan matches.

        If several Releases match, the newest by `metadata.creationTimestamp` is returned so
        adoption is deterministic rather than dependent on API response order.
        """
        try:
            result = self._api.list(
                f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/releases",
            )
            candidates = []
            for item in result.get("items", []):
                spec = item.get("spec", {})
                if spec.get("snapshot") != snapshot_name:
                    continue
                if release_plan is not None and spec.get("releasePlan") != release_plan:
                    continue
                released = next(
                    (c for c in item.get("status", {}).get("conditions", []) if c.get("type") == "Released"),
                    None,
                )
                # Skip terminally failed releases so a new one gets created
                if released and released.get("status") == "False" and released.get("reason") != "Progressing":
                    continue
                candidates.append(item)
            if not candidates:
                return None
            newest = max(candidates, key=lambda item: item.get("metadata", {}).get("creationTimestamp", ""))
            return newest["metadata"]["name"]
        except requests.RequestException as e:
            self._log_api_error(f"find Release for Snapshot {snapshot_name}, ReleasePlan {release_plan or 'any'}", e)
            return None
        except KeyError:
            return None

    def list_releases_for_snapshot_and_plan(
        self, snapshot_name: str, release_plan: str | None = None
    ) -> list[tuple[str, Literal["True", "False", "Unknown"]]]:
        """List every Release for a snapshot (optionally scoped to a ReleasePlan), newest first.

        Unlike :meth:`find_release_for_snapshot_and_plan`, this does *not* filter out
        terminally-failed Releases: reconciliation needs the complete set so it can
        count release retries (each terminal Release is one spent Attempt) and record
        the newest Release's real status. Each entry is ``(name, status)`` where status
        is the effective ``Released`` condition ("True"/"False"/"Unknown"), newest first
        by ``creationTimestamp``.

        Raises :class:`requests.RequestException` on a failed list, so the caller can
        tell "query failed" from "no Releases exist" rather than silently undercounting
        the retry budget (which would reset ``max_retries`` across a cache wipe).
        """
        result = self._api.list(
            f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/releases",
        )
        matches = []
        for item in result.get("items", []):
            spec = item.get("spec", {})
            if spec.get("snapshot") != snapshot_name:
                continue
            if release_plan is not None and spec.get("releasePlan") != release_plan:
                continue
            matches.append(item)
        matches.sort(key=lambda i: i.get("metadata", {}).get("creationTimestamp", ""), reverse=True)
        releases: list[tuple[str, Literal["True", "False", "Unknown"]]] = []
        for item in matches:
            name = (item.get("metadata", {}) or {}).get("name")
            if name:
                releases.append((name, self._released_status(item)))
        return releases

    @staticmethod
    def _released_status(item: dict) -> Literal["True", "False", "Unknown"]:
        """Effective ``Released``-condition status of a Release list item.

        Mirrors :meth:`get_release_status`: True on success, False only on a terminal
        failure (reason != "Progressing"), Unknown while in flight or unset.
        """
        released = next(
            (c for c in item.get("status", {}).get("conditions", []) if c.get("type") == "Released"),
            None,
        )
        if released is None:
            return "Unknown"
        status = released.get("status", "")
        reason = released.get("reason", "")
        if status == "True":
            return "True"
        if status == "False" and reason != "Progressing":
            return "False"
        return "Unknown"

    def get_snapshot_test_status(
        self, snapshot_name: str, snap: dict | None = None
    ) -> Literal["passed", "failed", "pending", "none"] | None:
        """Return the overall IntegrationTestScenario verdict for a Snapshot.

        Konflux auto-runs the scenarios that apply to the Snapshot and records the
        aggregate result in the ``AppStudioTestSucceeded`` status condition -- honouring
        scenario contexts and optional scenarios, and setting it True right away when no
        required scenario applies. Reading that condition (rather than re-deriving a
        verdict from the per-scenario annotation) is what keeps this in step with
        Konflux's own release decision:

        - ``"failed"`` if the condition is False (required tests failed),
        - ``"passed"`` if the condition is True and scenarios actually ran,
        - ``"none"`` if the condition is True but nothing ran (no required scenario),
        - ``"pending"`` if the condition is not set yet (tests still in flight).

        Returns None only if the Snapshot itself cannot be fetched, so the caller can
        retry rather than mistake a transient API error for a verdict. Pass ``snap`` to
        reuse an already-fetched Snapshot and avoid a redundant API call.
        """
        if snap is None:
            snap = self._get_snapshot(snapshot_name, "test status")
        if snap is None:
            return None

        condition = next(
            (
                c
                for c in snap.get("status", {}).get("conditions", [])
                if c.get("type") == self._KONFLUX_TEST_SUCCEEDED_CONDITION
            ),
            None,
        )
        if condition is None:
            return "pending"  # Not evaluated yet; tests are still running.
        status = condition.get("status")
        if status == "False":
            return "failed"
        if status == "True":
            # The condition is True both when required tests passed and when none were
            # required. A populated results annotation means scenarios actually ran.
            return "passed" if self._snapshot_has_test_results(snap) else "none"
        return "pending"  # "Unknown" or anything unexpected: wait.

    def get_snapshot_test_failure_detail(self, snapshot_name: str, snap: dict | None = None) -> str | None:
        """Summarise the failing scenarios from the Snapshot's results annotation.

        Used to enrich the integration-tests phase error with the scenario name, status,
        details and test PipelineRun. Optional scenarios and passing/in-flight scenarios
        are skipped (they do not drive the failure). Returns None when no detail is
        available (annotation absent, unparseable, or nothing to report). Pass ``snap``
        to reuse an already-fetched Snapshot and avoid a redundant API call.
        """
        if snap is None:
            snap = self._get_snapshot(snapshot_name, "test failure detail")
        if snap is None:
            return None
        results = self._parse_test_results(snap, snapshot_name)
        if not results:
            return None

        parts = []
        for entry in results:
            if not isinstance(entry, dict) or entry.get("isOptionalScenario"):
                continue
            status = str(entry.get("status", ""))
            if status in self._KONFLUX_ITS_PASSING_STATUSES or status in self._KONFLUX_ITS_PENDING_STATUSES:
                continue
            line = f"{entry.get('scenario', '?')}: {status or 'unknown'}"
            if entry.get("details"):
                line += f" - {entry['details']}"
            if entry.get("testPipelineRunName"):
                line += f" ({entry['testPipelineRunName']})"
            parts.append(line)
        return "; ".join(parts) if parts else None

    def get_snapshot(self, snapshot_name: str) -> dict | None:
        """Fetch a Snapshot once, or None (logged) if it cannot be read.

        Lets a caller read the Snapshot a single time and feed it to both
        ``get_snapshot_test_status`` and ``get_snapshot_test_failure_detail``.
        """
        return self._get_snapshot(snapshot_name, "test status")

    def get_snapshot_fingerprint(self, snapshot_name: str) -> str | None:
        """Read the fingerprint label Konflux propagated onto the Snapshot.

        The intake PipelineRun is stamped with
        :data:`~import_orchestrator.constants.FINGERPRINT_LABEL`, and Konflux copies
        labels under the ``custom.appstudio.openshift.io/`` prefix onto the Snapshot
        it derives (ADR-0001), so the Snapshot carries the same value. Returns None
        if the Snapshot cannot be read or has no fingerprint label -- in which case a
        Release created from it is simply left unlabeled.
        """
        snap = self._get_snapshot(snapshot_name, "fingerprint")
        if snap is None:
            return None
        return (snap.get("metadata", {}).get("labels") or {}).get(FINGERPRINT_LABEL)

    def _get_snapshot(self, snapshot_name: str, purpose: str) -> dict | None:
        """Fetch a Snapshot, or None (logged) if it cannot be read."""
        try:
            return self._api.get(
                f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/snapshots/{snapshot_name}",
            )
        except requests.RequestException as e:
            self._log_api_error(f"get Snapshot {snapshot_name} {purpose}", e, ignore_not_found=True)
            return None

    def _parse_test_results(self, snap: dict, snapshot_name: str) -> list | None:
        """Parse the per-scenario results annotation into a list, or None if absent/invalid."""
        raw = (snap.get("metadata", {}).get("annotations") or {}).get(self._KONFLUX_ITS_RESULTS_ANNOTATION)
        if not raw:
            return None
        try:
            results = json.loads(raw)
        except (ValueError, TypeError):
            print(f"  Unparseable test status on snapshot {snapshot_name}", file=sys.stderr)
            return None
        return results if isinstance(results, list) else None

    def _snapshot_has_test_results(self, snap: dict) -> bool:
        """Whether the Snapshot's results annotation reports at least one scenario."""
        results = self._parse_test_results(snap, snap.get("metadata", {}).get("name", "?"))
        return bool(results)

    def get_release_status(self, release_name: str) -> Literal["True", "False", "Unknown"] | None:
        """Get the effective status of the 'Released' condition.

        Returns "True" on success, "False" on terminal failure, "Unknown" while still progressing,
        and None if the Release object itself cannot be fetched.

        The Released condition starts as False/Progressing while in flight, so we only treat
        False as a failure when the reason is not "Progressing".
        """
        try:
            data = self._api.get(
                f"/apis/appstudio.redhat.com/v1alpha1/namespaces/{self.namespace}/releases/{release_name}",
            )
            released = next(
                (c for c in data.get("status", {}).get("conditions", []) if c.get("type") == "Released"),
                None,
            )
            if released is None:
                return "Unknown"
            status = released.get("status", "")
            reason = released.get("reason", "")
            if status == "True":
                return "True"
            if status == "False" and reason != "Progressing":
                return "False"
            return "Unknown"
        except requests.RequestException as e:
            self._log_api_error(f"get Release {release_name} status", e, ignore_not_found=True)
            return None
