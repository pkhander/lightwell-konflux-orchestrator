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

import sys

import requests

from import_orchestrator.clients import KubeClient
from import_orchestrator.engine.request_view import RequestView
from import_orchestrator.models import (
    Attempt,
    AttemptPhase,
    ClusterResource,
    Fingerprint,
    Phase,
    PhaseStatus,
    PipelineRunStatus,
    Request,
)
from import_orchestrator.utils import extract_tag


class _Unreadable(Exception):
    """A snapshot or integration-test read failed while reconstructing one Request.

    The Request is skipped with no cache write. A partial chain would let the release
    monitor create a Release without having counted the Releases already on the cluster.
    """


class Reconciler:
    """Rebuilds cached Request state from the cluster before the run loop acts.

    The local database is a cache; the cluster is the source of truth (ADR-0001). At
    the start of a run, for each Request this reconstructs the Attempt/phase history
    that the cluster implies -- from the intake PipelineRuns and Releases the
    orchestrator stamped with the Request fingerprint (LWLP-2511) -- and makes the
    cache match it. The existing trigger and monitors then resume from that state
    instead of re-triggering work that already exists.

    How the cluster maps to Attempts:

    - Each intake PipelineRun is one build. A new intake build is only submitted when
      the previous Attempt had no successful intake, so N intake PipelineRuns mean N
      Attempts that reached the build step (ordered oldest-first by creationTimestamp).
    - A *release* retry does not create a new intake PipelineRun: the trigger carries
      the successful intake forward and the release monitor creates another Release on
      the new Attempt. So the newest intake PipelineRun can stand for several Attempts
      -- one per Release object for its Snapshot. Counting those Releases is what keeps
      the reconstructed ``attempt_count`` (and therefore the ``max_retries`` budget)
      correct across a cache wipe.
    - An integration-test retry, and a ``create_release`` failure, add an Attempt that
      shares that intake and creates no new cluster object. The cluster cannot name
      those Attempts. Once the cache has recorded them they are kept, so a spent
      ``max_retries`` budget is not reset on the next run.

    Safety invariants:

    - Read-only against the cluster: it never creates or patches cluster resources.
    - A live query failure is *not* "nothing exists": the Request is skipped this run
      (its id is returned from :meth:`reconcile_all`) and the trigger must not fire for
      it, so a transient error cannot downgrade a Request to re-triggerable and
      duplicate a build that is already running.
    - An unreadable snapshot or integration-test read skips the Request with no
      cache write, same as a failed list. A partial chain would let the monitor
      create a Release without counting the ones already on the cluster.
    - An empty Release list is not proof the Releases are gone. When the cache
      already records Release phases, those Attempts are left in place so a
      truncated or momentarily empty list cannot wipe a settled success and
      cause another Release to be created.
    - No intake PipelineRun at all leaves the Request untouched for the trigger to
      start fresh.
    - Only Requests in this run's view are reconciled. A scratch run must not rewrite
      another target's Attempts in the same database.
    """

    def __init__(self, view: RequestView, kube: KubeClient, release_plan: str | None = None):
        self.view = view
        self.db = view.db
        self.kube = kube
        self.skip_release = view.skip_release
        self.release_plan = release_plan

    def reconcile_all(self) -> set[int]:
        """Reconcile this run's Requests against the cluster.

        The view is the scope. ``orchestrate --target SCRATCH`` and ``run`` for one
        ref must not rewrite other Requests that share the database: ``skip_release``
        is a property of this run, and applying it outside the view drops Release
        history the cluster still has.

        Returns the ids of Requests that could not be reconciled this run because a
        live cluster query failed. The caller must not trigger those Requests this
        run: a failed query is "unknown", not "nothing exists", and acting on it
        would risk duplicating work already on the cluster.
        """
        unreconciled: set[int] = set()
        for request in self.view.requests():
            if request.id is None:
                continue
            if not self._reconcile_request(request):
                unreconciled.add(request.id)
        return unreconciled

    def _reconcile_request(self, request: Request) -> bool:
        """Reconcile one Request. Returns False if a live query failed (skip it this run)."""
        if request.id is None:
            raise ValueError("cannot reconcile a Request that has not been persisted (id is None)")
        fingerprint = Fingerprint.from_request(request).hash()
        tag = extract_tag(request.artifact)
        try:
            specs = self._reconstruct(fingerprint)
        except (requests.RequestException, _Unreadable):
            print(
                f"  Skipping {tag}: cluster query failed; leaving cache untouched this run",
                file=sys.stderr,
            )
            return False
        if specs is None:
            # no intake PipelineRun: leave the Request for the trigger to start fresh
            print(
                f"  {tag}: no intake PipelineRun on cluster; leaving for the trigger to start fresh",
                file=sys.stderr,
            )
            return True
        self._apply(request.id, specs, tag)
        return True

    # -- Read phase: reconstruct the intended Attempt history from the cluster -----
    #
    # These methods only read. A failed snapshot list or an unreadable snapshot /
    # integration-test status raises, so reconstruction aborts before any cache
    # write. A snapshot that genuinely does not exist yet (an empty list) still
    # leaves that phase unrecorded for the monitor to discover in-loop.

    def _reconstruct(self, fingerprint: str) -> list[list[AttemptPhase]] | None:
        """Build the Attempt phase-plans the cluster implies, oldest Attempt first.

        Returns None when there is no intake PipelineRun. Raises
        :class:`requests.RequestException` on a live-list failure and
        :class:`_Unreadable` when a snapshot or integration-test read fails.
        """
        intake_prs = self.kube.find_intake_pipelinerun_statuses_by_fingerprint(fingerprint)
        if not intake_prs:
            return None

        # Every intake PipelineRun but the newest belongs to a prior Attempt whose
        # intake did not succeed (otherwise no new build would have been submitted).
        specs: list[list[AttemptPhase]] = [self._chain_for_pr(pr) for pr in intake_prs[:-1]]

        last_chain = self._chain_for_pr(intake_prs[-1])
        if not self._reached_release_gate(last_chain):
            specs.append(last_chain)
            return specs

        # The newest intake reached the release step. Each Release object for its
        # Snapshot is one Attempt (release retries share the carried-forward intake),
        # oldest-first; the newest Release decides the latest Attempt's state.
        snapshot_name = self._ref(last_chain, Phase.SNAPSHOT)
        if snapshot_name is None:
            raise RuntimeError("release gate reached but the intake chain has no Snapshot reference")
        releases = self.kube.list_releases_for_snapshot_and_plan(snapshot_name, self.release_plan)
        if not releases:
            specs.append(last_chain)  # no Release yet: leave it for the monitor to create in-loop
            return specs
        for name, status in reversed(releases):  # reversed(newest-first) -> oldest-first
            specs.append(last_chain + [self._release_phase(name, status)])
        return specs

    def _chain_for_pr(self, pr: PipelineRunStatus) -> list[AttemptPhase]:
        """Phase-plan for one Attempt, walked from its intake PipelineRun up to the ITS gate.

        The release phase is deliberately *not* added here; the caller decides it for the
        newest intake by counting Release objects (see :meth:`_reconstruct`).
        """
        if pr.is_failed:
            detail = self.kube.get_pipelinerun_failure_detail(pr.name)
            return [
                self._phase(
                    Phase.INTAKE,
                    PhaseStatus.FAILED,
                    ClusterResource.PIPELINE_RUN,
                    pr.name,
                    error_message=detail or "PipelineRun failed",
                )
            ]
        if pr.is_running:
            return [self._phase(Phase.INTAKE, PhaseStatus.RUNNING, ClusterResource.PIPELINE_RUN, pr.name)]

        phases = [self._phase(Phase.INTAKE, PhaseStatus.SUCCESS, ClusterResource.PIPELINE_RUN, pr.name)]
        snapshot_name = self.kube.find_snapshot_by_pipelinerun(pr.name, raise_on_error=True)
        if not snapshot_name:
            return phases
        phases.append(self._phase(Phase.SNAPSHOT, PhaseStatus.SUCCESS, ClusterResource.SNAPSHOT, snapshot_name))
        its = self._integration_tests_phase(snapshot_name)
        if its is not None:
            phases.append(its)
        return phases

    def _integration_tests_phase(self, snapshot_name: str) -> AttemptPhase | None:
        """The integration-tests phase from the Snapshot.

        Mirrors the release monitor's gate so reconciled and in-loop state agree.
        Raises :class:`_Unreadable` when the Snapshot cannot be read: omitting the
        gate would skip the Release count and the monitor would create one anyway.
        """
        snap = self.kube.get_snapshot(snapshot_name)
        status = None if snap is None else self.kube.get_snapshot_test_status(snapshot_name, snap)
        if status is None:
            raise _Unreadable(snapshot_name)
        if status == "none":
            return self._phase(Phase.INTEGRATION_TESTS, PhaseStatus.SKIPPED)
        if status == "pending":
            return self._phase(Phase.INTEGRATION_TESTS, PhaseStatus.RUNNING)
        if status == "passed":
            return self._phase(Phase.INTEGRATION_TESTS, PhaseStatus.SUCCESS)
        detail = self.kube.get_snapshot_test_failure_detail(snapshot_name, snap)
        return self._phase(
            Phase.INTEGRATION_TESTS,
            PhaseStatus.FAILED,
            error_message=detail or f"Integration tests failed for snapshot {snapshot_name}",
        )

    def _reached_release_gate(self, chain: list[AttemptPhase]) -> bool:
        """Whether this Attempt passed the ITS gate and would proceed to a Release."""
        if self.skip_release:
            return False
        has_snapshot = any(p.name == Phase.SNAPSHOT and p.status == PhaseStatus.SUCCESS for p in chain)
        its_ok = any(
            p.name == Phase.INTEGRATION_TESTS and p.status in (PhaseStatus.SUCCESS, PhaseStatus.SKIPPED) for p in chain
        )
        return has_snapshot and its_ok

    @staticmethod
    def _release_phase(name: str, status: str) -> AttemptPhase:
        """A release phase-plan item from a Release's effective ``Released`` status."""
        if status == "True":
            return Reconciler._phase(Phase.RELEASE, PhaseStatus.SUCCESS, ClusterResource.RELEASE, name)
        if status == "False":
            return Reconciler._phase(
                Phase.RELEASE,
                PhaseStatus.FAILED,
                ClusterResource.RELEASE,
                name,
                error_message=f"Release {name} failed",
            )
        return Reconciler._phase(Phase.RELEASE, PhaseStatus.RUNNING, ClusterResource.RELEASE, name)

    # -- Write phase: make the cache match the reconstructed history ----------------

    def _apply(self, request_id: int, specs: list[list[AttemptPhase]], tag: str) -> None:
        """Reconcile the cached Attempts for a Request to ``specs`` (oldest Attempt first).

        Fast path -- the cache already has one Attempt per spec with matching intake
        references, in order: only the latest Attempt (the sole live one; earlier ones
        are terminal and immutable) is refreshed in place, keeping ids and timestamps
        stable and the pass idempotent. Extra Attempts that repeat the newest intake
        are kept as well: the cluster has no object for an integration-test retry or
        a failed ``create_release``, and deleting them would reset ``attempt_count``.
        Otherwise the cache disagrees with the cluster on Attempt identity, count, or
        order (an empty/partial cache, an undercounted release-retry budget, or a
        chronology inversion from SQLite's monotonic ids): the cache is disposable,
        so the Request's Attempts are rebuilt from the cluster in timestamp order.
        """
        existing = self.db.list_attempts(request_id)
        if self._empty_release_list_would_drop_cached_releases(existing, specs):
            # The gate was passed and the Release list returned nothing, but the cache
            # already has Release phases. Rebuilding would delete that history (including
            # a settled SUCCESS) and the monitor would create a new Release. Keep the cache.
            print(
                f"  Keeping cached releases for {tag}: release list came back empty",
                file=sys.stderr,
            )
            return
        if self._matches(existing, specs):
            self._refresh_latest(existing, specs)
            return
        if self._keeps_unrepresented_retries(existing, specs):
            print(
                f"  Keeping cached retry history for {tag} ({len(existing)} attempt(s))",
                file=sys.stderr,
            )
            self._refresh_latest(existing, specs)
            return

        if existing:
            print(f"  Rebuilding {tag} attempt history from cluster ({len(specs)} attempt(s))", file=sys.stderr)
            self.db.delete_attempts(request_id)
        else:
            print(f"  Reconciled {len(specs)} attempt(s) for {tag} from cluster", file=sys.stderr)
        for spec in specs:
            self.db.create_attempt(request_id, initial_phases=spec)

    def _empty_release_list_would_drop_cached_releases(
        self, existing: list[Attempt], specs: list[list[AttemptPhase]]
    ) -> bool:
        """True when applying ``specs`` would erase Release phases the cache already has.

        Reaching the release gate with no Release phase on the newest spec means
        ``list_releases_for_snapshot_and_plan`` returned an empty list. That is safe to
        record only when the cache has no Release yet (the monitor then creates the
        first one). Once Release attempts are cached, an empty list must not shrink
        them: a one-Attempt SUCCESS survives the fast path by accident, but a retried
        Request has more Attempts than that single pre-release chain, and the rebuild
        would delete the successful Release.
        """
        newest = specs[-1]
        if not self._reached_release_gate(newest):
            return False
        if any(phase.name == Phase.RELEASE for phase in newest):
            return False
        for attempt in existing:
            if attempt.id is None:
                continue
            cached = self.db.get_phase(attempt.id, Phase.RELEASE)
            if cached is not None:
                return True
        return False

    def _matches(self, existing: list[Attempt], specs: list[list[AttemptPhase]]) -> bool:
        """True when the cached Attempts already line up, in order, with ``specs``.

        Matched by intake ``reference_id`` per position: this catches a count mismatch
        (empty/partial cache, undercounted release retries) and an order mismatch (an
        older PipelineRun that landed a higher rowid than a newer one), while a cached
        Attempt with no intake phase (a crash window) also forces a rebuild so it is
        reconstructed rather than duplicated.
        """
        if len(existing) != len(specs):
            return False
        for attempt, spec in zip(existing, specs):
            if self._cached_intake_ref(attempt) != self._ref(spec, Phase.INTAKE):
                return False
        return True

    def _keeps_unrepresented_retries(self, existing: list[Attempt], specs: list[list[AttemptPhase]]) -> bool:
        """True when shrinking to ``specs`` would drop retries the cluster cannot name.

        The reconstructed specs are a prefix of the cache, in order, and every extra
        Attempt carries the newest intake PipelineRun. Those extras are integration-test
        retries or ``create_release`` failures. A newer Release object or an older
        PipelineRun still fails this check, so that history is rebuilt.
        """
        if len(existing) <= len(specs):
            return False
        newest = self._ref(specs[-1], Phase.INTAKE)
        if newest is None:
            return False
        for attempt, spec in zip(existing, specs):
            if self._cached_intake_ref(attempt) != self._ref(spec, Phase.INTAKE):
                return False
        return all(self._cached_intake_ref(attempt) == newest for attempt in existing[len(specs) :])

    def _cached_intake_ref(self, attempt: Attempt) -> str | None:
        if attempt.id is None:
            return None
        cached = self.db.get_phase(attempt.id, Phase.INTAKE)
        return cached.reference_id if cached is not None else None

    def _refresh_latest(self, existing: list[Attempt], specs: list[list[AttemptPhase]]) -> None:
        """Write the newest spec onto the latest cached Attempt, leaving earlier ones."""
        latest_id = existing[-1].id
        assert latest_id is not None
        for phase in specs[-1]:
            self._upsert(latest_id, phase)

    # -- Small helpers -------------------------------------------------------------

    @staticmethod
    def _phase(
        name: Phase,
        status: PhaseStatus,
        reference_type: ClusterResource | None = None,
        reference_id: str | None = None,
        *,
        error_message: str | None = None,
    ) -> AttemptPhase:
        """Build a phase-plan item (an unsaved :class:`AttemptPhase`); validates invariants."""
        return AttemptPhase(
            id=None,
            attempt_id=0,  # placeholder; create_attempt/_upsert bind the real Attempt id
            name=name,
            status=status,
            reference_type=reference_type,
            reference_id=reference_id,
            error_message=error_message,
        )

    @staticmethod
    def _ref(chain: list[AttemptPhase], name: Phase) -> str | None:
        for phase in chain:
            if phase.name == name:
                return phase.reference_id
        return None

    def _upsert(self, attempt_id: int, phase: AttemptPhase) -> None:
        self.db.upsert_phase(
            attempt_id,
            phase.name,
            phase.status,
            reference_type=phase.reference_type,
            reference_id=phase.reference_id,
            error_message=phase.error_message,
        )
