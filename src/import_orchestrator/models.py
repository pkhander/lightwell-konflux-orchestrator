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

import hashlib
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal

# Kubernetes caps a label *value* at 63 characters.
_MAX_LABEL_VALUE_LEN = 63


@dataclass
class PipelineRunStatus:
    """Status of a PipelineRun."""

    name: str
    status: Literal["True", "False", "Unknown"]

    @property
    def is_running(self) -> bool:
        return self.status == "Unknown"

    @property
    def is_successful(self) -> bool:
        return self.status == "True"

    @property
    def is_failed(self) -> bool:
        return self.status == "False"

    @staticmethod
    def from_str(name: str, status: str) -> PipelineRunStatus | None:
        if name and status in ("True", "False", "Unknown"):
            return PipelineRunStatus(name, status)
        return None


# ---------------------------------------------------------------------------
# Request / Attempt / AttemptPhase model (ADR-0001).
#
# These replace the former flat import-item model: a ``Request`` is the intent to release
# one artifact for one target; each run of the workflow is an ``Attempt``; and an
# ``Attempt`` accrues an ``AttemptPhase`` row for each of the four phases as the
# workflow reaches it (rows are created on demand, not pre-seeded). Retrying
# creates a new ``Attempt`` that copies phases that already succeeded, so a
# completed phase is not re-run and no history is overwritten. A skipped phase
# is not copied; the next attempt decides again whether that phase applies.
# See docs/design/adr-0001-reconciliation.md.
# ---------------------------------------------------------------------------


class ClusterResource(str, Enum):
    """The kind of cluster resource an :class:`AttemptPhase` points at."""

    PIPELINE_RUN = "pipelineRun"
    SNAPSHOT = "snapshot"
    RELEASE = "release"


class PruneOutcome(str, Enum):
    """Result of deleting a Request's cluster resources (LWLP-5515).

    ``DELETED`` means every targeted resource is gone (deleted now or already
    absent). ``RETRY`` is a transient failure the caller should attempt again on a
    later cycle. ``FORBIDDEN`` is an RBAC denial (HTTP 403): a permanent condition
    for the run, so the caller should stop pruning rather than re-issuing a request
    that will 403 every cycle.
    """

    DELETED = "deleted"
    RETRY = "retry"
    FORBIDDEN = "forbidden"


class Phase(str, Enum):
    """One of the four phases of a release workflow (ADR-0001)."""

    INTAKE = "intake"
    SNAPSHOT = "snapshot"
    INTEGRATION_TESTS = "integration_tests"
    RELEASE = "release"


# The one cluster resource each phase may point at. Intake tracks a PipelineRun,
# release a Release, and so on.
PHASE_REFERENCE_TYPE = {
    Phase.INTAKE: ClusterResource.PIPELINE_RUN,
    Phase.SNAPSHOT: ClusterResource.SNAPSHOT,
    Phase.INTEGRATION_TESTS: ClusterResource.PIPELINE_RUN,
    Phase.RELEASE: ClusterResource.RELEASE,
}


class PhaseStatus(str, Enum):
    """Status of a single :class:`AttemptPhase`.

    ``SKIPPED`` marks a phase that does not apply to this attempt (e.g. no
    IntegrationTestScenario exists for the snapshot).
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

    @property
    def carries_forward(self) -> bool:
        """Whether a phase in this status is copied onto a retry Attempt (ADR-0001).

        Only SUCCESS is copied, so a completed phase is not re-run. PENDING,
        RUNNING, FAILED, and SKIPPED are left behind.

        TODO: Consider carrying forward SKIPPED phases as it is mostly related to
        IntegrationTestScenario not being found.
        """
        return self is PhaseStatus.SUCCESS


@dataclass
class Request:
    """An intent to release a single artifact for a specific target.

    The combination of (namespace, application, component, pipeline,
    release_plan, target, artifact, source_repo) is the request's fingerprint and is unique:
    re-importing the same artifact for the same target returns the existing
    Request rather than creating a duplicate.
    """

    id: int | None
    namespace: str
    application: str
    component: str
    pipeline: str
    release_plan: str
    artifact: str
    target: str
    created_at: datetime | None = None
    source_repo: str = ""
    # Set once the Request's cluster resources have been pruned; None while live.
    pruned_at: datetime | None = None


@dataclass(frozen=True)
class Fingerprint:
    """A stable identity for a :class:`Request`, derived from the fields that make
    it unique (ADR-0001, §Fingerprinting).

    :meth:`hash` projects these seven fields to a deterministic digest that is
    stamped as the :data:`~import_orchestrator.constants.FINGERPRINT_LABEL` label
    onto the resources the orchestrator creates (intake PipelineRun, Release). A
    later invocation can then recognise those resources by querying on the label
    instead of trusting the local cache (the querying side is LWLP-2512).
    """

    namespace: str
    application: str
    component: str
    pipeline: str
    # ``release_plan`` is optional: a Request that skips the release step has no
    # plan, so the fingerprint must stay well-defined when it is absent.
    # :meth:`hash` serialises ``None`` as an empty field.
    release_plan: str | None
    target: str
    artifact: str

    @classmethod
    def from_request(cls, request: Request) -> Fingerprint:
        """Project a :class:`Request` onto its fingerprint fields."""
        return cls(
            namespace=request.namespace,
            application=request.application,
            component=request.component,
            pipeline=request.pipeline,
            release_plan=request.release_plan,
            target=request.target,
            artifact=request.artifact,
        )

    def hash(self) -> str:
        """Return a deterministic, K8s-label-safe digest of the fingerprint.

        Uses :mod:`hashlib` (SHA-256) rather than the builtin ``hash()``: the
        builtin is salted per process, so it would produce a different value on
        every invocation and break the "same Request -> same label across runs"
        guarantee (AC #5). The seven fields are joined in a fixed order with a
        separator that cannot occur in the values, so distinct Requests cannot
        collide by field-boundary ambiguity. The hex digest (``[0-9a-f]``) is
        truncated to 63 chars to fit a Kubernetes label value.

        This serialization is a *wire format*: the field order and the delimiter
        below are load-bearing, because a resource stamped by an earlier run (or
        an earlier build of this tool) must still match on a later run. Changing
        either silently invalidates every previously stamped label, so they must
        not be reordered -- which is why the tuple is spelled out explicitly here
        rather than derived from the dataclass field order. A golden-value test
        (tests/test_models.py) pins the digest to catch any drift.
        """
        # NUL cannot appear in these string fields, so it is an unambiguous
        # delimiter: ("a", "bc") and ("ab", "c") serialise differently. An
        # absent release_plan serialises as the empty field; a real plan is a
        # non-empty resource name, so the two cannot collide.
        canonical = "\0".join(
            (
                self.namespace,
                self.application,
                self.component,
                self.pipeline,
                self.release_plan or "",
                self.target,
                self.artifact,
            )
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:_MAX_LABEL_VALUE_LEN]


@dataclass
class Attempt:
    """A single execution of the build/release workflow for a Request.

    A Request has one or more Attempts; a new Attempt is created on retry.
    """

    id: int | None
    request_id: int
    created_at: datetime | None = None


@dataclass
class AttemptPhase:
    """The state of one phase (intake/snapshot/integration tests/release)
    within an Attempt, including a reference to the cluster resource it tracks
    and any error message captured when the phase failed."""

    id: int | None
    attempt_id: int
    name: Phase
    status: PhaseStatus
    reference_type: ClusterResource | None = None
    reference_id: str | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        """Enforce the phase-model invariants (ADR-0001) on construction.

        Because every AttemptPhase validates itself here, the store can persist one
        without knowing any of these rules, and any backend reusing this model gets
        them for free:

        - ``reference_type`` and ``reference_id`` are a unit: both or neither.
        - the reference must match the phase (see :data:`PHASE_REFERENCE_TYPE`) --
          an intake phase may only point at a pipelineRun, etc.
        - ``error_message`` tracks the status: it is kept only while FAILED, so it
          is dropped for any other status.

        Raises:
            ValueError: if only one of reference_type/reference_id is supplied, or
                the reference type does not match the phase.
        """
        if (self.reference_type is None) != (self.reference_id is None):
            raise ValueError("reference_type and reference_id must be supplied together")
        expected = PHASE_REFERENCE_TYPE[self.name]
        if self.reference_type is not None and self.reference_type is not expected:
            raise ValueError(
                f"{self.name.value} phase must reference a {expected.value}, not a {self.reference_type.value}"
            )
        if self.status is not PhaseStatus.FAILED:
            self.error_message = None


class RequestState(str, Enum):
    """The overall state of a Request, derived from its latest Attempt's phases.

    This is a *reporting* projection of the per-phase model onto a single value
    (for progress statistics, trigger batching and completion checks); it is not
    persisted. The string values match the former flat status enum so statistics
    output and the ``run-single`` JSON payload stay backwards compatible after the
    Request/Attempt cutover.
    """

    PENDING = "pending"
    RUNNING = "running"
    AWAITING_RELEASE = "releasing"
    SUCCESS = "success"
    FAILED = "failed"


def derive_state(phases: dict[Phase, AttemptPhase], skip_release: bool) -> RequestState:
    """Reduce the latest Attempt's phase rows to a single :class:`RequestState`.

    ``phases`` maps each recorded :class:`Phase` to its :class:`AttemptPhase` for
    the request's latest attempt (as returned by
    :meth:`WorkflowDatabase.latest_phase_map`); an empty map means no attempt has
    started yet. ``skip_release`` is True for targets whose workflow ends at the
    integration tests (no Release CR is produced), so a passed (or skipped) ITS phase
    is the terminal success state; otherwise a successful Release is.

    A FAILED phase anywhere in the latest attempt makes the whole request FAILED
    (the attempt is retried by starting a new Attempt, not by mutating this one).
    """
    intake = phases.get(Phase.INTAKE)
    if intake is None:
        return RequestState.PENDING
    if _any_failed(phases):
        return RequestState.FAILED
    if intake.status in (PhaseStatus.PENDING, PhaseStatus.RUNNING):
        return RequestState.RUNNING
    # Intake succeeded.
    if skip_release:
        # No Release CR is produced; the integration-tests result is terminal. Success
        # requires the ITS phase to have completed (passed, or skipped when no scenario
        # applies). Until then the post-build flow (snapshot/ITS) is still running. An
        # ITS failure is already handled by the FAILED check above.
        its = phases.get(Phase.INTEGRATION_TESTS)
        if its is not None and its.status in (PhaseStatus.SUCCESS, PhaseStatus.SKIPPED):
            return RequestState.SUCCESS
        return RequestState.RUNNING
    release = phases.get(Phase.RELEASE)
    if release is not None and release.status == PhaseStatus.SUCCESS:
        return RequestState.SUCCESS
    # Intake done, release not yet successful: snapshot/integration/release in flight.
    return RequestState.AWAITING_RELEASE


def _any_failed(phases: dict[Phase, AttemptPhase]) -> bool:
    """True if any phase of the attempt is in the FAILED state."""
    return any(phase.status == PhaseStatus.FAILED for phase in phases.values())
