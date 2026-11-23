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

# ---------------------------------------------------------------------------
# Default configuration constants for the import orchestrator.
# ---------------------------------------------------------------------------

NAMESPACE = "lightwell-poc-tenant"
CLUSTER_API = "https://api.stone-prod-p01.wcfb.p1.openshiftapps.com:6443"
KUBEARCHIVE_API = "https://kubearchive-api-server-product-kubearchive.apps.stone-prod-p01.wcfb.p1.openshiftapps.com"

# Base of the Konflux UI, up to and including the namespace path segment.
KONFLUX_UI_NS_BASE = "https://konflux-ui.apps.stone-prod-p01.wcfb.p1.openshiftapps.com/ns"
# Template for a Release page in the Konflux UI; fill the named fields with
# ``KONFLUX_RELEASE_URL_TEMPLATE.format(namespace=..., application=..., name=...)``.
# Used to turn a failed Release into a clickable link in the orchestrator's logs.
KONFLUX_RELEASE_URL_TEMPLATE = KONFLUX_UI_NS_BASE + "/{namespace}/applications/{application}/releases/{name}"

# Label carrying a Request's fingerprint on the resources the orchestrator creates
# (intake PipelineRun, Release). The ``custom.appstudio.openshift.io/`` prefix is
# the one Konflux's integration service propagates onto the resulting Snapshot and
# integration-test PipelineRuns, so stamping the intake PipelineRun covers those
# two without the orchestrator patching them (ADR-0001, §Fingerprinting).
FINGERPRINT_LABEL = "custom.appstudio.openshift.io/lightwell-fingerprint"

DEFAULT_MAX_PARALLEL = 1
DEFAULT_POLL_INTERVAL = 30  # seconds
DEFAULT_MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# konflux-release-data (GitOps source of truth for ReleasePlan / RPA).
# Used by `orchestrate --print-resources` to resolve the release chain offline.
# ---------------------------------------------------------------------------

# Cloned here (relative to the working directory) when not supplied explicitly.
RELEASE_DATA_REPO_URL = "git@gitlab.cee.redhat.com:releng/konflux-release-data.git"
RELEASE_DATA_DEFAULT_PATH = "reference/konflux-release-data"
