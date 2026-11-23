# Lightwell-Konflux Orchestrator

CLI tool for orchestrating batch import PipelineRuns across ecosystems with intelligent state tracking, dynamic throttling, and automatic retry logic.

## Overview

This tool orchestrates the release of Lightwell artifacts through Konflux. Commands are grouped per ecosystem (e.g. `java`, `python`), each encapsulating the logic for triggering Konflux PipelineRuns with intelligent throttling and automatic retry on transient failures.

## Ecosystems

Commands are namespaced by ecosystem: `orchestrator <ecosystem> <command>`.

Two ecosystems are available today. Each uses its own default database (`--db` overrides it):

| Ecosystem | Default database | Description | Commands |
|-----------|------------------|-------------|----------|
| `java` | `./java_import_state.db` | PNC OCI image imports | `import-file`, `import-manifest`, `orchestrate`, `run`, `trigger` |
| `python` | `./python_import_state.db` | CVE-remediated Python wheel builds | `import-file`, `orchestrate`, `promote`, `run`, `trigger` |

The `python` ecosystem identifies each build by a `package==version` reference (e.g. `ntplib==0.4.0`) instead of an OCI image, and runs the `python-remediated-build` pipeline. It has no `import-manifest` command; populate its database with `import-file` (one `package==version` per line).

**Key Features:**
- **State persistence**: SQLite database tracks each OCI reference status (pending, triggered, running, success, failed)
- **Intelligent throttling**: Monitors actual running PipelineRun count instead of fixed time delays
- **Automatic retries**: Configurable retry logic for transient failures (default: 3 attempts)
- **Idempotent**: Can be stopped and resumed without losing progress
- **Completion tracking**: Runs until all imports succeed or exhaust retries

## Requirements

- Python 3.11+
- Cluster credentials via kubeconfig or `KONFLUX_TOKEN`
- Access to the KubeArchive API (for archived PipelineRun lookups)


## Installation

Install the package in editable mode:

```bash
pip install -e .
```

For development (includes pytest, ruff, etc.):

```bash
pip install -e ".[dev]"
```

## Usage

### Java Ecosystem

```bash
# Show help
orchestrator --help
orchestrator java --help
orchestrator java import-file --help
orchestrator java orchestrate --help
orchestrator java import-manifest --help
orchestrator java trigger --help
orchestrator java run --help

# Typical workflow: import from file then orchestrate
orchestrator java import-file refs.txt
orchestrator java orchestrate --max-parallel 10

# Alternative: import from a consolidated build manifest
orchestrator java import-manifest consolidated.yaml
orchestrator java orchestrate --max-parallel 10

# Trigger a single PNC import PipelineRun
orchestrator java trigger 'quay.io/light-castle/rebuild-pnc:tag@sha256:abc123...'

# Run a single reference end-to-end (trigger + monitor + release), then print
# a JSON result to stdout. Uses a throwaway database unless --db is given.
orchestrator java run 'quay.io/light-castle/rebuild-pnc:tag@sha256:abc123...'

# Same, but persist state to a database and also write the result to a file
orchestrator --db ./one-off.db java run \
  'quay.io/light-castle/rebuild-pnc:tag@sha256:abc123...' \
  --artifact-type VALIDATED --output-json ./result.json

# Import only (populate database for inspection)
orchestrator import-file refs.txt

# Resume interrupted orchestration from existing database
orchestrator java orchestrate

# Import REMEDIATED builds instead of STAGE (default)
orchestrator java import-file refs.txt --artifact-type REMEDIATED
orchestrator java orchestrate --max-parallel 5

# Reset database and start fresh
orchestrator --reset java import-file refs.txt
orchestrator java orchestrate
```

### Python Ecosystem

The `python` ecosystem builds CVE-remediated wheels from `lightwell-builds` sources. Each item is a `package==version` reference:

```bash
# Show help
orchestrator python --help
orchestrator python import-file --help
orchestrator python orchestrate --help
orchestrator python trigger --help
orchestrator python run --help
orchestrator python promote --help

# Import package references from a file, then orchestrate
orchestrator python import-file packages.txt
orchestrator python orchestrate --max-parallel 5

# Trigger a single build
orchestrator python trigger 'ntplib==0.4.0'

# Run a single package end-to-end (trigger + monitor + release), then print
# a JSON result to stdout. Uses a throwaway database unless --db is given.
orchestrator python run 'ntplib==0.4.0'

# Same, but persist state to a database and also write the result to a file
orchestrator --db ./one-off.db python run 'ntplib==0.4.0' \
  --output-json ./result.json

# Promote an already-built snapshot to production. No rebuild, no database --
# this re-releases existing content against a second ReleasePlan.
orchestrator python promote remediated-build-xyz12 \
  --release-plan remediated-build-prod
```

The `import-file` input lists one `package==version` per line; blank lines and lines starting with `#` are ignored:

```
# CVE-remediated Python packages
ntplib==0.4.0
requests==2.31.0
```

### Command-Line Options

#### Global Options

Global options are placed before the ecosystem: `orchestrator [--db PATH] [--reset] <ecosystem> <command>`.

| Option | Default | Description |
|--------|---------|-------------|
| `--db` | per-ecosystem (e.g. `./java_import_state.db`) | SQLite database path |
| `--reset` | `false` | Reset database (delete existing data before running) |

#### `import-file` Subcommand

Imports OCI references from a text file into the database.

```bash
orchestrator java import-file <file>
```

Reads OCI references from a text file (one per line) and adds them to the database as pending imports. Lines starting with `#` and blank lines are ignored.

**File format:**
```
# Comments are ignored
quay.io/namespace/repo:tag@sha256:abc123...
quay.io/namespace/repo:tag2@sha256:def456...

# Blank lines are also ignored
quay.io/namespace/repo:tag3@sha256:789abc...
```

#### `import-manifest` Subcommand

Imports OCI references from a consolidated build manifest (YAML) into the database.

```bash
orchestrator java import-manifest <file>
```

Each library entry's `output.artifact.tag` and `output.artifact.digest` are combined into a `tag@digest` reference when both are present, falling back to digest-only or tag-only. Entries missing both fields are skipped. A malformed digest (missing `@` separator) is treated as an error and aborts processing.

**Manifest format:**
```yaml
libraries:
  - output:
      artifact:
        tag: "quay.io/namespace/repo:build-100"
        digest: "quay.io/namespace/repo@sha256:abc123..."
  - output:
      artifact:
        digest: "quay.io/namespace/repo@sha256:def456..."
```

#### `orchestrate` Subcommand

Orchestrates the import process by triggering PipelineRuns and monitoring their status.

```bash
orchestrator java orchestrate [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--max-parallel` | `1` | Maximum parallel PipelineRuns |
| `--poll-interval` | `30` | Seconds between status checks |
| `--max-retries` | `3` | Max retry attempts for failed imports |
| `--artifact-type` | `STAGE` (or `LIGHTWELL_ARTIFACT_TYPE` env var) | Artifact type: STAGE, VALIDATED, REMEDIATED, or NOVEL |

#### `trigger` Subcommand

Triggers a single PNC import PipelineRun for manual or ad-hoc imports.

```bash
orchestrator java trigger <source_image> [tag] [OPTIONS]
```

| Argument/Option | Description |
|-----------------|-------------|
| `source_image` | OCI image reference to import (must be digest-pinned with @sha256:) |
| `tag` | Optional destination tag override (default: derived from source image) |
| `--artifact-type` | Artifact type: STAGE (default), VALIDATED, REMEDIATED, or NOVEL |
| `--dry-run` | Print the PipelineRun YAML without submitting it |

#### `run` Subcommand

Runs a **single** reference end-to-end and blocks until it finishes: ingest → trigger the
PipelineRun → monitor it to completion → monitor the release → emit a JSON result. It is the
one-shot counterpart to `import-file` + `orchestrate`, intended for CI jobs and ad-hoc runs that
handle exactly one artifact and want a machine-readable answer.

Unlike `trigger`, which submits a PipelineRun and returns immediately, `run` waits for the outcome.
It has no `--dry-run`.

```bash
# Java: one digest-pinned image
orchestrator java run <source_image> [tag] [OPTIONS]

# Python: one package==version reference
orchestrator python run <ref> [OPTIONS]
```

**Java arguments and options:**

| Argument/Option | Default | Description |
|-----------------|---------|-------------|
| `source_image` | — | OCI image reference to import (must be digest-pinned with `@sha256:`) |
| `tag` | derived from source image | Optional destination tag override |
| `--artifact-type` | `STAGE` (or `LIGHTWELL_ARTIFACT_TYPE` env var) | Artifact type: STAGE, VALIDATED, REMEDIATED, or NOVEL |
| `--poll-interval` | `30` | Seconds between status checks |
| `--max-retries` | `3` | Max retry attempts on transient failure |
| `--output-json` | — | Also write the result JSON to this path |

**Python arguments and options:**

| Argument/Option | Default | Description |
|-----------------|---------|-------------|
| `ref` | — | Package reference as `package==version` (e.g. `ntplib==0.4.0`) |
| `--target` | `REMEDIATED` (or `LIGHTWELL_PYTHON_TARGET` env var) | Build target |
| `--wheel-server-url` | `https://packages.redhat.com/lightwell/python/validated/simple/` (or `WHEEL_SERVER_URL` env var) | Wheel server for all build types, including Validated; pass `''` to disable |
| `--python-builder-image` | Pipeline parameter default | Override the Python builder execution image for this invocation |
| `--builds-tag` / `--builds-ref` | `<package>/<version>` | `lightwell-builds` git revision to build from |
| `--lightwell-builds-repo-url` | derived from repository base | Exact source Git URL passed as `LIGHTWELL_BUILDS_REPO_URL` |
| `--lightwell-builds-repo-base` | `https://gitlab.cee.redhat.com/lightwell/lightwell-builds` | Source namespace; appends `/pypi.org-<package>` |
| `--poll-interval` | `30` | Seconds between status checks |
| `--max-retries` | `3` | Max retry attempts on transient failure |
| `--output-json` | — | Also write the result JSON to this path |

The repository URL and base options are mutually exclusive. They and the
`--builds-tag` / `--builds-ref` aliases are available on `python run`,
`python trigger`, and `python orchestrate`. An explicit Git revision applies to
every package in the invocation; otherwise each uses its `<package>/<version>`
tag. For batch builds, use a repository
base to select a separate source repository for each package:

```bash
orchestrator python orchestrate --target VALIDATED-STAGE \
  --lightwell-builds-repo-base https://gitlab.example.com/lightwell/test-builds

orchestrator python run ntplib==0.4.0 --target VALIDATED-STAGE \
  --lightwell-builds-repo-url https://gitlab.example.com/lightwell/test-builds/custom-ntplib.git
```

An exact URL is used unchanged for every package in that invocation. Omitting
both options retains the production repository base. The resolved source
repository URL is part of the request identity, so changing repositories creates
a separate request even when using the same database. Equivalent URL and base
options that resolve to the same URL reuse the same request.

##### Database: ephemeral by default

`run` does **not** touch the ecosystem's shared database unless you ask it to. With no global
`--db`, it creates a throwaway SQLite database in a temporary directory and deletes it when the
command exits. That isolation is what keeps the run single-ref: the monitoring loop only ever sees
the one reference you passed, rather than picking up pending rows left behind by an earlier
`import-file` or `import-manifest`. Pass the global `--db` (before the ecosystem name) to persist state
instead — useful for resuming or for inspecting the row afterwards:

```bash
# Ephemeral — nothing left on disk
orchestrator java run 'quay.io/light-castle/rebuild-pnc:tag@sha256:abc...'

# Persistent — state kept in ./one-off.db
orchestrator --db ./one-off.db java run 'quay.io/light-castle/rebuild-pnc:tag@sha256:abc...'
```

> **Point `--db` at a fresh or single-purpose database.** `run` seeds your reference into the
> database and then runs the standard orchestration loop, which picks up **every** pending row it
> finds — it is not filtered to your reference. If you point `--db` at a shared database that
> already has pending work (from `import-file`, `import-manifest` or an interrupted `orchestrate`), `run`
> will work through all of it, one at a time, and won't return until the whole database reaches a
> terminal state. The result JSON still describes only your reference, but the exit code reflects
> the database as a whole (see Exit codes below). The default ephemeral database avoids this
> entirely.

> **Note:** `--reset` deletes the database it resolves *before* `run` swaps in its ephemeral one.
> Running `orchestrator --reset java run ...` without an explicit `--db` therefore deletes
> the shared `java_import_state.db` and then runs against a throwaway database anyway. Don't
> combine `--reset` with `run` unless you also pass `--db`.

##### Result JSON

On completion `run` prints exactly one JSON object to **stdout** — all progress and diagnostic
output goes to stderr, so `orchestrator java run ... | jq` is safe. Pass `--output-json
PATH` to write the same payload to a file as well (stdout still gets it).

```json
{
  "ref": "quay.io/light-castle/rebuild-pnc:tag@sha256:abc123...",
  "status": "success",
  "pipelinerun_name": "pnc-import-abc123",
  "snapshot_name": "rebuild-pnc-xyz",
  "release_name": "rebuild-pnc-xyz-release",
  "error_message": null,
  "retry_count": 0
}
```

| Field | Description |
|-------|-------------|
| `ref` | The reference that was run |
| `status` | Final state — `success` or `failed` |
| `pipelinerun_name` | Name of the triggered PipelineRun (`null` if it never got that far) |
| `snapshot_name` | Konflux Snapshot produced by the build |
| `release_name` | Release created from the snapshot |
| `error_message` | Failure detail, `null` on success |
| `retry_count` | How many retries were consumed |

#### `promote` Subcommand

**Python only.** Releases an **already-built** Snapshot a second time, against a different
ReleasePlan. Nothing is rebuilt and no PipelineRun is triggered: promotion re-releases the exact
content that was already built and verified, which is what makes it a promotion rather than a
second build. It touches no database.

The intended use is moving remediated Python content from staging to production once its
Cumulative Ticket is cleared for release (ADR-0005), but the command itself carries no policy —
the caller decides when a promotion is warranted and which plan to target.

```bash
orchestrator python promote <snapshot> --release-plan <plan> [OPTIONS]

# Promote a verified snapshot to production
orchestrator python promote remediated-build-xyz12 \
  --release-plan remediated-build-prod
```

| Argument/Option | Default | Description |
|-----------------|---------|-------------|
| `snapshot` | — | Name of the existing Konflux Snapshot to promote |
| `--release-plan` | — | **Required.** ReleasePlan to release against (e.g. `remediated-build-prod`) |
| `--poll-interval` | `30` | Seconds between status checks |
| `--timeout` | `14400` (4h) | Seconds to wait for a terminal state, matching the production RPA's pipeline timeout |
| `--output-json` | — | Also write the result JSON to this path |

> **`--release-plan` is required and never inferred.** Elsewhere the orchestrator resolves a
> Snapshot's plan from its application label, but `remediated-build` now has both a stage and a
> production plan. Inferring one would mean inferring whether content ships to staging or to
> production, so the caller states it. (The inference path itself refuses to guess in that
> situation — see `find_release_plan_for_snapshot`.)

**Result JSON** — one object on stdout, progress on stderr:

```json
{
  "snapshot": "remediated-build-xyz12",
  "release_plan": "remediated-build-prod",
  "release_name": "remediated-build-xyz12-prod-abc",
  "status": "True",
  "adopted": false
}
```

`adopted` is `true` when an in-flight Release against that plan already existed and was attached
to rather than duplicated.

**Exit codes:**
- `0` — Release succeeded
- `1` — Release failed terminally, or could not be created
- `2` — Timed out; the Release is still running

Exit `2` is deliberately distinct from `1`: a slow release has not failed. Re-running the same
command adopts the in-flight Release instead of creating a second one, so a timeout is safe to
retry. A terminally failed Release is *not* adopted, so a retry after `1` genuinely retries.

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KONFLUX_TOKEN` or `KUBECONFIG` | Yes (for `orchestrate`, `trigger`, and `run`) | Cluster authentication |
| `LIGHTWELL_ARTIFACT_TYPE` | No | `STAGE` (default), `VALIDATED`, `REMEDIATED`, or `NOVEL` |
| `LIGHTWELL_PYTHON_TARGET` | No | Build target for the `python` ecosystem: `REMEDIATED` (default) |
| `TEKTON_PIPELINE_DIR` | No | Path to directory containing Tekton pipeline definitions (defaults to `tekton/` in repository root) |
| `BUILD_CONFIGS_LOCAL_PATH` | No | Path to an already-checked-out build-config repository, for the `python-sdist` ecosystem only (the `python` ecosystem only ever passes `BUILD_CONFIGS_REPO_URL`/`BUILD_CONFIGS_REVISION` through as PipelineRun params — see below — so it has no local-checkout path to take). When set, `resolve_canonical_source` reads directly from this path instead of cloning — the common case, since the orchestrator is usually invoked from within a calunga-python checkout already (e.g. calunga-python's own CI calls `import-orchestrator python-sdist ...` from a job whose working directory **is** the calunga-python checkout). Takes priority over `BUILD_CONFIGS_REPO_URL`/`BUILD_CONFIGS_REVISION` when set; the clone-based path below remains available as a fallback for invocations that don't already have a local checkout (e.g. standalone/local developer use via `orchestrator python-sdist ...`). |
| `BUILD_CONFIGS_REPO_URL` | **Yes** — for the `python` ecosystem always, and for `python-sdist` unless `BUILD_CONFIGS_LOCAL_PATH` is set | HTTPS URL (no embedded credentials) of the calunga-python build-config repository. Required by both Python ecosystems: `python` passes it straight through as a `BUILD_CONFIGS_REPO_URL` PipelineRun param for its build task to clone itself; `python-sdist` clones it directly at trigger time (unless `BUILD_CONFIGS_LOCAL_PATH` is set) to run `resolve_canonical_source` — needed for every `python-sdist` trigger, not just `canonical_source.type: git` routing, since `build_pipelinerun` unconditionally resolves canonical source before building any PipelineRun manifest. |
| `BUILD_CONFIGS_REVISION` | No | Git revision (branch or tag) of the build-config repo to clone. Defaults to `main`. Used by both the `python` and `python-sdist` ecosystems the same way `BUILD_CONFIGS_REPO_URL` is (pass-through param vs. trigger-time clone, respectively); ignored for `python-sdist` when `BUILD_CONFIGS_LOCAL_PATH` is set. |

> **`python-sdist` now fails closed on a missing build-config entry.** Every package/version triggered through the `python-sdist` ecosystem must have a matching `packages/<name>.json` entry (with a valid PEP 440 version) in the configured calunga-python repo — if it's missing, or the entry's reported `canonical_source.type` is neither `git` nor `pypi-sdist`, the trigger now fails with `TriggerError` instead of proceeding as plain sdist ingestion, which was the behavior before this resolver existed. When `BUILD_CONFIGS_LOCAL_PATH` is unset (no local checkout available), `git` and network access to the configured `BUILD_CONFIGS_REPO_URL` host are hard runtime prerequisites for the `python-sdist` ecosystem, in addition to `BUILD_CONFIGS_REPO_URL` itself.


### Operation Flow

#### `import-file` subcommand

1. Reads OCI references from the specified text file
2. Skips blank lines and comment lines (starting with `#`)
3. Adds each reference to the database with `status='pending'`
4. Skips duplicates (already in database)
5. Prints summary of how many were added

**Exit codes:**
- `0` — Import successful (even if all duplicates)
- `2` — File not found

#### `import-manifest` subcommand

1. Reads a consolidated build manifest (YAML)
2. For each library entry, combines `output.artifact.tag` and `output.artifact.digest` into a `tag@digest` reference
3. Falls back to digest-only or tag-only when one is missing; skips entries missing both
4. Raises an error if a digest field is malformed (missing `@` separator)
5. Adds each reference to the database with `status='pending'`
6. Skips duplicates (already in database)

**Exit codes:**
- `0` — Import successful (even if all duplicates)
- `2` — File not found

#### `orchestrate` subcommand

1. Checks if database has any OCI references (warns if empty but continues)
2. **Orchestration loop:**
   - Checks status of triggered/running imports via `KubeClient`
   - Updates database with current PipelineRun statuses
   - Triggers new imports up to `--max-parallel` limit using the ecosystem's PipelineRun builder
   - Sleeps for `--poll-interval` seconds
   - Repeats until all imports are complete (success or retry-exhausted)

**Exit codes:**
- `0` — All imports successful or no work to do
- `1` — Some imports failed after exhausting retries

#### `trigger` subcommand

1. Validates that the source image is digest-pinned
2. Loads the pipeline definition from `tekton/pipelines/pnc-import/`
3. Builds the PipelineRun manifest with source and destination image references
4. Submits the PipelineRun to Konflux via the K8s API

**Exit codes:**
- `0` — PipelineRun triggered successfully
- `1` — Validation or submission errors

#### `run` subcommand

1. Resolves the database: a temporary throwaway unless a global `--db` was given
2. Ingests the single reference into that database with `status='pending'`
3. Triggers its PipelineRun (with `--max-parallel` fixed at 1)
4. Polls the PipelineRun every `--poll-interval` seconds until it completes, retrying transient
   failures up to `--max-retries` times
5. Monitors the resulting Snapshot and Release to completion
6. Prints the result JSON to stdout (and to `--output-json PATH` if given)
7. Removes the temporary database, if one was created

**Exit codes:**
- `0` — No import in the database ended in `failed`
- `1` — At least one import ended in `failed` after exhausting retries
- `2` — CLI usage error (no ecosystem or no subcommand given)

With the default ephemeral database the database holds only your reference, so `0`/`1` mean exactly
"this reference succeeded/failed". With an explicit `--db` that contains other pending rows, the
exit code covers all of them — read `status` in the result JSON if you need the verdict for your
reference specifically. The result JSON is printed for both `0` and `1`.

#### `promote` subcommand

1. Looks for an existing Release for the snapshot **against the named plan only** — a Release
   against a different plan (typically the stage one) is ignored, since promotion exists precisely
   to add a second one
2. Adopts that Release if one is in flight; otherwise creates a new Release referencing the
   snapshot and the named ReleasePlan. Nothing is built and no PipelineRun is triggered
3. Polls the Release every `--poll-interval` seconds until it reaches a terminal state or
   `--timeout` expires. A status the API can't currently supply is treated as "keep waiting",
   not as a verdict
4. Prints the result JSON to stdout (and to `--output-json PATH` if given)

**Exit codes:**
- `0` — Release succeeded
- `1` — Release failed terminally, or could not be created
- `2` — Timed out; the Release is still running

No database is opened at any point, so the global `--db` and `--reset` flags have no effect here.

### Database Inspection

The SQLite database can be queried directly for monitoring:

```bash
# View current state summary
sqlite3 java_import_state.db \
  "SELECT status, COUNT(*) FROM oci_references GROUP BY status"

# List all failed imports with errors
sqlite3 java_import_state.db \
  "SELECT oci_ref, error_message, retry_count FROM oci_references WHERE status='failed'"

# Show recent activity
sqlite3 java_import_state.db \
  "SELECT oci_ref, status, triggered_at, completed_at FROM oci_references ORDER BY id DESC LIMIT 10"

# Reset specific import to retry manually
sqlite3 java_import_state.db \
  "UPDATE oci_references SET status='pending', retry_count=0 WHERE oci_ref='quay.io/...'"
```

### Database Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PRIMARY KEY | Auto-increment ID |
| `oci_ref` | TEXT UNIQUE | OCI reference (e.g., `quay.io/repo:tag@sha256:...`) |
| `status` | TEXT | `pending`, `triggered`, `running`, `success`, `failed` |
| `pipelinerun_name` | TEXT | Name of triggered PipelineRun |
| `triggered_at` | TIMESTAMP | When import was triggered |
| `completed_at` | TIMESTAMP | When import finished (success or failure) |
| `last_checked_at` | TIMESTAMP | Last status check time |
| `error_message` | TEXT | Error details for failed imports |
| `retry_count` | INTEGER | Number of retry attempts |
| `created_at` | TIMESTAMP | When reference was first added |

### CLI aliases

The base `orchestrator` command can be replaced by the short alias `likor` (**Li**ghtwell-**K**onflux **Or**chestrator). The old `import-orchestrator` alias should be avoided and is only kept for backward compatibility purposes.

## Project Structure

```
import-orchestrator/
├── src/import_orchestrator/
│   ├── clients/           # Quay and Kubernetes API clients
│   ├── commands/          # CLI subcommand implementations
│   ├── ecosystems/        # Per-ecosystem config, parser, and PipelineRun builder (e.g. java/)
│   ├── engine/            # Core ecosystem-neutral logic
│   ├── database.py        # SQLite state persistence
│   ├── models.py          # Data models
│   └── constants.py       # Configuration constants
├── tekton/                # Tekton Pipeline definitions
├── policy/                # Conforma policy definitions
└── tests/                 # Pytest test suite
```

## Development

### Running Tests

```bash
pytest
```

Or via tox:

```bash
tox -e py311
```

### Code Standards

The project enforces the following standards, all configured with a **120-character line length** and targeting **Python 3.11**:

- **Ruff** -- Linter with rules: `E` (pycodestyle errors), `F` (pyflakes), `W` (pycodestyle warnings), `I` (import sorting)

### Linting and Formatting

```bash
ruff check src/ tests/
```

Or via tox:

```bash
tox -e ruff
```

To automatically format the code:
```bash
ruff format src/ tests/
```

### Running All Checks

```bash
tox
```

This runs all test and linting environments.

## License

Apache License 2.0
