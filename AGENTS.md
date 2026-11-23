# Repository Guidelines

## Python and Architecture

- Target Python 3.11+; use 120-character lines and Ruff's configured lint/import rules.
- Keep shared orchestration in `src/import_orchestrator/engine/`, API clients in
  `clients/`, and ecosystem-specific config, commands, and PipelineRun builders in `ecosystems/`.
  See the README's [Project Structure](README.md#project-structure) section for the repository layout.
- Preserve resumable, idempotent workflows and Request/Attempt/Phase history; see
  `docs/design/adr-0001-reconciliation.md`.
- Keep machine-readable output on stdout and progress/status messages on stderr.

## Tekton

- Store all pipeline definitions in this repository under `tekton/pipelines/<pipeline>/`.
- Reference every task, including `finally` tasks, through `taskRef` with `resolver: bundles`
  and a bundle reference pinned to `@sha256:<digest>`; never use tag-only references.
- Do not embed inline `taskSpec` definitions or Bash scripts in pipelines; use bundled tasks.
- Load pipeline YAML through the shared pipeline loader rather than duplicating it in Python.
- Keep pipeline parameters, results, and workspaces aligned with ecosystem PipelineRun builders.

## Validation

- Add or update tests under `tests/` for behavior changes; mock external APIs.
- Run `pytest`, `ruff check src/ tests/`, and `ruff format --check src/ tests/`.
- Use `tox` for the full suite, including the 95% coverage gate, Bandit, and pip-audit;
  use the Ruff version pinned in `tox.ini`.

## Git

- Use Conventional Commits, e.g. `feat: add pipeline validation`.
- When available, put the Jira ticket on the last line of the commit description, immediately before
  the `Signed-off-by` line, e.g. `Jira: LWLP-123`.
- Always sign off commits with `git commit -s`; include the ticket ID in branch names when available.
- Amend the previous commit when correcting an issue it introduced.
- Do not commit plans or design documents unless requested.
