# Changelog

All notable changes to `nexus-claude-plugin` are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [0.5.0] — 2026-06-22

### Changed
- **`SessionEnd` capture hook — P0 low-signal source filter**
  (`hooks/session_capture.py`, `_is_low_signal`): low-signal activities are now
  **SKIPPED before extraction** so the backend LLM extractor never sees
  navigation/search/read-only noise. C0d evaluation
  (`docs/qa/nexus-replace-claude-mem-c0c-extraction-quality.md`) measured **88%
  hallucination** when low-signal activities (bare `Read` / `Grep` / `ls` /
  `git status`) were fed to the extractor — `glm-4-flash` invented content —
  dragging the whole extraction quality gate below threshold, while high-signal
  segments scored **4.45 / 4.73 with 0 hallucination**. Filtering at the source
  (before `POST /v1/activities/stream`) is the highest-ROI fix and is expected to
  flip the quality gate to **PASS**.
  - **Dropped (low-signal)**: `read_file` / `agent_action` (Read / Grep / Glob /
    Task — pure navigation/search), and read-only `command_run` whose command
    head is one of `ls cat pwd which echo head tail tree less stat file wc`
    or `git status|log|diff|show|branch|remote|rev-parse`.
  - **Kept (high-signal)**: `user_message` / `commit` / `run_test` / `edit_file`
    / `create_file` / `delete_file` / non-read-only `command_run`
    (build / deploy / migration, e.g. `alembic upgrade head`).
  - **Boundary**: any write redirection (`>`, `>>`), pipe (`|`), command
    chaining/control op (`&&`, `||`, `;`, background `&`), or command
    substitution (`` ` ``, `$(`) — e.g. `cat > out.txt`, `tee`, `ls | tee log`,
    `ls && rm -rf dist`, `cat a; alembic upgrade head` — is conservatively
    treated as NOT read-only and is **kept** (a whitelisted head says nothing
    about a chained second command; never mis-skip a write disguised as a read).
    `find` is excluded from the read-only head whitelist entirely because
    `find . -delete` / `find . -exec rm {} +` mutate the filesystem.
  - **fail-open**: a predicate error keeps the activity rather than aborting
    capture; an all-low-signal session yields zero activities → **no POST**
    (unchanged empty-batch behavior). Provenance / cap / agent_id logic unchanged.

## [0.4.0] — 2026-06-21

### Added
- **`SessionEnd` activity-capture hook** (`hooks/session_capture.py`, P1 /
  workflow C): on session end, reads the session transcript, distills it into a
  bounded activity list, and POSTs to `POST {NEXUS_API_URL}/activities/stream`.
  The backend Arq worker (`activity_processor.process_activity`) extracts those
  activities into episodic Memory rows keyed `user_id == agent_id` — the **write
  side** of the claude-mem "auto-capture (feature a)" replacement, paired with
  the read-side `session_inject.py` (P2). Live against dev's
  `/v1/activities/stream` as of **migration 024**.
  - **Action mapping** (assistant `tool_use` + user text → `ActivityItem.action`
    enum): `Edit`→`edit_file`, `Write`→`create_file`, `Read`→`read_file`,
    `Bash 'git commit'`→`commit`, `Bash pytest|jest|'go test'|'npm test'|vitest`
    →`run_test`, other `Bash`→`command_run`, `Grep`/`Glob`/`Task`/…→`agent_action`,
    user text→`user_message`. `activity_data` carries `{tool, summary}` (file path
    or command head ≤200 chars) / `{text}` (≤500 chars).
  - **agent_id = project slug**: `NEXUS_DEFAULT_USER_ID` else normalized lowercase
    git-toplevel/cwd basename — IDENTICAL derivation to `session_inject.py` so the
    captured episodic memory lands on the same `user_id=project` the read side
    queries.
  - **Provenance**: every `activity_data` is augmented with `container_id`
    (`NEXUS_CONTAINER_ID` else hostname) + `branch`
    (`git -C cwd rev-parse --abbrev-ref HEAD`, omitted on failure) + `session_id`.
  - **Bounded**: keeps the most-recent `_MAX_ACTIVITIES` (200) extracted
    activities, well under the `ActivityStreamRequest` 1000 cap.
  - Mandatory `User-Agent` header (CF 1010 Bot Fight Mode) + `X-API-Key` +
    `X-Nexus-Source: session-capture-hook`; ~8s timeout. FAIL-OPEN on every path
    — missing config / no transcript / unreadable file / unreachable backend /
    timeout / malformed transcript line → exit 0 with no stdout, never blocking
    teardown. Empty activity list → no request sent. Transcript parsed
    line-by-line **defensively** (bad/blank/non-dict line skipped, never fatal).
  - Registered as a `SessionEnd` hook alongside the existing `UserPromptSubmit` +
    `SessionStart` hooks in `hooks/hooks.json`.
  - 22 deterministic unit tests (`hooks/test_session_capture.py`): subprocess
    fail-open coverage + in-process urllib monkeypatch for action-mapping /
    provenance / agent_id-slug / bad-line-skip / empty→no-POST / cap / non-vacuity
    (no real backend).

## [0.3.0] — 2026-06-21

### Added
- **`SessionStart` warm-start injection hook** (`hooks/session_inject.py`, P2 /
  workflow A): on a new Claude Code session, calls `POST {NEXUS_API_URL}/context/retrieve`
  and injects the project's recent **settled** summaries as
  `hookSpecificOutput.additionalContext` — the cross-container warm-start that
  replaces claude-mem "feature c".
  - Prefers `metadata.layer == "summary"` rows to guard the brief from
    half-finished observations (§6). **Forward-looking**: the write side that
    emits `metadata.layer` is workflow B (migration) / P1 (capture); until those
    populate it, no row carries `layer` and the hook gracefully falls back to all
    rows. Each line carries a `[<container_id> · <age> · <branch>]` provenance
    annotation so cross-container origin is legible.
  - Branch-scoped recall with a two-tier fallback (§6 same-branch-first →
    project-level): tier 1 sends `metadata_filter={branch, container_id}` (backed
    by backend workflow G's allowlist); if the profile is empty, tier 2 re-requests
    without `metadata_filter`. **Forward-looking**: tier 1 only narrows once the
    write side populates `branch`/`container_id` on memory metadata (B/P1); until
    then tier 1 is empty and tier 2 carries the brief — correct, just one extra
    short request. Per-request timeouts are 6s (tier 1) + 4s (tier 2) so a slow
    backend never stalls startup beyond ~10s.
  - Uses `profile_limit` (NOT `limit` — `ContextRequest` has no `limit` field) and
    a mandatory `User-Agent` header (CF 1010 Bot Fight Mode blocks UA-less
    requests). FAIL-OPEN on every path — missing config / unreachable backend /
    timeout / malformed stdin → exit 0 with no stdout, never blocking startup.
  - Registered alongside the existing `UserPromptSubmit` hook in `hooks/hooks.json`.
  - 18 deterministic unit tests (`hooks/test_session_inject.py`): subprocess
    fail-open coverage + in-process urllib monkeypatch for request-body / header /
    two-tier-fallback / provenance assertions (no real backend).

## [0.2.5] — 2026-06-21

### Changed
- Read-path routing rules refined (precision tuning); SKILL.md updated to reflect
  `context_retrieve` as the canonical recall tool and to document the server-side
  `user_id` pin behaviour (`NEXUS_DEFAULT_USER_ID` in mcp-server 0.1.4).

## [0.2.3] — 2026-06-21

### Added
- **Read-path intent nudge** (`hooks/rules.json` §2): `recall` / time-named queries
  routed toward `nexus.context_retrieve` (with optional `as_of` ≤90d for temporal
  queries). Shares the same `UserPromptSubmit` hook as the write-path (D3 one-hook
  two-rule-sections design).

## [0.2.0] — 2026-06-19

### Added
- **`UserPromptSubmit` write-path capture hook** (`hooks/memory_capture.py`):
  deterministically dispatches "remember X" / lifetime-axis intent to
  `nexus.memory_create` via `additionalContext`, bypassing the built-in
  Anthropic auto-memory routing that was winning the competition in SKILL.md
  prose-only mode.
- **`hooks/rules.json`**: machine-readable single source of truth for lifetime-axis
  examples shared between hook and SKILL.md; 31 positive + negative examples.
- **SKILL.md break-tie partition rule**: explicit split between Anthropic built-in
  auto-memory (meta-collaboration about the session) vs nexus plugin (user's domain
  facts / decisions / preferences). Resolves the routing ambiguity that caused
  2 consecutive dogfood runs to score 1/5.
- **CI gate** (`.forgejo/workflows/test.yml`): 61 deterministic hook unit tests
  covering parity/recall-floor/precision/fail-open/output-contract/non-vacuity.
  Uses `node:20-bookworm` image (git + node + python3; `python:3.11-slim` lacks git).

## [0.1.0] — 2026-05-23

### Added
- Initial plugin scaffold: `plugin.json`, `SKILL.md` (R1+R1.5), `README.md`,
  `.mcp.json` pinning `@nexusm/mcp-server@0.1.1`, GitHub mirror workflow.
- 4-tool MCP server integration: `context_retrieve`, `memory_create`,
  `memory_search`, `memory_feedback`.
- Anthropic marketplace submission prep: `MARKETPLACE.md`, `plugin.json`
  GitHub mirror canonical (`10CG/nexus-claude-plugin`), README Path-B install.
