# Changelog

All notable changes to `nexus-claude-plugin` are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [0.6.1] — 2026-10-31

### Fixed
- **Memory-file source metadata clearing** (change-2 A10-11): always send
  `aria.description`, `aria.memory_type`, and `aria.origin_session`; absent
  source fields become JSON null, replacing old values while retaining keys.
  Preserve present empty strings, literal `"null"`, and existing parser/redactor
  behavior. Assembly version 2 → 3 resyncs unedited files through normal batches
  of at most five without clearing state or bypassing `stale_local`. An unchanged
  body hash produces a metadata-only PATCH (no re-embedding or sentence-index
  rebuild), then converges. D keeps its first-300-body-characters fallback for
  missing/null/empty/blank descriptions; clearing one does not hide the fact.
- **SessionStart final output budget** (change-2 TASK-009): the 4000-character
  limit covers the final `additionalContext`, including baseline/identity/failure
  reports, not just the memory renderer. Reserve report space before dropping
  whole fact items and then episode tail items; keep the full report in
  `systemMessage`. The startup ledger records final emitted length (unknown on
  a failed write) and branch-page / peer-completed / project-fallback results,
  so an empty first page is not inferred from total hits or `tier=null`.
- **SessionEnd budget erratum in the manifest's load-bearing notes** (`hooks/hooks.json` description + `hooks/test_hooks_json.py` docstring): the old text claimed a plugin-declared `timeout` raises Claude Code's shared ~1.5 s SessionEnd budget — the host only honors budget-raising declarations from **settings files** ("Timeouts set on plugin-provided hooks don't raise the budget", current hooks doc). On the default budget `memory-sync` (~7–9 s, 9 network calls in memory-rich projects) is cancelled on every real session end and, by design, leaves no ledger row — workflow C never completed in a real session on the default budget ([#56](https://forgejo.10cg.pub/10CG/nexus-claude-plugin/issues/56), verified on Claude Code 2.1.312 with a same-session controlled experiment).

### Documented
- **SessionEnd hook budget requirement** (README "SessionEnd hook budget"): set `CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS=60000` in `claude`'s environment (owner ruling 2026-10-30; matches the hooks' declared `timeout: 60` ceiling), or declare a per-hook `timeout` in a settings file — without one of those the three SessionEnd hooks share ~1.5 s and memory-sync cannot finish. Post-release acceptance of 0.6.0 (2026-10-30, nexus change-2 TASK-008 probes): systemMessage rendering **verified** on Claude Code 2.1.312 (SessionStart injection renders — `additionalContext` attachment, `rendered` attachment, and api-request evidence in the session's own transcript); with the env var set, one real session end measured session-capture 130 ms + handoff-sync 1575 ms + memory-sync 7018 ms = **8723 ms total ≤ 20 s design target**, all three ledger rows landed. On the default budget handoff-sync has also been observed cancelled (1575 ms run exceeds ~1.5 s).

## [0.6.0] — 2026-10-22

### Added
- **`SessionEnd` capture hook — per-session send watermark**
  (`hooks/session_capture.py`, plugin#49 / nexus#468 layer 1): a session that
  ends more than once (resume-then-exit, double trigger) used to re-POST its
  whole bounded activity list every time — 71% of prod's duplicate activity
  rows and ~14% re-sent payloads. Each successful run now persists, per
  `session_id`, the count of RAW transcript lines consumed plus a sha256 over
  them (`_hook_state` "session-capture" state, bounded to the newest 64
  sessions); the next run re-hashes the prefix and sends **only what the
  transcript gained**.
  - **Degradation is designed, not accidental**: first run of a session, no
    `session_id` in the payload, a rewritten/shortened transcript prefix
    (compaction edits transcripts in place), eviction from the 64-session
    map, or a different machine each cost **one full resend**, which the
    backend's idempotent ingestion folds (nexus #468 A, `deduped` in the
    response) — never a rejection, never a lost capture.
  - **The watermark advances only after a successful POST** (or after a run
    that found nothing new); a failed POST leaves it in place so the whole
    delta retries at the next SessionEnd — at-least-once, deduplicated
    server-side.
  - **Adjacent identical payloads are folded client-side** before selection
    (the polling-command burst: the same `nomad job status` five times in a
    row is one payload on the wire); non-adjacent repeats stay distinct
    events. Backend folding (nexus #468 `within_batch`) remains the net.
  - **Watermark self-check**: a non-zero `deduped` in the ingest response is
    logged on stderr (one line) and recorded in the run ledger (`deduped`
    key) — the designed signal that a watermark missed (rewrite, eviction,
    another machine) or two SessionEnds raced. A watermark state write that
    fails records `state_write_failed` (surfaced at the next SessionStart)
    without costing the capture that already succeeded.
  - `read_activities` keeps its exact 2-tuple public contract
    (nexus `scripts/replay_session_capture.py` imports it); the incremental
    reader is `read_activities_incremental`, of which it is the no-resume
    view. `_USER_AGENT` bumped to `nexus-session-capture-hook/0.5`.
- **`SessionEnd` handoff-sync hook** (`hooks/handoff_sync.py`, change 2 TASK-005 + TASK-013):
  ingests this project's `docs/handoff/*.md` (and the `latest.md` pointer) as
  `session_summary` rows so the NEXT session — on any container — warm-starts
  from them. Ownership resolution is uuid-first (A9-2/3: candidates grouped by
  owner-container; unresolvable groups report `identity_unresolved`, never a
  silent fallback), session attribution uses the transcript's first timestamp
  with a synthetic-id fallback (A9-1), unstattable candidates fail the whole
  round loudly (`pointer_unresolved`, A9-19), a symlinked `latest.md` is
  resolved through its parent directory only (A9-23), and `skip`/`no`/`off`
  frontmatter opts the document out while unparsable values refuse the upload
  (`file_unparsable`, A9-5). External ids carry the Claude Code project
  directory key as a prefix (X1), so two projects' same-named handoffs never
  PATCH each other. No state file: container identity moves are detected and
  reported by `session_inject` at the next startup instead (A9-11).
- **`SessionEnd` memory-sync hook** (`hooks/memory_sync.py`, change 2 TASK-006 + A10):
  syncs the built-in auto-memory directory (`<config>/projects/<key>/memory/*.md`)
  to `fact` rows — value-level secret redaction (known token prefixes,
  key-value secrets, URL userinfo, bearer tokens), whole-file dirty tracking
  with a redaction+assembly fingerprint (A8-2/D10), per-round batches of N=5
  with per-file cursor advance only after a 2xx, orphan reconciliation with
  ratio guards, and pending deletes verified per-row before any DELETE.
  A10 rulings shipped here: the state directory is `.memory-sync/` (no project
  slug can collide with it), the state file name carries a digest of
  `(user_id, container_id)` (a changed identity starts from a fresh state; old
  rows read as a peer container's), the project key folds in the config
  directory (two `CLAUDE_CONFIG_DIR` stores are two namespaces — the
  alternating-stores ping-pong deletion is gone), a deleted row that was
  already gone counts as `already_gone` (not `deleted`), a NUL body is
  `file_unparsable` (never silently replaced), an empty body is the new
  `empty_body` skip class, and deterministic per-file failures report once per
  file version (`failed_hash`).
- **`SessionStart` three-segment injection recipe** (`hooks/session_inject.py`, TASK-007 + A9/A10):
  episode rows grouped by container with peer lookups when this container's
  page has none (peers ≤ 4, own ≥ 1, newest-first, `aria.source=handoff`
  rendered ahead with §6 truncated at a line boundary), a fact delta keyed by
  `updated_after` with cursor paging (empty RAW page ends the scan, never a
  filtered one), a 4000-character budget favoring episodes, project scoping on
  every query (`project=<slug>` + per-row `metadata.project` verification, the
  semantic segment via `metadata_filter`), at most 12 remote calls per startup,
  the fact marker advancing only on startup after a fully-clean read,
  `identity_changed`/`user_id` changes reported once in `systemMessage`
  (stdout-gated persistence), failure-class ledger reasons resurfaced by the
  A9-20 rescan (`dedup_merged` / `orphans_deleted` never stay buried), and
  `state_write_failed` surfaced on the next startup (A10 D4/D5).
- **Shared hook infrastructure** (change 2 TASK-001/002/003/011/012): a
  `_hook_state` ledger (bounded per-hook JSONL with failure/skip reason
  classification, `also_failed`, and identity-aware state files with locked
  read-modify-write), a `_hook_runner` worker/daemon-thread skeleton with
  self-imposed deadlines and stderr guards, an `_identity` module
  (git-common-dir project resolution — a linked worktree answers its MAIN
  worktree root, K31 machine-verified; container/aria identity; the
  config-dir-composite project key), and an `_ingest_client` idempotency
  protocol (lookup → verify → upsert/dedup-delete) shared by both write hooks,
  with `X-Nexus-Source` attribution on every call.
- **`UserPromptSubmit` system-content skip** (`hooks/memory_capture.py`, #48 item 2):
  `<task-notification>` and `[SYSTEM NOTIFICATION - NOT USER INPUT]` turns are
  harness-delivered, not the user stating a preference — skipped silently
  (fail-open) before any classification.

### Changed

- **`SessionEnd` capture hook — tiered priority selection replaces the most-recent tail**
  (`hooks/session_capture.py`, `select_activities`): the 200-activity cap now keeps
  high-value actions (user_message / commit / run_test / create_file / edit_file /
  delete_file) first, then `command_run` whose head is `rm` / `mv` / `cp` / `mkdir` /
  `chmod` or `git {push,merge,rebase,reset,checkout}` / `alembic {upgrade,downgrade,revision,stamp}` /
  `docker {push,build}` / `npm publish`, then the rest; the tier that overflows keeps
  both ends; session order preserved. Every activity carries
  `activity_data.internal.capture_dropped` {total, by_action, strategy}. If the
  selector raises, the run falls back to the plain tail and the ledger records
  `capture_tiering_degraded` — SessionStart reports the hook as failed even though
  the activities were uploaded (10CG/nexus-claude-plugin#28).
- **`SessionStart` injection renders `session_summary` rows** (#32 step 4, #40):
  the aggregated layer that the two new SessionEnd hooks produce is what the
  warm-start brief is built from — the interim client-side whitelist filter
  (#40) is replaced by server-side layer filtering plus per-row verification.
- **`SKILL.md` partition rewritten by purpose** (#48 item 1): the built-in
  auto-memory (`MEMORY.md`) is what it always was — persistent across sessions
  — and is the PRIMARY store for preferences, feedback, and project facts
  (the memory-sync hook ingests it into Nexus automatically); Nexus serves
  cross-container warm-start summaries, ranked cross-session retrieval, and
  retrieval feedback. Tool names now carry their full in-session form
  (`mcp__plugin_nexus-memory_nexus__*`, deferred — load via ToolSearch;
  #48 item 3), maintainer-facing notes moved out of the runtime text and the
  "planned for v7.1" promise became "not currently available" (#48 item 4).
- **Platform note (A9-14)**: native Windows (non-WSL) is not supported — the
  hooks rely on `fcntl` locking; there, `session_inject` also cannot report
  failures.


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
