---
name: nexus-memory
description: Cross-container memory warm-start via Nexusm. The built-in auto-memory (MEMORY.md) persists across sessions on this machine and is the primary store for preferences, feedback, and project facts; Nexus carries them across containers — warm-start summaries injected at session start, ranked cross-session retrieval, retrieval-quality feedback. Use the nexus MCP tools (deferred — load via ToolSearch) for cross-session / cross-machine recall and for explicit server-side durable writes.
---

# Nexus Memory

Cross-container memory for Claude, backed by the Nexusm cognitive services platform. Four MCP tools — **deferred tools**: load them via ToolSearch before the first call, and select them by their full in-session names (the plugin prefix is part of the name; a bare `nexus_context_retrieve` does not resolve):

- `mcp__plugin_nexus-memory_nexus__nexus_context_retrieve` — open-ended retrieval (memories + past conversation + knowledge graph), use this first
- `mcp__plugin_nexus-memory_nexus__nexus_memory_search` — targeted search by query (faster, narrower than context_retrieve)
- `mcp__plugin_nexus-memory_nexus__nexus_memory_create` — persist a new memory server-side
- `mcp__plugin_nexus-memory_nexus__nexus_memory_feedback` — rate the quality of the most recent `context_retrieve` so future retrieval ranking improves for this user

The short forms `context_retrieve` / `memory_search` / `memory_create` / `memory_feedback` below refer to these four.

## When to call which (decision table)

> **Default rule (read first) — division by purpose (the partition's single statement):** preferences, feedback, and project facts are stored primarily in the **built-in auto-memory** (`MEMORY.md` and per-fact files under the project's memory directory). The built-in auto-memory **persists across sessions** on this machine, and the memory-sync hook ingests it into Nexus automatically — nothing extra to do for durable recall. **Nexus is for cross-container / cross-machine visibility and ranked recall**: warm-start summaries injected at session start, cross-session retrieval, and retrieval-quality feedback. Route "remember X / I prefer X" (a stated durable preference or fact) → **built-in auto-memory**, not a Nexus write. Route "what did I say/do in an earlier session or from another container" → `context_retrieve`. `memory_create` is for **explicit server-side durable writes** — the user asks for server-side storage, or cross-container visibility must not wait for the next ingestion round. **Bounding (avoid over-capture):** write anything only when the user is *stating a first-person fact, preference, or decision about themselves or their world* — phrasing scoped to the current chat/session/task ("for now…", "for the rest of this chat…", a one-off reminder) goes to no store at all.

| User intent (paraphrase) | Use | Don't use |
|---|---|---|
| "remember X" / "I prefer X" / "I like X" / "I always use X" / "from now on use X" | **built-in auto-memory** (durable, cross-session on this machine; auto-ingested to Nexus by memory-sync) | `memory_create` (only when the user explicitly wants server-side storage or immediate cross-container visibility) · claude-mem (local-only) |
| "remember this bug fix" (code snippet) | built-in auto-memory (per-user fact, persists and ingests like any other) | `CLAUDE.md` (project-static; this is a per-user fact) |
| "how does this project handle auth" (project-static) | **`CLAUDE.md`** (static, in-repo) | nexus tools — not for project-static facts |
| "what did I say earlier about X" (no specific time named) | `context_retrieve` (no `as_of`) | `memory_search` (too narrow — pulls memories only, no conversation) |
| "what did I say last week / 3 months ago about X" (user names a time) | `context_retrieve` with `as_of='2026-02-15T00:00:00Z'` (RFC 3339 / ISO 8601, parse user's stated time) — **only when the user explicitly states a time** | don't infer `as_of` from current chat context or from your own assumptions |
| "find all my React-related notes" | `memory_search` with `mode='hybrid'` | `context_retrieve` (will dilute with conversation + knowledge) |
| "that was helpful" / "good answer" / "rate this 5" (after a recent retrieve) | `memory_feedback` with `rating=4 or 5` and the most recent `retrieve_id` | skip if no recent `retrieve_id` in current session — ask the user to re-run the query so a fresh `retrieve_id` is produced |
| "that memory was wrong" / "this retrieval missed X" / "rate this 1" | `memory_feedback` with `rating=1-2` and the most recent `retrieve_id` (set `expected_missing` if the user names what was missing) | creating a new memory on the same topic (will trigger a conflict) |
| "summarize what I learned this session" / "summarize the last 5 minutes" / "recap this conversation" (within-session, no cross-session recall asked) | built-in session context / TodoWrite — **do NOT call nexus** | nexus — this is current-session content, not a cross-session memory write or retrieval |
| "save today's meeting notes" | built-in auto-memory (durable, cross-session; ingested to Nexus) | claude-mem (local-only) |
| "the file I just edited" (within-session) | built-in tools (Read / Bash) | nexus — within-session only |
| "what I said 5 minutes ago" (within-session) | current conversation context | nexus — within-session only |
| "my past conversation history" (cross-session) | any nexus tool — **not available in Phase 1**; tell the user "cross-session conversation history is not currently available" | don't substitute `context_retrieve` — it returns past memories + recent conversation turns, not full session history |

## Break-tie priorities

When two paths look applicable:

1. **`CLAUDE.md` > nexus** for project-static instructions (anything that's true for every user of the same repo).
2. **Built-in within-session tools > nexus** when the answer is in the active conversation or the working tree.
3. **claude-mem (local) > nexus** *if and only if* all three hold: machine-local use only AND no Console / dashboard visibility needed AND no cross-session ranking of results needed.
4. **Built-in auto-memory vs Nexus — by purpose, stated once.** The Default rule at the top of this file is the partition's only statement: preferences, feedback, and project facts → built-in auto-memory (persists across sessions on this machine, auto-ingested to Nexus by memory-sync); cross-container / cross-machine recall, warm-start summaries, and retrieval feedback → Nexus. Session-scoped ephemera ("for the rest of *this* chat, keep answers short", a one-off task reminder) goes to no store.
   - **Runtime nudges:** the `UserPromptSubmit` hook (`hooks/memory_capture.py`) loads its pattern sets from [`hooks/rules.json`](../../hooks/rules.json) — write-intent detection (`capture_positive` / `transient_negative`) that nudges an explicit `memory_create`, and read-intent detection (`read_allowlist` / `read_timetravel_markers` / `read_blocklist`) that nudges `context_retrieve` on cross-session recall, plus a relative `as_of` (within 90 days) on time-named recall ("a few weeks ago I…"). Write intent is checked first, then read (mutually exclusive). This file describes that behavior; the patterns themselves live only in the artifact.
5. **Otherwise: ranked, cross-container, auditable recall → Nexus.** When the user asks for something that must be visible or ranked across sessions, machines, or containers — retrieval, warm-start, feedback — that is what Nexus is for. Every `memory_create` is a durable server row + ConflictResolver entry, so don't use it for session noise.

## Cross-tenant handling

Phase 1 is **single-tenant per server instance**. The `NEXUS_TENANT_ID` env var is set at MCP server start (in `.mcp.json` or the launcher) and stays fixed for the lifetime of the process.

If the user needs to switch tenants (e.g., personal account → work account), they must:

1. Stop the current Claude Code / Cursor / Windsurf process
2. Update `NEXUS_TENANT_ID` env var (or use a different `.mcp.json` profile)
3. Restart

Do not attempt to switch tenants by calling tools with a different `user_id` — `user_id` is per-call (scoped within the active tenant), `tenant_id` is per-server-instance.

## After a retrieve: thread the `retrieve_id` forward

Every `context_retrieve` response begins with a banner line in the LLM-readable text content:

```
## Retrieved context (retrieve_id=<uuid>)
...
```

**Capture that `retrieve_id`** in your working state. When the user later signals satisfaction or dissatisfaction with the retrieval ("this is helpful", "this missed X", "rate this 4/5"), call `memory_feedback` with that exact `retrieve_id`. The ranking of future retrievals for this user improves based on the rating.

If you don't have a recent `retrieve_id` (e.g., the user is rating something from a previous turn), it's fine to ask them to re-run the question first so a fresh `retrieve_id` is produced.

## When NOT to use Nexus tools

- Project-static facts → `CLAUDE.md`
- Within-session work → built-in tools (TodoWrite, Read, Bash, ...)
- Code edits to the user's repo → use Edit / Write, not memory tools
- Anything the user can solve by reading the working tree → do that first
- Phase 1 has **no** cross-session conversation append; if asked, tell the user that capability is not currently available

## Required environment

The MCP server requires three env vars (set in the launcher / `.mcp.json`):

- `NEXUS_API_URL` — base URL of the Nexus REST API. Canonical form includes the `/v1` API-version path (e.g. `https://your-host/v1`). If you point it at a bare origin or a local proxy without `/v1`, the server (≥ 0.1.3) auto-appends `/v1`, so both forms work.
- `NEXUS_API_TOKEN`
- `NEXUS_TENANT_ID`

`user_id` is **per-call** — pass it on every tool invocation based on whoever the active user of the Claude session is. Do not invent or carry over `user_id` between unrelated users.

### `user_id` stability (cross-session recall depends on this)

Cross-session recall only works if the `user_id` you read under **matches the `user_id` it was written under**. Because `user_id` is supplied per-call (and is *not* loaded from any server env), an inconsistent value across sessions means a recall can silently miss its own prior writes. Follow this convention:

- **Single-user-per-server deployments** (the common case — one operator, one Claude Code instance): set the **`NEXUS_DEFAULT_USER_ID`** env (e.g. `default`). The MCP server (≥ 0.1.4) then **pins every call to that id server-side**, overriding whatever `user_id` the model passes — so cross-session recall is reliable regardless of the model's per-call choice (the model is empirically inconsistent at picking a stable id, which silently broke recall before this pin). With the env set you don't need to do anything special; without it, pass a **stable** `user_id` identical across all sessions (consistency is what matters). **Do NOT derive `user_id` from `NEXUS_TENANT_ID`** — that collapses the compound id to `tenant::tenant`, semantically wrong and migration-hostile.
- **Multi-user deployments** (one server instance serving several distinct real users): you **MUST** pass an explicit per-call `user_id` for each real user. Do **not** fall back to the single-user `default` here — that would write everyone's memories under one id and leak them across users on recall (a privacy/correctness defect, not just a miss).

## Limits worth knowing

- `context_retrieve` `as_of` cap: maximum 90 days in the past; future dates rejected
- `memory_create` `metadata`: ≤ 10 keys, ≤ 200 chars per string value
- `memory_feedback` `rating`: integer 1–5; `item_feedback[].reason` ≤ 255 chars; `expected_missing` ≤ 2000 chars

The server enforces these with explicit `InvalidParams` errors — don't try to work around them.
