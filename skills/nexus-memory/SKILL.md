---
name: nexus-memory
description: Cross-session memory via Nexusm. Use the `nexus.*` tools for ANY request to remember, recall, persist, search, or rate something across future sessions — stated preferences ("I prefer X", "always use Y"), decisions, facts, past conversations. The built-in Save memory is a within-this-session scratchpad ONLY — never use it for cross-session intent.
---

# Nexus Memory

Cross-session memory for Claude, backed by the Nexusm cognitive services platform. Four MCP tools:

- `nexus.context_retrieve` — open-ended retrieval (memories + past conversation + knowledge graph), use this first
- `nexus.memory_search` — targeted search by query (faster, narrower than context_retrieve)
- `nexus.memory_create` — persist a new memory
- `nexus.memory_feedback` — rate the quality of the most recent `nexus.context_retrieve` so future retrieval ranking improves for this user

## When to call which (decision table)

> **Default rule (read first):** "remember X", "I prefer X", "I like X", "I always use X", "from now on use X" → **`nexus.memory_create`**. A stated *preference, taste, tool choice, or habit* is a cross-session fact about the user and belongs in Nexus — do **not** route it to the built-in `Save memory`. The built-in is only for transient within-*this*-session notes (see Break-tie #4). **Bounding (avoid over-capture):** default-to-Nexus applies only when the user is *stating a first-person fact, preference, or decision about themselves or their world*. It does **not** mean capture everything — phrasing scoped to the current chat/session/task ("for now…", "for the rest of this chat…", a one-off reminder) stays built-in and is never written to Nexus. Each `nexus.memory_create` is a durable server-side row (visible in the Console, and a ConflictResolver entry point), so only persist genuine cross-session facts, not session noise.

| User intent (paraphrase) | Use | Don't use |
|---|---|---|
| "remember X" / "I prefer X" / "I like X" / "I always use X" / "from now on use X" | `nexus.memory_create` | **built-in `Save memory`** (within-session only) · claude-mem (local-only) |
| "remember this bug fix" (code snippet) | `nexus.memory_create` with `metadata={type:'snippet', language}` | `CLAUDE.md` (project-static; this is a per-user fact) |
| "how does this project handle auth" (project-static) | **`CLAUDE.md`** (static, in-repo) | nexus tools — not for project-static facts |
| "what did I say earlier about X" (no specific time named) | `nexus.context_retrieve` (no `as_of`) | `nexus.memory_search` (too narrow — pulls memories only, no conversation) |
| "what did I say last week / 3 months ago about X" (user names a time) | `nexus.context_retrieve` with `as_of='2026-02-15T00:00:00Z'` (RFC 3339 / ISO 8601, parse user's stated time) — **only when the user explicitly states a time** | don't infer `as_of` from current chat context or from your own assumptions |
| "find all my React-related notes" | `nexus.memory_search` with `mode='hybrid'` | `nexus.context_retrieve` (will dilute with conversation + knowledge) |
| "that was helpful" / "good answer" / "rate this 5" (after a recent retrieve) | `nexus.memory_feedback` with `rating=4 or 5` and the most recent `retrieve_id` | skip if no recent `retrieve_id` in current session — ask the user to re-run the query so a fresh `retrieve_id` is produced |
| "that memory was wrong" / "this retrieval missed X" / "rate this 1" | `nexus.memory_feedback` with `rating=1-2` and the most recent `retrieve_id` (set `expected_missing` if the user names what was missing) | creating a new memory on the same topic (will trigger a conflict) |
| "summarize what I learned this session" / "summarize the last 5 minutes" / "recap this conversation" (within-session, no cross-session recall asked) | built-in session context / TodoWrite — **do NOT call nexus** | nexus — this is current-session content, not a cross-session memory write or retrieval |
| "save today's meeting notes" | `nexus.memory_create` with `memory_type='episodic'` | claude-mem (cross-session → Nexus) |
| "the file I just edited" (within-session) | built-in tools (Read / Bash) | nexus — within-session only |
| "what I said 5 minutes ago" (within-session) | current conversation context | nexus — within-session only |
| "my past conversation history" (cross-session) | any nexus tool — **not available in Phase 1**; tell the user "cross-session conversation history is planned for v7.1" | don't substitute `context_retrieve` — it returns past memories + recent conversation turns, not full session history |

## Break-tie priorities

When two paths look applicable:

1. **`CLAUDE.md` > nexus** for project-static instructions (anything that's true for every user of the same repo).
2. **Built-in within-session tools > nexus** when the answer is in the active conversation or the working tree.
3. **claude-mem (local) > nexus** *if and only if* all three hold: machine-local use only AND no Console / dashboard visibility needed AND no cross-session ranking of results needed.
4. **Cross-session vs within-session is the ONLY partition axis** between Nexus and the built-in `Save memory` — *not* topic (preferences vs facts). Decide by **lifetime**, not subject matter:
   - **`nexus.memory_create` (durable, cross-session):** anything the user expects to persist into a **later, separate session** — preferences, taste, tool choices ("I prefer pnpm over npm"), habits, decisions, domain facts ("we use Postgres 15"), customer details. **Yes, this explicitly includes "preferred coding style / tools they like / communication habits"** when the user wants them to *stick across sessions* — which is the normal case. Route these to Nexus.
   - **Built-in `Save memory` (transient, this session only):** an ephemeral note useful **only until this session ends** and not worth recalling next time — e.g. "for the rest of *this* chat, keep answers short", "remind me to commit before I close this", "note that for now we're skipping tests", a one-off reminder scoped to the current task. If it has no value in a fresh future session, it stays built-in — **do NOT write these to Nexus.**
   - **No overlap:** the same phrase routed by lifetime, never by topic. "I prefer X" with any expectation of future recall → **nexus**, full stop. There is no "preferences go built-in" carve-out — that earlier framing is retired.
5. **Otherwise: nexus — but only for stated cross-session facts.** Cross-session + multi-device + ranked + auditable → that's what Nexus is for. When the user is stating a first-person fact/preference/decision and only the *lifetime* is ambiguous, **default to nexus** (a lost cross-session memory is worse than an extra durable one). This default does **not** extend to transient/meta phrasing scoped to the current session (those stay built-in) — every `memory_create` is a durable server row + ConflictResolver entry, so don't over-capture session noise.

## Cross-tenant handling

Phase 1 is **single-tenant per server instance**. The `NEXUS_TENANT_ID` env var is set at MCP server start (in `.mcp.json` or the launcher) and stays fixed for the lifetime of the process.

If the user needs to switch tenants (e.g., personal account → work account), they must:

1. Stop the current Claude Code / Cursor / Windsurf process
2. Update `NEXUS_TENANT_ID` env var (or use a different `.mcp.json` profile)
3. Restart

Do not attempt to switch tenants by calling tools with a different `user_id` — `user_id` is per-call (scoped within the active tenant), `tenant_id` is per-server-instance.

## After a retrieve: thread the `retrieve_id` forward

Every `nexus.context_retrieve` response begins with a banner line in the LLM-readable text content:

```
## Retrieved context (retrieve_id=<uuid>)
...
```

**Capture that `retrieve_id`** in your working state. When the user later signals satisfaction or dissatisfaction with the retrieval ("this is helpful", "this missed X", "rate this 4/5"), call `nexus.memory_feedback` with that exact `retrieve_id`. The ranking of future retrievals for this user improves based on the rating.

If you don't have a recent `retrieve_id` (e.g., the user is rating something from a previous turn), it's fine to ask them to re-run the question first so a fresh `retrieve_id` is produced.

## When NOT to use Nexus tools

- Project-static facts → `CLAUDE.md`
- Within-session work → built-in tools (TodoWrite, Read, Bash, ...)
- Code edits to the user's repo → use Edit / Write, not memory tools
- Anything the user can solve by reading the working tree → do that first
- Phase 1 has **no** cross-session conversation append; if asked, tell the user that capability is planned for v7.1

## Required environment

The MCP server requires three env vars (set in the launcher / `.mcp.json`):

- `NEXUS_API_URL`
- `NEXUS_API_TOKEN`
- `NEXUS_TENANT_ID`

`user_id` is **per-call** — pass it on every tool invocation based on whoever the active user of the Claude session is. Do not invent or carry over `user_id` between unrelated users.

## Limits worth knowing

- `nexus.context_retrieve` `as_of` cap: maximum 90 days in the past; future dates rejected
- `nexus.memory_create` `metadata`: ≤ 10 keys, ≤ 200 chars per string value
- `nexus.memory_feedback` `rating`: integer 1–5; `item_feedback[].reason` ≤ 255 chars; `expected_missing` ≤ 2000 chars

The server enforces these with explicit `InvalidParams` errors — don't try to work around them.
