---
name: nexus-memory
description: Use when the user wants something **remembered across sessions** (preferences, decisions, past conversations, learned facts), **searched** in their long-term memory, or wants to **rate** the quality of a prior retrieval. Cross-session, server-backed memory via Nexusm. Use the four `nexus.*` tools — do not invent calls or use within-session scratchpads for cross-session intent.
---

# Nexus Memory

Cross-session memory for Claude, backed by the Nexusm cognitive services platform. Four MCP tools:

- `nexus.context_retrieve` — open-ended retrieval (memories + past conversation + knowledge graph), use this first
- `nexus.memory_search` — targeted search by query (faster, narrower than context_retrieve)
- `nexus.memory_create` — persist a new memory
- `nexus.memory_feedback` — rate the quality of the most recent `nexus.context_retrieve` so future retrieval ranking improves for this user

## When to call which (decision table)

| User intent (paraphrase) | Use | Don't use |
|---|---|---|
| "remember X" / "I prefer Y" | `nexus.memory_create` | claude-mem (cross-session needs Nexus, not local) |
| "remember this bug fix" (code snippet) | `nexus.memory_create` with `metadata={type:'snippet', language}` | `CLAUDE.md` (project-static; this is a per-user fact) |
| "how does this project handle auth" (project-static) | **`CLAUDE.md`** (static, in-repo) | nexus tools — not for project-static facts |
| "what did I say earlier about X" (no specific time named) | `nexus.context_retrieve` (no `as_of`) | `nexus.memory_search` (too narrow — pulls memories only, no conversation) |
| "what did I say last week / 3 months ago about X" (user names a time) | `nexus.context_retrieve` with `as_of='2026-02-15T00:00:00Z'` (RFC 3339 / ISO 8601, parse user's stated time) — **only when the user explicitly states a time** | don't infer `as_of` from current chat context or from your own assumptions |
| "find all my React-related notes" | `nexus.memory_search` with `mode='hybrid'` | `nexus.context_retrieve` (will dilute with conversation + knowledge) |
| "that was helpful" / "good answer" / "rate this 5" (after a recent retrieve) | `nexus.memory_feedback` with `rating=4 or 5` and the most recent `retrieve_id` | skip if no recent `retrieve_id` in current session — ask the user to re-run the query so a fresh `retrieve_id` is produced |
| "that memory was wrong" / "this retrieval missed X" / "rate this 1" | `nexus.memory_feedback` with `rating=1-2` and the most recent `retrieve_id` (set `expected_missing` if the user names what was missing) | creating a new memory on the same topic (will trigger a conflict) |
| "summarize what I learned this session" (within-session) | TodoWrite / built-in scratch | nexus — Phase 1 has no `conversation_append` |
| "save today's meeting notes" | `nexus.memory_create` with `memory_type='episodic'` | claude-mem (cross-session → Nexus) |
| "the file I just edited" (within-session) | built-in tools (Read / Bash) | nexus — within-session only |
| "what I said 5 minutes ago" (within-session) | current conversation context | nexus — within-session only |
| "my past conversation history" (cross-session) | any nexus tool — **not available in Phase 1**; tell the user "cross-session conversation history is planned for v7.1" | don't substitute `context_retrieve` — it returns past memories + recent conversation turns, not full session history |

## Break-tie priorities

When two paths look applicable:

1. **`CLAUDE.md` > nexus** for project-static instructions (anything that's true for every user of the same repo).
2. **Built-in within-session tools > nexus** when the answer is in the active conversation or the working tree.
3. **claude-mem (local) > nexus** *if and only if* all three hold: machine-local use only AND no Console / dashboard visibility needed AND no cross-session ranking of results needed.
4. **Otherwise: nexus.** Cross-session + multi-device + ranked + auditable → that's what Nexus is for.

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
