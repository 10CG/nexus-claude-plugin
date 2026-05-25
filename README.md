# nexus-claude-plugin

Anthropic Claude Code plugin packaging the **Nexusm MCP server** ([`@nexusm/mcp-server`](https://www.npmjs.com/package/@nexusm/mcp-server)) for one-step install into Claude Code, Cursor, Windsurf, Cline, and any other MCP-compatible client.

Exposes 4 MCP tools that wire your LLM into Nexusm's cognitive services platform:

| Tool | What it does |
|------|--------------|
| `nexus.context_retrieve` | Aggregated retrieval over memories + recent conversation + knowledge graph — the main entry point |
| `nexus.memory_search` | Targeted semantic / hybrid search over memories |
| `nexus.memory_create` | Persist a new memory (with conflict resolution + temporal validity) |
| `nexus.memory_feedback` | Submit feedback to close the feedback loop on a prior retrieve |

---

## Quick install

### Claude Code

```bash
# Add this marketplace source (one-time):
/plugin marketplace add 10CG/nexus-claude-plugin

# Then install the plugin:
/plugin install nexus-memory@10CG-nexus-claude-plugin
```

That's it. The plugin installs the `@nexusm/mcp-server` npm package on demand via `npx` and registers the 4 tools.

You still need to set 3 env vars (see [Required environment](#required-environment) below).

> **Anthropic marketplace status**: `nexus-memory` is also being submitted to Anthropic's official plugin marketplace — once accepted, the install command will simplify to `/plugin install nexus-memory`. Status tracked in [`MARKETPLACE.md`](./MARKETPLACE.md). The GitHub-source path above always works regardless of marketplace acceptance.

### Cursor

Cursor reads `~/.cursor/mcp.json`. Add:

```json
{
  "mcpServers": {
    "nexus": {
      "command": "npx",
      "args": ["-y", "@nexusm/mcp-server@0.1.0"],
      "env": {
        "NEXUS_API_URL": "https://your-nexus-api.example.com",
        "NEXUS_API_TOKEN": "your-bearer-token",
        "NEXUS_TENANT_ID": "your-tenant-id"
      }
    }
  }
}
```

Restart Cursor. The 4 `nexus.*` tools should appear in the Composer's tool list.

### Windsurf

Windsurf reads `~/.codeium/windsurf/mcp_config.json`. Use the same JSON as Cursor above (the `mcpServers` shape is identical).

Restart Windsurf.

### Cline (VS Code extension)

Cline reads `cline_mcp_settings.json` (location depends on your VS Code profile; see Cline docs). Use the same JSON as Cursor above.

Reload the VS Code window after editing.

### Any other MCP client

The plugin ships an `.mcp.json` at the repo root with the canonical config. Adapt the format to your client's MCP config convention; the key parts are:

- `command: npx`
- `args: ["-y", "@nexusm/mcp-server@0.1.0"]` (pin the version — don't use floating)
- 3 env vars: `NEXUS_API_URL`, `NEXUS_API_TOKEN`, `NEXUS_TENANT_ID`

---

## Required environment

| Variable | Purpose | Where to get it |
|----------|---------|-----------------|
| `NEXUS_API_URL` | Base URL of your Nexusm API (e.g., `https://api.nexusm.example.com`) | Your ops team or `https://<your-nexus-console>/settings/api` |
| `NEXUS_API_TOKEN` | Bearer token for authentication | Nexusm console → Settings → API Tokens → Generate |
| `NEXUS_TENANT_ID` | Tenant identifier for multi-tenant routing | Nexusm console → Settings → Tenant info |

`user_id` is **per-call** (passed at each tool invocation) and is **not** an env var — see [SKILL.md](skills/nexus-memory/SKILL.md) for how the LLM threads user_id from the active session.

---

## Setup wizard

If you're installing for the first time:

### 1. Verify your Nexusm API is reachable

```bash
curl -sI -H "Authorization: Bearer $NEXUS_API_TOKEN" \
     "$NEXUS_API_URL/health" | head -1
```

Expected: `HTTP/2 200`. If you get `401` → token wrong. If you get `404` → URL or path wrong (the Nexusm health endpoint is `/health`, not `/healthz`). If you get connect error → network/firewall.

### 2. Verify npm can fetch `@nexusm/mcp-server`

```bash
npm view @nexusm/mcp-server@0.1.0 version
```

Expected: prints `0.1.0`. If you get an npm 404 → the package isn't published yet (currently expected; see [Status](#status)). If you get auth errors → npm registry is misconfigured.

> Don't use `npx -y @nexusm/mcp-server@0.1.0 --version` here — the server has no `--version` flag (it starts on stdio and blocks on stdin). `npm view` is the right verification command.

### 3. First tool call (smoke test in your client)

After restarting your MCP client, ask the LLM something simple that should route through Nexus:

> "记一下我用 Postgres 15 在 dev 机"
> ("remember I use Postgres 15 on dev")

The LLM should call `nexus.memory_create` with that fact. You should see the call in your client's MCP tool log.

### 4. Verify the feedback loop

After step 3, ask:

> "what did I tell you about my database setup"

The LLM should call `nexus.context_retrieve`. The response includes a `retrieve_id` banner. If you then say "that was helpful, rate it 5", the LLM should call `nexus.memory_feedback` with that `retrieve_id` and `rating=5`.

This closes the feedback loop and feeds future retrieval ranking for this user.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Tools don't show up in client's tool list | Client didn't pick up the MCP config | Restart the client (full quit, not just window reload) |
| `command not found: npx` | Node.js not installed or not in PATH | Install Node ≥ 18 from https://nodejs.org |
| `npm ERR! 404 Not Found - @nexusm/mcp-server@0.1.0` | Package not yet published to npm | See [Status](#status) — Wave 4 publish carries this |
| `Authorization required` / `401` from a tool call | `NEXUS_API_TOKEN` wrong or expired | Regenerate in Nexusm console; update env var in client config; restart client |
| `Cannot resolve hostname` | `NEXUS_API_URL` typo or VPN not connected | Check URL spelling; verify reachability with `curl` (setup wizard step 1) |
| LLM keeps using its built-in memory instead of `nexus.memory_create` | SKILL.md not loaded by the client, OR the user prompt is genuinely meta-collaboration (Claude style preferences) not domain fact | Verify SKILL.md is in the client's plugin path; see SKILL.md break-tie rule #4 for the meta-collaboration vs domain-fact partition |
| LLM never calls `nexus.memory_feedback` even after user says "that was helpful" | The LLM lost the `retrieve_id` between turns | Tell the LLM explicitly: "use the retrieve_id from your last context_retrieve call". Future versions may surface this via per-tool prompt scaffolding. |
| Tool calls hang or time out | Nexusm API is slow / unreachable; or the MCP transport stalled | Check API health (setup step 1); kill and re-spawn the MCP client; check the MCP client's stderr log for `nexusm-mcp-server` lines |
| Metric scraping shows zeros | `nexus_mcp_tool_calls_total` is populated only after the first tool call; before any traffic it correctly reads 0 | Make a tool call (setup step 3); confirm metric increments on `http://localhost:9090/metrics` |

For deeper issues, the MCP server's runbook is at [`RUNBOOK.md`](https://forgejo.10cg.pub/10CG/nexusm-mcp-server/src/branch/main/RUNBOOK.md) on the server repo.

---

## End-to-end demo (ASCII)

A typical session:

```
You: "记一下我用 Postgres 15 在 dev 机"
LLM: → nexus.memory_create({
        user_id: "alice",
        content: "User runs Postgres 15 on dev machine",
        memory_type: "semantic",
        metadata: { component: "dev-stack", tool: "postgres", version: "15" }
      })
     ← { memory_id: "uuid-1", created_at: "2026-05-23T...", conflict_resolution: null }
     "Got it — saved that you use Postgres 15 on dev."

You: "what did I tell you about my database setup"
LLM: → nexus.context_retrieve({
        user_id: "alice",
        query: "database setup",
        limit: 10
      })
     ← { retrieve_id: "uuid-2", memories: [...Postgres 15 memory...], conversation_turns: [], knowledge_entities: [] }
     "## Retrieved context (retrieve_id=uuid-2)
      Earlier you mentioned you run Postgres 15 on dev. ..."

You: "good, rate that 5"
LLM: → nexus.memory_feedback({
        user_id: "alice",
        retrieve_id: "uuid-2",
        rating: 5,
        item_feedback: [{ memory_id: "uuid-1", useful: true }]
      })
     ← { feedback_id: "uuid-3", retrieve_id: "uuid-2", status: "accepted", created_at: "2026-05-23T..." }
     "Thanks, recorded. Future retrievals will weight that memory higher for you."
```

A real GIF/video walking through this in Claude Code is planned for Wave 4 (post-`npm publish`).

---

## Status

**Wave 3 in progress (2026-05-23)** — plugin scaffold + SKILL.md (R1+R1.5 audited) + GitHub mirror shipped. **Wave 4** will publish `@nexusm/sdk@1.3.0` + `@nexusm/mcp-server@0.1.0` to npm and submit to Anthropic marketplace.

Until Wave 4 ships, `npx -y @nexusm/mcp-server@0.1.0` will return 404 — the plugin is **not yet end-to-end usable**. The plugin scaffold + SKILL.md + setup wizard exist now so that as soon as the npm publishes land, install is one command.

---

## Distribution

- **Primary**: [Forgejo (`10CG/nexus-claude-plugin`)](https://forgejo.10cg.pub/10CG/nexus-claude-plugin)
- **Mirror**: [GitHub (`10CG/nexus-claude-plugin`)](https://github.com/10CG/nexus-claude-plugin) — auto-synced from Forgejo on every push to main via Forgejo Actions; bus-factor mitigation (US-037 §A2-D-3)

The Anthropic marketplace submission points to the GitHub mirror; the source of truth is Forgejo.

---

## How the LLM decides which tool to use

See [`skills/nexus-memory/SKILL.md`](skills/nexus-memory/SKILL.md) — that's the plugin's "skill" file, which the LLM (Claude / GPT / etc.) reads to decide:

- **When to call which of the 4 `nexus.*` tools** (13-row decision table)
- **When NOT to use Nexus tools** (within-session work, project-static facts, etc.)
- **How to disambiguate** between Nexus and other memory systems (`CLAUDE.md`, claude-mem, Anthropic's built-in auto-memory)
- **How to thread `retrieve_id`** forward to close the feedback loop

If the LLM is making wrong tool choices, the fix is almost always in `SKILL.md`, not in the tool implementations.

---

## Contributing

1. Fork from Forgejo (`10CG/nexus-claude-plugin`)
2. Branch + PR back to Forgejo `main`
3. GitHub mirror updates automatically; submit issues either on Forgejo (preferred) or GitHub (mirrored)

The plugin manifest is `.claude-plugin/plugin.json`. The MCP config is `.mcp.json`. The skill that teaches the LLM tool selection is `skills/nexus-memory/SKILL.md`.

---

## License

MIT © 10CG
