# nexus-claude-plugin

Anthropic Claude Code plugin packaging the **Nexusm MCP server** ([`@nexusm/mcp-server`](https://www.npmjs.com/package/@nexusm/mcp-server)) for one-step install into Claude Code, Cursor, Windsurf, Cline, and any other MCP-compatible client.

Exposes 4 MCP tools that wire Claude into Nexusm's cognitive services platform:

| Tool | What it does |
|------|--------------|
| `nexus.context_retrieve` | Aggregated retrieval over memories + conversation + knowledge graph (the main entry point) |
| `nexus.memory_search` | Targeted semantic / hybrid search over memories |
| `nexus.memory_create` | Persist a new memory (with conflict resolution & temporal validity) |
| `nexus.memory_feedback` | Submit feedback to close the v5.0 feedback loop |

## Install

### Claude Code

```bash
claude plugin install nexus-memory
```

### Cursor / Windsurf / Cline / other MCP clients

Add to your `.mcp.json` (or equivalent):

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

## Required environment

| Variable | Purpose |
|---------|---------|
| `NEXUS_API_URL` | Base URL of your Nexusm API (e.g., `https://api.nexusm.example.com`) |
| `NEXUS_API_TOKEN` | Bearer token for authentication |
| `NEXUS_TENANT_ID` | Tenant identifier for multi-tenant routing |

> Get these from your Nexusm console at `/settings/api-tokens` (per-user) or from your ops team (per-tenant).

## Status

**Wave 3 in progress (2026-05-22)** — plugin scaffold (plugin.json + .mcp.json) shipped. Next: SKILL.md decision table (TASK-021), README setup wizard expansion, GIF demo (TASK-022), cross-client E2E validation (TASK-019), marketplace submission (Wave 4).

## Distribution

- **Primary**: [Forgejo (`10CG/nexus-claude-plugin`)](https://forgejo.10cg.pub/10CG/nexus-claude-plugin)
- **Mirror**: [GitHub (`simonfishgit/nexus-claude-plugin`)](https://github.com/simonfishgit/nexus-claude-plugin) — auto-synced via Forgejo Actions (TASK-024, Wave 3); bus-factor mitigation (A2-D-3)

## License

MIT © 10CG
