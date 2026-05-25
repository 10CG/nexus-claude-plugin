# Anthropic Plugin Marketplace — Submission Record

> **Purpose**: per US-037 TASK-026 deliverable, this file is the canonical record of `nexus-memory` plugin's marketplace submission journey: status, outcome, fallback path, and any iteration history.
>
> **Maintenance**: append-only timeline at the bottom. Don't rewrite historical entries.

---

## Current status

| Field | Value |
|-------|-------|
| **Plugin name** | `nexus-memory` (per `.claude-plugin/plugin.json` `name` field) |
| **Plugin version** | `0.1.0` |
| **Backing npm package** | [`@nexusm/mcp-server@0.1.0`](https://www.npmjs.com/package/@nexusm/mcp-server) (live since 2026-05-25) |
| **Canonical source** | GitHub mirror at https://github.com/10CG/nexus-claude-plugin (auto-synced from Forgejo `10CG/nexus-claude-plugin`) |
| **Forgejo origin** | https://forgejo.10cg.pub/10CG/nexus-claude-plugin (issues/PRs land here; GitHub is mirror only) |
| **Submission status** | ⏳ **Not yet submitted** |
| **Last update** | 2026-05-25 — initial preparation |

---

## Submission readiness checklist (TASK-026 entry gate)

Items the plugin must satisfy **before** submission. Tick when verified.

### Hard requirements

- [x] `@nexusm/mcp-server@0.1.0` live on npm public registry — verified `HTTP 200 on registry.npmjs.org/@nexusm/mcp-server/0.1.0` (2026-05-25)
- [x] `@nexusm/sdk@1.3.0` live on npm — transitive dep of mcp-server, required for `npx -y` install
- [x] `.claude-plugin/plugin.json` valid against Anthropic plugin schema
- [x] `.mcp.json` points at the published npm package (not a local file path)
- [x] `LICENSE` present (MIT)
- [x] `README.md` with per-client install instructions (Claude Code / Cursor / Windsurf / Cline / mcp-cli)
- [x] `README.md` `Required environment` section listing 3 env vars (`NEXUS_API_URL`, `NEXUS_API_TOKEN`, `NEXUS_TENANT_ID`)
- [x] `README.md` troubleshooting section
- [x] Forgejo `10CG/nexus-claude-plugin` repo is public (visible without auth)
- [x] GitHub mirror `10CG/nexus-claude-plugin` reflects Forgejo HEAD (mirror workflow Run #2+ green)
- [x] `plugin.json` `repository` field points at GitHub mirror (Anthropic marketplace expects `github.com` URL)

### Soft requirements (Anthropic may reject without)

- [x] `skills/nexus-memory/SKILL.md` — discrimination layer with ≥12-row decision table + break-tie rules (incl. R1.5 Anthropic-auto-memory partition)
- [ ] **R2 dogfood ≥ 4/5 scenarios pass** in clean Claude Code session — gated by [FU-SKILL-MD-R2-DOGFOOD](../../openspec/changes/us-037-mcp-server-exposure/dogfood-scenarios.md); execute before submission to avoid marketplace user reports of bad routing
- [ ] **Demo GIF / video** embedded in README — gated by [FU-DEMO-GIF-WAVE4](../../openspec/changes/us-037-mcp-server-exposure/gif-recording-script.md); marketplace listings without media get less engagement

### Nice-to-have (not blocking submission, gathered post-acceptance)

- [ ] Cross-client E2E (TASK-019) 5×3 matrix ≥80% hit rate report (would be linked in MARKETPLACE.md follow-up)
- [ ] Real-user testimonial(s) — 3 internal teams per US-037.md DoD line 362

---

## Submission process

> ⚠️ **Anthropic's official marketplace submission channel is not yet a public form/API at time of writing.** This section captures the best-known path; update when concrete channel info lands.

### Path A — Official marketplace (preferred)

Per Anthropic's plugin marketplace documentation (as of 2026-05):

1. **Visit** https://docs.claude.com/en/docs/claude-code/plugin-marketplaces (or successor URL)
2. Follow their published submission instructions (PR to a marketplace registry repo / submission form / Discord channel — verify which is current)
3. Provide:
   - GitHub repo URL: `https://github.com/10CG/nexus-claude-plugin`
   - Plugin name: `nexus-memory`
   - Description (use the `description` field from `plugin.json` verbatim)
   - Keywords: `nexus`, `nexusm`, `mcp`, `memory`, `claude-code`, `anthropic`
   - Category: Memory / Knowledge management (whichever taxonomy Anthropic uses)

### Path B — Direct GitHub marketplace add (always works, no Anthropic approval needed)

Users can install today without waiting for official marketplace acceptance:

```bash
# In Claude Code:
/plugin marketplace add 10CG/nexus-claude-plugin
/plugin install nexus-memory@10CG-nexus-claude-plugin
```

This is the **primary fallback** if marketplace acceptance is delayed or rejected. The README's "Quick install" section should make this the documented "official" path until / unless Path A succeeds.

---

## Outcome tracking

### Status legend

| Symbol | Meaning |
|--------|---------|
| ⏳ | In progress / waiting |
| ✅ | Accepted / completed |
| ❌ | Rejected (with reason recorded below) |
| 🔄 | Delayed > 14 days → fallback to Path B per proposal §R2 M-17 |
| 🗒️ | Update / note (no status change) |

### Timeline

#### 2026-05-25 — Prep complete, awaiting submission

- ✅ All hard requirements met (npm package live, repo public, mirror synced)
- ⚠️ 2 soft requirements pending (R2 dogfood + demo GIF) — submission can proceed before these but strongly recommended to do dogfood first to avoid post-acceptance bad-routing reports
- 📝 Next: maintainer executes `dogfood-scenarios.md` 5-scenario probe in a clean Claude Code session, records ≥4/5 pass → then submits via Path A

#### [TIMESTAMP — submitted] (TBD)

Template for the actual submission entry:
```
- ✅ Submitted via [channel: form / PR / Discord etc.]
- Submission URL / reference: [...]
- Reviewer ETA: 3-7 business days per Anthropic
```

#### [TIMESTAMP — outcome] (TBD)

Template for outcome:
```
- ✅ Accepted / ❌ Rejected / 🔄 Delayed
- If rejected: reason verbatim from reviewer:
  > [reviewer feedback]
  Iteration plan:
    1. [...]
    2. [...]
- If delayed > 14 days: fallback to Path B activated; README updated to primary-document `/plugin marketplace add 10CG/nexus-claude-plugin`
```

---

## DoD reference

Per [`openspec/changes/us-037-mcp-server-exposure/proposal.md`](../../openspec/changes/us-037-mcp-server-exposure/proposal.md) §R2 M-17 lock:

> **DoD for TASK-026 = "submission completed OR rejection reason recorded"**
>
> Acceptance is NOT required to mark TASK-026 done. The marketplace decision is upstream-async; we commit to the **submission + outcome documentation**, not the verdict.

This file is that record.

---

## See also

- Plugin spec: [`.claude-plugin/plugin.json`](.claude-plugin/plugin.json)
- MCP server config: [`.mcp.json`](.mcp.json)
- Skill: [`skills/nexus-memory/SKILL.md`](skills/nexus-memory/SKILL.md)
- Dogfood scenarios (FU-SKILL-MD-R2-DOGFOOD gate): [nexus `openspec/changes/us-037-mcp-server-exposure/dogfood-scenarios.md`](https://forgejo.10cg.pub/10CG/nexus/src/branch/main/openspec/changes/us-037-mcp-server-exposure/dogfood-scenarios.md)
- GIF recording script (FU-DEMO-GIF-WAVE4): [nexus `openspec/changes/us-037-mcp-server-exposure/gif-recording-script.md`](https://forgejo.10cg.pub/10CG/nexus/src/branch/main/openspec/changes/us-037-mcp-server-exposure/gif-recording-script.md)
- Phase D archive checklist: [nexus `openspec/changes/us-037-mcp-server-exposure/phase-d-archive-checklist.md`](https://forgejo.10cg.pub/10CG/nexus/src/branch/main/openspec/changes/us-037-mcp-server-exposure/phase-d-archive-checklist.md)
