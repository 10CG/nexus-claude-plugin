# Anthropic Plugin Marketplace — Submission Record

> **Purpose**: per US-037 TASK-026 deliverable, this file is the canonical record of `nexus-memory` plugin's marketplace submission journey: status, outcome, fallback path, and any iteration history.
>
> **Maintenance**: append-only timeline at the bottom. Don't rewrite historical entries.

---

## Current status

| Field | Value |
|-------|-------|
| **Plugin name** | `nexus-memory` (per `.claude-plugin/plugin.json` `name` field) |
| **Plugin version** | `0.2.4` |
| **Backing npm package** | [`@nexusm/mcp-server@0.1.4`](https://www.npmjs.com/package/@nexusm/mcp-server) (`.mcp.json` pin; 0.1.4 = NEXUS_API_URL `/v1` auto-normalize + opt-in `NEXUS_DEFAULT_USER_ID` server-side user_id pin) |
| **Capabilities** | 4 MCP tools (`nexus.context_retrieve` / `memory_search` / `memory_create` / `memory_feedback`) **+ a `UserPromptSubmit` hook** that deterministically routes write intent → `memory_create` and cross-session recall → `context_retrieve` (the SKILL.md discrimination layer is now hook-backed, not prose-only) |
| **Canonical source** | GitHub mirror at https://github.com/10CG/nexus-claude-plugin (auto-synced from Forgejo `10CG/nexus-claude-plugin`; HEAD `2b4d6fe`) |
| **Forgejo origin** | https://forgejo.10cg.pub/10CG/nexus-claude-plugin (issues/PRs land here; GitHub is mirror only) |
| **Submission status** | ⏳ **Not yet submitted** (prerequisites cleared; submission is user-led) |
| **Last update** | 2026-06-21 — dogfood 5/5 passed, prerequisites cleared |

---

## Submission readiness checklist (TASK-026 entry gate)

Items the plugin must satisfy **before** submission. Tick when verified.

### Hard requirements

- [x] `@nexusm/mcp-server@0.1.4` live on npm public registry — `.mcp.json` pin; published 2026-06-21 (tag → GitHub Actions, Sigstore provenance)
- [x] `@nexusm/sdk@5.1.0` live on npm — transitive dep of mcp-server, required for `npx -y` install
- [x] `.claude-plugin/plugin.json` valid against Anthropic plugin schema (v0.2.4; no stray `hooks` manifest key — the standard `hooks/hooks.json` auto-loads)
- [x] `.mcp.json` points at the published npm package (`@nexusm/mcp-server@0.1.4`), not a local file path
- [x] `LICENSE` present (MIT)
- [x] `README.md` with per-client install instructions (Claude Code / Cursor / Windsurf / Cline / mcp-cli)
- [x] `README.md` `Required environment` section: 3 required env vars (`NEXUS_API_URL`, `NEXUS_API_TOKEN`, `NEXUS_TENANT_ID`) + 1 optional (`NEXUS_DEFAULT_USER_ID` — single-user pin for reliable cross-session recall)
- [x] `README.md` troubleshooting section
- [x] Forgejo `10CG/nexus-claude-plugin` repo is public (visible without auth)
- [x] GitHub mirror `10CG/nexus-claude-plugin` reflects Forgejo HEAD (`2b4d6fe`, native Push Mirror)
- [x] `plugin.json` `repository` field points at GitHub mirror (Anthropic marketplace expects `github.com` URL)

### Soft requirements (Anthropic may reject without)

- [x] `skills/nexus-memory/SKILL.md` — discrimination layer with decision table + break-tie rules + read-routing section; **now hook-backed** (`hooks/rules.json` + `hooks/memory_capture.py` UserPromptSubmit nudge) so routing is deterministic, not prose-dependent
- [x] **R2 dogfood ≥ 4/5 scenarios pass** — ✅ **5/5 in clean Claude Code session (2026-06-21)**: write→`memory_create`, recall→`context_retrieve` (returned the seeded preference), feedback→`memory_feedback` via chained `retrieve_id`, within-session correctly refused, time-travel→`context_retrieve`+`as_of`. Cross-session recall validated end-to-end (server-side `user_id` pin). See [`dogfood-run-2026-06-21.md`](https://forgejo.10cg.pub/10CG/nexus/src/branch/main/openspec/changes/us-037-mcp-server-exposure/dogfood-run-2026-06-21.md)
- [ ] **Demo GIF / video** embedded in README — gated by [FU-DEMO-GIF-WAVE4](../../openspec/changes/us-037-mcp-server-exposure/gif-recording-script.md); ASCII storyboard exists; recording is user-led. Non-blocking for submission.

### Nice-to-have (not blocking submission, gathered post-acceptance)

- [ ] Cross-client E2E (TASK-019) 5×3 matrix ≥80% hit rate report (would be linked in MARKETPLACE.md follow-up)
- [ ] Real-user testimonial(s) — 3 internal teams per US-037.md DoD line 362

---

## Submission process

> **Key fact (researched 2026-06-21):** Claude Code plugin marketplaces are **decentralized** — a public GitHub repo with valid `.claude-plugin/marketplace.json` + `plugin.json` IS a marketplace, installable immediately with **no Anthropic approval** (Path B below). There is **no approval gate to "ship"**. Anthropic's curated catalogs exist only for **discoverability**: the **community marketplace** (`anthropics/claude-plugins-community`, public submission via form) and the **official marketplace** (`claude-plugins-official`, Anthropic discretion, no public submission). Docs: https://code.claude.com/docs/en/plugin-marketplaces

### Path A — Anthropic community marketplace (for discoverability)

This is the real "submission" — it gets `nexus-memory` listed in Anthropic's vetted **community** catalog so users can find it in the `/plugin` **Discover** tab.

1. **Submit** via the plugin directory form: **https://clau.de/plugin-directory-submission**
2. Provide:
   - GitHub repo URL: `https://github.com/10CG/nexus-claude-plugin`
   - Plugin name: `nexus-memory`
   - Marketplace name: `10CG-nexus-claude-plugin`
   - Description (use the `description` field from `plugin.json` verbatim)
   - Keywords: `nexus`, `nexusm`, `mcp`, `memory`, `claude-code`, `anthropic`
   - Category: Memory / Knowledge management
3. Anthropic runs automated validation + safety screening, then pins your plugin to a specific commit SHA in `anthropics/claude-plugins-community`.
4. Once listed, users install via: `/plugin marketplace add anthropics/claude-plugins-community` → `/plugin install nexus-memory@claude-community`.

> The **official** marketplace (`claude-plugins-official`) has no public submission — it's Anthropic-curated only; don't wait on it.

### Path B — Direct GitHub marketplace add (works NOW, no Anthropic approval needed)

Users can install today without waiting for community-catalog listing:

```bash
# In Claude Code:
/plugin marketplace add 10CG/nexus-claude-plugin
/plugin install nexus-memory@10CG-nexus-claude-plugin
```

This is the **documented install path** and the fallback if the community listing is delayed/rejected.

> **Single-user tip (recommended in the listing):** set `NEXUS_DEFAULT_USER_ID` (e.g. `default`) so the server pins a stable `user_id` across sessions — cross-session recall is unreliable without it because the model picks an inconsistent id per call. Multi-user servers leave it unset and pass an explicit per-call `user_id`.

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

#### 2026-06-21 — Prerequisites cleared; ready to submit

- ✅ **R2 dogfood 5/5** in a clean Claude Code session (the last hard/soft blocker). Routing and cross-session recall validated end-to-end after the read-path nudge (plugin 0.2.3) + server-side `user_id` pin (mcp-server 0.1.4). Record: `dogfood-run-2026-06-21.md`.
- ✅ Plugin bumped 0.1.0 → **0.2.4**; mcp-server pin 0.1.1 → **0.1.4**; GitHub mirror current (`2b4d6fe`); public install path (Path B) verified working.
- 🗒️ Submission channel resolved (was "TBD"): **community marketplace** via https://clau.de/plugin-directory-submission (decentralized GitHub install needs no approval).
- 🗒️ Only Demo GIF remains (non-blocking, user-led recording).
- 📝 Next: maintainer submits via **Path A** form and records the entry below. Until then, **Path B** is fully functional and is the documented install path.

#### [TIMESTAMP — submitted] (TBD)

Template for the actual submission entry:
```
- ✅ Submitted via https://clau.de/plugin-directory-submission
- Submission URL / reference: [...]
- Reviewer ETA: per Anthropic
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
- If delayed > 14 days: fallback to Path B activated (already the documented install path)
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
- Dogfood run record (5/5, 2026-06-21): [nexus `openspec/changes/us-037-mcp-server-exposure/dogfood-run-2026-06-21.md`](https://forgejo.10cg.pub/10CG/nexus/src/branch/main/openspec/changes/us-037-mcp-server-exposure/dogfood-run-2026-06-21.md)
- GIF recording script (FU-DEMO-GIF-WAVE4): [nexus `openspec/changes/us-037-mcp-server-exposure/gif-recording-script.md`](https://forgejo.10cg.pub/10CG/nexus/src/branch/main/openspec/changes/us-037-mcp-server-exposure/gif-recording-script.md)
- Phase D archive checklist: [nexus `openspec/changes/us-037-mcp-server-exposure/phase-d-archive-checklist.md`](https://forgejo.10cg.pub/10CG/nexus/src/branch/main/openspec/changes/us-037-mcp-server-exposure/phase-d-archive-checklist.md)
