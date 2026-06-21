# Changelog

All notable changes to `nexus-claude-plugin` are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

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
