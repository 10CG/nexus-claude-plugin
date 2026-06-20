#!/usr/bin/env python3
"""UserPromptSubmit nudge hook for the nexus-memory Claude Code plugin.

Deterministically nudges Claude toward the right Nexus MCP tool because
Claude's built-in behavior otherwise structurally wins the routing:
  - WRITE intent ("remember X", "I prefer Y") → built-in auto-memory wins
    (mcp-memory-capture-hook FU; two dogfood runs scored 1/5).
  - READ intent ("what's my preferred X", "a few weeks ago I…") → built-in
    file memory (MEMORY.md) + repo inspection win, zero context_retrieve
    call (mcp-read-path-routing FU; priming-free re-test scored 2/5).

ONE hook, TWO rule sections in the single shared rules.json (proposal D3):
write-intent is checked FIRST, then read-intent — mutually-exclusive dispatch.

Design contract (proposal D1/D3/D5):
  - Stdlib-only Python 3, zero dependencies, deterministic, zero-latency.
  - Loads the SHARED rule sets from rules.json in this same dir (single source
    of truth — does NOT reimplement the rules inline).
  - WRITE classify: capture iff a capture_positive pattern matches AND no
    transient_negative pattern matches (negative overrides — over-capture bound).
  - READ classify (LEXICAL, not semantic — precision-first, D1): fire iff a
    read_allowlist pattern matches AND no read_blocklist pattern matches
    (blocklist OVERRIDES allowlist). If a read_timetravel_markers pattern also
    matches → "timetravel" (context_retrieve + RELATIVE as_of ≤90d, D5); else
    → "recall" (context_retrieve, no as_of). Authority assertion is scoped to
    the recall hit ONLY — never blanket "always prefer Nexus".
  - On a hit: print hookSpecificOutput.additionalContext (model-facing, NOT
    systemMessage), naming the exact tool. On no-hit: silent (no stdout).
  - FAIL-OPEN: ANY exception / malformed input / missing file / missing prompt
    key -> exit 0 with no stdout. NEVER exit non-zero (a non-zero
    UserPromptSubmit exit BLOCKS the user's prompt by contract). NEVER raise.
"""

import json
import os
import re
import sys

# WRITE nudge. MUST name nexus.memory_create explicitly and frame as durable
# cross-session persistence, while AVOIDING bare imperative "remember" / "save
# to memory" verbs that would re-arm Claude's built-in auto-memory routing.
_WRITE_NUDGE = (
    "This prompt looks like a durable cross-session preference, fact, or "
    "decision about the user. Persist it via the nexus.memory_create MCP tool "
    "(not built-in session memory), so it survives into future separate "
    "sessions. Pass an appropriate user_id; the tool defaults memory_type to "
    "semantic. If on reflection this is only scoped to the current session, "
    "skip the tool and treat it as transient."
)

# READ / recall nudge. Steers cross-session recall to nexus.context_retrieve
# and counters Claude's in-context / file-memory shortcut. Authority assertion
# is SCOPED to this recall hit (proposal D1) — NOT a blanket "always Nexus".
_RECALL_NUDGE = (
    "This prompt is a cross-session recall about the user's own durable facts "
    "(preferences, decisions, prior statements). Answer it by calling the "
    "nexus.context_retrieve MCP tool with a query paraphrasing the question — "
    "even if an answer appears in your current context or in MEMORY.md, the "
    "durable Nexus store is authoritative for cross-session facts. Do NOT pass "
    "an as_of (the user did not name a past time). Pass the same stable user_id "
    "you use on every call. Then surface the returned retrieve_id."
)

# READ / time-travel nudge. Same recall routing PLUS a RELATIVE as_of
# instruction (proposal D5) — the hook does NOT emit a literal date; Claude
# computes the concrete RFC3339 date, which MUST stay within the 90-day cap.
_TIMETRAVEL_NUDGE = (
    "This prompt is a cross-session recall and the user named a PAST time. "
    "Answer it by calling the nexus.context_retrieve MCP tool — even if an "
    "answer appears in your current context or in MEMORY.md, the durable Nexus "
    "store is authoritative for cross-session facts. Set the as_of argument to "
    "the user's stated time expressed as an RFC3339 date you compute yourself "
    "(e.g. resolve 'a few weeks ago' relative to today). The as_of MUST be "
    "within the last 90 days — the backend rejects anything older than 90 days "
    "with HTTP 422, so clamp to ~the stated time inside that window. Pass the "
    "same stable user_id you use on every call, then surface the retrieve_id."
)


def _classify(prompt, rules):
    """Return an intent label for the prompt, or None.

    Dispatch order (proposal D3, mutually exclusive):
      1. WRITE — capture_positive matches AND no transient_negative match
         (negative overrides — over-capture bound). -> "write"
      2. READ  — read_allowlist matches AND no read_blocklist match
         (blocklist overrides allowlist — precision-first, D1). Then:
           "timetravel" if a read_timetravel_markers pattern also matches (D5)
           else "recall".
      3. otherwise -> None (silent / fail-open no-op).
    """
    text = prompt.lower()

    # --- WRITE intent (checked first) ---
    positives = rules.get("capture_positive", [])
    negatives = rules.get("transient_negative", [])
    if any(re.search(pat, text) for pat in positives) and not any(
        re.search(pat, text) for pat in negatives
    ):
        return "write"

    # --- READ intent (lexical first-person recall allowlist) ---
    allowlist = rules.get("read_allowlist", [])
    if not any(re.search(pat, text) for pat in allowlist):
        return None

    blocklist = rules.get("read_blocklist", [])
    if any(re.search(pat, text) for pat in blocklist):
        return None  # blocklist overrides allowlist (precision-first)

    markers = rules.get("read_timetravel_markers", [])
    if any(re.search(pat, text) for pat in markers):
        return "timetravel"
    return "recall"


# Intent label -> additionalContext payload.
_NUDGE_BY_INTENT = {
    "write": _WRITE_NUDGE,
    "recall": _RECALL_NUDGE,
    "timetravel": _TIMETRAVEL_NUDGE,
}


def _extract_prompt(event):
    """Pull the user prompt out of the hook event, tolerating key variants."""
    for key in ("prompt", "user_prompt", "userPrompt"):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def main():
    # Load shared rule set from this script's own directory so it resolves
    # correctly under ${CLAUDE_PLUGIN_ROOT}.
    rules_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules.json")
    with open(rules_path, "r", encoding="utf-8") as fh:
        rules = json.load(fh)

    raw = sys.stdin.read()
    event = json.loads(raw)
    if not isinstance(event, dict):
        return  # malformed -> fail-open silent

    prompt = _extract_prompt(event)
    if prompt is None:
        return  # missing prompt -> fail-open silent

    intent = _classify(prompt, rules)
    if intent is None:
        return  # no write/read intent -> silent
    nudge = _NUDGE_BY_INTENT[intent]

    output = {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": nudge,
        }
    }
    sys.stdout.write(json.dumps(output))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # FAIL-OPEN (proposal D3/AC3): any exception, malformed JSON, missing
        # file, missing prompt key -> exit 0 with no stdout. A non-zero
        # UserPromptSubmit exit blocks the user's prompt by contract.
        pass
    sys.exit(0)
