#!/usr/bin/env python3
"""UserPromptSubmit nudge hook for the nexus-memory Claude Code plugin.

Deterministically nudges Claude toward the `nexus.memory_create` MCP tool when
the user's prompt states a durable, cross-session memory-WRITE intent — because
Claude's built-in auto-memory otherwise structurally wins the "remember" routing
(two dogfood runs scored 1/5; see proposal mcp-memory-capture-hook).

Design contract (proposal D3/D5/D6/D7):
  - Stdlib-only Python 3, zero dependencies, deterministic, zero-latency.
  - Loads the SHARED lifetime-axis rule set from rules.json in this same dir
    (single source of truth — does NOT reimplement the rules inline).
  - Classify: CAPTURE iff a capture_positive pattern matches AND no
    transient_negative pattern matches (negative overrides — over-capture bound).
  - On CAPTURE: print hookSpecificOutput.additionalContext (model-facing, NOT
    systemMessage). The nudge names `nexus.memory_create` and avoids bare
    "remember"/"save to memory" imperatives that could re-arm built-in memory.
  - On NO-CAPTURE: silent (no stdout).
  - FAIL-OPEN: ANY exception / malformed input / missing file / missing prompt
    key -> exit 0 with no stdout. NEVER exit non-zero (a non-zero
    UserPromptSubmit exit BLOCKS the user's prompt by contract). NEVER raise.
"""

import json
import os
import re
import sys

# The nudge text. MUST name the tool explicitly and frame as durable
# cross-session persistence, while AVOIDING bare imperative "remember" / "save
# to memory" verbs that would re-arm Claude's built-in auto-memory routing.
_NUDGE = (
    "This prompt looks like a durable cross-session preference, fact, or "
    "decision about the user. Persist it via the nexus.memory_create MCP tool "
    "(not built-in session memory), so it survives into future separate "
    "sessions. Pass an appropriate user_id; the tool defaults memory_type to "
    "semantic. If on reflection this is only scoped to the current session, "
    "skip the tool and treat it as transient."
)


def _classify(prompt, rules):
    """Return True if the prompt signals cross-session memory-write intent.

    CAPTURE iff a capture_positive pattern matches AND no transient_negative
    pattern matches (negative overrides — proposal D7 over-capture bound).
    """
    text = prompt.lower()

    positives = rules.get("capture_positive", [])
    negatives = rules.get("transient_negative", [])

    has_positive = any(re.search(pat, text) for pat in positives)
    if not has_positive:
        return False

    has_negative = any(re.search(pat, text) for pat in negatives)
    return not has_negative


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

    if not _classify(prompt, rules):
        return  # no-capture -> silent

    output = {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": _NUDGE,
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
