#!/usr/bin/env python3
"""SessionStart warm-start injection hook for the nexus-memory plugin (P2, workflow A).

On a new Claude Code session this hook calls the Nexus aggregated context API
(``POST {NEXUS_API_URL}/context/retrieve``) and injects the project's recent
*settled* work context as ``hookSpecificOutput.additionalContext`` — giving a
fresh session cross-container warm-start (the claude-mem "feature c" replacement).

Design contract (proposal nexus-replace-claude-mem workflow A + §6):
  - Stdlib-only Python 3, zero third-party deps.
  - FAIL-OPEN ALWAYS: any exception / missing config / malformed stdin /
    backend unreachable / timeout -> exit 0 with NO stdout. A SessionStart hook
    must NEVER block session startup just because memory could not be fetched.
  - Config (env):
      NEXUS_API_URL    -- MUST include the /v1 suffix (else every endpoint 404s
                          and masquerades as a network error,
                          [[feedback_nexus_api_url_needs_v1_suffix]]). Missing
                          -> fail-open.
      NEXUS_API_TOKEN  -- X-API-Key value (optional; sent if present).
      NEXUS_DEFAULT_USER_ID -- user_id; falls back to the normalized lowercase
                          basename of the git toplevel (or cwd) — the project
                          slug (§6 user_id mapping).
      NEXUS_CONTAINER_ID -- container/provenance id; falls back to hostname.
      NEXUS_HOOK_STATE_DIR -- where the run ledger lives (default ~/.nexus/hooks).
    user_id and container_id come from ``_identity`` — the one derivation the
    write side (session_capture.py) uses too. This file used to carry its own
    byte-identical copy, which is two chances to key one project two ways.
  - Branch: ``git -C <cwd> rev-parse --abbrev-ref HEAD``. Non-git dir / git
    failure -> branch is omitted from the metadata_filter (no branch scoping).
  - Request body uses ``profile_limit`` (NOT ``limit`` — ContextRequest has no
    ``limit`` field; a stray ``limit`` is silently ignored and the result
    degrades to the default 5). recent_hours=72, ranking_strategy=quality_rerank.
  - Two-tier fallback (§6 same-branch-first -> project-level, v1 simplification):
    first request carries metadata_filter (branch + container_id); if the
    returned profile is empty, a SECOND request is sent WITHOUT metadata_filter
    (project-level recall). This is the simplified stand-in for the full
    primary/same-branch-cross-container/project three-tier recall.
  - HTTP headers MUST include a User-Agent (SPIKE #8: requests through the CF
    proxy with no UA are blocked by CF 1010 Bot Fight Mode), plus X-API-Key,
    Content-Type, and X-Nexus-Source: sessionstart-hook/<plugin version>.
  - RUN LEDGER: every run appends one record (ok / reason / elapsed_ms / calls)
    via ``_hook_state.record_run`` — the fail-open paths above included, since
    "exit 0 with no stdout" is exactly what a hook that silently stopped working
    looks like from the outside. Two orderings are deliberate: the brief is
    written to stdout BEFORE the ledger is touched, so bookkeeping can never
    cost the session its injection; and if ``_hook_state`` cannot be imported
    at all (it needs ``fcntl``) the hook still injects and says so on stderr.
  - Render: ONLY settled summaries (metadata.layer == "summary" preferred; if no
    profile item carries a ``layer`` key at all, take all of them). Each line is
    prefixed ``[<container_id> · <age> · <branch>]`` provenance (§6 — guard the
    warm-start from half-finished observations + make cross-container origin
    legible). No results -> no stdout (fail-open silent).
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

try:
    import _identity
except Exception as exc:  # a broken or partial install
    # Imported by a test this must stay loud. Run as a hook it must not be:
    # a traceback is exit 1, which Claude Code reports as a hook error on
    # every single session start.
    if __name__ != "__main__":
        raise
    print(f"[session-inject] cannot import _identity: {exc!r}", file=sys.stderr)
    sys.exit(0)

try:
    import _hook_state
except Exception as exc:  # e.g. no fcntl on a native Windows Python
    # The ledger is bookkeeping. Losing it must not take the injection with it.
    _hook_state = None
    _LEDGER_IMPORT_ERROR = repr(exc)

HOOK = "session-inject"  # names the ledger file

# The name half of X-Nexus-Source. The backend attributes a request by this
# exact string against an allowlist (nexus `mcp_attribution._KNOWN_CLIENTS`).
# It was missing from that list once, and 212 SessionStart calls on prod were
# attributed to "unknown" before anyone looked. Renaming it here redoes that.
SOURCE_NAME = "sessionstart-hook"

# Per-request timeouts; worst-case total (tier1 + tier2) stays ~10s so a slow
# backend never stalls session start beyond that (fail-open caps it anyway).
_TIER1_TIMEOUT_SECONDS = 6
_TIER2_TIMEOUT_SECONDS = 4
_USER_AGENT = "nexus-sessionstart-hook/0.3"


class _NotJson(Exception):
    """A 2xx whose body is not JSON: a proxy or challenge page, not our API."""


def _current_branch(cwd):
    """Return the current git branch, or None if not a git repo / git failed."""
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        branch = result.stdout.decode().strip()
        if not branch or branch == "HEAD":  # detached HEAD -> no branch scoping
            return None
        return branch
    except Exception:
        return None


def _retrieve(base_url, token, user_id, metadata_filter, timeout):
    """POST one context/retrieve request; return the parsed JSON dict (or None)."""
    body = {
        "user_id": user_id,
        "query": "session start: recent work context",
        "recent_hours": 72,
        # MUST be profile_limit, NOT limit (ContextRequest has no `limit` field).
        "profile_limit": 10,
        "ranking_strategy": "quality_rerank",
    }
    if metadata_filter:
        body["metadata_filter"] = metadata_filter
    data = json.dumps(body).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        # User-Agent is mandatory: CF 1010 Bot Fight Mode blocks UA-less requests
        # through the proxy (SPIKE #8).
        "User-Agent": _USER_AGENT,
        "X-Nexus-Source": _identity.source_header(SOURCE_NAME),
    }
    if token:
        headers["X-API-Key"] = token
    req = urllib.request.Request(
        f"{base_url}/context/retrieve", data=data, method="POST", headers=headers
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        try:
            return json.load(resp)
        except ValueError as exc:
            raise _NotJson(str(exc)) from exc


def _settled_rows(profile):
    """Filter profile rows to settled summaries.

    layer=="summary" preferred; if NO row carries a ``layer`` metadata key at
    all, take every row (no layer dimension present -> nothing to filter on).
    """
    rows = [r for r in profile if isinstance(r, dict)]
    has_layer = any("layer" in (r.get("metadata") or {}) for r in rows)
    if not has_layer:
        return rows
    return [r for r in rows if (r.get("metadata") or {}).get("layer") == "summary"]


def _age(meta):
    """Human age string from metadata valid_from / original_created_at, or '?'."""
    raw = meta.get("valid_from") or meta.get("original_created_at")
    if not isinstance(raw, str) or not raw:
        return "?"
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - ts
        secs = delta.total_seconds()
        if secs < 0:
            return "0m"
        if secs < 3600:
            return f"{int(secs // 60)}m"
        if secs < 86400:
            return f"{int(secs // 3600)}h"
        return f"{int(secs // 86400)}d"
    except Exception:
        return "?"


def _render(rows):
    """Render the settled rows into a provenance-annotated brief (or None)."""
    if not rows:
        return None
    lines = ["Nexus project memory (cross-container warm-start, settled summaries):"]
    for r in rows:
        meta = r.get("metadata") or {}
        container = meta.get("container_id", "?")
        branch = meta.get("branch", "?")
        age = _age(meta)
        content = (r.get("content") or "").strip().replace("\n", " ")
        if len(content) > 300:
            content = content[:300] + "…"
        lines.append(f"  • [{container} · {age} · {branch}] {content}")
    return "\n".join(lines)


def _collect(run):
    """Do the work. Returns ``(reason, brief)``; raises if a remote call fails.

    ``run`` is filled in as facts become known, so that whatever happens next
    -- a return or an exception -- the ledger record has the right project and
    the number of calls actually attempted.
    """
    raw = sys.stdin.read()
    # SessionStart payload is parsed only to extract cwd.
    event = json.loads(raw) if raw.strip() else {}
    if not isinstance(event, dict):
        raise ValueError("SessionStart payload is not a JSON object")
    if isinstance(event.get("cwd"), str) and event["cwd"]:
        run["cwd"] = event["cwd"]
    cwd = run["cwd"] or os.getcwd()

    base_url = os.environ.get("NEXUS_API_URL", "").rstrip("/")
    if not base_url:
        return "not_configured", None  # the default for a fresh install
    token = os.environ.get("NEXUS_API_TOKEN", "")

    user_id = _identity.user_id(cwd)
    container_id = _identity.container_id()
    branch = _current_branch(cwd)

    # Tier 1: branch + container scoped (branch key omitted if branch unknown).
    metadata_filter = {"container_id": container_id}
    if branch:
        metadata_filter["branch"] = branch

    run["calls"] += 1  # counted before the call: a call that fails was still made
    ctx = _retrieve(base_url, token, user_id, metadata_filter, _TIER1_TIMEOUT_SECONDS)
    profile = (ctx or {}).get("profile") or []
    rows = _settled_rows(profile)
    run["extra"]["tier"] = 1

    # Tier 2: project-level fallback (no metadata_filter) when tier 1 is empty.
    if not rows:
        run["calls"] += 1
        ctx = _retrieve(base_url, token, user_id, None, _TIER2_TIMEOUT_SECONDS)
        profile = (ctx or {}).get("profile") or []
        rows = _settled_rows(profile)
        run["extra"]["tier"] = 2

    run["extra"]["rows"] = len(rows)
    brief = _render(rows)
    if not brief:
        return "nothing_to_do", None  # nothing to inject
    return (_hook_state.NO_REASON if _hook_state else "none"), brief


def _warn(reason, exc):
    print(f"[{HOOK}] {reason}: {exc!r}", file=sys.stderr)


def _record(reason, started, run):
    """Append this run to the ledger. Never raises."""
    if _hook_state is None:
        print(
            f"[{HOOK}] run ledger unavailable ({_LEDGER_IMPORT_ERROR}); "
            f"this run ({reason}) is not recorded",
            file=sys.stderr,
        )
        return
    try:
        _hook_state.record_run(
            HOOK,
            ok=not _hook_state.is_failure_reason(reason),
            reason=reason,
            elapsed_ms=int((time.monotonic() - started) * 1000),
            calls=run["calls"],
            cwd=run["cwd"],
            extra=run["extra"] or None,
        )
    except Exception as exc:  # record_run does not raise by contract; the net under it
        print(f"[{HOOK}] could not record this run ({reason}): {exc!r}", file=sys.stderr)


def main():
    started = time.monotonic()
    run = {"cwd": None, "calls": 0, "extra": {}}
    reason, brief = "unknown", None
    try:
        reason, brief = _collect(run)
    except _NotJson as exc:
        reason = "http_error"
        _warn(reason, exc)
    except Exception as exc:
        # Still exit 0 with no stdout -- but no longer without a trace.
        reason = _hook_state.reason_for_exception(exc) if _hook_state else "unknown"
        _warn(reason, exc)

    if brief:
        output = {
            "hookSpecificOutput": {
                "hookEventName": "SessionStart",
                "additionalContext": brief,
            }
        }
        # Before the ledger, not after: nothing below this line may cost the
        # session its injection.
        try:
            sys.stdout.write(json.dumps(output, ensure_ascii=False))
            sys.stdout.flush()
        except Exception as exc:
            reason = "unknown"
            _warn("could not write the brief", exc)

    _record(reason, started, run)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session startup over memory retrieval.
        pass
    sys.exit(0)
