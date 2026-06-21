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
    Content-Type, and X-Nexus-Source: sessionstart-hook.
  - Render: ONLY settled summaries (metadata.layer == "summary" preferred; if no
    profile item carries a ``layer`` key at all, take all of them). Each line is
    prefixed ``[<container_id> · <age> · <branch>]`` provenance (§6 — guard the
    warm-start from half-finished observations + make cross-container origin
    legible). No results -> no stdout (fail-open silent).
"""

import json
import os
import socket
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone

# Per-request timeouts; worst-case total (tier1 + tier2) stays ~10s so a slow
# backend never stalls session start beyond that (fail-open caps it anyway).
_TIER1_TIMEOUT_SECONDS = 6
_TIER2_TIMEOUT_SECONDS = 4
_USER_AGENT = "nexus-sessionstart-hook/0.3"


def _normalize_slug(text):
    """Normalize a path basename into a project slug (lowercase, safe chars)."""
    slug = "".join(c if (c.isalnum() or c in "-_") else "-" for c in text.strip().lower())
    slug = slug.strip("-")
    return slug or "default"


def _project_slug(cwd):
    """Derive the project slug: git toplevel basename, else cwd basename."""
    toplevel = None
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "--show-toplevel"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            toplevel = result.stdout.decode().strip() or None
    except Exception:
        toplevel = None
    base = os.path.basename(toplevel) if toplevel else os.path.basename(cwd.rstrip("/"))
    return _normalize_slug(base)


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
        "X-Nexus-Source": "sessionstart-hook",
    }
    if token:
        headers["X-API-Key"] = token
    req = urllib.request.Request(
        f"{base_url}/context/retrieve", data=data, method="POST", headers=headers
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


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


def main():
    raw = sys.stdin.read()
    # SessionStart payload is parsed only to extract cwd; malformed -> fail-open.
    event = json.loads(raw) if raw.strip() else {}
    if not isinstance(event, dict):
        return

    base_url = os.environ.get("NEXUS_API_URL", "").rstrip("/")
    if not base_url:
        return  # no backend configured -> fail-open silent
    token = os.environ.get("NEXUS_API_TOKEN", "")

    cwd = event.get("cwd") or os.getcwd()
    user_id = os.environ.get("NEXUS_DEFAULT_USER_ID") or _project_slug(cwd)
    container_id = os.environ.get("NEXUS_CONTAINER_ID") or socket.gethostname()
    branch = _current_branch(cwd)

    # Tier 1: branch + container scoped (branch key omitted if branch unknown).
    metadata_filter = {"container_id": container_id}
    if branch:
        metadata_filter["branch"] = branch

    ctx = _retrieve(base_url, token, user_id, metadata_filter, _TIER1_TIMEOUT_SECONDS)
    profile = (ctx or {}).get("profile") or []
    rows = _settled_rows(profile)

    # Tier 2: project-level fallback (no metadata_filter) when tier 1 is empty.
    if not rows:
        ctx = _retrieve(base_url, token, user_id, None, _TIER2_TIMEOUT_SECONDS)
        profile = (ctx or {}).get("profile") or []
        rows = _settled_rows(profile)

    brief = _render(rows)
    if not brief:
        return  # nothing to inject -> fail-open silent

    output = {
        "hookSpecificOutput": {
            "hookEventName": "SessionStart",
            "additionalContext": brief,
        }
    }
    sys.stdout.write(json.dumps(output, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session startup over memory retrieval.
        pass
    sys.exit(0)
