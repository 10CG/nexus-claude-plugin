#!/usr/bin/env python3
"""SessionEnd activity-capture hook for the nexus-memory plugin (P1, workflow C).

On Claude Code session end this hook reads the session transcript, distills it
into a bounded list of activities, and POSTs them to the Nexus activity stream
(``POST {NEXUS_API_URL}/activities/stream``). The backend Arq worker
(``activity_processor.process_activity``) extracts those activities into episodic
Memory rows keyed by ``user_id == agent_id`` — the write side of the claude-mem
"auto-capture feature (a)" replacement, paired with the read-side
``session_inject.py`` (P2). dev's ``/v1/activities/stream`` is live as of
migration 024.

Design contract (proposal nexus-replace-claude-mem workflow C):
  - Stdlib-only Python 3, zero third-party deps (mirrors session_inject.py).
  - FAIL-OPEN ALWAYS: any exception / missing field / unreachable backend /
    timeout / malformed transcript line -> exit 0 with NO stdout. A SessionEnd
    hook must NEVER block session teardown just because capture failed.
  - SessionEnd payload (stdin JSON): {session_id, transcript_path, cwd, ...}.
    No transcript_path / unreadable file -> fail-open (nothing to capture).
  - Transcript is JSONL — one message per line. Parsed DEFENSIVELY line-by-line;
    a bad/blank/non-dict line is skipped, never fatal (transcripts can be
    partially written or contain tool-result noise).
  - Activity extraction (bounded — most-recent ``_MAX_ACTIVITIES`` kept, and the
    ActivityStreamRequest schema caps at 1000):
      * assistant message tool_use block -> mapped action via _classify_tool:
          Edit  -> edit_file        Write -> create_file      Read  -> read_file
          Bash 'git commit'  -> commit
          Bash pytest|jest|'go test'|'npm test'|vitest -> run_test
          Bash (other)       -> command_run
          Grep/Glob/Task/... -> agent_action
        activity_data carries {tool, <summary>} where summary is the file path
        or the command's first 200 chars.
      * user text message -> action=user_message, activity_data={text: truncated}.
      * unknown / unmapped tool -> action=agent_action (an unrecognised tool is
        still an agent operation; the "other" enum value is reserved for future
        non-tool, non-message activity kinds and is not currently emitted).
  - agent_id = project slug: NEXUS_DEFAULT_USER_ID, else the normalized lowercase
    basename of the git toplevel (or cwd) — IDENTICAL derivation to
    session_inject.py so the captured episodic memories land on the same
    user_id=project that the read side queries.
  - provenance: every activity_data is augmented with container_id
    (NEXUS_CONTAINER_ID, else hostname) + branch
    (``git -C cwd rev-parse --abbrev-ref HEAD``, omitted on failure) + session_id.
  - HTTP headers MUST include a User-Agent (CF 1010 Bot Fight Mode blocks UA-less
    requests through the proxy), plus X-API-Key (if token present), Content-Type,
    and X-Nexus-Source: session-capture-hook. ~8s timeout. fail-open.
  - Empty activity list -> no request sent. SessionEnd never injects context, so
    success produces NO stdout.
"""

import json
import os
import socket
import subprocess
import sys
import urllib.request

# Bounded capture: keep at most the most-recent _MAX_ACTIVITIES extracted
# activities. The backend ActivityStreamRequest caps at 1000; we stay well under
# so a long session never produces an oversized 422-bound payload.
_MAX_ACTIVITIES = 200
_HTTP_TIMEOUT_SECONDS = 8
_USER_AGENT = "nexus-session-capture-hook/0.4"
_SUMMARY_CAP = 200  # max chars of a command / path summary
_USER_TEXT_CAP = 500  # max chars of a captured user message

# Bash command substring -> run_test classification markers.
_TEST_MARKERS = ("pytest", "jest", "go test", "npm test", "vitest")

# Non-mutating / generic tools that collapse to a single coarse action.
_AGENT_ACTION_TOOLS = frozenset(
    {"Grep", "Glob", "Task", "WebFetch", "WebSearch", "TodoWrite", "NotebookEdit"}
)


def _normalize_slug(text):
    """Normalize a path basename into a project slug (lowercase, safe chars)."""
    slug = "".join(c if (c.isalnum() or c in "-_") else "-" for c in text.strip().lower())
    slug = slug.strip("-")
    return slug or "default"


def _project_slug(cwd):
    """Derive the project slug: git toplevel basename, else cwd basename.

    IDENTICAL to session_inject._project_slug so the write side lands episodic
    memory on the same user_id the read side queries.
    """
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
        if not branch or branch == "HEAD":  # detached HEAD -> no branch
            return None
        return branch
    except Exception:
        return None


def _truncate(text, cap):
    """Coerce to a stripped string capped at `cap` chars."""
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception:
            return ""
    text = text.strip()
    return text[:cap] if len(text) > cap else text


def _classify_tool(tool, tool_input):
    """Map an assistant tool_use to an (action, summary) pair.

    summary is a short human string (file path or command head) stored under a
    tool-appropriate key in activity_data by the caller.
    """
    ti = tool_input if isinstance(tool_input, dict) else {}
    if tool == "Edit":
        return "edit_file", _truncate(ti.get("file_path", ""), _SUMMARY_CAP)
    if tool == "Write":
        return "create_file", _truncate(ti.get("file_path", ""), _SUMMARY_CAP)
    if tool == "Read":
        return "read_file", _truncate(ti.get("file_path", ""), _SUMMARY_CAP)
    if tool == "Bash":
        command = ti.get("command", "")
        cmd_lc = command.lower() if isinstance(command, str) else ""
        summary = _truncate(command, _SUMMARY_CAP)
        if "git commit" in cmd_lc:
            return "commit", summary
        if any(marker in cmd_lc for marker in _TEST_MARKERS):
            return "run_test", summary
        return "command_run", summary
    if tool in _AGENT_ACTION_TOOLS:
        return "agent_action", _truncate(tool or "", _SUMMARY_CAP)
    # Unknown / unmapped tool -> coarse agent_action (it is still an agent op).
    return "agent_action", _truncate(tool or "", _SUMMARY_CAP)


def _message_content(entry):
    """Pull the `content` out of a transcript entry, tolerating shape variants.

    Claude Code transcripts wrap the model/user message under a `message` key:
      {"type": "assistant", "message": {"role": ..., "content": [...] | "str"}}
    Some variants put `content` at the top level. Returns the content (list|str)
    or None.
    """
    msg = entry.get("message")
    if isinstance(msg, dict) and "content" in msg:
        return msg.get("content")
    if "content" in entry:
        return entry.get("content")
    return None


def _extract_from_entry(entry):
    """Yield (action, activity_data_partial) tuples from one transcript entry.

    activity_data_partial holds only the semantic fields (tool/summary/text);
    provenance (container_id/branch/session_id) is injected by the caller so it
    is uniform across every activity.
    """
    if not isinstance(entry, dict):
        return
    role = entry.get("type") or entry.get("role")
    content = _message_content(entry)

    if role == "assistant":
        # content is normally a list of blocks; a tool_use block carries name+input.
        blocks = content if isinstance(content, list) else []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "tool_use":
                tool = block.get("name", "")
                action, summary = _classify_tool(tool, block.get("input"))
                ad = {"tool": tool}
                if summary:
                    ad["summary"] = summary
                yield action, ad
        return

    if role == "user":
        # content may be a plain string or a list of blocks; capture the text.
        text = None
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(str(block.get("text", "")))
                elif isinstance(block, str):
                    parts.append(block)
            text = " ".join(p for p in parts if p)
        text = _truncate(text or "", _USER_TEXT_CAP)
        if text:
            yield "user_message", {"text": text}
        return

    # Any other entry type is not a capturable activity (tool_result noise, etc.).
    return


def _parse_transcript(path):
    """Parse a JSONL transcript into a list of (action, activity_data_partial).

    Defensive: each line is JSON-decoded independently; a bad/blank/non-dict line
    is skipped, never fatal. Returns the most-recent _MAX_ACTIVITIES.
    """
    extracted = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except Exception:
                continue  # malformed line -> skip
            for action, ad in _extract_from_entry(entry):
                extracted.append((action, ad))
    # Bound to the most-recent activities (tail of the session).
    if len(extracted) > _MAX_ACTIVITIES:
        extracted = extracted[-_MAX_ACTIVITIES:]
    return extracted


def _build_activities(extracted, container_id, branch, session_id):
    """Wrap extracted (action, partial) pairs into ActivityItem dicts with
    uniform provenance injected into each activity_data."""
    activities = []
    for action, partial in extracted:
        ad = dict(partial)
        ad["container_id"] = container_id
        if branch:
            ad["branch"] = branch
        if session_id:
            ad["session_id"] = session_id
        item = {"action": action, "activity_data": ad}
        if session_id:
            item["session_id"] = session_id
        activities.append(item)
    return activities


def _post(base_url, token, agent_id, activities):
    """POST the ActivityStreamRequest to /activities/stream. Raises on failure
    (caller is wrapped in fail-open)."""
    body = {"agent_id": agent_id, "activities": activities}
    data = json.dumps(body).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        # CF 1010 Bot Fight Mode blocks UA-less requests through the proxy.
        "User-Agent": _USER_AGENT,
        "X-Nexus-Source": "session-capture-hook",
    }
    if token:
        headers["X-API-Key"] = token
    req = urllib.request.Request(
        f"{base_url}/activities/stream", data=data, method="POST", headers=headers
    )
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
        resp.read()  # drain; we do not need the body


def main():
    raw = sys.stdin.read()
    event = json.loads(raw) if raw.strip() else {}
    if not isinstance(event, dict):
        return  # malformed -> fail-open silent

    base_url = os.environ.get("NEXUS_API_URL", "").rstrip("/")
    if not base_url:
        return  # no backend configured -> fail-open silent

    transcript_path = event.get("transcript_path")
    if not transcript_path or not os.path.isfile(transcript_path):
        return  # nothing to capture -> fail-open silent

    token = os.environ.get("NEXUS_API_TOKEN", "")
    cwd = event.get("cwd") or os.getcwd()
    agent_id = os.environ.get("NEXUS_DEFAULT_USER_ID") or _project_slug(cwd)
    container_id = os.environ.get("NEXUS_CONTAINER_ID") or socket.gethostname()
    branch = _current_branch(cwd)
    session_id = event.get("session_id")

    extracted = _parse_transcript(transcript_path)
    activities = _build_activities(extracted, container_id, branch, session_id)
    if not activities:
        return  # nothing to send -> no request, fail-open silent

    _post(base_url, token, agent_id, activities)
    # SessionEnd injects no context -> no stdout on success.


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session teardown over activity capture.
        pass
    sys.exit(0)
