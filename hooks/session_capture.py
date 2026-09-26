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
  - P0 LOW-SIGNAL SOURCE FILTER (``_is_low_signal``): low-signal activities are
    SKIPPED before extraction so the backend LLM extractor never sees them. C0d
    (``docs/qa/nexus-replace-claude-mem-c0c-extraction-quality.md``) measured 88%
    hallucination when low-signal activities (bare Read / Grep / ls / git status)
    were extracted — glm-4-flash invented content — dragging the whole quality
    gate below threshold while high-signal segments scored 4.45/4.73 with 0
    hallucination. Dropped: ``read_file`` / ``agent_action`` (Read/Grep/Glob/Task)
    and read-only ``command_run`` (ls/cat/pwd/which/echo/head/tail/tree/less/
    stat/file/wc + ``git status|log|diff|show|branch|remote|rev-parse``). Any
    write redirection (``>``/``>>``), pipe (``|``), command chaining/control op
    (``&&``/``||``/``;``/``&``), or command substitution (`` ` ``/``$(``) — e.g.
    ``cat > out.txt`` / ``tee`` / ``ls && rm -rf dist`` — is conservatively NOT
    read-only and is kept (a whitelisted head says nothing about a chained
    second command); ``find`` is excluded from the head whitelist entirely
    (``find . -delete`` mutates). High-signal kept:
    user_message / commit / run_test / edit_file / create_file / delete_file /
    non-read-only command_run (build/deploy/migration). The filter is fail-open:
    a predicate error keeps the activity rather than aborting capture.
  - Activity extraction, bounded to ``_MAX_ACTIVITIES`` by a TIERED selection
    (``select_activities``: high-value actions first, then command_run with a
    write marker, then the rest; the tier that overflows keeps both its ends;
    session order preserved -- not the most-recent tail, which threw away the
    head of every long session). The ActivityStreamRequest schema caps at
    1000. What the cap dropped rides on every activity as
    ``activity_data["internal"]["capture_dropped"]`` ({total, by_action,
    strategy}; ``internal`` is the key the backend keeps out of the LLM
    prompt). If the selector itself raises, the run falls back to the plain
    tail and is recorded / reported as ``capture_tiering_degraded``.
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
    basename of the git toplevel (or cwd) — the SAME call session_inject.py
    makes (``_identity.user_id``), so the captured episodic memories land on the
    user_id=project that the read side queries. Both files used to carry a
    byte-identical copy of the derivation instead.
  - provenance: every activity_data is augmented with container_id
    (``_identity.container_id``: NEXUS_CONTAINER_ID, else hostname) + branch
    (``git -C cwd rev-parse --abbrev-ref HEAD``, omitted on failure) + session_id.
  - HTTP headers MUST include a User-Agent (CF 1010 Bot Fight Mode blocks UA-less
    requests through the proxy), plus X-API-Key (if token present), Content-Type,
    and X-Nexus-Source: session-capture-hook/<plugin version>. ~8s timeout.
    fail-open.
  - Empty activity list -> no request sent. SessionEnd never injects context, so
    success produces NO stdout.
  - RUN LEDGER: every run the process survives appends one record (ok / reason
    / elapsed_ms / calls) via ``_hook_state.record_run`` — the fail-open paths
    above included, since "exit 0 with no stdout" is exactly what a capture hook
    that silently stopped working looks like from the outside. A hook the host
    kills leaves nothing, so the work runs against a deadline of its own
    (``_WORK_BUDGET_SECONDS``, recorded as ``timeout``) and the ledger write is
    capped (``_LEDGER_BUDGET_SECONDS``). If ``_hook_state`` cannot be imported
    (it needs ``fcntl``) the hook still captures and says so on stderr.
  - A 2xx is not an acknowledgement: the response must be the API's JSON with a
    positive integer ``accepted``, else ``http_error``. And zero activities is
    only ``nothing_to_do`` when the transcript was readable; a transcript that
    will not open, parses to nothing, or is long and holds no messages is
    ``file_unparsable`` — its format belongs to Claude Code, not to this plugin.
  - hooks.json gives this hook ``timeout: 60``. That is not slack: SessionEnd
    hooks share a 1.5 s budget unless one declares a longer timeout, and this
    hook's own HTTP timeout is 8 s.
"""

import collections
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

# Siblings are imported by name, which only works while this file's directory
# is on sys.path. PYTHONSAFEPATH=1 / `python -P` (3.11+) takes it off, and this
# hook was one self-contained file until TASK-002 -- so put it back rather than
# let an interpreter setting switch the plugin off.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import _identity
except Exception as exc:  # a broken or partial install
    # Imported by a test this must stay loud. Run as a hook it must not be:
    # a traceback is exit 1, reported as a hook error on every session end.
    if __name__ != "__main__":
        raise
    print(f"[session-capture] cannot import _identity: {exc!r}", file=sys.stderr)
    sys.exit(0)

try:
    import _hook_state
except Exception as exc:  # e.g. no fcntl on a native Windows Python
    # The ledger is bookkeeping. Losing it must not take the capture with it.
    _hook_state = None
    _LEDGER_IMPORT_ERROR = repr(exc)

HOOK = "session-capture"  # names the ledger file

# The name half of X-Nexus-Source. The backend attributes a request by this
# exact string against an allowlist (nexus `mcp_attribution._KNOWN_CLIENTS`);
# renaming it here sends every capture to source="unknown".
SOURCE_NAME = "session-capture-hook"

# Bounded capture: at most _MAX_ACTIVITIES extracted activities, chosen by
# select_activities (tiered; see the capture budget section) -- no longer the
# most-recent tail. The backend ActivityStreamRequest caps at 1000; we stay
# well under so a long session never produces an oversized 422-bound payload.
# Also imported by nexus:scripts/replay_session_capture.py as the "OLD" side
# of its before/after comparison: renaming it breaks that script.
_MAX_ACTIVITIES = 200
_HTTP_TIMEOUT_SECONDS = 8
# How long the ledger write may hold up the exit. Normally it takes
# milliseconds; this is the cap for when it does not (see _record).
_LEDGER_BUDGET_SECONDS = 2.0
# The hook's own deadline for everything before the ledger (see main). It has
# to clear the nominal worst case -- two 5 s git calls and the POST -- and, with
# the ledger budget, stay under the host's timeout.
_WORK_BUDGET_SECONDS = 20.0
# A transcript this long with not one user / assistant entry in it is not a
# quiet session, it is a format this parser no longer understands. Below the
# threshold it is just a session that was opened and closed.
_SHAPE_SUSPECT_MIN_LINES = 20
_USER_AGENT = "nexus-session-capture-hook/0.4"
_SUMMARY_CAP = 200  # max chars of a command / path summary
_USER_TEXT_CAP = 500  # max chars of a captured user message

# Bash command substring -> run_test classification markers.
_TEST_MARKERS = ("pytest", "jest", "go test", "npm test", "vitest")

# Non-mutating / generic tools that collapse to a single coarse action.
# NOTE: NotebookEdit is deliberately NOT here — it MUTATES a notebook (real
# work), so it maps to edit_file (high-signal, kept), not agent_action (dropped).
_AGENT_ACTION_TOOLS = frozenset(
    {"Grep", "Glob", "Task", "WebFetch", "WebSearch", "TodoWrite"}
)

# ── P0 low-signal source filter (C0d evidence) ──────────────────────────────────
# C0d (docs/qa/nexus-replace-claude-mem-c0c-extraction-quality.md) measured 88%
# hallucination when low-signal (navigation/search/read-only) activities were fed
# to the LLM extractor (glm-4-flash invented content for bare read_file/ls/grep),
# dragging the whole quality gate below threshold while high-signal segments
# scored 4.45/4.73 with 0 hallucination. P0 root-cause fix: SKIP low-signal
# activities at the source — before they ever reach POST /v1/activities/stream
# (and thus the extractor) — so only high-signal work is captured.

# Actions that are pure navigation / search -> always low-signal (drop).
_LOW_SIGNAL_ACTIONS = frozenset({"read_file", "agent_action"})

# Read-only command first-words: running these mutates nothing.
# NOTE: `find` is intentionally NOT in this set — `find . -delete` /
# `find . -exec rm {} +` mutate the filesystem, and gating it on flag inspection
# is fragile; dropping `find` entirely is the safe choice (it is low-value
# navigation, not worth the false-drop-a-write risk). See _is_readonly_command.
_READONLY_CMD_HEADS = frozenset(
    {"ls", "cat", "pwd", "which", "echo", "head", "tail",
     "tree", "less", "stat", "file", "wc"}
)

# Read-only `git <sub>` subcommands.
_READONLY_GIT_SUBS = frozenset(
    {"status", "log", "diff", "show", "branch", "remote", "rev-parse"}
)

# Shell metacharacters that can chain / substitute / background a SECOND command.
# A read-only head says nothing about what follows `ls && rm -rf dist` — the
# mutating half would be silently dropped. Any of these disqualifies the whole
# command from being treated as read-only (conservative: keep it).
_CHAIN_OR_REDIRECT_OPS = (">", "|", "&", ";", "`", "$(", "\n")


def _is_readonly_command(command):
    """True iff `command` is a read-only shell command (mutates nothing).

    Conservative on every axis — we would rather over-capture (keep) than drop a
    command with a write side-effect:

      * Any write redirection (`>`, `>>`), pipe (`|`), command chaining/control
        operator (`&&`, `||`, `;`, background `&`), command substitution
        (backtick, `$(`), or embedded newline disqualifies the command. A
        whitelisted head says NOTHING about a chained second command —
        `ls && rm -rf dist` / `cat a; alembic upgrade head` must be KEPT, not
        skipped on the strength of the read-only head alone.
      * `tee` writes its stdin to a file -> not read-only.
      * Unknown heads default to NOT read-only (kept).
    """
    if not isinstance(command, str):
        return False
    # Any chaining / redirection / substitution could hide a write -> not read-only.
    if any(op in command for op in _CHAIN_OR_REDIRECT_OPS):
        return False
    tokens = command.split()
    if not tokens:
        return False
    head = tokens[0]
    if head == "git":
        # `git <sub> ...` — read-only only for the whitelisted subcommands.
        sub = tokens[1] if len(tokens) > 1 else ""
        return sub in _READONLY_GIT_SUBS
    if head == "tee":  # `tee` writes its stdin to a file -> not read-only.
        return False
    return head in _READONLY_CMD_HEADS


def _is_low_signal(action, activity_data):
    """True iff an activity is low-signal and should be SKIPPED before capture.

    Low-signal (C0d-confirmed extractor noise):
      - read_file / agent_action (Read / Grep / Glob / Task: pure nav/search).
      - command_run whose command is read-only (see _is_readonly_command).
    High-signal (kept): user_message / commit / run_test / edit_file /
    create_file / delete_file / non-read-only command_run (build/deploy/migration).
    """
    if action in _LOW_SIGNAL_ACTIONS:
        return True
    if action == "command_run":
        ad = activity_data if isinstance(activity_data, dict) else {}
        return _is_readonly_command(ad.get("summary", ""))
    return False


# ── Capture budget: tiered selection when the pool exceeds _MAX_ACTIVITIES ──────
# (OpenSpec change session-capture-priority-truncation, plugin#28.) The budget
# itself is right -- the backend runs one LLM extraction per activity, so the
# cap is a cost line -- but truncating to the TAIL (`extracted[-200:]`) threw
# away the head of the session, and the head is where the intent lives. Offline
# replay of 20 real transcripts: high-value activities dropped 426 -> 15.
#
# Three tiers, collected in order. The first tier that does not fit keeps BOTH
# ENDS of itself (the same shape as the aggregator's own "keep the ends, elide
# the middle" roll-up), and the survivors are merged back into session order.

# Tier 1. NOT the same set as the "High-signal (kept)" list in _is_low_signal's
# docstring: that list is what survives the source filter, and it INCLUDES
# every non-read-only command_run. This one is what the budget protects first,
# and command_run is never in it -- a command_run that provably writes is
# tier 2 (_WRITE_MARKERS), everything else is tier 3. _LOW_SIGNAL_ACTIONS
# (above) is the far end of the same spectrum: dropped before any budget
# applies. delete_file is listed for the day _classify_tool emits it; today
# nothing does (I-D), so no fixture may be built on it. Also imported by
# nexus:scripts/replay_session_capture.py (its high-value drop counts are
# computed over this set): renaming it breaks that script.
_HIGH_VALUE_ACTIONS = frozenset(
    {"user_message", "commit", "run_test", "create_file", "edit_file", "delete_file"}
)

# Tier 2: a command_run whose command PROVABLY has a write side effect. This is
# a POSITIVE table on purpose -- `not _is_readonly_command` is the wrong
# predicate: what reaches the selector is "could not be proven read-only"
# (unknown heads, anything piped or chained), i.e. every surviving
# command_run, and tier 3 would be empty. Key = tokens[0]; None = the bare
# head is enough (rm / mv / cp / mkdir / chmod); a set = tokens[1] must be in
# it. Only what tokens[1] can tell apart is listed: `nomad job run` and
# `nomad job status` share tokens[1], and bare `alembic current` / `heads` are
# read-only. A head missing from the table lands in tier 3 = exactly the old
# behaviour (nothing regresses, it just is not rescued). An over-match is not
# free either: when the pool exceeds the limit every wrong tier-2 entry takes
# one tier-3 slot, and tier 3 holds real writes hiding behind a chain
# (`cd deploy && git push`). Maintenance: extend it when a toolchain appears;
# git add / stash / pull / fetch are deliberately not listed yet.
_WRITE_MARKERS = {
    "rm": None,
    "mv": None,
    "cp": None,
    "mkdir": None,
    "chmod": None,
    "git": frozenset({"push", "merge", "rebase", "reset", "checkout"}),
    "alembic": frozenset({"upgrade", "downgrade", "revision", "stamp"}),
    "docker": frozenset({"push", "build"}),
    "npm": frozenset({"publish"}),
}


def _has_write_marker(command):
    """True iff `command` opens with a head (and, where the table says so, a
    subcommand) from _WRITE_MARKERS. Only tokens[0] and tokens[1] are looked
    at -- a write behind `&&` or `sudo` is invisible here and stays tier 3."""
    if not isinstance(command, str):
        return False
    tokens = command.split()
    if not tokens or tokens[0] not in _WRITE_MARKERS:
        return False
    subcommands = _WRITE_MARKERS[tokens[0]]
    if subcommands is None:
        return True
    return len(tokens) > 1 and tokens[1] in subcommands


def _tier(action, activity_data):
    """1 = protected, 2 = provable write, 3 = everything else (unenumerated)."""
    if action in _HIGH_VALUE_ACTIONS:
        return 1
    if action == "command_run" and isinstance(activity_data, dict):
        # `summary` is optional by construction (_extract_from_entry sets it
        # only when non-empty), hence the membership test.
        summary = activity_data["summary"] if "summary" in activity_data else ""
        if _has_write_marker(summary):
            return 2
    return 3


def select_activities(extracted, limit=_MAX_ACTIVITIES):
    """Choose at most `limit` of `extracted`, protecting the high-value tiers.

    Returns ``(selected, strategy, dropped_by_action)``:

    * ``selected`` -- elements of ``extracted`` (the same objects, in the same
      ``(action, activity_data)`` shape), in their ORIGINAL order. When
      ``len(extracted) <= limit`` it is ``extracted`` itself, untouched: that
      path carries most real deliveries and must not re-order anything.
    * ``strategy`` -- ``"layered"``, or ``"degenerate"`` iff tier 1 alone
      exceeds ``limit``. A lower tier keeping both ends does NOT change it.
      This function never returns ``"fallback_tail"``: that value is written
      by _parse_transcript when this function fails (fail-open).
    * ``dropped_by_action`` -- ``{action: count}`` of what was not selected;
      ``{}`` when nothing was.

    Tiers (_HIGH_VALUE_ACTIONS / _WRITE_MARKERS): 1 = high-value actions,
    2 = command_run with a write marker, 3 = the rest. Collected in tier
    order; the first tier that does not fit keeps ``remaining // 2`` from
    its front and the rest from its back (both ends, middle dropped -- an odd
    remainder still fills the budget exactly), and once the budget is spent
    nothing further is taken. A remainder of 0 is an explicit empty
    selection, never a slice: ``tier[-0:]`` is the whole tier (I-B).
    ``limit`` is expected to be >= 1 (the hook passes _MAX_ACTIVITIES); with
    ``limit <= 0`` nothing is selected and the strategy stays ``layered``.

    ORDER IS LOAD-BEARING DOWNSTREAM. The result is a NON-CONTIGUOUS
    subsequence of the session -- there are holes where the middle of a tier
    was dropped -- but what is kept stays in session order, and the backend
    depends on that: it processes the POSTed list serially, one LLM call per
    activity (nexus ``workers/activity_processor.py:697``, the
    ``enumerate(activity_ids)`` loop in ``process_activity_batch``), so
    ``Memory.created_at`` ends up strictly increasing, and
    ``workers/session_aggregator.py:287`` (``_collect_observations``) orders
    by it to build the "what it set out to do / what it concluded" roll-up.
    Parallelising that loop would silently break this without touching any
    file named here.

    Public surface: imported across repos by
    ``nexus:scripts/replay_session_capture.py`` (together with
    ``read_activities`` and ``_MAX_ACTIVITIES``). Renaming or re-shaping it
    means updating that script in the same change.
    """
    if len(extracted) <= limit:
        return extracted, "layered", {}

    tiers = {1: [], 2: [], 3: []}
    for index, item in enumerate(extracted):
        action, activity_data = item
        tiers[_tier(action, activity_data)].append((index, item))

    chosen = []
    strategy = "layered"
    remaining = limit
    for tier in (1, 2, 3):
        if remaining <= 0:
            break  # I-B: an explicit stop, never a slice with a 0 or negative bound
        members = tiers[tier]
        if len(members) <= remaining:
            chosen.extend(members)
            remaining -= len(members)
            continue
        head = remaining // 2
        tail = remaining - head  # >= 1 here (remaining >= 1), so the back slice is real
        chosen.extend(members[:head])
        chosen.extend(members[len(members) - tail:])  # head + tail < len: no overlap
        remaining = 0
        if tier == 1:
            strategy = "degenerate"

    chosen.sort(key=lambda pair: pair[0])
    kept = {index for index, _ in chosen}
    dropped_by_action = dict(collections.Counter(
        action for index, (action, _) in enumerate(extracted) if index not in kept
    ))
    return [item for _, item in chosen], strategy, dropped_by_action


class _BadResponse(Exception):
    """A 2xx that is not the API's acknowledgement."""


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
    if tool == "NotebookEdit":  # mutates a notebook → edit_file (high-signal, kept)
        return "edit_file", _truncate(ti.get("notebook_path", ""), _SUMMARY_CAP)
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


def read_activities(path):
    """Read a JSONL transcript into its FULL activity pool. Returns ``(full, stats)``.

    ``full`` is every (action, activity_data_partial) pair that survives the
    low-signal source filter, in session order and with NO cap -- the cap is
    _parse_transcript's job (select_activities). Defensive: each line is
    JSON-decoded independently; a bad/blank/non-dict line is skipped, never
    fatal. An unreadable FILE, on the other hand, raises: this function is
    deliberately wrapped in no try/except, because its OSError is the one
    path by which _collect tells ``file_unparsable`` (a reported failure)
    from ``nothing_to_do`` (a quiet skip).

    ``stats`` counts non-blank ``lines``, lines that ``parsed`` as JSON and
    ``messages`` (user / assistant entries) -- what _unreadable reads -- and
    carries ``tiering_error`` (None here; _parse_transcript fills it with an
    exception class name when the selector fails). Downstream reads these
    keys by index, so every key is present from the start.

    Public surface, no underscore on purpose: imported across repos by
    nexus:scripts/replay_session_capture.py (with select_activities and
    _MAX_ACTIVITIES) so the offline replay runs the production reader rather
    than a copy of it. Renaming or re-shaping it means updating that script.
    """
    full = []
    stats = {"lines": 0, "parsed": 0, "messages": 0, "tiering_error": None}
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            stats["lines"] += 1
            try:
                entry = json.loads(line)
            except Exception:
                continue  # malformed line -> skip
            stats["parsed"] += 1
            if isinstance(entry, dict) and (entry.get("type") or entry.get("role")) in ("user", "assistant"):
                stats["messages"] += 1
            for action, ad in _extract_from_entry(entry):
                # P0: drop low-signal activities at the source so the LLM
                # extractor never sees navigation/search/read-only noise (C0d:
                # 88% hallucination on low-signal). fail-open: a buggy predicate
                # must not abort capture -> on error, KEEP the activity.
                try:
                    if _is_low_signal(action, ad):
                        continue
                except Exception:
                    pass
                full.append((action, ad))
    return full, stats


def _count_actions(items):
    """``{action: count}`` over (action, activity_data) pairs; ``{}`` for none."""
    return dict(collections.Counter(action for action, _ in items))


def _parse_transcript(path):
    """Parse a JSONL transcript. Returns ``(extracted, stats)``.

    ``extracted`` is at most _MAX_ACTIVITIES (action, activity_data_partial)
    pairs chosen by select_activities: the high-value tiers protected, both
    ends kept when a tier overflows, session order preserved. ``stats`` is
    read_activities's dict plus ``dropped`` = {total, by_action, strategy}:
    ``total`` counts what the CAP dropped -- len(full) - len(extracted); the
    pool is already past _is_low_signal, so source-filter drops are never in
    it -- ``by_action`` sums to ``total`` and is ``{}`` when that is 0, and
    ``strategy`` is one of ``layered`` / ``degenerate`` / ``fallback_tail``.
    Three values, final here. The wire payload has a fourth,
    ``telemetry_failed``, written only by _build_activities's own fallback;
    this dict never holds it.

    Fail-open, block 1: the selector is new logic between the reader and the
    POST, and a bug in it must not cost the capture. If it raises, the run
    falls back to the plain tail (the pre-tiering behaviour, byte for byte),
    ``dropped`` is recomputed for that tail so the builder still reads a
    complete dict (block 3), and ``stats["tiering_error"]`` carries the
    exception class name -- outside ``dropped``, which is copied onto the
    wire whole. The reader call itself stays OUTSIDE the try: see
    read_activities.
    """
    full, stats = read_activities(path)
    try:
        extracted, strategy, by_action = select_activities(full)
    except Exception as exc:  # fail-open block 1: never lose the capture to the selector
        extracted = full[-_MAX_ACTIVITIES:]
        strategy = "fallback_tail"
        stats["tiering_error"] = type(exc).__name__
        try:
            by_action = _count_actions(full[:len(full) - len(extracted)])
        except Exception:
            # The pool itself is malformed -- unreachable from read_activities,
            # which only ever appends pairs. The breakdown is bookkeeping;
            # the upload is not. `total` below still counts.
            by_action = {}
    stats["dropped"] = {
        "total": len(full) - len(extracted),  # cap drops only: the pool is past _is_low_signal
        "by_action": by_action,
        "strategy": strategy,
    }
    return extracted, stats


def _unreadable(stats):
    """True when zero activities means "could not read it", not "nothing there".

    The transcript format belongs to Claude Code. If it changes, every session
    parses to zero activities, and recording that as `nothing_to_do` -- an
    expected skip, never reported -- would stop capture for good without a
    word. Same split as empty_sections / sections_unparsed.
    """
    if stats["lines"] and not stats["parsed"]:
        return True
    return stats["parsed"] >= _SHAPE_SUSPECT_MIN_LINES and not stats["messages"]


def _capture_dropped_payload(dropped):
    """The per-activity telemetry: what the capture budget dropped this run.

    A fresh dict per call -- the caller injects one copy per activity on
    purpose (see _build_activities), and no two rows may share an object.
    Read by index: stats["dropped"] always carries all three keys, so a
    missing one is a bug here, not something to paper over.
    """
    return {
        "total": dropped["total"],
        "by_action": dict(dropped["by_action"]),
        "strategy": dropped["strategy"],
    }


def _build_activities(extracted, container_id, branch, session_id, dropped):
    """Wrap extracted (action, partial) pairs into ActivityItem dicts with
    uniform provenance injected into each activity_data, plus the capture
    budget's telemetry under ``activity_data["internal"]["capture_dropped"]``.

    ``internal`` is the one key the backend keeps out of the LLM prompt
    (nexus workers/activity_processor.py:192 in ``_format_activity_log``,
    utils/formatters.py:138 in ``format_memory``); every other activity_data
    key is rendered "key: value" into it, and a bare "total: 63" reads like
    a fact to extract. The whole activity_data is also copied into
    memories.metadata and served by GET /v1/memories and
    POST /v1/context/retrieve, which is why the key's shape is a contract --
    recorded in nexus docs/architecture/memory-layers.md §4 by this change's
    TASK-003, which also bumps the gitlink to the commit that ships it.
    """
    activities = []
    for action, partial in extracted:
        ad = dict(partial)
        ad["container_id"] = container_id
        if branch:
            ad["branch"] = branch
        if session_id:
            ad["session_id"] = session_id
        # Injected on EVERY activity, and rebuilt on every iteration rather
        # than built once and shared (owner ruling 2): a row-level query must
        # read the run's drop count off whichever row it lands on -- "which
        # row was first" stops being answerable once a session is re-sent --
        # and a shared object would let one row's mutation leak into all.
        try:
            payload = _capture_dropped_payload(dropped)
        except Exception:  # fail-open block 2: telemetry must never cost the upload
            # Selection was fine and the run IS a success: no reason, no
            # extra, no local trace (owner-acknowledged, Amendment A1-16).
            # The only trace is strategy=telemetry_failed on the wire -- the
            # key is still written, so a MISSING key keeps meaning "a client
            # older than this" and nothing else.
            try:
                total = dropped["total"]
            except Exception:
                total = None
            payload = {"total": total, "by_action": {}, "strategy": "telemetry_failed"}
        ad["internal"] = {"capture_dropped": payload}
        item = {"action": action, "activity_data": ad}
        if session_id:
            item["session_id"] = session_id
        activities.append(item)
    return activities


def _post(base_url, token, agent_id, activities):
    """POST the ActivityStreamRequest to /activities/stream. Returns how many
    activities the backend acknowledged; raises on failure (caller is wrapped in
    fail-open).

    The response used to be drained and ignored, so ANY 2xx counted as a
    capture. Reproduced: a POST answered with a 302 to a login page is re-issued
    by urllib as a GET, comes back 200 text/html, and was recorded as a clean
    run with nothing captured. The backend's answer is 201 with an integer
    `accepted`; anything else is not an acknowledgement.
    """
    body = {"agent_id": agent_id, "activities": activities}
    data = json.dumps(body).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        # CF 1010 Bot Fight Mode blocks UA-less requests through the proxy.
        "User-Agent": _USER_AGENT,
        "X-Nexus-Source": _identity.source_header(SOURCE_NAME),
    }
    if token:
        headers["X-API-Key"] = token
    req = urllib.request.Request(
        f"{base_url}/activities/stream", data=data, method="POST", headers=headers
    )
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
        raw = resp.read()
    try:
        answer = json.loads(raw)
    except ValueError as exc:
        raise _BadResponse(f"body is not JSON: {exc}") from exc
    accepted = answer.get("accepted") if isinstance(answer, dict) else None
    if type(accepted) is not int or accepted <= 0:  # `type is`, so True is not 1
        raise _BadResponse(f"no activities acknowledged (accepted={accepted!r})")
    return accepted


def _collect(run):
    """Do the work. Returns the reason; raises if the remote call fails.

    ``run`` is filled in as facts become known, so that whatever happens next
    -- a return or an exception -- the ledger record has the right project and
    the number of calls actually attempted.
    """
    raw = sys.stdin.read()
    event = json.loads(raw) if raw.strip() else {}
    if not isinstance(event, dict):
        raise ValueError("SessionEnd payload is not a JSON object")
    if isinstance(event.get("cwd"), str) and event["cwd"]:
        run["cwd"] = event["cwd"]
    cwd = run["cwd"] or os.getcwd()

    base_url = os.environ.get("NEXUS_API_URL", "").rstrip("/")
    if not base_url:
        return "not_configured"  # the default for a fresh install

    transcript_path = event.get("transcript_path")
    if not transcript_path or not os.path.isfile(transcript_path):
        return "nothing_to_do"  # nothing to capture

    token = os.environ.get("NEXUS_API_TOKEN", "")
    agent_id = _identity.user_id(cwd)
    container_id = _identity.container_id()
    branch = _current_branch(cwd)
    session_id = event.get("session_id")

    try:
        extracted, stats = _parse_transcript(transcript_path)
    except OSError:
        # It is a file (checked above) and it would not open. Not a quiet session.
        return "file_unparsable"
    # (4a) The budget's health goes into the ledger extra right here -- before
    # the builder, the two early returns and the POST -- so a POST that fails
    # (or a builder that raises) cannot lose it. Two-valued like the wire key:
    # "ok" is written too, so a clean run is told apart from a run that never
    # got this far. Flattened into the ledger entry by record_run.
    run["extra"].update(
        tiering="ok" if stats["tiering_error"] is None else "degraded",
        exc=stats["tiering_error"],
    )
    activities = _build_activities(extracted, container_id, branch, session_id, stats["dropped"])
    run["extra"].update(activities=len(activities), lines=stats["lines"], parsed=stats["parsed"])
    if not activities:
        if _unreadable(stats):
            return "file_unparsable"  # a failure already: not folded, the detail is in the extra
        # Nothing to send -> no request. A degraded selection on an EMPTY pool
        # cannot happen on its own (the selector returns early below the
        # limit); it is reachable by patching, and is folded like the clean
        # run so the fold has no hole.
        return _with_tiering_verdict("nothing_to_do", stats)

    run["calls"] += 1  # counted before the call: a call that fails was still made
    run["extra"]["accepted"] = _post(base_url, token, agent_id, activities)
    # SessionEnd injects no context -> no stdout on success.
    return _with_tiering_verdict(_hook_state.NO_REASON if _hook_state else "none", stats)


def _with_tiering_verdict(reason, stats):
    """(4b) Fold a degraded selection into a reason that is not a failure yet.

    Called only at the two return points that are not failures already --
    ``nothing_to_do`` and the clean run -- so a run whose selector fell back
    is recorded with ``capture_tiering_degraded`` and reported at the next
    SessionStart while it is still the hook's latest ledger entry (the
    reporter reads only the last one; a clean run after it goes unreported
    -- owner ruling 4: the degradation is user-visible; the run's ledger
    entry says ok=false even though the upload succeeded).
    ``file_unparsable`` is not folded: it is a failure in
    its own right and the detail is in the extra. A POST that raises never
    reaches here: main() maps the exception (http_error wins) and the extra
    written in (4a) still says degraded.

    Failure-over-skip is worst_reason's structural rule (``if failures:``),
    not a property of its priority table -- the new reason is deliberately
    NOT in that table (see _hook_state._REASON_FLOOR). The ``if _hook_state``
    guard mirrors the clean-run return: without the ledger module there is
    no table to consult, and the literal is what worst_reason would pick.
    """
    if stats["tiering_error"] is None:
        return reason
    if _hook_state:
        return _hook_state.worst_reason([reason, "capture_tiering_degraded"])
    return "capture_tiering_degraded"


def _record(reason, started, run):
    """Append this run to the ledger, within a budget. Never raises.
    Returns True when the write had to be left behind.

    The write happens on a daemon thread that is abandoned after
    ``_LEDGER_BUDGET_SECONDS``. Catching exceptions is not enough: the ledger
    takes a blocking lock, and a write that STALLS keeps this process alive
    until the host's timeout kills it -- and the host only uses the stdout of a
    hook that exited 0, so the stall would cost the very output that was
    written first to keep it safe. An abandoned daemon thread dies with the
    interpreter.
    """
    if _hook_state is None:
        print(
            f"[{HOOK}] run ledger unavailable ({_LEDGER_IMPORT_ERROR}); "
            f"this run ({reason}) is not recorded",
            file=sys.stderr,
        )
        return False
    elapsed_ms = int((time.monotonic() - started) * 1000)  # the hook's work, not the wait below
    # Snapshot: if the work was abandoned, its thread may still be writing to `run`.
    calls, cwd, extra = run["calls"], run["cwd"], dict(run["extra"])

    def write():
        try:
            _hook_state.record_run(
                HOOK,
                ok=not _hook_state.is_failure_reason(reason),
                reason=reason,
                elapsed_ms=elapsed_ms,
                calls=calls,
                cwd=cwd,
                extra=extra or None,
            )
        except Exception as exc:  # record_run does not raise by contract; the net under it
            print(f"[{HOOK}] could not record this run ({reason}): {exc!r}", file=sys.stderr)

    worker = threading.Thread(target=write, name=f"{HOOK}-ledger", daemon=True)
    worker.start()
    worker.join(_LEDGER_BUDGET_SECONDS)
    if worker.is_alive():
        print(
            f"[{HOOK}] ledger write still running after {_LEDGER_BUDGET_SECONDS}s; "
            f"leaving it behind, this run ({reason}) may go unrecorded",
            file=sys.stderr,
        )
        return True
    return False


def main():
    """Run the hook. Returns True when a worker thread had to be left behind."""
    started = time.monotonic()
    run = {"cwd": None, "calls": 0, "extra": {}}
    outcome = {}

    def work():  # never prints: a thread that may be abandoned must stay off stdio
        try:
            outcome["result"] = _collect(run)
        except Exception as exc:
            outcome["error"] = exc

    # The work runs against a deadline of its own. urllib's timeout is per
    # socket operation, not per request -- one "6 s" request was measured at
    # 24 s against a server that drips bytes -- and a hook the host has to kill
    # leaves no record and, for SessionStart, no brief. So leave first.
    worker = threading.Thread(target=work, name=f"{HOOK}-work", daemon=True)
    worker.start()
    worker.join(_WORK_BUDGET_SECONDS)
    left_behind = worker.is_alive()

    reason = "unknown"
    if left_behind:
        reason = "timeout"
        print(
            f"[{HOOK}] {reason}: no result after {_WORK_BUDGET_SECONDS}s; leaving the work behind",
            file=sys.stderr,
        )
    elif "result" not in outcome:
        # Still exit 0 with no stdout -- but no longer without a trace.
        # `.get`: a worker that died of something `except Exception` does not
        # catch (SystemExit) leaves neither key. Indexing "result" below would
        # then raise out of main() into the blanket handler -- exit 0, no
        # record -- which is the one outcome this file is built to rule out.
        exc = outcome.get("error", RuntimeError("the worker ended without a result"))
        if isinstance(exc, _BadResponse):
            reason = "http_error"
        else:
            reason = _hook_state.reason_for_exception(exc) if _hook_state else "unknown"
        print(f"[{HOOK}] {reason}: {exc!r}", file=sys.stderr)
    else:
        reason = outcome["result"]
    return _record(reason, started, run) or left_behind


if __name__ == "__main__":
    left_behind = False
    try:
        left_behind = bool(main())
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session teardown over activity capture.
        pass
    if left_behind:
        # A daemon thread is still running. Ordinary interpreter shutdown can
        # die with "could not acquire lock for <stderr>" if that thread happens
        # to be printing at that instant -- a non-zero exit, which for
        # SessionStart costs the brief. Everything that matters has been
        # flushed, so leave without the ceremony.
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        os._exit(0)
    sys.exit(0)
