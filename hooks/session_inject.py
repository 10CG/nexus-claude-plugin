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
  - Three segments, each with its own try and timeout (workflow D, TASK-007;
    a failure in one segment never costs the others their brief):
    (1) episodes -- one ``GET /v1/memories?layer=session_summary&branch=&project=&limit=100``
    page, verified row by row (layer / branch / project; mismatches are
    dropped and counted as ``filter_suspect`` -- the CLIENT layer whitelist is
    gone, layers are decided server-side and CHECKED here), grouped by
    container: peers first (latest 2 per container, groups by their newest
    row, peers <= 4), own rows filling to 5. No peer rows in the page -> one
    look-up per state-remembered peer (<= 2). Still nothing -> the page is
    retried once WITHOUT branch (project-level, same rules). Handoff rows
    (``aria.source=handoff``) render first, the newest per container in full
    (title + pre-§2 head + §6, ~1500 chars) and older ones as one title line
    (A9-10); their age anchor is ``aria.updated_at``. Every other row's age
    is the response's top-level ``created_at`` (metadata keys are legacy
    fallback only).
    (2) fact delta -- ``GET /v1/memories?layer=fact&updated_after=<marker -
    10min>&project=&limit=100``, keyset-paged with ``before_created_at`` +
    ``before_id`` (byte-exact from the previous page's last row; NEVER
    offset -- ``updated_after`` + offset is a 422, #499), <= 5 pages, deduped
    by id, own container's rows dropped, M=10 by ``aria.modified`` desc,
    rendered as ``aria.description`` only. The marker advances ONLY on a
    cleanly-completed startup round (any page failure or truncation -> no
    advance; compact/resume -> no advance), is bound to user_id, and a first
    run / state loss / user change starts from now - 7 days with a
    systemMessage note.
    (3) semantic -- project-scoped ``POST /context/retrieve`` (query
    non-empty, ``layers=["semantic"]``, no graph/history/recent,
    ``metadata_filter={"project": slug}``), rows verified against the project;
    ``ai_profile`` is NEVER rendered (it rides include_profile, cannot be
    turned off, and is not project-scoped -- nexus PR #620). No derivable
    project -> the segment is not sent at all.
  - The three-state profile contract (nexus#613): ``profile: null`` PLUS
    ``errors.profile`` is a semantic-layer failure (reported); ``[]`` is an
    honest "nothing matched"; ``null`` without errors means not requested.
  - The whole brief is <= 4000 chars: episode-priority -- fact items are
    dropped whole first, then the episode tail goes.
  - HTTP headers MUST include a User-Agent (SPIKE #8: requests through the CF
    proxy with no UA are blocked by CF 1010 Bot Fight Mode), plus X-API-Key,
    Content-Type, and X-Nexus-Source: sessionstart-hook/<plugin version>.
  - RUN LEDGER: every run the process survives appends one record (ok / reason
    / elapsed_ms / calls) via ``_hook_state.record_run`` — the fail-open paths
    above included, since "exit 0 with no stdout" is exactly what a hook that
    silently stopped working looks like from the outside. A hook the host kills
    leaves nothing, and its stdout is discarded too; so this one is built to
    never need killing:
      * the work runs against a deadline of its own (``_WORK_BUDGET_SECONDS``)
        and records ``timeout`` — urllib's timeouts are per socket operation and
        do not bound a request;
      * the brief is written and flushed BEFORE the ledger is touched;
      * the ledger write is capped (``_LEDGER_BUDGET_SECONDS``): it takes a
        blocking lock, and ordering alone does not protect a brief from
        bookkeeping that stalls;
      * hooks.json ``timeout`` sits above deadline + cap, as a backstop.
    If ``_hook_state`` cannot be imported at all (it needs ``fcntl``) the hook
    still injects and says so on stderr.
  - FAILURE REPORT (TASK-003, workflow V(3)): this is the only user-visible
    channel any nexus hook has, so at every start it reads EVERY hook's ledger
    in this project's state dir and, when a hook's most recent run failed
    (a failure-class reason, ``unknown``, an unreadable ledger, or -- once a
    previous session has happened -- no ledger at all for a hook that should
    have run), says so: one line in ``systemMessage`` (shown to the user) and
    one line prepended to ``additionalContext`` (seen by Claude). Each finding
    is reported ONCE; the marker lives in this hook's own state file, keyed by
    hook, because an unreadable or missing ledger has no entry to mark. The
    report never blocks the brief and never changes the exit code.
  - A 200 is not an answer. The backend degrades gracefully: a failed memory
    lookup comes back 200 with ``profile: null`` and the error under ``errors``.
    That, a body that is not JSON, and JSON of the wrong shape are all
    ``http_error``; only an honest empty profile is ``nothing_to_do``.
  - Render: ONLY settled summaries -- ``session_summary`` rows (what the
    backend's session aggregator writes, and what its layer preference ranks
    first) ahead of ``summary`` rows (the claude-mem migration), each group in
    the order the backend returned it; if no profile item carries a ``layer``
    key at all, take all of them. Filtering on ``summary`` alone dropped every
    aggregated row (10CG/nexus-claude-plugin#32). Each line is prefixed
    ``[<container_id> · <age> · <branch>]`` provenance (§6 — guard the
    warm-start from half-finished observations + make cross-container origin
    legible). A migrated ``summary`` never had a branch and shows ``-``; a
    ``session_summary`` shows ``?`` for its age, because neither its metadata
    nor a profile row carries a timestamp (workflow D, TASK-007, moves to the
    list endpoint and its ``created_at``). No results -> no stdout (fail-open
    silent).
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

# Siblings are imported by name, which only works while this file's directory
# is on sys.path. PYTHONSAFEPATH=1 / `python -P` (3.11+) takes it off, and this
# hook was one self-contained file until TASK-002 -- so put it back rather than
# let an interpreter setting switch the plugin off.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)


def _import_warn(message):
    """Print one diagnostic line to stderr, never raising (A9-21 R1 fix
    round, finding C1).

    Used ONLY by the two import guards immediately below: at this point in
    the file ``_hook_runner`` may itself be the missing piece (one of these
    guards exists to report exactly that), so nothing here may depend on
    it -- in particular not ``_hook_runner.guard_stderr()``, which is what
    protects every OTHER stderr write in this file (installed at the top of
    ``main()``, before the work thread starts). Named distinctly from this
    file's own ``_warn(reason, exc)`` (defined further down, after these
    guards, with a different signature for the main-path diagnostics) --
    a same-named redefinition would silently shadow whichever one is
    defined first -- mirroring ``handoff_sync.py``'s identical local pair:
    Amendment A9-21 moved the ``_StderrGuard`` CLASS into ``_hook_runner``,
    but left each hook's own import-time ``_warn`` / ``_silence_stderr``
    local on purpose.

    ``sys.stderr is None`` (fd 2 closed before the interpreter even
    started) is checked first: a bare ``print(message, file=None)`` does
    not raise -- it silently FALLS BACK to ``sys.stdout``, which for THIS
    hook is the injected context itself, not an empty channel (confirmed
    empirically). There is no real file descriptor to redirect in this
    shape, so this simply skips the write.

    A closed stderr PIPE is a different shape (a live stream whose
    ``write`` raises ``BrokenPipeError``, an ``OSError`` subclass):
    swallowing that from this one call is not enough on its own, because
    CPython's own interpreter shutdown unconditionally reflushes stdout
    AND stderr again once this process is on its way out, retrying the
    SAME write against the SAME closed pipe with no Python-level ``except``
    left near it at that point -- exit code 120, not 0.
    ``_silence_stderr`` below reroutes the underlying descriptor to
    ``os.devnull`` so that retry lands somewhere that accepts anything.
    """
    if sys.stderr is None:
        return
    try:
        print(message, file=sys.stderr)
    except OSError:
        _silence_stderr()


def _silence_stderr():
    """After a failed write through ``_import_warn`` above, stop the
    interpreter retrying it on the way out.

    ``sys.stderr.fileno()`` is resolved BEFORE ``os.open``: the reverse
    order opened the devnull fd first, and a failing ``fileno()`` (a test
    double, or any future stderr replacement with no real descriptor)
    would then leave it dangling, swallowed by the blanket
    ``except Exception: pass`` below.
    """
    try:
        target_fd = sys.stderr.fileno()
        devnull = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(devnull, target_fd)
        finally:
            os.close(devnull)
    except Exception:
        pass  # not a real file descriptor (tests), or nothing left to protect


try:
    import _identity
except Exception as exc:  # a broken or partial install
    # Imported by a test this must stay loud. Run as a hook it must not be:
    # a traceback is exit 1, which Claude Code reports as a hook error on
    # every single session start.
    if __name__ != "__main__":
        raise
    _import_warn(f"[session-inject] cannot import _identity: {exc!r}")
    sys.exit(0)

try:
    import _hook_runner
except Exception as exc:  # a broken or partial install
    # Same contract as the _identity guard above: loud when imported by a
    # test, quiet exit(0) when run as a hook (a traceback here is exit 1,
    # which Claude Code reports as a hook error on every single session
    # start). main() cannot run its deadline / ledger-budget races without
    # this module, so it is treated as required, not as optional
    # bookkeeping like _hook_state below.
    if __name__ != "__main__":
        raise
    _import_warn(f"[session-inject] cannot import _hook_runner: {exc!r}")
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

# Per-request timeouts (workflow D, TASK-007). The episode page is the one
# expensive call (6 s); every other call -- peer look-ups, the project-level
# retry, each fact page, the semantic segment -- gets 3 s. Worst case stays
# under _WORK_BUDGET_SECONDS, and run_with_deadline's abandon is the backstop
# (a round cut short there never advances the fact marker).
_EPISODE_TIMEOUT_SECONDS = 6
_SMALL_CALL_TIMEOUT_SECONDS = 3

# The recipe (proposal D + TASK-007 verification): episode page + peer
# look-ups + project-level retry, fact delta with keyset paging, semantic
# segment, 4000-char budget.
_EPISODE_LIMIT = 100            # one page, the endpoint's cap
_EPISODE_RENDER_TOTAL = 5       # peers first, own rows fill up to this
_PEER_ROWS_PER_GROUP = 2        # per peer container, latest first
_PEER_TOTAL = 4                 # across all peer containers
_PEER_QUERY_MAX = 2             # state-known peers actually queried
_PEER_STATE_MAX = 4             # peers remembered in state (query newest 2)
_FACT_MAX_PAGES = 5             # beyond this: fact_delta_truncated
_FACT_DIAL_BACK_MINUTES = 10    # covers stamp-then-commit rows and clock skew
_FACT_RENDER_LIMIT = 10         # M, ordered by aria.modified desc
_BASELINE_DAYS = 7              # first run / state loss / user change
_BRIEF_CHAR_BUDGET = 4000       # episode-priority; facts dropped item-wise first
_HANDOFF_RENDER_CHARS = 1500    # latest handoff per container, line-boundary cap

# session-inject's own state keys, next to `reported` / `container_id`:
_FACT_MARKER_KEY = "fact_marker"          # ISO instant the last round advanced from
_FACT_MARKER_USER_KEY = "fact_marker_user_id"  # the marker is bound to user_id (owner 2026-10-17)
_SEEN_CONTAINERS_KEY = "seen_containers"  # peer container ids seen in episodes, newest first
# How long the ledger write may hold up the exit. Normally it takes
# milliseconds; this is the cap for when it does not (see _record).
# hooks.json `timeout` is checked against these numbers by a test.
_LEDGER_BUDGET_SECONDS = 2.0
# The hook's own deadline for everything before the ledger (see main). It has
# to clear the nominal worst case -- two 5 s git calls and the two tiers -- and,
# with the ledger budget, stay under the host's timeout.
_WORK_BUDGET_SECONDS = 25.0
_USER_AGENT = "nexus-sessionstart-hook/0.3"

# Hooks that must have left a ledger by the time a SECOND session starts.
# "Never recorded a run" is reported only for these: a SessionEnd hook that
# stopped firing (killed at the shared 1.5 s budget, manifest not loaded) is
# invisible in every other way, and a hook not named here could stop for good
# without anyone noticing. New SessionEnd hooks add themselves; a test walks
# hooks.json to make sure they do.
_EXPECTED_LEDGERS = ("session-capture", "handoff-sync", "memory-sync")
# State key (ruling 4, TASK-005 R1 fix round, R1-c21): which of
# _EXPECTED_LEDGERS this project has already been told about. A plugin
# upgrade that adds a new SessionEnd hook here (handoff-sync was the first)
# must not report it "has never recorded a run" on the very next
# SessionStart -- its first SessionEnd is still ahead of it, and that is not
# the hook failing to fire. Legacy state written before this key existed (or
# before handoff-sync was added) reads as the set that was expected then.
_SEEN_EXPECTED_LEDGERS_KEY = "seen_expected_ledgers"
_LEGACY_EXPECTED_LEDGERS = ("session-capture",)
_LEDGER_SUFFIX = ".json"
_STATE_SUFFIX = ".state.json"
_TMP_PREFIX = ".tmp-"


class _BadResponse(Exception):
    """A 2xx that is not the API's answer: a body that is not JSON (a proxy or
    challenge page), or JSON of the wrong shape."""



def _current_branch(cwd):
    """Return the current git branch, or None if not a git repo / git failed.

    Delegates to _identity.current_branch (TASK-005): this hook and
    session_capture.py each carried a byte-identical copy of the derivation
    until now. Kept as a module-level name (not inlined as
    ``_identity.current_branch`` at the one call site) because tests patch
    ``mod._current_branch`` directly, and _collect must keep calling the
    module-level name for that patch to take effect.
    """
    return _identity.current_branch(cwd)


def _retrieve(base_url, token, user_id, metadata_filter, timeout):
    """POST one context/retrieve request (the semantic segment). Returns the
    parsed JSON dict; raises _BadResponse / urllib errors."""
    # Workflow D: the semantic segment is project-scoped, semantic layer only.
    # NO recent layer (it bypasses metadata_filter), no graph, no history; the
    # query must be non-empty or the backend falls back to an UNFILTERED
    # recent-memories list. `ai_profile` comes back with include_profile and
    # cannot be turned off -- it is simply never rendered (PR nexus#620).
    body = {
        "user_id": user_id,
        "query": "session start: recent work context",
        "layers": ["semantic"],
        "include_profile": True,
        "include_history": False,
        "include_graph": False,
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
            raise _BadResponse(f"body is not JSON: {exc}") from exc


def _rows_from(ctx, run):
    """The profile rows of one context/retrieve answer, after checking it is
    one -- and after the #613 contract made the three states readable.

    ``profile: null`` PLUS ``errors.profile`` is a semantic-layer FAILURE
    (reported; the rest of the brief still renders). ``profile: []`` is an
    honest "queried fine, nothing matched". ``null`` with no errors means the
    layer was not requested. Before nexus#613 the failure state arrived as
    ``profile: null`` with NO ``errors`` -- indistinguishable from an empty
    project -- and the run was recorded as `nothing_to_do`, an expected skip
    that is never reported.
    """
    if not isinstance(ctx, dict):
        raise _BadResponse(f"expected a JSON object, got {type(ctx).__name__}")
    errors = ctx.get("errors")
    if isinstance(errors, dict) and "profile" in errors:
        run["extra"]["backend_errors"] = sorted(
            set(run["extra"].get("backend_errors", [])) | {"profile"}
        )
    profile = ctx.get("profile")
    if profile is None:
        return []
    if not isinstance(profile, list) or not all(isinstance(row, dict) for row in profile):
        raise _BadResponse("`profile` is not a list of objects")
    return list(profile)


def _list_memories(base_url, token, user_id, params, timeout):
    """GET one /v1/memories page; return the raw rows (list of dicts).

    ``params`` are the deterministic list filters (layer / branch /
    container_id / project / updated_after / before_created_at / before_id /
    limit). Values are urlencoded -- cursor values go back EXACTLY as the
    response gave them, and hand-joining a query string would re-encode them
    differently.
    """
    from urllib.parse import urlencode
    query = {"user_id": user_id, **{k: v for k, v in params.items() if v is not None}}
    req = urllib.request.Request(
        f"{base_url}/memories?{urlencode(query)}",
        headers={
            "User-Agent": _USER_AGENT,
            "X-Nexus-Source": _identity.source_header(SOURCE_NAME),
            **({"X-API-Key": token} if token else {}),
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        try:
            body = json.load(resp)
        except ValueError as exc:
            raise _BadResponse(f"body is not JSON: {exc}") from exc
    if not isinstance(body, dict) or not isinstance(body.get("memories"), list):
        raise _BadResponse("`memories` is not a list")
    if not all(isinstance(row, dict) for row in body["memories"]):
        raise _BadResponse("`memories` holds a non-object row")
    return body["memories"]


def _verify_rows(rows, run, *, layer=None, branch=None, project=None):
    """Row-by-row verification of what the server was asked to filter.

    The client layer whitelist is GONE (#30's empty-recall mechanism): layers
    are decided server-side and CHECKED here, one row at a time -- trusting
    the filter without checking it is how a filter bug would silently empty
    the brief. Every dropped row counts toward `filter_suspect`.
    """
    kept = []
    dropped = 0
    for row in rows:
        meta = row.get("metadata") or {}
        if layer is not None and meta.get("layer") != layer:
            dropped += 1
            continue
        if branch is not None and meta.get("branch") != branch:
            dropped += 1
            continue
        if project is not None and meta.get("project") != project:
            dropped += 1
            continue
        kept.append(row)
    if dropped:
        run["extra"]["filter_suspect"] = run["extra"].get("filter_suspect", 0) + dropped
    return kept


def _row_time(row):
    """The row's sort time: top-level ``created_at`` (the list endpoint puts
    it there; metadata keys are a LEGACY fallback only). ``None`` when no
    usable timestamp -- callers sort those last."""
    raw = row.get("created_at")
    if not (isinstance(raw, str) and raw):
        meta = row.get("metadata") or {}
        raw = meta.get("valid_from") or meta.get("original_created_at")
    if not (isinstance(raw, str) and raw):
        return None
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except ValueError:
        return None


def _handoff_time(row):
    """A9-10: a handoff row's age anchor is ``aria.updated_at`` (parsed as a
    time), falling back to ``created_at`` -- its content is REWRITTEN on every
    sync, so created_at would show the first write, not the last."""
    raw = (row.get("metadata") or {}).get("aria.updated_at")
    if not (isinstance(raw, str) and raw):
        return _row_time(row)
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except ValueError:
        return _row_time(row)


def _aria_modified(row):
    """The fact delta's ordering key: ``aria.modified`` (peer backfills share
    a created_at week; created_at order would be a lottery). Missing parses as
    the epoch, sorting last."""
    raw = (row.get("metadata") or {}).get("aria.modified")
    if isinstance(raw, str) and raw:
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def _age_row(row):
    """Age string for one row. Handoff rows anchor on aria.updated_at
    (A9-10); everything else on created_at (TASK-007), with the metadata
    keys as the legacy fallback."""
    if (row.get("metadata") or {}).get("aria.source") == "handoff":
        ts = _handoff_time(row)
    else:
        ts = _row_time(row)
    if ts is None:
        return "?"
    secs = (datetime.now(timezone.utc) - ts).total_seconds()
    if secs < 0:
        return "0m"
    if secs < 3600:
        return f"{int(secs // 60)}m"
    if secs < 86400:
        return f"{int(secs // 3600)}h"
    return f"{int(secs // 86400)}d"


def _row_container(row):
    container = (row.get("metadata") or {}).get("container_id")
    return container if isinstance(container, str) and container else "?"


def _pick_episodes(rows, own_container):
    """Group by container and pick: peers first (latest 2 per group, groups
    by their newest row, peers total <= 4), own rows filling up to 5 total --
    own >= 1 whenever own rows exist (peers can never take the 5th slot).

    Returns ``(picked, peer_ids)``; ``picked`` is handoff-first (proposal D).
    """
    groups = {}
    for row in rows:
        groups.setdefault(_row_container(row), []).append(row)
    peers = [c for c in groups if c != own_container]
    peers.sort(key=lambda c: _handoff_time(groups[c][0]) or _row_time(groups[c][0]) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    picked = []
    peer_total = 0
    for c in peers:
        if peer_total >= _PEER_TOTAL:
            break
        take = groups[c][:_PEER_ROWS_PER_GROUP]
        picked.extend(take)
        peer_total += len(take)
    for row in groups.get(own_container, []):
        if len(picked) >= _EPISODE_RENDER_TOTAL:
            break
        picked.append(row)
    # aria.source=handoff rows render first (stable within each band).
    picked.sort(key=lambda r: (r.get("metadata") or {}).get("aria.source") != "handoff")
    return picked, peers


def _episode_segment(base_url, token, user_id, branch, project, own_container, state, run):
    """One episode page + verification + grouping + peer look-ups + the
    project-level retry. Returns ``(picked_rows, peers_seen)``.

    Peer look-ups fire only when the page that produced the result had no
    peer rows (the call budget: 1 page + <= 2 peer queries + <= 1 retry).
    """
    def page(extra, timeout):
        params = {"layer": "session_summary", "limit": _EPISODE_LIMIT, "project": project}
        if branch is not None:
            params["branch"] = branch
        params.update(extra)
        run["calls"] += 1
        return _verify_rows(
            _list_memories(base_url, token, user_id, params, timeout),
            run, layer="session_summary", branch=branch, project=project,
        )

    kept = page({}, _EPISODE_TIMEOUT_SECONDS)
    picked, peers = _pick_episodes(kept, own_container)
    if not peers:
        # Grouping can only reorder rows the page already returned; a peer
        # whose rows fell outside the window needs a look-up of its own.
        remembered = [
            c for c in (state.get(_SEEN_CONTAINERS_KEY) or [])
            if isinstance(c, str) and c != own_container
        ][:_PEER_QUERY_MAX]
        for peer in remembered:
            peer_rows = page({"container_id": peer, "limit": _PEER_ROWS_PER_GROUP},
                             _SMALL_CALL_TIMEOUT_SECONDS)
            picked.extend(peer_rows[:_PEER_ROWS_PER_GROUP])
        if remembered and not any(_row_container(r) != own_container for r in picked):
            run["extra"]["peer_absent"] = 1
    if not picked and branch is not None:
        # Project-level retry: same rules, no branch scope.
        branch = None
        kept = page({}, _SMALL_CALL_TIMEOUT_SECONDS)
        picked, peers = _pick_episodes(kept, own_container)
        if not peers:
            remembered = [
                c for c in (state.get(_SEEN_CONTAINERS_KEY) or [])
                if isinstance(c, str) and c != own_container
            ][:_PEER_QUERY_MAX]
            for peer in remembered:
                peer_rows = page({"container_id": peer, "limit": _PEER_ROWS_PER_GROUP},
                                 _SMALL_CALL_TIMEOUT_SECONDS)
                picked.extend(peer_rows[:_PEER_ROWS_PER_GROUP])
            if remembered and not any(_row_container(r) != own_container for r in picked):
                run["extra"]["peer_absent"] = 1
    peers_seen = [
        _row_container(r) for r in kept
        if _row_container(r) != own_container and _row_container(r) != "?"
    ]
    return picked, peers_seen


def _fact_segment(base_url, token, user_id, project, own_container, since, run):
    """The fact delta: pages of layer=fact rows updated since ``since``
    (already dialed back), keyset-paged (before_created_at + before_id, the
    previous page's last row, byte-exact; NEVER offset -- updated_after +
    offset > 0 is a 422, #499), deduped by id, own container dropped, M rows
    by aria.modified desc.

    Returns ``(fact_rows, truncated)``. Any page failure raises to the
    caller; a truncated round is flagged so the marker does not advance.
    """
    collected = []
    seen_ids = set()
    last_created = None
    last_id = None
    truncated = False
    for page_no in range(1, _FACT_MAX_PAGES + 1):
        params = {
            "layer": "fact",
            "limit": _EPISODE_LIMIT,
            "updated_after": since.isoformat(),
            "project": project,
        }
        if page_no > 1:
            params["before_created_at"] = last_created
            params["before_id"] = last_id
        run["calls"] += 1
        rows = _list_memories(base_url, token, user_id, params, _SMALL_CALL_TIMEOUT_SECONDS)
        rows = _verify_rows(rows, run, layer="fact", project=project)
        rows = [r for r in rows if _row_container(r) != own_container]
        if not rows:
            break  # an empty page is the end of the delta
        for row in rows:
            row_id = str(row.get("id") or row.get("memory_id") or "")
            if row_id and row_id in seen_ids:
                continue  # dial-back overlap: render once
            if row_id:
                seen_ids.add(row_id)
            collected.append(row)
        last_created = rows[-1].get("created_at")
        last_id = str(rows[-1].get("id") or "")
        if page_no == _FACT_MAX_PAGES:
            truncated = True  # a 5th page that still had rows: unread remainder
    collected.sort(key=_aria_modified, reverse=True)
    return collected[:_FACT_RENDER_LIMIT], truncated


def _semantic_segment(base_url, token, user_id, project, run):
    """The optional third segment: project-scoped /context/retrieve, query
    non-empty, semantic layer only. Returns verified profile rows (rendered
    like episode rows, after the grouped episodes). ``ai_profile`` is never
    rendered (it rides include_profile, cannot be switched off, and is not
    project-scoped -- PR nexus#620). No project -> the segment is NOT sent:
    an unscoped query only looks scoped."""
    if not project:
        return []
    run["calls"] += 1
    ctx = _retrieve(
        base_url, token, user_id, {"project": project}, _SMALL_CALL_TIMEOUT_SECONDS
    )
    rows = _rows_from(ctx, run)
    return _verify_rows(rows, run, project=project)


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


def _age_of(ts):
    """Age of a ledger timestamp (``%Y-%m-%dT%H:%M:%SZ``), for the report."""
    try:
        when = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return "?"
    return _age({"valid_from": when.isoformat()})


def _ledger_names(cwd):
    """Every hook that has a ledger here, plus the ones that should."""
    names = set(_EXPECTED_LEDGERS)
    try:
        entries = os.listdir(_hook_state.project_dir(cwd))
    except OSError:
        entries = []
    for name in entries:
        # .tmp-*.json is an abandoned atomic write; *.state.json is state.
        if name.endswith(_LEDGER_SUFFIX) and not name.endswith(_STATE_SUFFIX) \
                and not name.startswith(_TMP_PREFIX):
            names.add(name[: -len(_LEDGER_SUFFIX)])
    return sorted(names)


def _failure_report(cwd):
    """Look at every hook's ledger. Returns ``(findings, marks)``.

    ``findings`` is a list of one-line strings, one per hook whose latest run
    failed and has not been reported yet; ``marks`` is the full "reported"
    map to persist (hook -> identity of what was reported), including entries
    already reported -- a hook whose ledger became clean again drops out, so
    the next failure is news again.
    """
    state, _ = _hook_state.read_state(HOOK, cwd)
    already = state.get("reported") if isinstance(state.get("reported"), dict) else {}
    seen_raw = state.get(_SEEN_EXPECTED_LEDGERS_KEY)
    # ruling 4 (R1-c21): a hook newly present in _EXPECTED_LEDGERS but absent
    # from what THIS project was already told about gets one silent grace
    # SessionStart -- `main()` persists the current _EXPECTED_LEDGERS here
    # unconditionally afterwards (see below), so it is only ever absent once
    # per hook, on the first start after it was added (or, for legacy state
    # written before this key existed, before the key's own introduction).
    #
    # Each ELEMENT is also type-checked, not just the list itself (R3-c08):
    # `set(seen_raw)` alone raises `TypeError` on an unhashable element (a
    # stray list/dict, however it got there), which escapes this function
    # entirely -- `_collect`'s own `except Exception` catches it, but that
    # leaves `run["marks"]` at its initial `None`, and `_record` reads
    # `marks is None` as "the ledgers were never read this run; leave the
    # markers alone" and skips the persist that would otherwise have
    # overwritten the bad value. Every later SessionStart hits the exact
    # same `TypeError` forever, with no self-healing write to break the
    # loop -- unlike the sibling `reported` key three lines up, which
    # already has this same `isinstance` guard.
    seen_expected = (
        {h for h in seen_raw if isinstance(h, str)}
        if isinstance(seen_raw, list)
        else set(_LEGACY_EXPECTED_LEDGERS)
    )
    own_entries, _ = _hook_state.read_ledger(HOOK, cwd)
    first_start = not own_entries
    marks = {}
    findings = []
    for hook in _ledger_names(cwd):
        entries, reasons = _hook_state.read_ledger(hook, cwd)
        exists = os.path.exists(_hook_state.ledger_path(hook, cwd))
        if not exists:
            if hook in _EXPECTED_LEDGERS and hook in seen_expected and not first_start:
                key, text = "missing", f"{hook} has never recorded a run (is its hook firing?)"
            else:
                continue  # a first start, a hook not installed, or one newly expected (grace period)
        elif not entries and "unknown" in reasons:
            key, text = "unreadable", f"{hook} ledger is unreadable"
        elif not entries:
            continue  # exists and empty: nothing has happened yet
        else:
            last = entries[-1]
            reason = last.get("reason")
            if not (_hook_state.is_failure_reason(reason) or last.get("ok") is False):
                continue
            ts = str(last.get("ts", ""))
            key, text = f"{ts}|{reason}", f"{hook} failed its last run ({reason}, {_age_of(ts)} ago)"
        marks[hook] = key
        if already.get(hook) != key:
            findings.append(text)
    return findings, marks


def _handoff_pieces(row):
    """(title, head, sect6, full) for a handoff row's content.

    head = everything before the `## §2` heading (title, intro and §0 -- the
    entry section is warm-start gold); sect6 = the `## §6` section, up to the
    next `## ` heading. A9-10; the real handoff docs spell sections `## §0`,
    `## §1`, ... so the marker forms are stable.
    """
    content = (row.get("content") or "").strip()
    lines = content.splitlines()
    title = next((l[2:].strip() for l in lines if l.startswith("# ")), None) \
        or next((l.strip() for l in lines if l.strip()), "")
    head_lines, sect6_lines, zone = [], [], "head"
    for line in lines:
        if line.startswith("## "):
            if zone == "head" and "§2" in line:
                zone = "past2"          # head ends here
            elif zone == "sect6":
                zone = "done"           # §6 section ends at the next heading
            elif "§6" in line:
                zone = "sect6"
            continue
        if zone == "head":
            head_lines.append(line)
        elif zone == "sect6":
            sect6_lines.append(line)
    return title, "\n".join(head_lines).strip(), "\n".join(sect6_lines).strip(), content


def _cap_at_line(text, cap):
    """Truncate to ~cap chars at a line boundary (never mid-line)."""
    if len(text) <= cap:
        return text
    cut = text[:cap]
    nl = cut.rfind("\n")
    return cut[:nl] if nl > 0 else cut


def _render_handoff_row(row):
    """The LATEST handoff of one container: (title, head + §6), the body
    line-boundary capped at ~1500 chars; no §6 anywhere -> the whole document
    capped the same way. A9-10."""
    title, head, sect6, full = _handoff_pieces(row)
    parts = [part for part in (head, sect6) if part]
    body = _cap_at_line("\n".join(parts) if parts else full, _HANDOFF_RENDER_CHARS)
    return title, body


def _episode_line(row):
    """One non-handoff episode/semantic row: provenance bracket + capped
    content."""
    meta = row.get("metadata") or {}
    container = _row_container(row)
    branch = meta.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        branch = "-" if meta.get("layer") == "summary" else "?"
    content = (row.get("content") or "").strip().replace("\n", " ")
    if len(content) > 300:
        content = content[:300] + "…"
    return f"  • [{container} · {_age_row(row)} · {branch}] {content}"


def _fact_line(row):
    """One fact row: only aria.description (proposal D; the 4000-char budget
    drops whole items, not half of one)."""
    meta = row.get("metadata") or {}
    desc = meta.get("aria.description")
    if not isinstance(desc, str) or not desc.strip():
        desc = (row.get("content") or "").strip().replace("\n", " ")[:300]
    return f"  • [{_row_container(row)} · {_age_row(row)}] {desc.strip()}"


def _render(episode_rows, fact_rows, budget=_BRIEF_CHAR_BUDGET):
    """Assemble the brief: handoff episode groups (A9-10) ahead of plain
    episode lines (grouped episodes, then semantic rows -- the caller already
    ordered them), then the fact delta.

    The budget is episode-priority: drop fact items first (whole items), then
    episode lines from the tail. Returns None when nothing survives.
    """
    header = "Nexus project memory (cross-container warm-start):"

    # Split the handoff rows into per-container groups; within each group the
    # newest row (by aria.updated_at) renders fully, older ones one title line.
    handoff = [r for r in episode_rows if (r.get("metadata") or {}).get("aria.source") == "handoff"]
    plain = [r for r in episode_rows if (r.get("metadata") or {}).get("aria.source") != "handoff"]
    groups = {}
    for row in handoff:
        groups.setdefault(_row_container(row), []).append(row)
    handoff_lines = []
    for container in groups:
        ordered = sorted(groups[container], key=_handoff_time, reverse=True)
        for pos, row in enumerate(ordered):
            meta = row.get("metadata") or {}
            branch = meta.get("branch") if isinstance(meta.get("branch"), str) and meta.get("branch") else "?"
            bracket = f"[{container} · {_age_row(row)} · {branch}]"
            title, body = _render_handoff_row(row)
            if pos == 0:
                handoff_lines.append(f"  • {bracket} {title}\n{body}")
            else:
                handoff_lines.append(f"  • {bracket} {title}")

    episode_lines = handoff_lines + [_episode_line(r) for r in plain]
    fact_lines = [_fact_line(r) for r in fact_rows]

    def assemble(els, fls):
        parts = [header] + els
        if fls:
            parts.append("Facts since last session:")
            parts.extend(fls)
        text = "\n".join(parts)
        return text.encode("utf-8", "replace").decode("utf-8")

    text = assemble(episode_lines, fact_lines)
    while len(text) > budget and fact_lines:
        fact_lines.pop()  # whole items only, newest-preserved order
        text = assemble(episode_lines, fact_lines)
    while len(text) > budget and episode_lines:
        episode_lines.pop()  # the episode tail goes last
        text = assemble(episode_lines, fact_lines)
    if not episode_lines and not fact_lines:
        return None  # a bare header is not a brief
    return text


def _collect(run):
    """Do the work. Returns ``(reason, brief)``; each remote SEGMENT has its
    own try (proposal D: 段一抛错 → 段二仍渲染, and the other way round).

    ``run`` is filled in as facts become known, so that whatever happens next
    -- a return or an exception -- the ledger record has the right project and
    the number of calls actually attempted.
    """
    raw = sys.stdin.read()
    # SessionStart payload: cwd plus `source` (startup / resume / clear /
    # compact). Only `startup` advances the fact marker (proposal D).
    event = json.loads(raw) if raw.strip() else {}
    if not isinstance(event, dict):
        raise ValueError("SessionStart payload is not a JSON object")
    if isinstance(event.get("cwd"), str) and event["cwd"]:
        run["cwd"] = event["cwd"]
    cwd = run["cwd"] or os.getcwd()
    source = event.get("source") if isinstance(event.get("source"), str) else ""

    # The failure report first, before anything that can return early or
    # raise: the other hooks' failures are worth knowing regardless of how
    # this run goes. Read here, on the worker, so it is under the deadline.
    if _hook_state is not None:
        try:
            run["report"], run["marks"] = _failure_report(cwd)
        except Exception as exc:  # noqa: BLE001 - the report must never cost the brief
            print(f"[{HOOK}] could not read the hook ledgers: {exc!r}", file=sys.stderr)

    base_url = os.environ.get("NEXUS_API_URL", "").rstrip("/")
    if not base_url:
        return "not_configured", None  # the default for a fresh install
    token = os.environ.get("NEXUS_API_TOKEN", "")

    user_id = _identity.user_id(cwd)
    container_id = _identity.container_id()
    branch = _current_branch(cwd)
    # X1-policy (TASK-007): the project scope for every query. Underivable
    # (None) -> the project-scoped queries are simply not sent: an unscoped
    # query only LOOKS scoped, and an empty-string scope is an empty answer.
    project, project_source = _identity.project_key(
        cwd, os.environ.get(_identity.PROJECT_DIR_ENV)
    )
    if project is not None and project_source == _identity.PROJECT_SOURCE_GIT:
        run["extra"]["project_source"] = "git_fallback"

    # Own state: the fact marker (bound to user_id), the peers remembered
    # from past episodes, and the last container_id (A9-11 compares here).
    state = {}
    state_unknown = False
    if _hook_state is not None:
        state, state_reasons = _hook_state.read_state(HOOK, cwd)
        if not isinstance(state, dict):
            state = {}
        # A10 D4: a non-ENOENT read failure must not be silent -- it counts
        # into THIS round's ledger reason (and forces the fact baseline).
        state_unknown = "unknown" in (state_reasons or [])
        if state_unknown:
            run["extra"]["state_read_unknown"] = True
        prev_container = state.get("container_id")
        if isinstance(prev_container, str) and prev_container and prev_container != container_id:
            # A9-11: report the change once, here, NOT as a ledger reason.
            # Old rows now count as another container's; the remedy is the env
            # override. Persisted only after the stdout write succeeded.
            run["notes"].append(
                f"container_id changed from {prev_container} to {container_id}; "
                f"rows written as {prev_container} now count as a peer's. "
                f"Set NEXUS_CONTAINER_ID={prev_container} and restart to keep the old identity."
            )

    # The fact marker, bound to user_id (owner 2026-10-17): a different
    # user_id restarts from the baseline and says so (old value vs new).
    marker = state.get(_FACT_MARKER_KEY)
    if state.get(_FACT_MARKER_USER_KEY) != user_id:
        if isinstance(marker, str) and marker:
            run["notes"].append(
                f"fact marker rebound: user_id {state.get(_FACT_MARKER_USER_KEY)!r} -> {user_id!r};"
                " starting from the 7-day baseline"
            )
        marker = None
    if isinstance(marker, str) and marker:
        try:
            since = datetime.fromisoformat(marker.replace("Z", "+00:00"))
            if since.tzinfo is None:
                since = since.replace(tzinfo=timezone.utc)
        except ValueError:
            since = None
    else:
        since = None
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(days=_BASELINE_DAYS)
        run["notes"].append(
            f"fact baseline starts at {since.strftime('%Y-%m-%d %H:%M UTC')} (7 days)"
        )
    # The NEW marker is the moment before this round's first request (the
    # whole round then re-reads anything committed while it ran); it only
    # advances after the round completes cleanly, on a startup event.
    marker_now = datetime.now(timezone.utc).isoformat()
    run["marker_candidate"] = marker_now
    run["marker_user"] = user_id

    # ── segment 1: episodes (own container grouped first-class, peers
    # looked up, project-level retry) ──
    episode_rows = []
    peers_seen = []
    episode_failed = False
    try:
        episode_rows, peers_seen = _episode_segment(
            base_url, token, user_id, branch, project, container_id, state, run
        )
    except Exception:
        episode_failed = True  # segment 2 must still render (independent try)

    # ── segment 2: the fact delta (keyset pages, dedupe, own rows dropped) ──
    fact_rows = []
    fact_failed = False
    fact_truncated = False
    try:
        fact_rows, fact_truncated = _fact_segment(
            base_url, token, user_id, project, container_id,
            since - timedelta(minutes=_FACT_DIAL_BACK_MINUTES), run,
        )
    except Exception:
        fact_failed = True
    if fact_truncated:
        run["extra"]["fact_delta_truncated"] = 1
    run["advance_fact_marker"] = (
        not fact_failed and not fact_truncated and source == "startup"
    )

    # ── segment 3: project-scoped semantic retrieve (query non-empty) ──
    semantic_rows = []
    semantic_failed = False
    try:
        semantic_rows = _semantic_segment(base_url, token, user_id, project, run)
    except Exception:
        semantic_failed = True

    # Remember the peers this round actually saw (own container excluded),
    # newest first, bounded: next round's 补查 queries the two newest.
    seen = [c for c in peers_seen if c not in (container_id, "?")]
    for prev in (state.get(_SEEN_CONTAINERS_KEY) or []):
        if isinstance(prev, str) and prev not in seen and prev != container_id:
            seen.append(prev)
    run["seen_containers"] = seen[:_PEER_STATE_MAX]

    run["extra"].update(
        episodes=len(episode_rows), facts=len(fact_rows), semantic=len(semantic_rows),
        project=project, tier=None,
    )
    brief = _render(episode_rows + semantic_rows, fact_rows)
    if brief is None and not (run["notes"] or run["report"]):
        return "nothing_to_do", None  # nothing to inject at all

    # Reason precedence: a hard failure of any segment, then a backend-said-
    # it-failed (errors.profile), then rows dropped by verification, then a
    # failed own-state read (A10 D4), else the run is a clean skip.
    if episode_failed or fact_failed or semantic_failed:
        return "http_error", brief
    if run["extra"].get("backend_errors"):
        return "http_error", brief
    if run["extra"].get("filter_suspect"):
        return "filter_suspect", brief
    if state_unknown:
        return "unknown", brief
    return (_hook_state.NO_REASON if _hook_state else "none"), brief


def _warn(reason, exc):
    print(f"[{HOOK}] {reason}: {exc!r}", file=sys.stderr)


def _record(reason, started, run):
    """Append this run to the ledger, within a budget. Never raises.
    Returns True when the write had to be left behind.

    The write happens on a daemon thread (``_hook_runner.write_with_budget``)
    that is abandoned after ``_LEDGER_BUDGET_SECONDS``. Catching exceptions is
    not enough: the ledger takes a blocking lock, and a write that STALLS
    keeps this process alive until the host's timeout kills it -- and the
    host only uses the stdout of a hook that exited 0, so the stall would
    cost the very output that was written first to keep it safe. An
    abandoned daemon thread dies with the interpreter.
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
    calls, cwd, extra, marks = run["calls"], run["cwd"], dict(run["extra"]), run["marks"]

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
        # The fact marker advances only on a cleanly-completed startup round
        # (any page failure or truncation leaves it where it was -- the next
        # round re-reads); the peers this round saw ride the same write.
        # A10 D4: update_state runs AFTER this round's ledger row (it always
        # did), so its own failure is a state_write_failed the NEXT start
        # reports; it must never be back-filled into the row just written.
        advance = run.get("advance_fact_marker")
        marker_candidate = run.get("marker_candidate")
        marker_user = run.get("marker_user")
        seen_containers = run.get("seen_containers")
        try:
            def mutate(st):
                next_state = dict(st)
                if advance and marker_candidate:
                    next_state[_FACT_MARKER_KEY] = marker_candidate
                    next_state[_FACT_MARKER_USER_KEY] = marker_user
                if isinstance(seen_containers, list):
                    next_state[_SEEN_CONTAINERS_KEY] = seen_containers
                if run.get("stdout_ok"):
                    # A9-11: the new container_id and the report marks persist
                    # only once the stdout write succeeded -- reporting a
                    # change the session never saw would eat it forever.
                    if marks is not None:
                        next_state["reported"] = marks
                    next_state["container_id"] = _identity.container_id()
                    # seen_expected_ledgers travels alongside `reported`
                    # (ruling 4, R1-c21): a hook newly added to
                    # _EXPECTED_LEDGERS is "seen" from the very next start --
                    # exactly one grace period, then ordinary "missing"
                    # reporting resumes.
                    next_state[_SEEN_EXPECTED_LEDGERS_KEY] = list(_EXPECTED_LEDGERS)
                return next_state
            _hook_state.update_state(HOOK, cwd or os.getcwd(), mutate)
        except Exception as exc:  # noqa: BLE001
            print(f"[{HOOK}] could not persist the inject state: {exc!r}", file=sys.stderr)

    left_behind = _hook_runner.write_with_budget(write, _LEDGER_BUDGET_SECONDS, f"{HOOK}-ledger")
    if left_behind:
        print(
            f"[{HOOK}] ledger write still running after {_LEDGER_BUDGET_SECONDS}s; "
            f"leaving it behind, this run ({reason}) may go unrecorded",
            file=sys.stderr,
        )
    return left_behind


def _silence_stdout():
    """After a failed write, stop the interpreter retrying it on the way out.

    Python flushes stdout again at exit. Against a pipe the host has already
    closed that fails again, and the process exits 120 -- a hook error from a
    plugin whose contract is exit 0, always.

    ``sys.stdout.fileno()`` is resolved BEFORE ``os.open`` (R2-K2: an earlier
    revision opened the devnull fd FIRST and left it dangling whenever
    ``fileno()`` itself raised -- a stand-in stream with no real descriptor
    at all, such as this file's own ``_RaisingStdout`` test fixture, or a
    real ``sys.stdout`` with no fd behind it). Mirrors the ordering this
    file's own local ``_silence_stderr`` (for the import guards, above) and
    ``_hook_runner._silence_stderr`` already use, for the same reason.
    """
    try:
        target_fd = sys.stdout.fileno()
        devnull = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(devnull, target_fd)
        finally:
            os.close(devnull)
    except Exception:
        pass  # not a real file descriptor (tests), or nothing left to protect


def main():
    """Run the hook. Returns True when a worker thread had to be left behind."""
    started = time.monotonic()
    # Installed before the work thread starts (Amendment A9-21, carried over
    # from handoff_sync.py's TASK-005 original): every later print(...,
    # file=sys.stderr) in this process -- _warn's own calls below, _record's
    # ledger-write prints, _hook_runner.write_with_budget's own fallback
    # print -- looks up sys.stderr fresh at call time, so this one call
    # protects all of them, whichever thread reaches them. Every _warn(...)
    # call below now runs AFTER _record (see the comment right above that
    # call, further down), so this guard is no longer the ONLY thing
    # standing between a closed stderr pipe and a lost ledger row -- but it
    # still matters independently of that ordering: CPython's own
    # unconditional reflush of stdout AND stderr at interpreter shutdown
    # (behind every exit path, including the "clean" one) retries the SAME
    # write against the SAME closed pipe regardless of what main() itself
    # already did, with no Python-level except left anywhere near it --
    # that is exit code 120, not 0. And with fd 2 closed before the
    # interpreter even started, sys.stderr is None, and a bare print(msg,
    # file=None) silently falls back to sys.stdout -- which for THIS hook is
    # the injected context itself, not an empty channel: a leaked diagnostic
    # here would corrupt the very brief SessionStart reads, independent of
    # ordering too. guard_stderr() is idempotent, so repeated in-process
    # main() calls within the same test process do not double-wrap
    # (production runs this once per process).
    _hook_runner.guard_stderr()
    run = {
        "cwd": None, "calls": 0, "extra": {}, "report": [], "marks": None,
        "notes": [], "stdout_ok": False,
    }

    # The work runs against a deadline of its own. urllib's timeout is per
    # socket operation, not per request -- one "6 s" request was measured at
    # 24 s against a server that drips bytes -- and a hook the host has to kill
    # leaves no record and, for SessionStart, no brief. So leave first.
    # NOTE (A9-21 R1 fix round, finding C2 -- corrects an earlier revision of
    # this comment, which claimed "_collect never prints: a thread that may
    # be abandoned must stay off stdio"): _collect DOES print, on this same
    # worker thread -- its own `except Exception` around `_failure_report`
    # above reports a broken ledger read via a bare `print(...,
    # file=sys.stderr)`. That is exactly WHY `guard_stderr()` above must run
    # before this call, not after: without it, that print would hit the
    # SAME two failure shapes the import guards' own `_import_warn` (above)
    # handles locally (fd 2 closed before start -> falls back to sys.stdout,
    # corrupting this hook's own injected-context channel; stderr's read end
    # already closed -> BrokenPipeError escapes `_collect` entirely, turning
    # a benign `not_configured` run into a spurious `http_error`). This
    # file's own `_warn` (below) is a bare `print(..., file=sys.stderr)`
    # too, with no such handling of its own -- it relies solely on
    # `guard_stderr()` above, same as this print (R2-K4, correcting an
    # earlier revision of this comment that credited `_warn` itself with
    # guarding against these). Pinned by test_session_inject.py's
    # TestGuardStderrPrecedesTheWorkerThread.
    outcome, left_behind = _hook_runner.run_with_deadline(
        lambda: _collect(run), _WORK_BUDGET_SECONDS, f"{HOOK}-work"
    )

    # Every _warn(...) call below used to fire immediately; now each one is
    # deferred (appended here as a (label, detail) pair) and only actually
    # printed AFTER _record has run -- see the comment right before that
    # _record call, further down, for why (R2-c05, mirrored from
    # handoff_sync.py / session_capture.py).
    diagnostics = []

    reason, brief = "unknown", None
    if left_behind:
        reason = "timeout"
        diagnostics.append((reason, f"no result after {_WORK_BUDGET_SECONDS}s; leaving the work behind"))
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
        diagnostics.append((reason, exc))
    else:
        reason, brief = outcome["result"]
        if run["extra"].get("backend_errors"):
            diagnostics.append((reason, f"backend reported failures in {run['extra']['backend_errors']}"))

    # A9-11 / marker notes are user-visible exactly like failure findings --
    # but they only RIDE real output. With neither a brief nor a report
    # finding (an unreachable backend on a first run), fail-open silence
    # wins over bookkeeping notes: the baseline note would otherwise turn
    # "backend down" into a non-empty stdout, which is exactly the shape
    # fail-open exists to prevent. It surfaces on the first start that has
    # something real to say.
    findings = list(run["report"])
    if brief or findings:
        findings = list(run["notes"]) + findings
    if findings:
        # One line for the user, one for Claude, then the brief (if any).
        headline = "; ".join(findings)
        where = _hook_state.project_dir(run["cwd"] or os.getcwd()) if _hook_state else "?"
        context_line = f"[nexus-memory] Hook failures since last session: {headline}. Ledgers: {where}"
        brief = context_line if not brief else f"{context_line}\n\n{brief}"

    if brief:
        output = {
            "hookSpecificOutput": {
                "hookEventName": "SessionStart",
                "additionalContext": brief,
            }
        }
        if findings:
            output["systemMessage"] = f"nexus-memory: {'; '.join(findings)}"
        # Before the ledger, not after: nothing below this line may cost the
        # session its injection.
        try:
            sys.stdout.write(json.dumps(output, ensure_ascii=False))
            sys.stdout.flush()
            run["stdout_ok"] = True  # A9-11: only now may the state persist
        except Exception as exc:
            reason = "unknown"
            diagnostics.append(("could not write the brief", exc))
            _silence_stdout()

    # _record (the ledger row for THIS run) runs BEFORE any of the deferred
    # diagnostics above are actually printed, not after (R2-c05, mirrored
    # from handoff_sync.py): the old order called _warn as it went, and a
    # stderr write that fails -- a closed pipe, the host already exiting --
    # used to raise straight out of main() before _record ever ran, silently
    # losing the row for a run that had a genuine, useful reason to report.
    # guard_stderr() above is a first, independent net (it keeps a stderr
    # write from raising at all); this ordering is a second, independent
    # one -- it still protects the ledger row even if some OTHER exception
    # source in a diagnostic (not an OSError) were to escape that guard. The
    # stdout brief write just above still runs BEFORE this, unchanged: that
    # ordering protects the session's injection from a slow/stuck ledger
    # write, a different concern from the one this reorder addresses.
    record_left_behind = _record(reason, started, run)
    for label, detail in diagnostics:
        _warn(label, detail)
    return record_left_behind or left_behind


if __name__ == "__main__":
    left_behind = False
    try:
        left_behind = bool(main())
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session startup over memory retrieval.
        pass
    _hook_runner.finish(left_behind)
