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
    Known limit since the layer whitelist (10CG/nexus-claude-plugin#32): tier
    1 used to come back empty almost always (migrated summaries carry no
    branch, observations are filtered out), so tier 2 ran and rendered the
    migrated summaries. Now this container's own aggregated episodes fill
    tier 1 -- on the current branch, or across all branches when the branch
    is unknown, since the filter is then ``container_id`` alone -- and tier 2
    is not sent. Tier 2 is the only source of the migrated summaries and of
    this container's other branches, and the usual source of the other
    container's rows (a hybrid tenant's sentence channel ignores the filter,
    so some can arrive in tier 1 anyway). A start then sees its own episodes
    and typically not the other container's -- before the whitelist it saw
    neither. Workflow D (change 2 TASK-007) replaces these tiers with
    per-container grouping and peer look-ups and ships in the same release
    (TASK-008); until then an installed client stays on the snapshot it was
    installed from, but the marketplace source pins no ref, so a fresh
    install or an update takes main HEAD. A test pins it.
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
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

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
_EXPECTED_LEDGERS = ("session-capture",)
_LEDGER_SUFFIX = ".json"
_STATE_SUFFIX = ".state.json"
_TMP_PREFIX = ".tmp-"


class _BadResponse(Exception):
    """A 2xx that is not the API's answer: a body that is not JSON (a proxy or
    challenge page), or JSON of the wrong shape."""


# The server-side tasks whose results are merged into `profile`. The backend
# degrades gracefully: when one of them raises, the response is still 200, with
# the exception text under `errors[<task>]`.
_PROFILE_TASKS = ("profile", "recent")

# The layers the brief renders, best first. `session_summary` is what the
# backend's session aggregator writes (and what `services/context.py`'s layer
# preference ranks first); `summary` is the claude-mem migration (no branch,
# no session_id). Matching `summary` alone dropped every aggregated row
# (10CG/nexus-claude-plugin#32). `observation` stays out on purpose: raw
# activity granularity is noise in a warm-start.
_SETTLED_LAYERS = ("session_summary", "summary")
_SETTLED_RANK = {layer: rank for rank, layer in enumerate(_SETTLED_LAYERS)}


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
            raise _BadResponse(f"body is not JSON: {exc}") from exc


def _rows_from(ctx, run):
    """The settled rows of one response, after checking it is one.

    Reading only `profile` is how a backend outage used to look exactly like a
    project with no memories: `profile: null` plus `errors: {"profile": ...}`
    came back as an empty list, and the run was recorded as `nothing_to_do` --
    an expected skip, never reported.
    """
    if not isinstance(ctx, dict):
        raise _BadResponse(f"expected a JSON object, got {type(ctx).__name__}")
    errors = ctx.get("errors")
    if isinstance(errors, dict):
        failed = sorted(set(run["extra"].get("backend_errors", [])) | (set(errors) & set(_PROFILE_TASKS)))
        if failed:
            run["extra"]["backend_errors"] = failed
    profile = ctx.get("profile")
    if profile is None:
        return []
    if not isinstance(profile, list) or not all(isinstance(row, dict) for row in profile):
        raise _BadResponse("`profile` is not a list of objects")
    return _settled_rows(profile)


def _settled_rank(row):
    """The row's position in ``_SETTLED_LAYERS``, or ``None`` if not rendered."""
    layer = (row.get("metadata") or {}).get("layer")
    # isinstance first: a dict lookup hashes, and a list-valued layer from a
    # confused writer would raise and take the whole brief down with it.
    return _SETTLED_RANK.get(layer) if isinstance(layer, str) else None


def _settled_rows(profile):
    """Filter profile rows to settled summaries, ``session_summary`` first.

    If NO row carries a ``layer`` metadata key at all, take every row (no layer
    dimension present -> nothing to filter on).
    """
    rows = [r for r in profile if isinstance(r, dict)]
    has_layer = any("layer" in (r.get("metadata") or {}) for r in rows)
    if not has_layer:
        return rows
    ranked = []
    for row in rows:
        rank = _settled_rank(row)
        if rank is not None:
            ranked.append((rank, row))
    # Sorted on the rank alone, and sort is stable: within a layer the
    # backend's order is kept.
    ranked.sort(key=lambda pair: pair[0])
    return [row for _, row in ranked]


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
    own_entries, _ = _hook_state.read_ledger(HOOK, cwd)
    first_start = not own_entries
    marks = {}
    findings = []
    for hook in _ledger_names(cwd):
        entries, reasons = _hook_state.read_ledger(hook, cwd)
        exists = os.path.exists(_hook_state.ledger_path(hook, cwd))
        if not exists:
            if hook in _EXPECTED_LEDGERS and not first_start:
                key, text = "missing", f"{hook} has never recorded a run (is its hook firing?)"
            else:
                continue  # a first start, or a hook that is simply not installed
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


def _render(rows):
    """Render the settled rows into a provenance-annotated brief (or None)."""
    if not rows:
        return None
    lines = ["Nexus project memory (cross-container warm-start, settled summaries):"]
    for r in rows:
        meta = r.get("metadata") or {}
        container = meta.get("container_id", "?")
        branch = meta.get("branch")
        if not isinstance(branch, str) or not branch.strip():
            # A migrated summary never had a branch: "-" says "none", where
            # "?" would suggest one went missing. Empty, blank and non-string
            # values are missing too; rendered as they came they left an empty
            # (or nonsense) slot in the bracket.
            branch = "-" if meta.get("layer") == "summary" else "?"
        age = _age(meta)
        content = (r.get("content") or "").strip().replace("\n", " ")
        if len(content) > 300:
            content = content[:300] + "…"
        lines.append(f"  • [{container} · {age} · {branch}] {content}")
    # One row carrying a lone surrogate made the stdout write raise
    # UnicodeEncodeError, and every other row went down with it.
    return "\n".join(lines).encode("utf-8", "replace").decode("utf-8")


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

    # Tier 1: branch + container scoped (branch key omitted if branch unknown).
    metadata_filter = {"container_id": container_id}
    if branch:
        metadata_filter["branch"] = branch

    run["calls"] += 1  # counted before the call: a call that fails was still made
    ctx = _retrieve(base_url, token, user_id, metadata_filter, _TIER1_TIMEOUT_SECONDS)
    rows = _rows_from(ctx, run)
    run["extra"]["tier"] = 1

    # Tier 2: project-level fallback (no metadata_filter) when tier 1 is empty.
    if not rows:
        run["calls"] += 1
        ctx = _retrieve(base_url, token, user_id, None, _TIER2_TIMEOUT_SECONDS)
        rows = _rows_from(ctx, run)
        run["extra"]["tier"] = 2

    run["extra"]["rows"] = len(rows)
    brief = _render(rows)
    if run["extra"].get("backend_errors"):
        # Inject whatever did come back, and report what did not.
        return "http_error", brief
    if not brief:
        return "nothing_to_do", None  # nothing to inject
    return (_hook_state.NO_REASON if _hook_state else "none"), brief


def _warn(reason, exc):
    print(f"[{HOOK}] {reason}: {exc!r}", file=sys.stderr)


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
        if marks is None:
            return  # the ledgers were never read this run; leave the markers alone
        try:
            # container_id travels with every state write: identity_drift
            # (TASK-007) reads it back, and a state written without it makes
            # every later run report unknown.
            _hook_state.update_state(
                HOOK,
                cwd or os.getcwd(),
                lambda s: {**s, "reported": marks, "container_id": _identity.container_id()},
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[{HOOK}] could not persist the report markers: {exc!r}", file=sys.stderr)

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


def _silence_stdout():
    """After a failed write, stop the interpreter retrying it on the way out.

    Python flushes stdout again at exit. Against a pipe the host has already
    closed that fails again, and the process exits 120 -- a hook error from a
    plugin whose contract is exit 0, always.
    """
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        os.close(devnull)
    except Exception:
        pass  # not a real file descriptor (tests), or nothing left to protect


def main():
    """Run the hook. Returns True when a worker thread had to be left behind."""
    started = time.monotonic()
    run = {"cwd": None, "calls": 0, "extra": {}, "report": [], "marks": None}
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

    reason, brief = "unknown", None
    if left_behind:
        reason = "timeout"
        _warn(reason, f"no result after {_WORK_BUDGET_SECONDS}s; leaving the work behind")
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
        _warn(reason, exc)
    else:
        reason, brief = outcome["result"]
        if run["extra"].get("backend_errors"):
            _warn(reason, f"backend reported failures in {run['extra']['backend_errors']}")

    findings = run["report"]
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
        except Exception as exc:
            reason = "unknown"
            _warn("could not write the brief", exc)
            _silence_stdout()

    return _record(reason, started, run) or left_behind


if __name__ == "__main__":
    left_behind = False
    try:
        left_behind = bool(main())
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session startup over memory retrieval.
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
