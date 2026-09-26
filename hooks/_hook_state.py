"""Local ledger + state primitives shared by every nexus hook.

The whole point of these primitives is to turn silent stalls into something a
user sees at the next SessionStart. So the design rule that governs every
decision below is: **no path here may produce a clean-looking run that did not
happen.** Three things follow from it.

**The two reason tables.** Every ledger entry carries a ``reason``, and each is
either a failure (surfaced) or an expected skip (recorded only). The split is
the substance: "this handoff belongs to the other container" must stay quiet
forever, while "I could not tell whose it is" must be loud — from the outside
the two look identical, and conflating them is how ingestion stops without
anyone noticing. An unknown reason is recorded as ``unknown`` and named on
stderr; it is never raised, because every caller runs under the hooks'
``except Exception: pass`` + ``exit(0)`` idiom, where raising deletes the whole
ledger entry and leaves yesterday's success as the most recent record.

**Nothing here raises on I/O.** Same reason. A hook that cannot write its state
must still record *that*, so ``write_state`` returns ``state_write_failed``
rather than propagating: an exception would reach the blanket handler, the run
would exit 0 with no stdout and no ledger row, and the next SessionStart would
see a stale success and report nothing. (An earlier revision propagated, on the
argument that swallowing would let the memory-sync cursor stall invisibly. That
was a false choice — returning a failure reason keeps exit 0 *and* is more
visible than either alternative.)

**Locks span the read-modify-write, not the write.** ``os.replace`` is already
atomic; the part that actually needs exclusion is read → modify → write, which
two SessionEnd runs for the same session (10CG/nexus-claude-plugin#31) perform
concurrently. Locking only the write leaves the interesting race wide open —
measured at 100% entry loss with two writers before this was fixed.
"""

import errno
import fcntl
import http.client
import json
import os
import socket
import sys
import tempfile
import time
import urllib.error

try:
    import ssl

    _SSL_ERRORS = (ssl.SSLError,)
except ImportError:  # a Python built without ssl
    _SSL_ERRORS = ()
from contextlib import contextmanager

import _identity

DEFAULT_STATE_DIR = "~/.nexus/hooks"
STATE_DIR_ENV = "NEXUS_HOOK_STATE_DIR"
LEDGER_LIMIT = 50

# Reported to the user at the next SessionStart.
FAILURE_REASONS = frozenset(
    {
        "http_error",
        "timeout",
        "filter_suspect",
        "budget_exhausted",
        "ingest_disabled",
        "identity_unresolved",
        "identity_changed",
        "sections_unparsed",
        "file_unparsable",
        "rejected_422",
        "orphan_guard",
        "lock_unavailable",
        # The two destructive actions: both delete rows on the server, so a
        # non-zero count is reported even though a successful cleanup is not an
        # error. dedup_merged started in the skip table, next to a comment here
        # that called orphans_deleted "the only destructive action" (Amendment
        # A4-4, owner ruling 2026-09-21). The rows it merges can only be this
        # hook's own race, so a non-zero count also says the idempotency
        # protocol lost one.
        "orphans_deleted",
        "dedup_merged",
        "unknown",
        # Added by the TASK-001 pre-merge audit (Amendment A4-1): conditions
        # the B/C/D rows name but had no reason, which would have forced the
        # next author to reuse a wrong one.
        "rate_limited",  # 429; spec C row: "遇 429 停止本轮"
        "state_write_failed",  # could not persist state; the run will repeat
        # Handoff files exist and none could be located (Amendment A4-5, owner
        # ruling 2026-09-21). It used to be a skip that also covered "this
        # project keeps no handoffs" -- now no_handoff, below. Same split as
        # empty_sections / sections_unparsed one level down, for the same
        # reason: a renamed template or a broken frontmatter must not stop
        # ingestion silently.
        "pointer_unresolved",
        # session-capture-priority-truncation (owner ruling 4, 2026-09-25):
        # the SessionEnd capture's tiered selection raised and the run fell
        # back to the plain tail. The upload itself succeeded and the run is
        # still recorded as a failure (reported at the next SessionStart
        # while it is the hook's latest entry), because a selector that
        # keeps failing quietly is the tiering not existing. Kept OUT of
        # _REASON_PRIORITY and under _REASON_FLOOR: see worst_reason.
        "capture_tiering_degraded",
    }
)

# Recorded in the ledger, never reported: these are the hook working correctly.
SKIP_REASONS = frozenset(
    {
        "not_owner",
        "opted_out",
        "empty_sections",
        "stale_local",
        "unchanged",
        "peer_absent",
        "fact_delta_truncated",
        # Amendment A4-1, as above.
        "not_configured",  # no backend configured; the default for a fresh install
        "nothing_to_do",  # nothing to send this run
        # Amendment A4-5: no handoff directory, or no handoff files in it. The
        # default for any project that does not use aria handoffs, so it must
        # stay quiet. "Files exist but none resolved" is pointer_unresolved.
        "no_handoff",
    }
)

# "nothing to report" — a clean run.
NO_REASON = "none"

ALL_REASONS = FAILURE_REASONS | SKIP_REASONS | {NO_REASON}


# Checked in order when a run produced several failures. Everything not
# listed keeps its given order behind these.
_REASON_PRIORITY = (
    "state_write_failed",  # nothing was persisted: outranks how it failed
    "ingest_disabled",
    "http_error",
    "rate_limited",
    "timeout",
)

# Failures that lose to every OTHER failure in the same run. worst_reason's
# rule is "the priority table, then the first given", and a member of the
# table outranks every non-member -- so appending capture_tiering_degraded
# to the table would have made "the tail was uploaded fine" outrank
# file_unparsable, lock_unavailable and unknown (measured: 13 real failures
# sit outside the table). A floor is the other direction: it still wins over
# skips and clean runs, and over nothing else.
_REASON_FLOOR = frozenset({"capture_tiering_degraded"})


def is_failure_reason(reason):
    """True when a reason must be surfaced at the next SessionStart."""
    return reason in FAILURE_REASONS


def reason_for_exception(exc):
    """The failure reason for an exception raised around a remote call.

    One mapping for every hook. Each hook growing its own is how one of them
    ends up recording a rate limit as a generic error -- and the rate limit is
    the case the spec treats differently (stop this round, do not retry).

    Always a failure reason, never a skip: an exception is by definition not
    the hook working correctly, and a skip is recorded without being reported.
    Anything unrecognised is ``unknown`` rather than a guess.
    """
    # HTTPError first: it subclasses URLError, so the other order turns every
    # 429 into http_error.
    if isinstance(exc, urllib.error.HTTPError):
        return "rate_limited" if exc.code == 429 else "http_error"
    # socket.timeout is TimeoutError from 3.10 on, a separate OSError before.
    timeouts = (socket.timeout, TimeoutError)
    if isinstance(exc, timeouts):
        return "timeout"  # raised bare when the read, not the connect, stalls
    if isinstance(exc, urllib.error.URLError):
        return "timeout" if isinstance(exc.reason, timeouts) else "http_error"
    # HTTPException: IncompleteRead, BadStatusLine, InvalidURL (a NEXUS_API_URL
    # that is not one). SSLError: a TLS failure raised bare rather than wrapped.
    if isinstance(exc, (http.client.HTTPException, ConnectionError) + _SSL_ERRORS):
        return "http_error"
    return "unknown"


def worst_reason(reasons):
    """Collapse several reasons into the one to record.

    A run can produce more than one — ``lock_unavailable`` alongside
    ``unchanged``, say — but a ledger entry holds a single scalar and the
    SessionStart reporter reads only that. Without one rule for collapsing,
    each hook would pick its own and some would drop the failure. Failures win;
    among failures the priority table wins, then the first given -- except
    that a _REASON_FLOOR member yields to any other failure.
    """
    if isinstance(reasons, str):  # a bare string would iterate per character
        reasons = [reasons]
    reasons = [r for r in (reasons or []) if r and r != NO_REASON]
    if not reasons:
        return NO_REASON
    failures = [r for r in reasons if is_failure_reason(r)]
    if failures:
        # Ordered, not first-wins. A read-only directory produces both
        # lock_unavailable (the lock file could not be opened) and
        # state_write_failed (the write itself failed), and reporting "could
        # not lock" for "nothing was persisted" describes the wrong problem --
        # in precisely the scenario state_write_failed was added for.
        for preferred in _REASON_PRIORITY:
            if preferred in failures:
                return preferred
        for failure in failures:
            if failure not in _REASON_FLOOR:
                return failure
        return failures[0]
    return reasons[0]


def state_root():
    """Base directory for ledgers and state, overridable for tests."""
    return os.path.expanduser(os.environ.get(STATE_DIR_ENV) or DEFAULT_STATE_DIR)


def project_dir(cwd):
    """Per-project directory, keyed by the same slug that keys user_id.

    Reached through the module attribute rather than a ``from`` import so that
    patching ``_identity.project_slug`` reaches this too — a ``from`` import
    binds at import time and silently ignores the patch.
    """
    return os.path.join(state_root(), _identity.project_slug(cwd))


def _atomic_write(path, text):
    """Write via temp file + rename, so readers never see a partial file.

    The temp file is created in the destination directory: rename is only
    atomic within a filesystem, and /tmp is frequently a different one.
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
    try:
        # backslashreplace, because a lone surrogate is valid JSON (as an
        # escape) but cannot be encoded as UTF-8: one such string anywhere in a
        # ledger made every later write raise UnicodeEncodeError, and since the
        # entry could then never rotate out, the ledger froze for good. A lone
        # surrogate is the only thing UTF-8 cannot encode, and its replacement
        # text is exactly the JSON escape for it, so the file stays valid JSON
        # and round-trips.
        with os.fdopen(fd, "w", encoding="utf-8", errors="backslashreplace") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


@contextmanager
def _locked(path, reasons):
    """Hold an exclusive lock across a read-modify-write of ``path``.

    Appends ``lock_unavailable`` to ``reasons`` and proceeds unlocked when the
    filesystem refuses to lock (NFS and friends). Degrading is better than
    dropping the write, but a lock that silently stops locking is worse than no
    lock — the concurrency assumption still reads as satisfied — so it is
    reported.

    The ``.lock`` file is left behind on purpose: unlinking it races with the
    next process opening it, and an empty file per state name is cheap.
    """
    lock_fd = None
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        lock_fd = os.open(path + ".lock", os.O_CREAT | os.O_RDWR, 0o644)
    except OSError as exc:
        if exc.errno not in (errno.EACCES, errno.EPERM, errno.EROFS, errno.ENOSPC):
            raise
        reasons.append("lock_unavailable")
    if lock_fd is not None:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
        except OSError:
            reasons.append("lock_unavailable")
    try:
        yield
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(lock_fd)


def ledger_path(hook, cwd):
    return os.path.join(project_dir(cwd), f"{hook}.json")


def read_ledger(hook, cwd):
    """Return ``(entries, reasons)`` — recent runs of one hook, oldest first.

    Distinguishes the three states V(2) asks for: a missing ledger is a first
    run (``[]``, no reason), an unreadable or malformed one reports ``unknown``.
    Non-dict elements are dropped rather than handed on, because the reporter
    does ``entry.get(...)`` and an AttributeError there reaches the blanket
    handler and loses the whole injection.
    """
    return _read_ledger_file(ledger_path(hook, cwd))


def _read_ledger_file(path):
    """``read_ledger`` for a path already resolved.

    Resolving a path asks git for the project (5 s timeout). ``record_run``
    used to resolve it twice, once directly and once through ``read_ledger``.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)
    except FileNotFoundError:
        return [], []
    except (OSError, ValueError):
        return [], ["unknown"]
    if not isinstance(entries, list):
        return [], ["unknown"]
    clean = [e for e in entries if isinstance(e, dict)]
    return clean, ([] if len(clean) == len(entries) else ["unknown"])


def record_run(hook, ok, reason=NO_REASON, elapsed_ms=None, calls=0, cwd=None, extra=None):
    """Append one run to the hook's ledger, keeping the most recent 50.

    Never raises. A reason outside the two tables is recorded as ``unknown``
    (a failure reason, so it is reported) and named on stderr — a typo becomes
    loud instead of deleting the record, which is what raising did under the
    hooks' blanket exception handler.

    Write failures go to stderr and are swallowed: by the time a hook records
    its run the remote work is already done, and failing here would throw away
    a completed run over local bookkeeping. "Failures" means every exception,
    not only OSError -- a vanished working directory, an ``extra`` that will
    not serialise and an unencodable string all used to escape from here.

    ``ok`` is the caller's view, formed before the ledger was looked at. If
    recording upgrades the reason to a failure (a corrupt ledger, a degraded
    lock), ``ok`` follows it: a record never says ok next to a failure reason.
    """
    if reason not in ALL_REASONS:
        print(
            f"[{hook}] unknown reason {reason!r} recorded as 'unknown' — add it to "
            f"FAILURE_REASONS or SKIP_REASONS in _hook_state.py",
            file=sys.stderr,
        )
        reason = "unknown"
    entry = {
        "hook": hook,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ok": bool(ok) and not is_failure_reason(reason),
        "reason": reason,
        "elapsed_ms": elapsed_ms,
        "calls": calls,
    }
    if extra:
        entry.update(extra)
    try:
        path = ledger_path(hook, cwd or os.getcwd())
        lock_reasons = []
        with _locked(path, lock_reasons):
            entries, read_reasons = _read_ledger_file(path)
            # A degraded lock on THIS path has to be reported: the ledger is
            # what the whole visibility baseline is carried in, and a lock that
            # silently stopped locking leaves the concurrency assumption
            # reading as satisfied. Earlier revisions computed lock_reasons
            # here and dropped them.
            entry["reason"] = worst_reason([entry["reason"], *lock_reasons, *read_reasons])
            entry["ok"] = entry["ok"] and not is_failure_reason(entry["reason"])
            entries.append(entry)
            # default=str: an `extra` value that will not serialise is a caller
            # bug, and losing the whole record over it is the worse outcome.
            _atomic_write(
                path, json.dumps(entries[-LEDGER_LIMIT:], ensure_ascii=False, default=str)
            )
    except Exception as exc:  # noqa: BLE001 - "never raises" means never
        print(f"[{hook}] could not write ledger: {exc!r}", file=sys.stderr)
    return entry


def state_path(name, cwd):
    return os.path.join(project_dir(cwd), f"{name}.state.json")


def read_state(name, cwd):
    """Return ``(state, reasons)``.

    A missing file is a first run, not a problem. A corrupt one rebuilds from
    empty and reports ``unknown``, because the hook is about to behave as
    though it had never synced anything.
    """
    try:
        with open(state_path(name, cwd), encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}, []
    except (OSError, ValueError):
        return {}, ["unknown"]
    if not isinstance(data, dict):
        return {}, ["unknown"]
    return data, []


def state_exists(name, cwd):
    """Whether state has ever been written — see ``identity_drift``."""
    return os.path.exists(state_path(name, cwd))


def write_state(name, data, cwd):
    """Atomically write state under an exclusive lock. Returns reasons.

    Returns rather than raises on I/O failure (``state_write_failed``): see the
    module docstring for why propagating is the quietest of the options here.
    """
    reasons = []
    path = state_path(name, cwd)
    if not isinstance(data, dict):
        print(f"[{name}] refusing to write {type(data).__name__} as state", file=sys.stderr)
        return ["state_write_failed"]
    try:
        with _locked(path, reasons):
            _atomic_write(path, json.dumps(data, ensure_ascii=False, indent=2))
    except OSError as exc:
        print(f"[{name}] could not write state: {exc}", file=sys.stderr)
        reasons.append("state_write_failed")
    return reasons


def update_state(name, cwd, mutate):
    """Read-modify-write under one lock. Returns ``(new_state, reasons)``.

    The primitive every hook actually needs: ``read_state`` then ``write_state``
    leaves the gap between them unprotected, and two runs racing there lose one
    another's updates while each individual write looks perfectly atomic.

    ``mutate`` receives the current state dict and returns the new one.
    """
    reasons = []
    path = state_path(name, cwd)
    current = {}
    new = {}
    try:
        with _locked(path, reasons):
            current, read_reasons = read_state(name, cwd)
            reasons.extend(read_reasons)
            try:
                new = mutate(dict(current))
            except Exception as exc:  # noqa: BLE001 - caller bug, not ours
                # Propagating would reach the hooks' blanket handler and delete
                # the run, which is the failure C3 was about.
                print(f"[{name}] state mutation raised: {exc!r}", file=sys.stderr)
                return current, reasons + ["unknown"]
            if not isinstance(new, dict):
                # `lambda s: s.update(...)` returns None. Writing it would put
                # `null` on disk -- destroying the cursor and the container_id
                # the identity guard depends on -- while this run recorded as
                # clean and the damage surfaced as an unattributable `unknown`
                # on the NEXT run.
                print(
                    f"[{name}] state mutation returned {type(new).__name__}, "
                    f"expected dict; state left unchanged",
                    file=sys.stderr,
                )
                return current, reasons + ["state_write_failed"]
            _atomic_write(path, json.dumps(new, ensure_ascii=False, indent=2))
    except OSError as exc:
        print(f"[{name}] could not update state: {exc}", file=sys.stderr)
        reasons.append("state_write_failed")
        # The caller gets what is actually on disk, not the update it wanted.
        return current, reasons
    return new, reasons


def identity_drift(previous, current, state_existed):
    """Reasons for a container-id change between runs.

    Worth reporting rather than absorbing: rows written under the old id are
    indistinguishable from another container's, so the read side starts
    treating this machine's own history as a peer's — the injection recipe
    gives away slots to it, and the orphan reconciliation, which lists only
    ``container_id=<self>``, sees none of those rows at all.

    ``state_existed`` is required, not defaulted: a caller that forgets it is
    the exact caller who would get the dangerous answer. Pass what
    ``state_exists`` returned. False with no previous id is a genuine first run
    and is quiet. But state *vanishing* is not a first run, and it is correlated with
    exactly the event this guard exists for (a wiped state directory, a changed
    NEXUS_HOOK_STATE_DIR, a project slug that degraded when git timed out). In
    that case the prior identity is unknowable locally, which is reported as
    ``unknown`` rather than passed off as "nothing moved".
    """
    if previous and current and previous != current:
        return ["identity_changed"]
    if not previous and state_existed:
        return ["unknown"]
    return []
