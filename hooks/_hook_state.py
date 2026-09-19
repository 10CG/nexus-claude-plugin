"""Local ledger + state primitives shared by every nexus hook.

Three things live here because getting any of them slightly different per hook
would be invisible until it mattered:

**The two reason tables.** Every ledger entry carries a ``reason``, and each
reason is either a failure (surfaced to the user at the next SessionStart) or
an expected skip (recorded only). The split is the whole point: "this handoff
belongs to the other container" must stay quiet forever, while "I could not
tell whose it is" must be loud, because the second one looks exactly like the
first from the outside and would otherwise stop ingestion silently. A reason
outside both tables is a programming error and raises.

**Atomic whole-file rewrite, never append.** SessionEnd can fire more than once
for one session (10CG/nexus-claude-plugin#31), so two processes may write the
same file concurrently. Appending would interleave into unparsable JSON;
writing a temp file and renaming means a reader either sees the old complete
array or the new one.

**Failure legs that stay visible.** Two are specified and both are exercised by
tests:
  - the state directory is read-only: the hook's real work has already happened
    by then, so the ledger write complains on stderr and the hook still exits 0
    rather than losing the run it just completed;
  - ``fcntl.flock`` raises (NFS, exotic filesystem): carry on without the lock
    but return ``lock_unavailable`` so it is reported. A lock that silently
    stops locking is worse than no lock, because the concurrency assumption
    still reads as satisfied.
"""

import errno
import fcntl
import json
import os
import sys
import tempfile
import time

from _identity import project_slug

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
        # The only destructive action any hook takes, so a non-zero count is
        # reported even though a successful cleanup is not an error.
        "orphans_deleted",
        "unknown",
    }
)

# Recorded in the ledger, never reported: these are the hook working correctly.
SKIP_REASONS = frozenset(
    {
        "not_owner",
        "opted_out",
        "empty_sections",
        "pointer_unresolved",
        "stale_local",
        "unchanged",
        "peer_absent",
        "fact_delta_truncated",
        "dedup_merged",
    }
)

# "nothing to report" — a clean run.
NO_REASON = "none"

ALL_REASONS = FAILURE_REASONS | SKIP_REASONS | {NO_REASON}


def is_failure_reason(reason):
    """True when a reason must be surfaced at the next SessionStart."""
    return reason in FAILURE_REASONS


def state_root():
    """Base directory for ledgers and state, overridable for tests."""
    return os.path.expanduser(os.environ.get(STATE_DIR_ENV) or DEFAULT_STATE_DIR)


def project_dir(cwd):
    """Per-project directory, keyed by the same slug that keys user_id."""
    return os.path.join(state_root(), project_slug(cwd))


def _atomic_write(path, text):
    """Write via temp file + rename, so readers never see a partial file.

    The temp file is created in the destination directory: rename is only
    atomic within a filesystem, and /tmp is frequently a different one.
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
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


def ledger_path(hook, cwd):
    return os.path.join(project_dir(cwd), f"{hook}.json")


def read_ledger(hook, cwd):
    """Recent runs of one hook, oldest first. Unreadable/corrupt reads as empty.

    A corrupt ledger is not itself an incident — it is local cache. The run
    that could not be recorded is the loss, and that was already reported when
    it happened.
    """
    try:
        with open(ledger_path(hook, cwd), encoding="utf-8") as fh:
            entries = json.load(fh)
    except (OSError, ValueError):
        return []
    return entries if isinstance(entries, list) else []


def record_run(hook, ok, reason=NO_REASON, elapsed_ms=None, calls=0, cwd=None, extra=None):
    """Append one run to the hook's ledger, keeping the most recent 50.

    Rewrites the whole file rather than appending — see the module docstring.
    Write failures go to stderr and are swallowed: by the time a hook records
    its run the remote work is already done, and failing here would throw away
    a completed run over a local bookkeeping problem.

    Raises ValueError for a reason outside the two tables. That is a
    programming error, caught by the enumeration test rather than in
    production; every caller wraps this in the same try/except it already needs
    for the read-only-directory leg.
    """
    if reason not in ALL_REASONS:
        raise ValueError(
            f"unknown reason {reason!r}: add it to FAILURE_REASONS or "
            f"SKIP_REASONS in _hook_state.py (deciding which one is the point)"
        )
    cwd = cwd or os.getcwd()
    entry = {
        "hook": hook,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ok": bool(ok),
        "reason": reason,
        "elapsed_ms": elapsed_ms,
        "calls": calls,
    }
    if extra:
        entry.update(extra)
    entries = read_ledger(hook, cwd)
    entries.append(entry)
    entries = entries[-LEDGER_LIMIT:]
    try:
        _atomic_write(ledger_path(hook, cwd), json.dumps(entries, ensure_ascii=False))
    except OSError as exc:
        print(f"[{hook}] could not write ledger: {exc}", file=sys.stderr)
    return entry


def state_path(name, cwd):
    return os.path.join(project_dir(cwd), f"{name}.state.json")


def read_state(name, cwd):
    """Return ``(state, reasons)``.

    A missing file is a first run, not a problem: ``({}, [])``. A corrupt one
    rebuilds from empty and reports ``unknown``, because the hook is about to
    behave as though it had never synced anything and that is worth seeing.
    """
    path = state_path(name, cwd)
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}, []
    except (OSError, ValueError):
        return {}, ["unknown"]
    if not isinstance(data, dict):
        return {}, ["unknown"]
    return data, []


def write_state(name, data, cwd):
    """Atomically write state under an exclusive lock. Returns reasons.

    ``["lock_unavailable"]`` means the write happened without the lock held:
    the filesystem refused to lock (NFS and friends), and carrying on unlocked
    is better than dropping the state — but it has to be visible, so the caller
    records it as a failure reason.
    """
    reasons = []
    path = state_path(name, cwd)
    lock_path = path + ".lock"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    lock_fd = None
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    except OSError as exc:
        if exc.errno not in (errno.EACCES, errno.EPERM, errno.EROFS):
            raise
        reasons.append("lock_unavailable")

    if lock_fd is not None:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
        except OSError:
            reasons.append("lock_unavailable")
    try:
        _atomic_write(path, json.dumps(data, ensure_ascii=False, indent=2))
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(lock_fd)
    return reasons


def identity_drift(previous, current):
    """``["identity_changed"]`` when the container id moved between runs.

    Worth reporting rather than absorbing: rows written under the old id are
    indistinguishable from another container's, so the read side will start
    treating this machine's own history as a peer's.
    """
    if previous and current and previous != current:
        return ["identity_changed"]
    return []
