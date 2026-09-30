#!/usr/bin/env python3
"""Shared "run the work under a deadline, then bound the ledger write" hook
skeleton (TASK-005, the "third user" rule from TASK-002's notes: session_capture.py
and session_inject.py each carried a byte-identical copy of this skeleton, and
handoff_sync.py -- the next SessionEnd hook -- would have made three).

Pure stdlib, and imports nothing from this plugin: unlike ``_hook_state`` (which
hard-imports ``fcntl`` for its file locks) this module must stay importable on
any platform a hook itself would otherwise run on, because unlike the ledger
-- which every hook already treats as optional bookkeeping -- the deadline
mechanics here wrap the hook's actual job. ``os._exit`` is the only "spooky"
stdlib surface touched, documented at the call site below (an earlier
revision of this paragraph also named ``threading.excepthook`` as touched
here; this module neither sets nor reads it -- R3-c15).

Hook-specific policy stays OUT of this file on purpose: what "the work" is,
what a reason string means, what gets printed to stderr and why, the ledger's
own shape -- all of that stays in each hook. This module only knows about
threads, deadlines, and how the process ends.

Two races, one shape, for the same reason both times: the host only uses the
stdout of a hook that exits 0, and a hook the host has to kill leaves nothing
behind -- no output, no ledger row, no trace at all. So neither the work nor
the ledger write may be left to run past the hook's own budget:

  * ``run_with_deadline`` bounds the WORK (the hook's actual job -- an HTTP
    call, a transcript parse, a filesystem walk). urllib's ``timeout`` is per
    socket operation, not per request -- a server that drips a few bytes every
    tick can keep a nominal "6 s" request open for 24 s -- so a deadline
    enforced from OUTSIDE the call is the only thing that reliably bounds it.
    The work runs on a daemon thread and is simply abandoned at the deadline:
    the caller gets ``left_behind=True`` back immediately rather than waiting
    on a socket that may never time out on its own.
  * ``write_with_budget`` bounds the LEDGER WRITE the same way, on its own
    (shorter) budget. Wrapping the write in try/except is not enough on its
    own: the write takes a blocking ``flock``, and a write that STALLS
    (another process holding the lock) keeps this process alive until the
    *host's* timeout kills it -- which would cost the very stdout that was
    written first, before the ledger, specifically to keep it safe from this.

Both races can leave a daemon thread running past the deadline, and the
interpreter is then allowed to exit out from under it -- fine for the work
(nothing downstream reads its result once it is abandoned) but not for the
ledger write: ordinary interpreter shutdown flushes stdout again, and if the
abandoned thread happens to be mid-print on stderr at that exact instant,
CPython can raise "could not acquire lock for <stderr>" on the way out -- a
non-zero exit that, for a SessionStart hook, would cost the very brief this
whole file exists to protect (argued, not reproduced deterministically: see
the two hooks' pre-merge history). ``finish`` is the shared tail every hook's
``__main__`` block ends on: flush both streams by hand and call
``os._exit(0)`` (skips the second, risky flush and every other shutdown step)
whenever a thread might have been left running, ``sys.exit(0)`` -- the normal
shutdown path -- otherwise.
"""

import os
import sys
import threading


def run_with_deadline(target, budget, name):
    """Run ``target()`` (no arguments) on a daemon thread, abandoning it
    after ``budget`` seconds. Returns ``(outcome, left_behind)``.

    ``outcome`` holds at most one key:

      * ``"result"`` -- what ``target()`` returned, whatever that value is
        (including ``None`` / ``False`` / ``0``: callers must test
        ``"result" in outcome``, never truthiness).
      * ``"error"`` -- the ``Exception`` ``target()`` raised.

    Neither key is present when ``left_behind`` is True (the thread never
    finished), and -- deliberately -- neither key is present when
    ``target()`` ends via something ``except Exception`` does not catch,
    most notably ``SystemExit``. That is not an oversight: it is what lets a
    caller's own "the worker ended without a result" fallback treat an
    escaped ``SystemExit`` as the generic unknown-failure case it already
    has to handle for a killed/left-behind thread, rather than papering over
    a control-flow exception as an ordinary one. A ``SystemExit`` escaping a
    non-main thread cannot terminate the process (CPython does not let it);
    the default ``threading.excepthook`` silently drops it for exactly that
    reason, so nothing here needs to catch it to keep the run quiet.

    ``left_behind`` is True when the thread is still alive after ``budget``
    seconds. The caller must not wait for it further -- it is a daemon
    thread named ``name`` and dies with the interpreter, whatever it was
    doing.
    """
    outcome = {}

    def work():
        try:
            outcome["result"] = target()
        except Exception as exc:  # SystemExit and friends propagate -- see above
            outcome["error"] = exc

    worker = threading.Thread(target=work, name=name, daemon=True)
    worker.start()
    worker.join(budget)
    return outcome, worker.is_alive()


def write_with_budget(write, budget, name):
    """Run ``write()`` (no arguments, return value ignored) on a daemon
    thread, abandoning it after ``budget`` seconds. Returns True when it was
    left behind (still running at the deadline -- e.g. blocked on another
    process's held lock).

    ``write``'s own exceptions are caught here and named on stderr, never
    left to escape this function silently (R1-c14). ``run_with_deadline``
    below already stores any exception the target raises in its own
    ``outcome["error"]`` instead of letting it propagate, so catching it a
    second time here is not protecting the interpreter from a crash it was
    never going to have: an earlier revision of this docstring argued the
    opposite -- that skipping this catch would surface a traceback via
    ``threading.excepthook`` "at interpreter shutdown" and risk a non-zero
    exit -- but a bare thread's uncaught exception is in fact reported by
    that hook immediately, when it happens, not at shutdown, and does not
    touch the process's own exit code either way. The reason to catch it
    HERE, specifically, is that this function discards ``run_with_deadline``'s
    ``outcome`` entirely (only ``left_behind`` matters to its own callers):
    without a print in this except clause, a ``write()`` that raises would
    vanish with no trace, the very failure mode ``_hook_state.record_run``'s
    own module docstring warns a hook's ledger must never produce. Every
    current caller's own ``write()`` already catches its own exceptions and
    reports them on stderr too (so this is normally a net under a net); a
    future caller that does not is still covered.

    Built on ``run_with_deadline`` rather than duplicating its thread
    start/join: the two races are the same mechanism at different budgets,
    and ``write``'s return value is never used, so its ``outcome`` is
    discarded.
    """

    def guarded():
        try:
            write()
        except Exception as exc:  # noqa: BLE001 - the last-resort net; see above
            print(f"[{name}] write() raised: {exc!r}", file=sys.stderr)

    _, left_behind = run_with_deadline(guarded, budget, name)
    return left_behind


def finish(left_behind):
    """The shared tail of every hook's ``__main__`` block.

    ``left_behind`` (typically ``main()``'s own return value: the work OR
    the ledger write had to be abandoned) means a daemon thread may still be
    running. Ordinary interpreter shutdown flushes stdout again; against a
    pipe the host has already closed that flush fails, and letting that
    escape on the way out is exit 120 -- a hook error, for a plugin whose
    entire contract is exit 0, always. Flushing by hand first (best-effort:
    a failure here must not block the exit either) and then calling
    ``os._exit(0)`` skips that second flush, and every other interpreter
    shutdown step, entirely -- including the narrower stderr-lock race
    described in the module docstring.

    Without a left-behind thread there is LESS to protect against -- no
    daemon thread racing this shutdown -- but not NOTHING (an earlier
    revision of this paragraph claimed there was, corrected in
    handoff_sync.py's TASK-005 R2 fix round, finding R2-c05): a pipe the
    host has already closed makes THIS path's own ordinary stdout flush
    fail too, the same exit-120 outcome described above, just without a
    daemon thread to blame it on. ``sys.exit(0)`` is still used here
    anyway, because running the normal shutdown sequence matters for
    anything elsewhere in the interpreter that relies on it (buffered file
    writes, atexit handlers), and because there is no daemon thread for
    this path to protect the process FROM.

    A caller cannot cover the closed-pipe case merely by swallowing a
    write's own ``OSError`` (R3-c03, correcting an earlier revision of
    this paragraph that claimed handoff_sync.py's stderr writes already
    did, by doing exactly that): CPython's shutdown sequence
    (``flush_std_files``, behind every plain ``sys.exit()``) flushes
    stdout AND stderr again UNCONDITIONALLY once this function returns
    control to it, and a buffered writer whose earlier ``write()`` raised
    does not discard what it failed to write -- the retry targets the
    SAME closed pipe and fails again, this time with no Python-level
    ``except`` anywhere near it (confirmed empirically against a real
    closed pipe, not a mock stand-in: swallowing every explicit
    ``print(..., file=sys.stderr)`` in a hook still exits 120). The only
    thing that actually stops the retry from failing too is rerouting the
    file descriptor itself to ``os.devnull`` on the first failure --
    ``session_inject._silence_stdout`` for stdout, ``handoff_sync._silence_
    stderr`` for stderr -- so that BOTH this function's own flush above
    and the interpreter's later one land on a descriptor that accepts
    anything.
    """
    if left_behind:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        os._exit(0)
    else:
        sys.exit(0)
