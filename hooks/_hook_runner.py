#!/usr/bin/env python3
"""Shared "run the work under a deadline, then bound the ledger write" hook
skeleton (TASK-005, the "third user" rule from TASK-002's notes: session_capture.py
and session_inject.py each carried a byte-identical copy of this skeleton, and
handoff_sync.py -- the next SessionEnd hook -- would have made three).

Pure stdlib, and imports nothing from this plugin: unlike ``_hook_state`` (which
hard-imports ``fcntl`` for its file locks) this module must stay importable on
any platform a hook itself would otherwise run on, because unlike the ledger
-- which every hook already treats as optional bookkeeping -- the deadline
mechanics here wrap the hook's actual job. unittest's `threading.excepthook`
and `os._exit` are the only "spooky" stdlib surfaces touched, and both are
documented at the call site below.

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

    ``write``'s own exceptions must not escape this function: a write that
    raises past whatever handling it already has would otherwise surface
    through ``threading.excepthook`` at interpreter shutdown -- exactly the
    non-zero-exit risk ``finish`` (below) exists to route around. Every
    current caller already catches its own exceptions inside ``write`` (and
    reports them on stderr, so nothing is lost); this is the net under a
    future one that does not.

    Built on ``run_with_deadline`` rather than duplicating its thread
    start/join: the two races are the same mechanism at different budgets,
    and ``write``'s return value is never used, so its ``outcome`` is
    discarded.
    """

    def guarded():
        try:
            write()
        except Exception:
            pass

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

    Without a left-behind thread there is nothing to protect against, so the
    ordinary ``sys.exit(0)`` is used instead: it runs the normal shutdown
    sequence, which matters for anything elsewhere in the interpreter that
    relies on it (buffered file writes, atexit handlers).
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
