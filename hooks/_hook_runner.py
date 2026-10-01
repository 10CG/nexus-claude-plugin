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
threads, deadlines, and how the process ends -- plus, since Amendment A9-21,
one more process-wide concern every hook that starts a worker thread shares:
``guard_stderr`` below, and the ``_StderrGuard`` class it installs. Each
hook's own ``_warn`` / ``_silence_stderr`` pair (where one exists) stays
local, on purpose: the import guards that may need to report a MISSING
``_hook_runner`` run before this module is importable at all, so they cannot
depend on anything defined here. ``_StderrGuard`` has no such constraint --
nothing calls ``guard_stderr`` until after ``import _hook_runner`` has
already succeeded -- so it lives here once, instead of as a fourth
byte-identical copy (handoff_sync.py's TASK-005 original, plus one each for
session_capture.py / session_inject.py / the next SessionEnd hook to need
it).

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
    write's own ``OSError`` in the ORDINARY sense -- wrapping just THAT one
    call in a try/except (R3-c03, correcting an earlier revision of this
    paragraph that claimed handoff_sync.py's stderr writes already did,
    by doing exactly that): CPython's shutdown sequence
    (``flush_std_files``, behind every plain ``sys.exit()``) flushes
    stdout AND stderr again UNCONDITIONALLY once this function returns
    control to it, and a buffered writer whose earlier ``write()`` raised
    does not discard what it failed to write -- the retry targets the
    SAME closed pipe and fails again (confirmed empirically against a real
    closed pipe, not a mock stand-in: swallowing every explicit
    ``print(..., file=sys.stderr)`` in a hook still exits 120 unless
    something ELSE also protects this later, unconditional retry).

    Two DIFFERENT things stand between that retry and exit 120 today, one
    per stream -- STDOUT's predates Amendment A9-21 and is unchanged;
    STDERR's is what that amendment added (A9-21 R1 fix round, finding
    C4 -- corrects an earlier revision of this paragraph, which named only
    the reroute below and did not yet distinguish the two streams, written
    before the guard class existed here at all):

      * STDOUT has no guard object wrapping it. ``session_inject.
        _silence_stdout`` reroutes the underlying file descriptor itself
        to ``os.devnull`` on the FIRST failed write, so this function's
        own ``sys.stdout.flush()`` above, and the interpreter's later
        unconditional one, both land on a descriptor that accepts
        anything -- reroute is the WHOLE story for this stream; nothing
        here catches a stdout ``OSError`` directly.
      * STDERR, once ``guard_stderr()`` has installed a ``_StderrGuard``
        as ``sys.stderr`` (every hook that starts a worker thread calls it
        before doing so), is different: this function's own
        ``sys.stderr.flush()`` above, and CPython's later unconditional
        one, both go THROUGH that guard, whose own ``except OSError``
        swallows the failure directly -- the SAME protection every other
        stderr write in the process gets from it once installed. That is
        what actually stops THIS retry from reaching Python with no
        ``except`` anywhere near it; the guard's own reroute
        (``_hook_runner._silence_stderr``, called from that same
        ``except``) is a SECOND, independent layer on top -- it is what
        makes a LATER write through the SAME guard actually succeed
        rather than merely not raise, not what protects this one.

    Before any guard is installed at all -- the import guards each hook
    runs before anything in this module is even imported -- there is no
    ``_StderrGuard`` yet for an ``except OSError`` to live on, and a
    hook's own LOCAL ``_silence_stderr`` (handoff_sync.py's original;
    session_capture.py / session_inject.py each gained an identical local
    copy of their own in the A9-21 R1 fix round) is the only thing on
    that narrower, earlier path -- reroute, by itself, is the whole story
    THERE.
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


# ── shared stderr guard (Amendment A9-21) ───────────────────────────────

def _silence_stderr(stream):
    """After a failed write THROUGH an installed ``_StderrGuard``, stop the
    interpreter retrying it on the way out -- the ``_StderrGuard.write`` /
    ``.flush`` methods below call this, from their own ``except OSError``,
    passing their OWN ``self._real`` (A9-21 R1 fix round, finding C3: an
    earlier revision of this function took no argument and read the
    GLOBAL ``sys.stderr`` instead -- in every current hook path those agree
    by construction, since nothing calls ``guard_stderr()``'s installed
    guard's ``write``/``flush`` before ``sys.stderr`` IS that guard, but a
    test -- or a future caller -- that builds a ``_StderrGuard`` directly
    WITHOUT first installing it as ``sys.stderr`` does not: the old
    no-argument form would then silence whatever ``sys.stderr`` happened
    to be at that moment -- in a real process, the actual fd 2 -- instead
    of the stream this guard actually wraps).

    This is NOT the same function as a hook's own local ``_silence_stderr``
    (handoff_sync.py keeps one, for its import guards -- see the module
    docstring above for why that copy cannot simply call this one instead):
    this copy exists only to back the guard CLASS that lives here now, and
    is never called before ``guard_stderr()`` has installed that guard.

    ``stream.fileno()`` is resolved BEFORE ``os.open``: a stream with no
    real descriptor at all (a test double, or ``self._real is None`` --
    the guard checks that itself before ever calling this) never leaves a
    target-less devnull fd open for the blanket ``except Exception: pass``
    below to silently leak -- the reverse order opened the devnull fd
    first, and a failing ``fileno()`` afterward left it dangling (this
    ordering carries over the same fix handoff_sync.py's own local copy
    already made, R4-c08 in its history, now made once here instead of
    risked again per copy). For an INSTALLED guard this is the same
    descriptor ``sys.stderr.fileno()`` would have resolved to anyway (the
    guard's own ``fileno()`` delegates to ``self._real.fileno()``), so
    production behaviour is unchanged; only a caller that never installed
    the guard it is using sees a different (correct) target now.
    """
    try:
        target_fd = stream.fileno()
        devnull = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(devnull, target_fd)
        finally:
            os.close(devnull)
    except Exception:
        pass  # not a real file descriptor (tests), or nothing left to protect


class _StderrGuard:
    """Wraps ``sys.stderr`` so that ANY later write to it cannot raise out
    into its caller, and so a ``None`` stream (fd 2 closed before the
    interpreter even started) cannot silently fall back to ``sys.stdout``
    either (what a bare ``print(msg, file=sys.stderr)`` does when
    ``sys.stderr`` is literally ``None`` -- confirmed empirically -- which
    would put a diagnostic line on the one channel a SessionEnd hook's
    contract requires to stay empty, or inside a SessionStart hook's own
    injected-context payload).

    Originally handoff_sync.py's own class (TASK-005); moved here by
    Amendment A9-21 so ``guard_stderr()`` can install the SAME protection
    for session_capture.py and session_inject.py (and any future hook that
    starts a worker thread) with one call each, rather than a byte-identical
    copy of this class per hook. Only the class moved -- see the module
    docstring above for why each hook's own ``_warn`` / ``_silence_stderr``,
    where one exists, stays local.

    Once installed (see ``guard_stderr`` below), every module that does
    ``print(..., file=sys.stderr)`` -- this file's own ``write_with_budget``
    fallback, a hook's own diagnostic prints, a sibling module's unguarded
    ones (``_ingest_client``'s, in particular: several print sites of its
    own, never routed through any hook's ``_warn``) -- looks up
    ``sys.stderr`` FRESH at call time, so replacing the global attribute
    here protects writes from ANY of them, not just whichever hook installed
    it.

    A write failing here is swallowed by the ``except OSError`` below on
    EVERY call through this object, not only the first -- including the
    ``.flush()`` CPython's own unconditional reflush at shutdown makes
    against this SAME guard again. ``_silence_stderr()``, run on that first
    failure, additionally reroutes the underlying file descriptor to
    ``os.devnull`` so a LATER write through this object does not merely get
    swallowed but actually succeeds -- a second, non-redundant layer for
    anything that reads the written bytes back (nothing in this plugin
    does, but a future consumer might).

    ``self._real`` may itself be ``None`` -- ``sys.stderr`` is ``None``,
    never a stream object, when fd 2 was already closed BEFORE the
    interpreter even started (a pipe that closes MID-run is a direct, live
    stream object instead, whose ``write`` simply starts raising). ``write``
    and ``flush`` each check ``self._real is None`` independently and no-op
    before ever touching it: losing JUST the ``write`` guard lets whichever
    diagnostic call this class exists to protect hit ``None.write(...)``,
    raising ``AttributeError`` the ``except OSError`` here does not catch
    (on a worker thread this is caught one layer up, by
    ``run_with_deadline``'s own blanket ``except Exception``, and resolves
    to the generic ``unknown`` reason -- quieter than a crash, but still the
    same "silently abandons whatever call it was mid-loop on" failure this
    class exists to prevent); losing JUST the ``flush`` guard instead means
    CPython's own unconditional shutdown-time ``sys.stderr.flush()`` -- made
    regardless of whether ANY write was ever attempted through this object
    -- hits ``None.flush()`` with no Python-level ``except`` anywhere near
    it, which is exit code 120, the same code a genuinely broken pipe
    produces at that same call site. The two guards protect two DIFFERENT
    call sites with a different, unequal consequence each; neither makes
    the other redundant.

    There is no real file descriptor behind a ``None`` stream to redirect
    either, so both methods simply no-op in that case, same as after
    ``_silence_stderr`` has already run once for a real one.
    """

    def __init__(self, real):
        self._real = real

    def write(self, s):
        if self._real is None:
            return len(s)
        try:
            return self._real.write(s)
        except OSError:
            _silence_stderr(self._real)
            return len(s)

    def flush(self):
        if self._real is None:
            return
        try:
            self._real.flush()
        except OSError:
            _silence_stderr(self._real)

    def fileno(self):
        return self._real.fileno()

    def __getattr__(self, name):
        return getattr(self._real, name)


def guard_stderr():
    """Idempotently wrap the CURRENT ``sys.stderr`` in a ``_StderrGuard`` and
    install it as the new global ``sys.stderr``. Returns the installed guard
    (always the object already there, if this was already called) -- most
    callers just want the side effect and can ignore the return value.

    Call this ONCE, on the MAIN thread, before starting a hook's worker
    thread (``run_with_deadline`` above), and before anything else on that
    thread may write a diagnostic. Installing it there -- not later, and not
    per call site -- is what lets one call protect every later stderr write
    for the rest of the process (see ``_StderrGuard`` above): the work
    thread's own calls into a sibling module, this file's own
    ``write_with_budget`` fallback print, and the main thread's own
    post-join diagnostic all look up ``sys.stderr`` fresh, after this
    function has already run.

    Idempotent, checking by TYPE rather than unconditionally re-wrapping:
    a second call -- repeated in-process ``main()`` calls within the same
    test process; production runs this once per process, so it never
    matters there -- sees ``sys.stderr`` already a ``_StderrGuard`` and
    leaves it alone, rather than wrapping a guard in a second one (harmless
    on its own, since every method already tolerates a chain, but pointless
    and it would leave ``guard._real`` one layer removed from the thing
    that can actually fail).
    """
    if not isinstance(sys.stderr, _StderrGuard):
        sys.stderr = _StderrGuard(sys.stderr)
    return sys.stderr
