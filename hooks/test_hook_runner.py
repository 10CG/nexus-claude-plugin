#!/usr/bin/env python3
"""Tests for hooks/_hook_runner.py (TASK-005, the shared hook-run skeleton
consolidated out of session_capture.py / session_inject.py -- the "third
user" rule from TASK-002's notes).

Runnable as: python3 hooks/test_hook_runner.py   (stdlib unittest only)

_hook_runner has no filesystem or network surface of its own -- it is pure
threading and process-exit mechanics -- so unlike the hook test suites this
file needs no fake-HOME / NEXUS_HOOK_STATE_DIR module fixture: nothing here
reaches a real hook's main(), and nothing here writes anywhere.
"""

import io
import os
import sys
import threading
import time
import unittest
from unittest import mock

import _hook_runner


def _assert_stderr_not_left_wrapped():
    """Backstop, mirroring test_handoff_sync.py's own (that file sorts
    first, alphabetically, among this plugin's test_*.py files, and carries
    the larger-surface version of this same check): every test below that
    installs a ``_StderrGuard`` does so through ``mock.patch.object(sys,
    "stderr", ...)``, which restores the ORIGINAL value on exit regardless
    of what the code under test did to the attribute meanwhile -- this
    catches a future test here that reassigns ``sys.stderr`` directly
    instead and forgets to restore it. This file sorts second, right after
    test_handoff_sync.py, so a leak here would still reach every other
    test_*.py module run in the same ``unittest discover`` process."""
    if isinstance(sys.stderr, _hook_runner._StderrGuard):
        raise AssertionError(
            "a test in this file left sys.stderr wrapped in _StderrGuard -- "
            "every OTHER test_*.py module run in this same `unittest "
            "discover` process would inherit it"
        )


def setUpModule():
    unittest.addModuleCleanup(_assert_stderr_not_left_wrapped)


# ════════════════════════════════════════════════════════════════════════════════
# run_with_deadline
# ════════════════════════════════════════════════════════════════════════════════

class TestRunWithDeadline(unittest.TestCase):

    def test_the_result_comes_back_under_the_result_key(self):
        outcome, left_behind = _hook_runner.run_with_deadline(lambda: "the-answer", 5, "t")
        self.assertEqual(outcome, {"result": "the-answer"})
        self.assertFalse(left_behind)

    def test_a_falsy_result_still_counts_as_a_result(self):
        """Callers key off ``"result" in outcome``, never truthiness -- a
        target that legitimately returns None / False / 0 (session_capture's
        _collect returns plain reason strings, but nothing here should rely
        on that) must not be confused with a worker that never finished."""
        for value in (None, False, 0, ""):
            with self.subTest(value=value):
                outcome, left_behind = _hook_runner.run_with_deadline(lambda v=value: v, 5, "t")
                self.assertEqual(outcome, {"result": value})
                self.assertFalse(left_behind)

    def test_a_raised_exception_comes_back_under_the_error_key(self):
        boom = RuntimeError("boom")

        def target():
            raise boom

        outcome, left_behind = _hook_runner.run_with_deadline(target, 5, "t")
        self.assertEqual(outcome, {"error": boom})
        self.assertFalse(left_behind)

    def test_systemexit_leaves_neither_key(self):
        """SystemExit is not an Exception: it must propagate out of the
        worker thread rather than being caught as an ordinary error, leaving
        `outcome` empty -- the same shape a caller sees when the deadline
        catches the worker mid-flight, so its "worker ended without a
        result" fallback covers both without needing to know which happened.
        threading.excepthook is patched to a no-op the same way the two
        hooks' own SystemExit tests do: CPython's default excepthook already
        drops a SystemExit escaping a non-main thread silently (it cannot
        terminate the process from there), but patching it here keeps this
        test's output clean regardless of interpreter version."""
        def target():
            raise SystemExit(3)

        with mock.patch.object(threading, "excepthook", lambda args: None):
            outcome, left_behind = _hook_runner.run_with_deadline(target, 5, "t")
        self.assertEqual(outcome, {})
        self.assertFalse(left_behind)

    def test_budget_exceeded_leaves_a_named_daemon_thread_running_behind(self):
        release = threading.Event()
        self.addCleanup(release.set)  # let the abandoned worker finish and exit

        began = time.monotonic()
        outcome, left_behind = _hook_runner.run_with_deadline(
            lambda: release.wait(30), 0.2, "stall-work"
        )
        self.assertLess(time.monotonic() - began, 5, "must not wait for the stalled worker")
        self.assertTrue(left_behind)
        self.assertEqual(outcome, {}, "an abandoned worker has reported neither result nor error")

        stalled = next(t for t in threading.enumerate() if t.name == "stall-work")
        self.assertTrue(stalled.daemon, "must not be able to keep the interpreter alive on its own")

    def test_a_budget_that_stopped_working_fails_fast_not_slow(self):
        """R1-c34 (non-mandatory): a target blocked on a threading.Event
        that is NEVER set catches a `worker.join(budget) -> worker.join()`
        regression in under a second. The existing budget-exceeded test
        above (bounded by its own `release.wait(30)`) would also go red
        under that mutant, but only after riding out the whole 30 s wait --
        a slow red on a regression that should be instant. An
        immediately-returning target (the other natural choice for a fast
        test) would not catch this mutant at all: with or without a budget,
        it returns before either `join()` call matters.

        Runs `run_with_deadline` on a watchdog thread of THIS test's own so
        it fails within its own bound even if the code under test regressed
        to blocking forever, rather than hanging the whole suite.
        """
        never_set = threading.Event()
        result = {}

        def call_it():
            result["value"] = _hook_runner.run_with_deadline(
                lambda: never_set.wait(), 0.2, "never-set-work"
            )

        watchdog = threading.Thread(target=call_it, daemon=True)
        began = time.monotonic()
        watchdog.start()
        watchdog.join(1.0)
        elapsed = time.monotonic() - began
        self.assertFalse(
            watchdog.is_alive(), f"run_with_deadline did not return within 1s (took >{elapsed:.1f}s)"
        )
        outcome, left_behind = result["value"]
        self.assertTrue(left_behind)
        self.assertEqual(outcome, {})

    def test_target_is_called_with_no_arguments_on_its_own_thread(self):
        """Callers close over their own state (e.g. ``lambda: _collect(run)``);
        run_with_deadline itself must call target() with nothing, on a
        thread of its own -- not the caller's."""
        seen = {}

        def target():
            seen["thread"] = threading.current_thread().name
            return "ok"

        outcome, left_behind = _hook_runner.run_with_deadline(target, 5, "named-worker")
        self.assertEqual((outcome, left_behind), ({"result": "ok"}, False))
        self.assertEqual(seen["thread"], "named-worker")
        self.assertNotEqual(seen["thread"], threading.current_thread().name)


# ════════════════════════════════════════════════════════════════════════════════
# write_with_budget
# ════════════════════════════════════════════════════════════════════════════════

class TestWriteWithBudget(unittest.TestCase):

    def test_a_normal_write_completes_and_is_not_left_behind(self):
        calls = []
        left_behind = _hook_runner.write_with_budget(lambda: calls.append(1), 5, "w")
        self.assertFalse(left_behind)
        self.assertEqual(calls, [1], "write() must actually have run")

    def test_an_exception_inside_write_does_not_escape(self):
        def write():
            raise RuntimeError("the caller forgot to catch this")

        # Must not raise here, and must not report left_behind: the thread
        # ended (badly) well inside the budget -- this is the net under a
        # write() that does not already guard itself (every current caller's
        # write() does; see the module docstring).
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            left_behind = _hook_runner.write_with_budget(write, 5, "w")
        self.assertFalse(left_behind)
        # R1-c14: the exception must not vanish with no trace either -- this
        # function discards run_with_deadline's own outcome entirely, so
        # without this print the only copy of `exc` anywhere is gone.
        self.assertIn("w", stderr.getvalue())
        self.assertIn("the caller forgot to catch this", stderr.getvalue())

    def test_budget_exceeded_reports_left_behind(self):
        release = threading.Event()
        self.addCleanup(release.set)

        began = time.monotonic()
        left_behind = _hook_runner.write_with_budget(lambda: release.wait(30), 0.2, "stall-write")
        self.assertLess(time.monotonic() - began, 5, "must not wait for the stalled write")
        self.assertTrue(left_behind)
        names = [t.name for t in threading.enumerate()]
        self.assertIn("stall-write", names, "the abandoned write must still be a live, named thread")


# ════════════════════════════════════════════════════════════════════════════════
# _silence_stderr / _StderrGuard / guard_stderr (Amendment A9-21)
# ════════════════════════════════════════════════════════════════════════════════

class _BrokenStderr:
    """A stand-in for ``sys.stderr`` whose ``write`` always raises
    ``BrokenPipeError`` -- what a real closed pipe (the host process has
    already exited) looks like to a ``print(..., file=sys.stderr)`` call.
    Duplicated from test_handoff_sync.py's own copy of the same fixture
    rather than imported: every test_*.py file here is independently
    runnable (see the module docstring), and this one is a handful of
    lines."""

    def write(self, *args, **kwargs):
        raise BrokenPipeError("stderr closed")

    def flush(self):
        raise BrokenPipeError("stderr closed")


class TestSilenceStderr(unittest.TestCase):
    """``_hook_runner._silence_stderr`` backs ``_StderrGuard`` below -- a
    SEPARATE copy from any hook's own local ``_silence_stderr`` (handoff_
    sync.py keeps one, for its import guards; see this module's own
    docstring for why that copy cannot simply call this one instead).

    Takes the target stream explicitly (A9-21 R1 fix round, finding C3: an
    earlier revision took no argument and read the GLOBAL ``sys.stderr``
    instead -- see the function's own docstring for why that silences the
    WRONG stream for a guard used without first being installed as
    ``sys.stderr``). This test passes its stand-in stream straight to the
    function rather than patching the global, which is no longer what the
    function even looks at."""

    def test_does_not_leak_a_devnull_fd_when_fileno_is_unavailable(self):
        """Mirrors test_handoff_sync.py's own pin for its hook-local copy:
        resolving ``stream.fileno()`` BEFORE ``os.open`` means a
        target-less devnull fd is never opened in the first place when
        ``fileno()`` itself raises (a test double, or any future ``sys.
        stderr`` replacement with no real descriptor behind it) -- the
        reverse order would open one and leave it dangling, swallowed by
        the same blanket ``except Exception: pass``."""
        opened = []
        real_open = os.open

        def tracking_open(path, flags):
            fd = real_open(path, flags)
            opened.append(fd)
            return fd

        with mock.patch.object(_hook_runner.os, "open", side_effect=tracking_open):
            _hook_runner._silence_stderr(_BrokenStderr())  # no .fileno() at all; must not raise
        for fd in opened:
            with self.assertRaises(OSError):
                os.fstat(fd)  # fstat on a closed fd raises EBADF; a leaked one would not

    def test_redirects_the_passed_streams_own_fd_not_a_different_one(self):
        """R2-K1: the test above (and every other test in this class) passes
        a stand-in object straight to the function -- none of them can tell
        ``stream.fileno()`` (the argument, correct since C3) apart from
        ``sys.stderr.fileno()`` (the GLOBAL, what an earlier revision read
        instead) in a case where the two are not already the same fd by
        construction. This one gives the function a private pipe's write
        end -- deliberately never fd 2 -- and checks THAT descriptor, not
        merely that nothing raised. Found, by mutation, to be the gap it
        looks like: reverting line 282 back to ``sys.stderr.fileno()`` --
        the exact pre-C3 shape -- redirects the real fd 2 instead and the
        REST of this file's own test output silently vanishes into
        /dev/null from that point on (no failure text, no summary line,
        only a non-zero exit code) -- the live consequence ``guard_stderr``
        exists to prevent, reproduced here against this suite's own fd 2
        rather than argued about.

        Protects the real fd 2 regardless of which way this goes: if the
        regression above is present, this call redirects the test process's
        OWN real stderr to devnull instead of the pipe, and the restore
        below undoes exactly that before the test ends."""
        saved = os.dup(2)
        self.addCleanup(os.close, saved)
        self.addCleanup(os.dup2, saved, 2)  # LIFO: restore fd 2 BEFORE closing `saved`
        before_fd2 = os.fstat(2)

        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)

        class _PipeStream:
            def fileno(self):
                return write_fd

        try:
            _hook_runner._silence_stderr(_PipeStream())
            self.assertTrue(
                os.path.samestat(os.fstat(write_fd), os.stat(os.devnull)),
                "the STREAM's own fd must now point at devnull",
            )
            self.assertTrue(
                os.path.samestat(os.fstat(2), before_fd2),
                "fd 2 (this test process's real stderr) must be untouched",
            )
        finally:
            os.close(write_fd)


class TestStderrGuard(unittest.TestCase):
    """``_StderrGuard`` itself (moved here from handoff_sync.py by Amendment
    A9-21) plus its integration with ``write_with_budget`` above -- that
    function's own fallback ``print(..., file=sys.stderr)`` is a bare,
    unguarded write too (a "net under a net" for a ``write()`` that does not
    already protect itself, per this module's own docstring). Once a
    ``_StderrGuard`` is installed, it protects THIS print as well, since
    ``write_with_budget`` looks up ``sys.stderr`` fresh at call time same as
    everything else -- the fix is not scoped to only one hook's own
    diagnostic call sites."""

    def test_write_and_flush_delegate_to_the_real_stream_when_it_works(self):
        """The common case: wrapping must not change ordinary, successful
        behaviour -- write still delivers the bytes and returns the real
        stream's own return value, flush still flushes."""
        real = io.StringIO()
        guard = _hook_runner._StderrGuard(real)
        self.assertEqual(guard.write("hello"), 5)
        self.assertEqual(real.getvalue(), "hello")
        guard.flush()  # must not raise; io.StringIO.flush() is a no-op

    def test_write_with_budget_completes_even_when_its_own_fallback_print_also_fails(self):
        """Pins the LAYERED integration: ``write_with_budget`` completes
        (not left behind) even when its target raises AND the fallback
        diagnostic print that follows also fails -- but this does NOT, on
        its own, pin ``_StderrGuard``'s OWN ``except OSError`` specifically:
        ``run_with_deadline``'s own OUTER ``except Exception`` (``work()``
        above) would swallow whatever escaped ``_StderrGuard`` just the
        same, so ``left_behind`` reads ``False`` whether or not
        ``_StderrGuard`` protects anything at all. The tests below call the
        class directly, which is what actually pins its own contract."""
        guarded_stderr = _hook_runner._StderrGuard(_BrokenStderr())  # no real fd behind it either
        with mock.patch.object(sys, "stderr", guarded_stderr):
            def failing_write():
                raise RuntimeError("boom, escapes write()'s own protections")

            left_behind = _hook_runner.write_with_budget(failing_write, 2.0, "probe")
        self.assertFalse(left_behind)  # the thread completed; nothing hung or crashed the test

    def test_wrapping_a_broken_real_stream_does_not_raise(self):
        """The direct pin the test above cannot provide -- calls
        ``_StderrGuard``'s own ``write``/``flush`` straight, with nothing
        upstream able to paper over a regression here. Since C3, the guard's
        own ``except OSError`` calls ``_silence_stderr(self._real)`` -- the
        stream THIS guard wraps, never the global ``sys.stderr`` -- so it
        delegates through ``_BrokenStderr``'s missing ``fileno()`` (an
        ``AttributeError``, swallowed by ``_silence_stderr``'s own blanket
        except) regardless of what ``sys.stderr`` happens to be at the time.
        Patching ``sys.stderr`` to this SAME guard object below is therefore
        not load-bearing for THIS test -- it is only what the comment block
        further down (ahead of ``test_write_failure_silences_the_wrapped_
        stream_not_the_global_one``) calls the "invisible either way" case,
        kept so this test does not quietly depend on whatever ``sys.stderr``
        is left as by an earlier test. ``TestSilenceStderr.test_redirects_
        the_passed_streams_own_fd_not_a_different_one`` above is what
        actually pins the argument-vs-global distinction itself, with a
        real fd instead of a mocked ``_silence_stderr``."""
        guard = _hook_runner._StderrGuard(_BrokenStderr())
        with mock.patch.object(sys, "stderr", guard):
            self.assertEqual(guard.write("x"), 1)  # swallowed, not raised
            guard.flush()  # must not raise either

    def test_passes_through_unknown_attributes(self):
        """``__getattr__`` is purely defensive -- nothing in this plugin
        currently reads anything off ``sys.stderr`` beyond ``write`` /
        ``flush`` / ``fileno`` (all three explicitly defined), but a future
        consumer (stdlib code, a sibling module, ``_ingest_client``)
        reading e.g. ``sys.stderr.encoding`` off an already-installed guard
        would otherwise hit a bare ``AttributeError`` with nothing here to
        catch that regression."""
        class _Extra:
            encoding = "utf-8"

            def isatty(self):
                return False

        real = _Extra()
        guard = _hook_runner._StderrGuard(real)
        self.assertEqual(guard.encoding, "utf-8")
        self.assertFalse(guard.isatty())  # a bound method, delegated and callable
        with self.assertRaises(AttributeError):
            guard.does_not_exist_anywhere

    def test_wrapping_none_does_not_raise(self):
        """``sys.stderr is None`` is the interpreter-startup shape (fd 2
        closed before the interpreter even started): ``guard_stderr()``
        wraps WHATEVER ``sys.stderr`` currently is, ``None`` included, and
        any LATER write through that guard (from a hook's own ``_warn``, or
        from ``_ingest_client``'s own unguarded prints -- ruling 15, this
        plugin's shared ingest client is not touched to add its own guards)
        must not raise ``AttributeError`` calling ``.write``/``.flush`` on a
        ``None`` ``_real``."""
        guard = _hook_runner._StderrGuard(None)
        self.assertEqual(guard.write("x"), 1)
        guard.flush()  # must not raise

    # ── A9-21 R1 fix round (findings C3 / C8): the except branches below
    # must silence THIS guard's own wrapped stream (``self._real``), not
    # whatever ``sys.stderr`` happens to be at call time. Every test above
    # keeps that distinction invisible by first patching the GLOBAL
    # ``sys.stderr`` to be this SAME guard (`mock.patch.object(sys,
    # "stderr", guard)`) -- in production, once ``guard_stderr()`` has
    # installed a guard, ``sys.stderr`` and ``self._real`` agree by
    # construction, so every existing hook path exercises this correctly
    # either way. A test (or any future caller) that instead builds a
    # ``_StderrGuard`` directly and uses it WITHOUT installing it as
    # ``sys.stderr`` first does not: the old, no-argument
    # ``_silence_stderr()`` read the global ``sys.stderr`` regardless,
    # silencing -- in a real process -- the WRONG descriptor (the actual
    # fd 2 of whatever process is running, not this guard's own broken
    # stream). Patching ``_hook_runner._silence_stderr`` itself, rather
    # than driving a real descriptor, is what lets this test assert the
    # exact call without ever touching a real fd either way.

    def test_write_failure_silences_the_wrapped_stream_not_the_global_one(self):
        real = _BrokenStderr()
        guard = _hook_runner._StderrGuard(real)
        with mock.patch.object(_hook_runner, "_silence_stderr") as fake_silence:
            self.assertEqual(guard.write("x"), 1)  # still swallowed, not raised
        fake_silence.assert_called_once_with(real)

    def test_flush_failure_silences_the_wrapped_stream_not_the_global_one(self):
        real = _BrokenStderr()
        guard = _hook_runner._StderrGuard(real)
        with mock.patch.object(_hook_runner, "_silence_stderr") as fake_silence:
            guard.flush()  # still swallowed, not raised
        fake_silence.assert_called_once_with(real)

    def test_a_successful_write_never_silences_anything(self):
        guard = _hook_runner._StderrGuard(io.StringIO())
        with mock.patch.object(_hook_runner, "_silence_stderr") as fake_silence:
            self.assertEqual(guard.write("x"), 1)
        fake_silence.assert_not_called()

    def test_a_none_real_never_silences_anything(self):
        """``self._real is None`` no-ops before ever reaching the
        ``except OSError`` in either method -- there is nothing to
        silence."""
        guard = _hook_runner._StderrGuard(None)
        with mock.patch.object(_hook_runner, "_silence_stderr") as fake_silence:
            guard.write("x")
            guard.flush()
        fake_silence.assert_not_called()


class TestGuardStderr(unittest.TestCase):
    """``guard_stderr()`` itself: the idempotent installer every hook that
    starts a worker thread now calls once, before starting it (handoff_
    sync.py / session_capture.py / session_inject.py each do)."""

    def test_wraps_the_current_stderr(self):
        real = io.StringIO()
        with mock.patch.object(sys, "stderr", real):
            guard = _hook_runner.guard_stderr()
        self.assertIsInstance(guard, _hook_runner._StderrGuard)
        self.assertIs(guard._real, real)

    def test_wraps_none(self):
        """fd 2 closed before the interpreter starts: ``sys.stderr`` is
        ``None``, not a stream -- ``guard_stderr()`` must still wrap it
        rather than raising or skipping the install."""
        with mock.patch.object(sys, "stderr", None):
            guard = _hook_runner.guard_stderr()
        self.assertIsInstance(guard, _hook_runner._StderrGuard)
        self.assertIsNone(guard._real)

    def test_is_idempotent_across_repeated_calls(self):
        """A second call, under the SAME unrestored patch (not two separate
        ``with`` blocks, which would each start from a fresh, unwrapped
        value and never actually exercise the isinstance check), must not
        wrap a ``_StderrGuard`` in a second one -- the installed guard's own
        ``_real`` must still point at the ORIGINAL stream, not at a first
        guard layer, and both calls must return the identical object."""
        sentinel = _BrokenStderr()
        with mock.patch.object(sys, "stderr", sentinel):
            first = _hook_runner.guard_stderr()
            second = _hook_runner.guard_stderr()
            self.assertIs(first, second)
            self.assertIs(sys.stderr, first)
            self.assertIs(first._real, sentinel)  # not wrapping `first` itself


# ════════════════════════════════════════════════════════════════════════════════
# finish
# ════════════════════════════════════════════════════════════════════════════════

class TestFinish(unittest.TestCase):
    """Both branches mock BOTH exit functions: real os._exit / sys.exit would
    otherwise tear down the test process (os._exit) or abort the test with an
    uncaught SystemExit (sys.exit). Mocking both, in both tests, also makes a
    regression that calls the wrong one (or both) a visible assertion
    failure instead of a hung / crashed test run."""

    def test_not_left_behind_uses_sys_exit_only(self):
        with mock.patch.object(sys, "exit") as fake_sys_exit, \
                mock.patch.object(os, "_exit") as fake_os_exit:
            _hook_runner.finish(False)
        fake_sys_exit.assert_called_once_with(0)
        fake_os_exit.assert_not_called()

    def test_left_behind_flushes_then_uses_os_exit_only(self):
        with mock.patch.object(sys, "exit") as fake_sys_exit, \
                mock.patch.object(os, "_exit") as fake_os_exit, \
                mock.patch.object(sys.stdout, "flush") as out_flush, \
                mock.patch.object(sys.stderr, "flush") as err_flush:
            _hook_runner.finish(True)
        fake_os_exit.assert_called_once_with(0)
        fake_sys_exit.assert_not_called()
        out_flush.assert_called_once()
        err_flush.assert_called_once()

    def test_left_behind_still_exits_even_if_the_flush_raises(self):
        """The flush is best-effort: a pipe the host already closed must not
        keep this process from reaching os._exit(0) (mirrors the two hooks'
        own belt-and-suspenders around stdout after a failed write, e.g.
        session_inject._silence_stdout)."""
        with mock.patch.object(sys, "exit") as fake_sys_exit, \
                mock.patch.object(os, "_exit") as fake_os_exit, \
                mock.patch.object(sys.stdout, "flush", side_effect=OSError("closed")), \
                mock.patch.object(sys.stderr, "flush", side_effect=OSError("closed")):
            _hook_runner.finish(True)
        fake_os_exit.assert_called_once_with(0)
        fake_sys_exit.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
