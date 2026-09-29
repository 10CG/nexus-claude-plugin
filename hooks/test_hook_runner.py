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

import os
import sys
import threading
import time
import unittest
from unittest import mock

import _hook_runner


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
        left_behind = _hook_runner.write_with_budget(write, 5, "w")
        self.assertFalse(left_behind)

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
