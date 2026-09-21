"""Tests for the shared ledger / state primitives.

Hermetic by construction: every test redirects NEXUS_HOOK_STATE_DIR into a
temp directory via mock.patch.dict, and one test asserts that nothing was
written outside it — these primitives default to ~/.nexus and a test suite
that quietly writes there would be both wrong and hard to notice.

Isolation rule for this file (TASK-011 pre-merge audit): CI now runs every
hook test in ONE process, so any patch that is not restored leaks into every
later module. Use mock.patch / addCleanup only — never save-and-restore by
hand.
"""

import fcntl
import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock

import _hook_state
import _identity


class _TempStateDir(unittest.TestCase):
    """Base: point the primitives at a throwaway directory."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "state")
        self.cwd = os.path.join(self.tmp.name, "myproject")
        os.makedirs(self.cwd)
        self.home = os.path.join(self.tmp.name, "home")
        os.makedirs(self.home)
        patcher = mock.patch.dict(
            os.environ,
            {_hook_state.STATE_DIR_ENV: self.root, "HOME": self.home},
            clear=False,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        # project_slug shells out to git; pin it so these tests do not depend on
        # whether the temp dir happens to sit inside a repository. Patched on
        # the _identity module because _hook_state reaches it by attribute --
        # a `from` import would bind at import time and ignore this.
        slug_patcher = mock.patch.object(_identity, "project_slug", return_value="proj")
        self.slug = slug_patcher.start()
        self.addCleanup(slug_patcher.stop)
        self.addCleanup(self._assert_nothing_under_home)

    def _assert_nothing_under_home(self):
        """Nothing may be written under HOME -- the real default is ~/.nexus.

        The previous version walked only the override directory and asserted
        every path found there started with it, which is true by construction.
        An injected write to ~/.nexus passed it.
        """
        stray = []
        for base, _dirs, files in os.walk(self.home):
            stray.extend(os.path.join(base, f) for f in files)
        # self.assertFalse, not a bare assert: `python3 -O` strips assert
        # statements, and a guard that silently disappears does not belong in
        # a file whose subject is things silently disappearing.
        self.assertFalse(stray, f"wrote outside the override: {stray}")


class TestLedgerRotation(_TempStateDir):
    def test_keeps_only_the_last_fifty_and_stays_parseable(self):
        """51 writes leave 50 entries, and the file parses after every one."""
        for i in range(51):
            _hook_state.record_run("demo", ok=True, calls=i, cwd=self.cwd)
            with open(_hook_state.ledger_path("demo", self.cwd), encoding="utf-8") as fh:
                entries = json.load(fh)  # parses at every step, not just the end
            self.assertLessEqual(len(entries), _hook_state.LEDGER_LIMIT)
        self.assertEqual(len(entries), 50)
        # oldest dropped, newest kept
        self.assertEqual(entries[0]["calls"], 1)
        self.assertEqual(entries[-1]["calls"], 50)

    def test_entry_carries_the_required_fields(self):
        _hook_state.record_run("demo", ok=False, reason="timeout", elapsed_ms=12, calls=3,
                               cwd=self.cwd)
        entry = _hook_state.read_ledger("demo", self.cwd)[0][-1]
        for field in ("hook", "ts", "ok", "reason", "elapsed_ms", "calls"):
            self.assertIn(field, entry)
        self.assertEqual(entry["hook"], "demo")
        self.assertFalse(entry["ok"])
        self.assertEqual(entry["reason"], "timeout")

    def test_never_appends_in_place(self):
        """The file is replaced, not appended to.

        Appending is what makes concurrent SessionEnd runs produce unparsable
        JSON, so pin the mechanism rather than only the result: the inode must
        change on every write.
        """
        _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        first = os.stat(_hook_state.ledger_path("demo", self.cwd)).st_ino
        _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        second = os.stat(_hook_state.ledger_path("demo", self.cwd)).st_ino
        self.assertNotEqual(first, second)

    def test_corrupt_ledger_reads_as_empty_and_next_write_recovers(self):
        path = _hook_state.ledger_path("demo", self.cwd)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("{not json")
        self.assertEqual(_hook_state.read_ledger("demo", self.cwd)[0], [])
        _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        self.assertEqual(len(_hook_state.read_ledger("demo", self.cwd)[0]), 1)


class TestReasonTables(_TempStateDir):
    def test_failure_table_is_exactly_the_specified_set(self):
        self.assertEqual(
            _hook_state.FAILURE_REASONS,
            frozenset(
                {
                    "http_error", "timeout", "filter_suspect", "budget_exhausted",
                    "ingest_disabled", "identity_unresolved", "identity_changed",
                    "sections_unparsed", "file_unparsable", "rejected_422",
                    "orphan_guard", "lock_unavailable", "orphans_deleted", "unknown",
                    # Amendment A4-1 (TASK-001 pre-merge audit C3): conditions
                    # the spec's B/C/D rows name but left without a reason.
                    "rate_limited", "state_write_failed",
                    # Amendments A4-4 / A4-5 (owner rulings 2026-09-21): both
                    # started in the skip table.
                    "dedup_merged", "pointer_unresolved",
                }
            ),
        )

    def test_skip_table_is_exactly_the_specified_set(self):
        self.assertEqual(
            _hook_state.SKIP_REASONS,
            frozenset(
                {
                    "not_owner", "opted_out", "empty_sections",
                    "stale_local", "unchanged", "peer_absent", "fact_delta_truncated",
                    # Amendment A4-1, as above.
                    "not_configured", "nothing_to_do",
                    # Amendment A4-5: the quiet half of the pointer split.
                    "no_handoff",
                }
            ),
        )

    def test_the_two_tables_do_not_overlap(self):
        """A reason in both tables would be reported or not depending on lookup
        order, which is exactly the ambiguity the split exists to remove."""
        self.assertEqual(_hook_state.FAILURE_REASONS & _hook_state.SKIP_REASONS, frozenset())

    def test_reason_outside_both_tables_is_recorded_loudly_not_raised(self):
        """A typo must be loud, not fatal.

        Raising looked strict, but every caller runs under the hooks'
        `except Exception: pass` + `exit(0)` idiom, so the exception deleted
        the entire ledger entry -- leaving yesterday's success as the newest
        record and the reporter with nothing to say. Recording `unknown` (a
        failure reason) surfaces it at the next SessionStart instead.
        """
        with mock.patch("sys.stderr") as stderr:
            entry = _hook_state.record_run(
                "demo", ok=False, reason="oops_new_reason", cwd=self.cwd
            )
        self.assertEqual(entry["reason"], "unknown")
        self.assertTrue(_hook_state.is_failure_reason(entry["reason"]))
        printed = "".join(c.args[0] for c in stderr.write.call_args_list)
        self.assertIn("oops_new_reason", printed)
        # and it really reached the ledger
        entries, _ = _hook_state.read_ledger("demo", self.cwd)
        self.assertEqual(entries[-1]["reason"], "unknown")

    def test_reason_none_does_not_vanish(self):
        """`reason=None` is an easy slip when the constant is NO_REASON."""
        with mock.patch("sys.stderr"):
            entry = _hook_state.record_run("demo", ok=True, reason=None, cwd=self.cwd)
        self.assertEqual(entry["reason"], "unknown")

    def test_new_reasons_from_the_audit_are_present(self):
        """Amendment A4-1 additions, each tied to a condition the spec names."""
        for reason in ("rate_limited", "state_write_failed"):
            self.assertIn(reason, _hook_state.FAILURE_REASONS)
        for reason in ("not_configured", "nothing_to_do"):
            self.assertIn(reason, _hook_state.SKIP_REASONS)

    def test_classification_matches_the_table(self):
        # The pair the design hinges on: one means "not mine", the other
        # means "cannot tell whose", and only the second is a fault.
        self.assertFalse(_hook_state.is_failure_reason("not_owner"))
        self.assertTrue(_hook_state.is_failure_reason("identity_unresolved"))

    def test_every_destructive_action_is_a_failure_reason(self):
        """Both reasons that delete rows on the server are reported, even
        though the cleanup itself may be correct.

        Amendment A4-4 (owner ruling 2026-09-21): dedup_merged sat in the skip
        table while orphans_deleted -- the same kind of action -- sat here
        under a comment calling it "the only destructive action". And the rows
        dedup merges can only be this hook's own race, so a non-zero count also
        says the idempotency protocol lost one.
        """
        for reason in ("orphans_deleted", "dedup_merged"):
            with self.subTest(reason=reason):
                self.assertTrue(_hook_state.is_failure_reason(reason))
                self.assertNotIn(reason, _hook_state.SKIP_REASONS)

    def test_no_handoffs_is_quiet_but_unlocatable_handoffs_are_loud(self):
        """Amendment A4-5 (owner ruling 2026-09-21).

        The same split the spec already makes one level down (empty_sections /
        sections_unparsed), for the same reason: "this project keeps no
        handoffs" must stay quiet forever, while "there are handoffs and none
        could be located" is ingestion stalling -- a renamed template or a
        broken frontmatter -- and from the outside the two look identical.
        """
        self.assertIn("no_handoff", _hook_state.SKIP_REASONS)
        self.assertFalse(_hook_state.is_failure_reason("no_handoff"))
        self.assertTrue(_hook_state.is_failure_reason("pointer_unresolved"))
        self.assertNotIn("pointer_unresolved", _hook_state.SKIP_REASONS)


class TestStateRoundTrip(_TempStateDir):
    def test_write_then_read(self):
        reasons = _hook_state.write_state("memory-sync", {"cursor": 7}, self.cwd)
        self.assertEqual(reasons, [])
        data, read_reasons = _hook_state.read_state("memory-sync", self.cwd)
        self.assertEqual(data, {"cursor": 7})
        self.assertEqual(read_reasons, [])

    def test_missing_state_is_a_first_run_not_a_fault(self):
        data, reasons = _hook_state.read_state("never-written", self.cwd)
        self.assertEqual(data, {})
        self.assertEqual(reasons, [])

    def test_corrupt_state_rebuilds_and_reports_unknown(self):
        path = _hook_state.state_path("memory-sync", self.cwd)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("]]not json[[")
        data, reasons = _hook_state.read_state("memory-sync", self.cwd)
        self.assertEqual(data, {})
        self.assertEqual(reasons, ["unknown"])

    def test_non_dict_state_is_treated_as_corrupt(self):
        path = _hook_state.state_path("memory-sync", self.cwd)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("[1, 2, 3]")
        data, reasons = _hook_state.read_state("memory-sync", self.cwd)
        self.assertEqual(data, {})
        self.assertEqual(reasons, ["unknown"])


class TestFailureLegs(_TempStateDir):
    def test_unwritable_state_dir_does_not_lose_the_run(self):
        """A failing ledger write complains and returns instead of raising.

        By the time a hook records its run the remote work is done; raising
        here would turn a local bookkeeping problem into a lost run.

        The failure is injected rather than produced with chmod 0o500,
        because CI runs as root in the container and root bypasses directory
        permission checks (CAP_DAC_OVERRIDE). The permission version exercised
        this leg locally at uid 1000 and did nothing in CI. Injecting the
        OSError pins the same contract at any uid.
        """
        with mock.patch.object(
            _hook_state, "_atomic_write", side_effect=OSError("read-only fs")
        ) as write, mock.patch("sys.stderr") as stderr:
            entry = _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        self.assertTrue(write.called, "the failing write must actually be reached")
        self.assertEqual(entry["hook"], "demo")  # returned, not raised
        self.assertTrue(stderr.write.called, "the failure must reach stderr")
        printed = "".join(c.args[0] for c in stderr.write.call_args_list)
        self.assertIn("ledger", printed)

    @unittest.skipIf(os.geteuid() == 0, "root bypasses directory permissions")
    def test_readonly_dir_is_the_real_shape_of_that_failure(self):
        """The same contract through a genuinely unwritable directory.

        Skipped as root -- which is exactly why the injected version above
        exists. This one is the evidence that a read-only directory really does
        raise the OSError the other test injects, so the injection is not
        testing a failure mode that cannot happen.
        """
        os.makedirs(_hook_state.project_dir(self.cwd), exist_ok=True)
        os.chmod(_hook_state.project_dir(self.cwd), 0o500)
        self.addCleanup(os.chmod, _hook_state.project_dir(self.cwd), 0o700)
        with mock.patch("sys.stderr") as stderr:
            entry = _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        self.assertEqual(entry["hook"], "demo")
        self.assertTrue(stderr.write.called, "the failure must reach stderr")

    def test_flock_failure_is_reported_not_swallowed(self):
        """A lock that silently stops locking is worse than no lock: the
        concurrency assumption still reads as satisfied."""
        with mock.patch.object(_hook_state.fcntl, "flock", side_effect=OSError("no locks")):
            reasons = _hook_state.write_state("memory-sync", {"cursor": 1}, self.cwd)
        self.assertIn("lock_unavailable", reasons)
        # and the state was still written — degrading, not dropping
        data, _ = _hook_state.read_state("memory-sync", self.cwd)
        self.assertEqual(data, {"cursor": 1})

    def test_ledger_lock_degradation_is_recorded(self):
        """The ledger carries the whole visibility baseline.

        An earlier revision computed the lock reasons here and dropped them,
        so on an NFS home -- the case `lock_unavailable` exists for -- every
        ledger write ran unlocked and nothing ever said so.
        """
        with mock.patch.object(_hook_state.fcntl, "flock", side_effect=OSError("no locks")):
            entry = _hook_state.record_run("demo", ok=True, reason="unchanged", cwd=self.cwd)
        self.assertEqual(entry["reason"], "lock_unavailable")
        entries, _ = _hook_state.read_ledger("demo", self.cwd)
        self.assertEqual(entries[-1]["reason"], "lock_unavailable")

    def test_lock_unavailable_is_a_failure_reason(self):
        self.assertTrue(_hook_state.is_failure_reason("lock_unavailable"))


class TestHermetic(_TempStateDir):
    def test_everything_lands_under_the_override(self):
        """The override receives the files; the HOME check in tearDown proves
        nothing landed anywhere else (walking only self.root cannot)."""
        _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        _hook_state.write_state("memory-sync", {"cursor": 1}, self.cwd)
        written = []
        for base, _dirs, files in os.walk(self.root):
            written.extend(os.path.join(base, f) for f in files)
        self.assertTrue(written, "expected the override dir to receive the files")

    def test_default_root_is_under_home_when_unset(self):
        """Documents the default without creating it."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_hook_state.STATE_DIR_ENV, None)
            root = _hook_state.state_root()
        self.assertEqual(root, os.path.expanduser(_hook_state.DEFAULT_STATE_DIR))
        self.assertNotIn("$", root)

    def test_temp_files_are_cleaned_up(self):
        """The rename target is gone, so a crashed write cannot accumulate."""
        _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        leftovers = [f for f in os.listdir(_hook_state.project_dir(self.cwd))
                     if f.startswith(".tmp-")]
        self.assertEqual(leftovers, [])


class TestIdentityDrift(_TempStateDir):
    def test_change_is_reported(self):
        self.assertEqual(
            _hook_state.identity_drift("old-box", "new-box", True), ["identity_changed"]
        )

    def test_same_is_quiet(self):
        self.assertEqual(_hook_state.identity_drift("box", "box", True), [])

    def test_genuine_first_run_is_quiet(self):
        """No state file and no previous value: nothing moved.

        Reporting here would fire on every fresh install.
        """
        self.assertEqual(_hook_state.identity_drift(None, "box", False), [])
        self.assertEqual(_hook_state.identity_drift("", "box", False), [])

    def test_state_lost_is_not_treated_as_a_first_run(self):
        """State present but carrying no id means the prior identity is gone.

        This is the case the first draft got backwards: a wiped state dir, a
        changed NEXUS_HOOK_STATE_DIR, or a project slug that degraded when git
        timed out all destroy the previous id — and that same event is what
        makes a container-id change invisible. Rows written under the old id
        then read as a peer's: the injection recipe gives away slots to them
        and the orphan reconciliation, which lists only container_id=<self>,
        cannot see them at all.
        """
        self.assertEqual(_hook_state.identity_drift(None, "box", True), ["unknown"])

    def test_state_existed_is_required(self):
        """Not defaulted: whoever forgets it is exactly who gets the dangerous
        answer, so the signature makes them say it."""
        with self.assertRaises(TypeError):
            _hook_state.identity_drift("a", "b")


class TestConcurrency(_TempStateDir):
    """record_run must hold an exclusive lock across its read-modify-write.

    Pinned deterministically rather than by racing two processes: the window is
    microseconds, so a race test passes with the lock removed most of the time.
    (It did, in the injection matrix -- which is how this version came to be.)
    Holding the lock here and requiring the child to block tests the contract
    itself, and fails every time if the lock goes away.

    The reason it matters: two SessionEnd runs for one session
    (10CG/nexus-claude-plugin#31) do read-modify-write on the same ledger, and
    an unlocked loser silently drops its entry -- the rename stays atomic, so
    the file is perfectly parseable and one run has simply vanished.
    """

    def _spawn_record_run(self, hook, reason):
        code = (
            "import os, sys\n"
            f"sys.path.insert(0, {os.path.dirname(os.path.abspath(__file__))!r})\n"
            f"os.environ['NEXUS_HOOK_STATE_DIR'] = {self.root!r}\n"
            "import _identity, _hook_state\n"
            "_identity.project_slug = lambda cwd: 'proj'\n"
            f"_hook_state.record_run({hook!r}, True, reason={reason!r}, cwd={self.cwd!r})\n"
        )
        return subprocess.Popen([sys.executable, "-c", code])

    def test_the_ledger_lock_spans_the_read(self):
        """Not just that a lock exists -- that the read is inside it.

        Asserting only "the child blocks" passes even when the read sits
        outside the lock, because the child still has to acquire it to write.
        So: while holding the lock, add a third entry. A child that read the
        ledger before waiting will write back a copy that predates it, and the
        third entry disappears -- which is the original defect, and is what a
        second SessionEnd for the same session does in real life.
        """
        _hook_state.record_run("demo", ok=True, reason="unchanged", cwd=self.cwd)
        lock_path = _hook_state.ledger_path("demo", self.cwd) + ".lock"

        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX)

        child = self._spawn_record_run("demo", "timeout")
        self.addCleanup(child.kill)
        with self.assertRaises(subprocess.TimeoutExpired):
            child.wait(timeout=2)  # blocked, so its read has not happened yet

        # Written directly rather than via record_run: this process already
        # holds the file's flock, and flock is per open file description, so
        # record_run would block taking it again against itself.
        path = _hook_state.ledger_path("demo", self.cwd)
        existing, _ = _hook_state.read_ledger("demo", self.cwd)
        existing.append({"hook": "demo", "ok": True, "reason": "peer_absent"})
        _hook_state._atomic_write(path, json.dumps(existing, ensure_ascii=False))
        fcntl.flock(fd, fcntl.LOCK_UN)
        self.assertEqual(child.wait(timeout=30), 0)

        entries, reasons = _hook_state.read_ledger("demo", self.cwd)
        self.assertEqual(reasons, [])
        self.assertEqual(
            sorted(e["reason"] for e in entries),
            ["peer_absent", "timeout", "unchanged"],
            "the entry written while the child waited was overwritten: the "
            "child read the ledger before taking the lock",
        )


class TestUpdateState(_TempStateDir):
    def test_sequential_updates_accumulate(self):
        _hook_state.write_state("sync", {"cursor": 0}, self.cwd)

        def bump(state):
            state["cursor"] = state.get("cursor", 0) + 1
            return state

        for _ in range(5):
            _, reasons = _hook_state.update_state("sync", self.cwd, bump)
            self.assertEqual(reasons, [])
        data, _ = _hook_state.read_state("sync", self.cwd)
        self.assertEqual(data["cursor"], 5)

    def test_the_lock_spans_the_read_modify_write(self):
        """`read_state` then `write_state` leaves the gap between them open.

        Two runs racing there lose one another's updates while each individual
        write looks perfectly atomic -- which is the whole reason this
        primitive exists. Asserted by holding the lock and requiring the child
        to block: a sequential accumulation test passes either way, as the
        injection matrix showed.
        """
        _hook_state.write_state("sync", {"cursor": 0}, self.cwd)
        lock_path = _hook_state.state_path("sync", self.cwd) + ".lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX)

        code = (
            "import os, sys\n"
            f"sys.path.insert(0, {os.path.dirname(os.path.abspath(__file__))!r})\n"
            f"os.environ['NEXUS_HOOK_STATE_DIR'] = {self.root!r}\n"
            "import _identity, _hook_state\n"
            "_identity.project_slug = lambda cwd: 'proj'\n"
            f"_hook_state.update_state('sync', {self.cwd!r}, lambda s: dict(s, cursor=9))\n"
        )
        child = subprocess.Popen([sys.executable, "-c", code])
        self.addCleanup(child.kill)
        with self.assertRaises(subprocess.TimeoutExpired):
            child.wait(timeout=2)  # blocked, so its read has not happened yet

        # Set another key while the child waits. A child that read before
        # waiting writes back a state without it -- a lost update, which is
        # what read_state + write_state does and what this primitive exists to
        # prevent. Asserting only "the child eventually wins" cannot see it.
        # Direct write: this process holds the flock, and write_state would
        # block taking it again against itself (flock is per open file
        # description, not reentrant).
        _hook_state._atomic_write(
            _hook_state.state_path("sync", self.cwd),
            json.dumps({"cursor": 0, "marker": "set-while-locked"}),
        )
        fcntl.flock(fd, fcntl.LOCK_UN)
        self.assertEqual(child.wait(timeout=30), 0)
        data, _ = _hook_state.read_state("sync", self.cwd)
        self.assertEqual(data["cursor"], 9)
        self.assertEqual(
            data.get("marker"),
            "set-while-locked",
            "the child read the state before taking the lock and lost the update",
        )

    def test_mutation_returning_non_dict_leaves_state_intact(self):
        """`lambda s: s.update(...)` returns None.

        Writing it put `null` on disk -- destroying the cursor and the
        container_id the identity guard reads -- while the run recorded as
        clean, and the damage surfaced one run later as an `unknown` with
        nothing pointing at its cause.
        """
        _hook_state.write_state("sync", {"cursor": 42, "container_id": "bfe8285d"}, self.cwd)
        with mock.patch("sys.stderr") as stderr:
            data, reasons = _hook_state.update_state(
                "sync", self.cwd, lambda s: s.update({"cursor": 43})
            )
        self.assertIn("state_write_failed", reasons)
        self.assertEqual(data, {"cursor": 42, "container_id": "bfe8285d"})
        on_disk, _ = _hook_state.read_state("sync", self.cwd)
        self.assertEqual(on_disk, {"cursor": 42, "container_id": "bfe8285d"})
        printed = "".join(c.args[0] for c in stderr.write.call_args_list)
        self.assertIn("NoneType", printed)

    def test_mutation_raising_does_not_reach_the_blanket_handler(self):
        """A caller bug must not delete the run.

        read_state returns {} for a corrupt file, so a mutate written as
        `s["cursor"] + 1` raises on the first run after corruption -- and
        propagating would hit `except Exception: pass` + exit(0) upstream.
        """
        _hook_state.write_state("sync", {"cursor": 1}, self.cwd)
        with mock.patch("sys.stderr") as stderr:
            data, reasons = _hook_state.update_state("sync", self.cwd, lambda s: s["missing"])
        self.assertIn("unknown", reasons)
        self.assertEqual(data, {"cursor": 1})
        printed = "".join(c.args[0] for c in stderr.write.call_args_list)
        self.assertIn("KeyError", printed)

    def test_write_state_refuses_a_non_dict(self):
        _hook_state.write_state("sync", {"cursor": 1}, self.cwd)
        with mock.patch("sys.stderr"):
            reasons = _hook_state.write_state("sync", None, self.cwd)
        self.assertEqual(reasons, ["state_write_failed"])
        on_disk, _ = _hook_state.read_state("sync", self.cwd)
        self.assertEqual(on_disk, {"cursor": 1})

    def test_write_failure_returns_a_reason_and_the_on_disk_value(self):
        _hook_state.write_state("sync", {"cursor": 3}, self.cwd)
        with mock.patch.object(
            _hook_state, "_atomic_write", side_effect=OSError("read-only fs")
        ), mock.patch("sys.stderr"):
            data, reasons = _hook_state.update_state(
                "sync", self.cwd, lambda s: {"cursor": 99}
            )
        self.assertIn("state_write_failed", reasons)
        self.assertTrue(_hook_state.is_failure_reason("state_write_failed"))
        self.assertEqual(data, {"cursor": 3}, "caller must see what is on disk")


class TestStateWriteFailure(_TempStateDir):
    def test_write_state_returns_a_reason_instead_of_raising(self):
        """Propagating here is the quietest option, not the loudest.

        Every hook runs under `except Exception: pass` + `exit(0)`, so an
        exception means exit 0, no stdout, and -- because record_run is never
        reached -- no ledger row either. The next SessionStart sees yesterday's
        success and reports nothing.
        """
        with mock.patch.object(
            _hook_state, "_atomic_write", side_effect=OSError("read-only fs")
        ), mock.patch("sys.stderr") as stderr:
            reasons = _hook_state.write_state("sync", {"cursor": 1}, self.cwd)
        self.assertEqual(reasons, ["state_write_failed"])
        printed = "".join(c.args[0] for c in stderr.write.call_args_list)
        self.assertIn("state", printed)


class TestProjectDirSource(_TempStateDir):
    def test_project_dir_goes_through_project_slug(self):
        """Not just equal today -- the same call.

        A `from _identity import project_slug` binds at import time, so the
        fixture's patch could not reach it and nothing noticed that
        project_dir had its own derivation. Replacing it with a cwd basename
        kept all 199 tests green.
        """
        self.slug.reset_mock()
        self.slug.return_value = "sentinel-slug"
        result = _hook_state.project_dir("/some/where")
        self.slug.assert_called_once_with("/some/where")
        # The return value has to be what keys the directory. Asserting only
        # the call passes when project_dir calls project_slug, throws the
        # answer away, and derives its own -- which is the drift this pins.
        self.assertTrue(
            result.endswith("sentinel-slug"),
            f"project_dir ignored project_slug's answer: {result}",
        )

    def test_ledger_and_state_share_that_directory(self):
        self.assertEqual(
            os.path.dirname(_hook_state.ledger_path("h", self.cwd)),
            os.path.dirname(_hook_state.state_path("s", self.cwd)),
        )


class TestLedgerGuards(_TempStateDir):
    def test_non_dict_elements_are_dropped_and_reported(self):
        """The reporter does entry.get(...); an int there raises inside the
        hooks' blanket handler and loses the whole injection."""
        path = _hook_state.ledger_path("demo", self.cwd)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write('[1, "x", null, {"hook": "demo", "reason": "none"}]')
        entries, reasons = _hook_state.read_ledger("demo", self.cwd)
        self.assertEqual(entries, [{"hook": "demo", "reason": "none"}])
        self.assertEqual(reasons, ["unknown"])

    def test_missing_corrupt_and_empty_are_distinguishable(self):
        """V(2) asks for missing / unparsable to report unknown; an empty
        array is neither."""
        self.assertEqual(_hook_state.read_ledger("never", self.cwd), ([], []))
        path = _hook_state.ledger_path("bad", self.cwd)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("{not json")
        self.assertEqual(_hook_state.read_ledger("bad", self.cwd), ([], ["unknown"]))
        with open(_hook_state.ledger_path("empty", self.cwd), "w", encoding="utf-8") as fh:
            fh.write("[]")
        self.assertEqual(_hook_state.read_ledger("empty", self.cwd), ([], []))


class TestWorstReason(_TempStateDir):
    def test_failure_beats_skip(self):
        """A ledger entry holds one scalar and the reporter reads only that, so
        a run that hit both must record the failure."""
        self.assertEqual(
            _hook_state.worst_reason(["unchanged", "lock_unavailable"]),
            "lock_unavailable",
        )

    def test_empty_and_none_collapse_to_no_reason(self):
        self.assertEqual(_hook_state.worst_reason([]), _hook_state.NO_REASON)
        self.assertEqual(_hook_state.worst_reason(None), _hook_state.NO_REASON)
        self.assertEqual(
            _hook_state.worst_reason([_hook_state.NO_REASON]), _hook_state.NO_REASON
        )

    def test_skip_only_keeps_the_first(self):
        self.assertEqual(_hook_state.worst_reason(["unchanged", "not_owner"]), "unchanged")

    def test_state_write_failed_outranks_lock_unavailable(self):
        """Both appear together on a read-only directory.

        First-wins reported "could not lock" for "nothing was persisted" -- in
        the very scenario state_write_failed was added for.
        """
        self.assertEqual(
            _hook_state.worst_reason(["lock_unavailable", "state_write_failed"]),
            "state_write_failed",
        )
        self.assertEqual(
            _hook_state.worst_reason(["state_write_failed", "lock_unavailable"]),
            "state_write_failed",
        )

    def test_a_bare_string_is_not_iterated_per_character(self):
        """`worst_reason("timeout")` used to return "t"."""
        self.assertEqual(_hook_state.worst_reason("timeout"), "timeout")

    def test_a_merge_in_an_otherwise_quiet_run_is_what_gets_recorded(self):
        """The B row carries on after merging duplicates, so such a run ends on
        a skip or clean -- and `unchanged` is appended first. Table membership
        alone would not surface the merge: it has to win this collapse, or the
        scalar the reporter reads says `unchanged`.
        """
        self.assertEqual(
            _hook_state.worst_reason(["unchanged", "dedup_merged"]), "dedup_merged"
        )
        entry = _hook_state.record_run(
            "handoff-ingest",
            ok=True,
            reason=_hook_state.worst_reason(["unchanged", "dedup_merged"]),
            cwd=self.cwd,
            extra={"dedup_merged": 1},
        )
        entries, _ = _hook_state.read_ledger("handoff-ingest", self.cwd)
        self.assertEqual(entries[-1]["reason"], "dedup_merged")
        self.assertEqual(entries[-1]["dedup_merged"], 1)
        self.assertTrue(_hook_state.is_failure_reason(entry["reason"]))

    def test_unlocatable_handoffs_outrank_a_skip_in_the_same_run(self):
        self.assertEqual(
            _hook_state.worst_reason(["not_configured", "pointer_unresolved"]),
            "pointer_unresolved",
        )
        self.assertEqual(
            _hook_state.worst_reason(["unchanged", "no_handoff"]), "unchanged"
        )


def _http_error(code):
    return urllib.error.HTTPError("https://nexus.example/v1/x", code, "msg", {}, None)


class TestReasonForException(unittest.TestCase):
    """One classifier for every hook that makes a remote call (TASK-002).

    Two hooks each growing their own mapping is how one of them ends up
    recording a rate limit as a generic error -- and the rate limit is the one
    the spec treats differently (stop this round, do not retry).
    """

    CASES = (
        ("429 is its own reason", lambda: _http_error(429), "rate_limited"),
        ("other 4xx", lambda: _http_error(403), "http_error"),
        ("5xx", lambda: _http_error(503), "http_error"),
        ("connection refused", lambda: urllib.error.URLError(ConnectionRefusedError()), "http_error"),
        ("connect timeout, wrapped", lambda: urllib.error.URLError(socket.timeout("t")), "timeout"),
        ("connect timeout, wrapped (3.10+)", lambda: urllib.error.URLError(TimeoutError("t")), "timeout"),
        ("read timeout, bare", lambda: socket.timeout("timed out"), "timeout"),
        ("read timeout, bare (3.10+)", lambda: TimeoutError("timed out"), "timeout"),
        ("server hung up", lambda: http.client.RemoteDisconnected("bye"), "http_error"),
        ("reset mid-response", lambda: ConnectionResetError("reset"), "http_error"),
        ("anything else", lambda: KeyError("profile"), "unknown"),
        ("a bare ValueError is not assumed to be the network", lambda: ValueError("x"), "unknown"),
    )

    def test_each_exception_maps_to_the_expected_reason(self):
        for label, make, expected in self.CASES:
            with self.subTest(case=label):
                self.assertEqual(_hook_state.reason_for_exception(make()), expected)

    def test_every_result_is_a_failure_reason(self):
        """An exception is never an expected skip. If this ever returned a
        skip-class reason, the failure would be recorded and never reported."""
        for label, make, _ in self.CASES:
            with self.subTest(case=label):
                reason = _hook_state.reason_for_exception(make())
                self.assertTrue(_hook_state.is_failure_reason(reason), reason)

    def test_http_error_is_checked_before_its_parent_class(self):
        """HTTPError subclasses URLError. Testing URLError first turns every
        429 into http_error -- the exact conflation this function prevents."""
        self.assertTrue(issubclass(urllib.error.HTTPError, urllib.error.URLError))
        self.assertEqual(_hook_state.reason_for_exception(_http_error(429)), "rate_limited")


class TestStateAtomicity(_TempStateDir):
    def test_write_state_replaces_rather_than_truncating(self):
        """A reader must never see a half-written state file."""
        _hook_state.write_state("sync", {"cursor": 1}, self.cwd)
        first = os.stat(_hook_state.state_path("sync", self.cwd)).st_ino
        _hook_state.write_state("sync", {"cursor": 2}, self.cwd)
        second = os.stat(_hook_state.state_path("sync", self.cwd)).st_ino
        self.assertNotEqual(first, second)

    def test_state_exists_distinguishes_never_written(self):
        self.assertFalse(_hook_state.state_exists("sync", self.cwd))
        _hook_state.write_state("sync", {}, self.cwd)
        self.assertTrue(_hook_state.state_exists("sync", self.cwd))


if __name__ == "__main__":
    unittest.main()
