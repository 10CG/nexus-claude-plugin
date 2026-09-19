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

import json
import os
import tempfile
import unittest
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
        patcher = mock.patch.dict(
            os.environ, {_hook_state.STATE_DIR_ENV: self.root}, clear=False
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        # project_slug shells out to git; pin it so these tests do not depend on
        # whether the temp dir happens to sit inside a repository.
        slug_patcher = mock.patch.object(_identity, "project_slug", return_value="proj")
        slug_patcher.start()
        self.addCleanup(slug_patcher.stop)


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
        entry = _hook_state.read_ledger("demo", self.cwd)[-1]
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
        self.assertEqual(_hook_state.read_ledger("demo", self.cwd), [])
        _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        self.assertEqual(len(_hook_state.read_ledger("demo", self.cwd)), 1)


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
                }
            ),
        )

    def test_skip_table_is_exactly_the_specified_set(self):
        self.assertEqual(
            _hook_state.SKIP_REASONS,
            frozenset(
                {
                    "not_owner", "opted_out", "empty_sections", "pointer_unresolved",
                    "stale_local", "unchanged", "peer_absent", "fact_delta_truncated",
                    "dedup_merged",
                }
            ),
        )

    def test_the_two_tables_do_not_overlap(self):
        """A reason in both tables would be reported or not depending on lookup
        order, which is exactly the ambiguity the split exists to remove."""
        self.assertEqual(_hook_state.FAILURE_REASONS & _hook_state.SKIP_REASONS, frozenset())

    def test_reason_outside_both_tables_raises(self):
        with self.assertRaises(ValueError) as ctx:
            _hook_state.record_run("demo", ok=False, reason="oops_new_reason", cwd=self.cwd)
        self.assertIn("oops_new_reason", str(ctx.exception))

    def test_classification_matches_the_table(self):
        self.assertTrue(_hook_state.is_failure_reason("identity_unresolved"))
        self.assertFalse(_hook_state.is_failure_reason("not_owner"))
        # The pair that is easiest to get backwards: one means "not mine",
        # the other means "cannot tell whose", and only the second is a fault.
        self.assertFalse(_hook_state.is_failure_reason("not_owner"))
        self.assertTrue(_hook_state.is_failure_reason("identity_unresolved"))

    def test_orphans_deleted_is_a_failure_reason(self):
        """It is the only destructive action, so a non-zero count is reported
        even though the cleanup itself may be correct."""
        self.assertTrue(_hook_state.is_failure_reason("orphans_deleted"))


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

    def test_lock_unavailable_is_a_failure_reason(self):
        self.assertTrue(_hook_state.is_failure_reason("lock_unavailable"))


class TestHermetic(_TempStateDir):
    def test_everything_lands_under_the_override(self):
        """Nothing is written outside NEXUS_HOOK_STATE_DIR."""
        _hook_state.record_run("demo", ok=True, cwd=self.cwd)
        _hook_state.write_state("memory-sync", {"cursor": 1}, self.cwd)
        written = []
        for base, _dirs, files in os.walk(self.root):
            written.extend(os.path.join(base, f) for f in files)
        self.assertTrue(written, "expected the override dir to receive the files")
        for path in written:
            self.assertTrue(path.startswith(self.root))

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
        self.assertEqual(_hook_state.identity_drift("old-box", "new-box"),
                         ["identity_changed"])

    def test_same_is_quiet(self):
        self.assertEqual(_hook_state.identity_drift("box", "box"), [])

    def test_first_run_is_not_drift(self):
        """No previous value means nothing moved — reporting here would fire on
        every fresh install."""
        self.assertEqual(_hook_state.identity_drift(None, "box"), [])
        self.assertEqual(_hook_state.identity_drift("", "box"), [])


if __name__ == "__main__":
    unittest.main()
