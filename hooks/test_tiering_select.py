#!/usr/bin/env python3
"""Tests for the capture budget's tiered selection (session_capture.select_activities).

OpenSpec change session-capture-priority-truncation, TASK-001 (gates 1 / 2 /
3 / 7 / 8 and the I-B slicing trap) and TASK-002 (gates 4 / 5). Everything
here calls pure functions -- select_activities, and from TASK-002 on
_parse_transcript / _build_activities -- and never main() or _collect(), so no
ledger is written and no fake HOME is needed. The fail-open gates (6a-6e) walk
main() and therefore live in test_session_capture.py, under its setUpModule
isolation.

File naming: the plugin CI gate (.forgejo/workflows/test.yml) decides whether a
file contributed tests by `p.stem in test_id` -- a substring match. So this
stem must not appear inside any test id of another file, and no class or
method in test_session_capture.py may contain "test_tiering_select".

Fixtures are one distinct activity per index, so "which ones survived" is
decidable and the middle of a tier can be told from its ends.
"""

import json
import os
import tempfile
import unittest

import session_capture as sc

LAYERED = "layered"
DEGENERATE = "degenerate"


# ── Fixture builders (activity_data shapes follow _classify_tool / _extract_from_entry) ──

def _edit(i):  # tier 1
    return ("edit_file", {"tool": "Edit", "summary": f"/a/{i}.py"})


def _msg(i):  # tier 1 -- the text lives under "text", not "summary"
    return ("user_message", {"text": f"message {i}"})


def _commit(i):  # tier 1
    return ("commit", {"tool": "Bash", "summary": f"git commit -m 'c{i}'"})


def _push(i):  # tier 2: a command_run that provably writes
    return ("command_run", {"tool": "Bash", "summary": f"git push origin b{i}"})


def _script(i):  # tier 3: survives the source filter (unknown head) but proves nothing
    return ("command_run", {"tool": "Bash", "summary": f"python3 x{i}.py"})


def _cmd(text):
    return ("command_run", {"tool": "Bash", "summary": text})


def _interleaved(n, *builders):
    """Round-robin the builders so every tier is spread across the session.

    Gate 1 / gate 3 demand this: an implementation that concatenates the
    tiers in tier order passes an ordering assertion on a fixture that is
    already grouped by tier.
    """
    pool = []
    for i in range(n):
        for build in builders:
            pool.append(build(i))
    return pool


def _positions(pool, selected):
    """Original index of each selected element, by identity."""
    by_id = {id(e): k for k, e in enumerate(pool)}
    return [by_id[id(e)] for e in selected]


def _summaries(selected):
    return [e[1]["summary"] for e in selected]


# ════════════════════════════════════════════════════════════════════════════════
# Gate 1 -- pool > limit and tier 1 < limit: tier 1 is never touched, the budget
# is filled exactly, and the output is in session order.
# ════════════════════════════════════════════════════════════════════════════════

class TestGate1TierOneIsProtected(unittest.TestCase):

    def test_every_tier_one_item_survives_in_session_order(self):
        pool = _interleaved(4, _script, _edit, _push, _msg)  # 16: 8 tier 1, 4 tier 2, 4 tier 3
        selected, strategy, dropped = sc.select_activities(pool, limit=10)
        self.assertEqual(len(selected), 10)
        self.assertEqual(strategy, LAYERED, "tier 2/3 overflow must not change the strategy")
        tier_one = [e for e in pool if e[0] in ("edit_file", "user_message")]
        self.assertEqual(len(tier_one), 8)
        for e in tier_one:
            self.assertTrue(any(s is e for s in selected), f"tier-1 item dropped: {e}")
        positions = _positions(pool, selected)
        self.assertEqual(positions, sorted(positions), "output must follow the session order")
        # The two remaining slots go to tier 2 -- both ends of it -- and tier 3 gets none.
        self.assertEqual(
            [e[1]["summary"] for e in selected if e[0] == "command_run"],
            ["git push origin b0", "git push origin b3"],
        )
        self.assertEqual(dropped, {"command_run": 6})
        self.assertEqual(sum(dropped.values()), len(pool) - len(selected))


# ════════════════════════════════════════════════════════════════════════════════
# Gate 2 -- tier 1 alone exceeds the limit: keep both ends, drop the middle,
# admit nothing from the lower tiers, and say so (strategy == degenerate).
# ════════════════════════════════════════════════════════════════════════════════

class TestGate2TierOneOverflowKeepsBothEnds(unittest.TestCase):

    def test_at_the_real_default_limit(self):
        limit = sc._MAX_ACTIVITIES  # the default the hook runs with, not a toy limit
        pool = []
        for i in range(260):
            pool.append(_edit(i))
            if i % 10 == 0:  # lower-tier candidates: they MUST lose ([][0:] hides I-B)
                pool.append(_push(i))
                pool.append(_script(i))
        selected, strategy, dropped = sc.select_activities(pool)
        self.assertEqual(len(selected), limit)
        self.assertEqual(strategy, DEGENERATE)
        self.assertTrue(all(e[0] == "edit_file" for e in selected), "lower tiers admitted")
        head, tail = limit // 2, limit - limit // 2
        self.assertEqual(
            _summaries(selected),
            [f"/a/{i}.py" for i in range(head)] + [f"/a/{i}.py" for i in range(260 - tail, 260)],
            "must be the two ends, not the tail and not the head",
        )
        self.assertEqual(dropped, {"edit_file": 60, "command_run": 52})
        positions = _positions(pool, selected)
        self.assertEqual(positions, sorted(positions))

    def test_an_odd_limit_still_fills_the_budget_exactly(self):
        pool = [_edit(i) for i in range(20)] + [_script(0)]
        selected, strategy, dropped = sc.select_activities(pool, limit=7)
        self.assertEqual(strategy, DEGENERATE)
        self.assertEqual(
            _summaries(selected),
            ["/a/0.py", "/a/1.py", "/a/2.py", "/a/16.py", "/a/17.py", "/a/18.py", "/a/19.py"],
        )
        self.assertEqual(dropped, {"edit_file": 13, "command_run": 1})


# ════════════════════════════════════════════════════════════════════════════════
# Gate 3 -- at or under the limit nothing moves: same list, same order.
# ════════════════════════════════════════════════════════════════════════════════

class TestGate3UnderTheLimitNothingMoves(unittest.TestCase):

    def test_the_input_list_is_returned_as_is(self):
        pool = _interleaved(4, _script, _edit, _push, _msg)
        selected, strategy, dropped = sc.select_activities(pool, limit=20)
        self.assertIs(selected, pool)
        self.assertEqual((strategy, dropped), (LAYERED, {}))

    def test_membership_and_order_are_untouched(self):
        pool = _interleaved(4, _script, _edit, _push, _msg)
        snapshot = list(pool)
        selected, _, _ = sc.select_activities(pool, limit=len(pool) + 5)
        self.assertEqual(selected, snapshot)
        self.assertEqual(pool, snapshot, "the input must not be mutated")


# ════════════════════════════════════════════════════════════════════════════════
# Gate 7 -- boundaries: exactly == limit is not truncation; tier 1 exactly ==
# limit leaves a remainder of 0, which is the I-B trap (filler[-0:] is filler).
# ════════════════════════════════════════════════════════════════════════════════

class TestGate7Boundaries(unittest.TestCase):

    def test_exactly_at_the_default_limit_is_not_truncated(self):
        pool = _interleaved(50, _script, _edit, _push, _msg)
        self.assertEqual(len(pool), sc._MAX_ACTIVITIES)
        selected, strategy, dropped = sc.select_activities(pool)
        self.assertIs(selected, pool)
        self.assertEqual((strategy, dropped), (LAYERED, {}))

    def test_tier_one_exactly_at_the_limit_leaves_no_room_and_stays_layered(self):
        pool = [_push(0), _script(0)] + [_edit(i) for i in range(10)] + [_push(1), _script(1)]
        selected, strategy, dropped = sc.select_activities(pool, limit=10)
        self.assertEqual(len(selected), 10, "a remainder of 0 must select nothing more")
        self.assertEqual(strategy, LAYERED, "tier 1 == limit is not degenerate")
        self.assertEqual([e[0] for e in selected], ["edit_file"] * 10)
        self.assertEqual(dropped, {"command_run": 4})

    def test_tier_one_and_two_exactly_fill_the_limit(self):
        # remainder hits 0 after tier 2: tier 3 must get nothing, not everything
        pool = [_script(0), _edit(0), _push(0), _script(1), _push(1), _script(2)]
        selected, strategy, dropped = sc.select_activities(pool, limit=3)
        self.assertEqual(_summaries(selected), ["/a/0.py", "git push origin b0", "git push origin b1"])
        self.assertEqual((strategy, dropped), (LAYERED, {"command_run": 3}))


# ════════════════════════════════════════════════════════════════════════════════
# Gate 8 -- tier 2 is a POSITIVE write-marker table, not `not _is_readonly_command`.
# ════════════════════════════════════════════════════════════════════════════════

class TestGate8WriteMarkers(unittest.TestCase):

    TIER_TWO = (
        "alembic upgrade head",
        "git push",
        "docker push registry.example/nexus:1.2.3",
        "rm -rf build",
        "npm publish --access public",
        "mkdir -p out",
        "git checkout -b feature/x",
    )
    TIER_THREE = (
        "python3 x.py",
        "cd /some/where",
        "grep a | head",
        "nomad job status nexus-api-dev",
        "alembic current",
        "git",                        # bare head: no tokens[1] to decide on
        "sudo rm -rf /tmp/x",         # only tokens[0] is inspected
        "cd deploy && git push",      # the chain is invisible at tokens[0]
    )

    def test_the_tier_three_examples_do_reach_the_selector(self):
        """Gate 8 is vacuous unless these survive _is_low_signal: they do only
        because unknown heads and anything piped are kept as not-read-only."""
        for cmd in ("python3 x.py", "cd /some/where", "grep a | head"):
            self.assertFalse(sc._is_low_signal("command_run", {"summary": cmd}), cmd)

    def test_tier_two_is_admitted_before_tier_three(self):
        pool = [_edit(0)] + [_cmd(c) for c in self.TIER_THREE] + [_cmd(c) for c in self.TIER_TWO]
        limit = 1 + len(self.TIER_TWO) + 2  # every tier-2 command fits, only two tier-3 do
        selected, strategy, dropped = sc.select_activities(pool, limit=limit)
        kept = [e[1]["summary"] for e in selected if e[0] == "command_run"]
        for cmd in self.TIER_TWO:
            self.assertIn(cmd, kept, "a write-marker command lost to a tier-3 one")
        self.assertEqual(len([c for c in kept if c in self.TIER_THREE]), 2)
        self.assertEqual(strategy, LAYERED)
        self.assertEqual(dropped, {"command_run": len(self.TIER_THREE) - 2})
        positions = _positions(pool, selected)
        self.assertEqual(positions, sorted(positions))

    def test_each_tier_two_example_outranks_each_tier_three_example(self):
        # The winner goes FIRST: with limit=1 the fallback of "both ends" is
        # the tail, so a classifier that ranks everything tier 2 would still
        # pick a trailing winner and this test would not notice
        # (post_implementation R1 mutant d). Both orders are asserted.
        for two in self.TIER_TWO:
            for three in self.TIER_THREE:
                with self.subTest(two=two, three=three):
                    selected, _, _ = sc.select_activities([_cmd(two), _cmd(three)], limit=1)
                    self.assertEqual(_summaries(selected), [two])
                    selected, _, _ = sc.select_activities([_cmd(three), _cmd(two)], limit=1)
                    self.assertEqual(_summaries(selected), [two])

    def test_unknown_heads_are_tier_three_not_tier_two(self):
        """`not _is_readonly_command` would rank every surviving command_run
        as tier 2 and leave tier 3 empty; an unknown head must lose to a marker."""
        # Winner first, distractors after it (see the note in the test above).
        pool = [_push(0), _cmd("python3 x.py"), _cmd("uv run pytest -q"), _cmd("forgejo GET /x")]
        selected, _, _ = sc.select_activities(pool, limit=1)
        self.assertEqual(_summaries(selected), ["git push origin b0"])

    def test_command_run_without_a_usable_summary_is_tier_three(self):
        odd = [
            ("command_run", {"tool": "Bash"}),               # no summary at all
            ("command_run", {"tool": "Bash", "summary": ""}),
            ("command_run", {"tool": "Bash", "summary": 42}),
            ("command_run", "not-a-dict"),
        ]
        for e in odd:
            with self.subTest(e=e):
                selected, _, _ = sc.select_activities([e, _push(0)], limit=1)
                self.assertEqual(_summaries(selected), ["git push origin b0"])

    def test_the_tier_predicate_on_bare_heads_and_subcommands(self):
        """Classification itself, not selection: a bare-head marker (table
        value None) is tier 2 on its own; a set-valued head only with a
        listed subcommand. Selection-only assertions let an inversion of the
        None branch through (post_implementation R2 mutant i)."""
        def tier(cmd):
            return sc._tier("command_run", {"tool": "Bash", "summary": cmd})
        for cmd in ("rm -rf build", "mv a b", "cp a b", "mkdir -p out", "chmod +x f", "rm"):
            self.assertEqual(tier(cmd), 2, cmd)
        for cmd in ("git push", "alembic upgrade head", "docker build .", "npm publish"):
            self.assertEqual(tier(cmd), 2, cmd)
        for cmd in ("git", "git status", "alembic", "alembic current", "docker ps", "npm install", "python3 x.py"):
            self.assertEqual(tier(cmd), 3, cmd)
        self.assertEqual(sc._tier("user_message", {"text": "hi"}), 1)
        self.assertEqual(sc._tier("read_file", {"summary": "/x"}), 3)

    def test_marker_table_shape(self):
        self.assertIsInstance(sc._WRITE_MARKERS, dict)
        for head, subs in sc._WRITE_MARKERS.items():
            self.assertIsInstance(head, str)
            self.assertTrue(subs is None or isinstance(subs, frozenset), head)
        self.assertNotIn("nomad", sc._WRITE_MARKERS, "job run / job status share tokens[1]")
        self.assertIsNotNone(sc._WRITE_MARKERS["alembic"], "bare alembic (current/heads) is read-only")
        for head in ("rm", "mv", "cp", "mkdir", "chmod"):
            self.assertIsNone(sc._WRITE_MARKERS[head], head)


# ════════════════════════════════════════════════════════════════════════════════
# Contract -- element shape, strategy domain, identity of what comes back.
# ════════════════════════════════════════════════════════════════════════════════

class TestSelectorContract(unittest.TestCase):

    def test_selected_elements_are_the_input_objects(self):
        pool = _interleaved(3, _script, _edit, _commit)
        selected, _, _ = sc.select_activities(pool, limit=5)
        for e in selected:
            self.assertTrue(any(e is p for p in pool), "elements must be passed through, not rebuilt")
            self.assertIsInstance(e, tuple)
            self.assertEqual(len(e), 2)

    def test_strategy_is_one_of_the_selectors_two_values(self):
        cases = (
            (_interleaved(4, _script, _edit), 5, LAYERED),
            ([_edit(i) for i in range(6)], 4, DEGENERATE),
            ([_edit(0)], 4, LAYERED),
        )
        for pool, limit, expected in cases:
            with self.subTest(expected=expected):
                _, strategy, _ = sc.select_activities(pool, limit=limit)
                self.assertEqual(strategy, expected)

    def test_degenerate_iff_tier_one_exceeds_the_limit(self):
        _, strategy, _ = sc.select_activities([_edit(i) for i in range(5)] + [_push(0)], limit=5)
        self.assertEqual(strategy, LAYERED, "tier 1 == limit is not an overflow")
        _, strategy, _ = sc.select_activities([_edit(i) for i in range(6)] + [_push(0)], limit=5)
        self.assertEqual(strategy, DEGENERATE)

    def test_empty_pool(self):
        self.assertEqual(sc.select_activities([]), ([], LAYERED, {}))

    def test_dropped_counts_are_plain_per_action_totals(self):
        pool = _interleaved(3, _script, _edit, _msg, _push)  # 12: 6 tier 1, 3 tier 2, 3 tier 3
        selected, strategy, dropped = sc.select_activities(pool, limit=8)
        self.assertEqual(strategy, LAYERED)
        self.assertEqual(sum(dropped.values()), len(pool) - len(selected))
        self.assertIs(type(dropped), dict)
        self.assertEqual(dropped, {"command_run": 4})  # 1 of tier 2 (its middle) + all 3 of tier 3
        self.assertNotIn("edit_file", dropped)
        self.assertNotIn("user_message", dropped)


# ════════════════════════════════════════════════════════════════════════════════
# TASK-002 -- gates 4 / 5: the wired path (_parse_transcript -> stats["dropped"]
# -> _build_activities -> activity_data["internal"]["capture_dropped"]).
# Direct calls only: no main(), no _collect(), so no ledger and no HOME.
# ════════════════════════════════════════════════════════════════════════════════

def _tool_line(tool, tool_input):
    return {"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "tool_use", "name": tool, "input": tool_input}]}}


def _user_line(text):
    return {"type": "user", "message": {"role": "user", "content": text}}


class _TranscriptCase(unittest.TestCase):

    def _transcript(self, lines):
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(json.dumps(line) + "\n")
        self.addCleanup(os.unlink, path)
        return path

    def _overflow_lines(self):
        """Tier 1 alone exceeds the limit, and it is MIXED: the dropped middle
        must contain more than one action, or the gate-4 sum collapses to a
        single key and proves nothing."""
        lines = []
        for i in range(90):
            lines.append(_tool_line("Edit", {"file_path": f"/a/{i}.py"}))
            lines.append(_user_line(f"message {i}"))
            lines.append(_tool_line("Bash", {"command": f"git commit -m 'c{i}'"}))
        for i in range(10):
            lines.append(_tool_line("Bash", {"command": f"python3 x{i}.py"}))
        return lines  # 270 tier 1 + 10 tier 3 = 280 > 200


class TestGate4DroppedCountsAddUp(_TranscriptCase):

    def test_by_action_sums_to_total_across_several_actions(self):
        selected, stats = sc._parse_transcript(self._transcript(self._overflow_lines()))
        dropped = stats["dropped"]
        self.assertEqual(set(dropped), {"total", "by_action", "strategy"})
        self.assertEqual(dropped["total"], 280 - sc._MAX_ACTIVITIES)
        self.assertEqual(dropped["total"], 280 - len(selected))
        self.assertEqual(sum(dropped["by_action"].values()), dropped["total"])
        self.assertEqual(
            set(dropped["by_action"]), {"edit_file", "user_message", "commit", "command_run"},
            "the dropped middle of a mixed tier 1 must show every action it held",
        )
        self.assertEqual(dropped["strategy"], "degenerate")
        self.assertIsNone(stats["tiering_error"])
        activities = sc._build_activities(selected, "c1", "main", "s1", dropped)
        self.assertEqual(len(activities), sc._MAX_ACTIVITIES)
        for item in activities:
            payload = item["activity_data"]["internal"]["capture_dropped"]
            self.assertEqual(payload["total"], dropped["total"])
            self.assertEqual(sum(payload["by_action"].values()), payload["total"])
            self.assertEqual(payload["strategy"], "degenerate")


class TestGate5UntruncatedRunsStillCarryTheKey(_TranscriptCase):

    def test_zero_drops_are_written_not_omitted(self):
        lines = [_tool_line("Edit", {"file_path": "/a/1.py"}), _user_line("hello"),
                 _tool_line("Bash", {"command": "git push"})]
        selected, stats = sc._parse_transcript(self._transcript(lines))
        self.assertEqual(len(selected), 3)
        self.assertEqual(stats["dropped"], {"total": 0, "by_action": {}, "strategy": "layered"})
        self.assertIsNone(stats["tiering_error"])
        activities = sc._build_activities(selected, "c1", "main", "s1", stats["dropped"])
        self.assertEqual(len(activities), 3)
        for item in activities:
            # Key presence is asserted on its own, then read by index: a
            # `.get("capture_dropped", {}).get("total", 0) == 0` would pass
            # against a hook that never wrote the key at all.
            self.assertIn("internal", item["activity_data"])
            self.assertIn("capture_dropped", item["activity_data"]["internal"])
            payload = item["activity_data"]["internal"]["capture_dropped"]
            self.assertEqual(set(payload), {"total", "by_action", "strategy"})
            self.assertEqual(payload["total"], 0)
            self.assertEqual(payload["by_action"], {})
            self.assertEqual(payload["strategy"], "layered")

    def test_each_activity_gets_its_own_payload_object(self):
        lines = [_tool_line("Edit", {"file_path": f"/a/{i}.py"}) for i in range(4)]
        selected, stats = sc._parse_transcript(self._transcript(lines))
        activities = sc._build_activities(selected, "c1", None, "s1", stats["dropped"])
        payloads = [item["activity_data"]["internal"]["capture_dropped"] for item in activities]
        self.assertEqual(len({id(p) for p in payloads}), len(activities), "injected per activity, not shared")
        self.assertEqual(len({id(p["by_action"]) for p in payloads}), len(activities))

    def test_stats_strategy_never_says_telemetry_failed(self):
        """stats["dropped"]["strategy"] is the selector's verdict (3 values);
        telemetry_failed exists only on the wire, written by _build_activities."""
        for lines in (self._overflow_lines(), [_tool_line("Edit", {"file_path": "/a/1.py"})]):
            _, stats = sc._parse_transcript(self._transcript(lines))
            self.assertIn(stats["dropped"]["strategy"], {"layered", "degenerate", "fallback_tail"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
