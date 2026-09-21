"""Tests for hooks/hooks.json itself.

The manifest is configuration, but two of its values are load-bearing in ways
that are invisible from the file and easy to "tidy away":

``timeout`` on SessionEnd
    Claude Code gives every SessionEnd hook a SHARED budget of 1.5 seconds
    unless a hook declares a longer ``timeout``, in which case the budget is
    raised to match, capped at 60. Without the field the capture hook -- whose
    own HTTP timeout is 8 seconds -- is killed long before it can finish, and
    nothing records that it was.

``matcher`` on SessionStart
    ``compact`` and ``clear`` also fire SessionStart. Injecting the warm-start
    brief again there spends a retrieval on a session that already has it.

``timeout`` on SessionStart
    Not in the original spec; added when the hook started doing local ledger
    I/O (TASK-002). A command hook's default timeout is 600 seconds, and until
    then everything this hook did was individually bounded. It has to clear the
    hook's own network budget, or a slow-but-successful retrieval is killed.

JSON has no comments, so these tests (and the manifest's ``description``) are
where the reasons live.
"""

import importlib.util
import json
import os
import re
import unittest

_HOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
_MANIFEST = os.path.join(_HOOKS_DIR, "hooks.json")

# The sources SessionStart can report. `fork` is newer than the spec this
# plugin was written against; it inherits the parent's context, brief included.
_SESSION_START_SOURCES = ("startup", "resume", "clear", "compact", "fork")


def _load():
    with open(_MANIFEST, encoding="utf-8") as fh:
        return json.load(fh)


def _commands(event):
    """Every command-hook dict registered for one event."""
    found = []
    for entry in _load()["hooks"].get(event, []):
        for hook in entry.get("hooks", []):
            if hook.get("type") == "command":
                found.append((entry, hook))
    return found


class TestManifestParses(unittest.TestCase):
    def test_is_valid_json_with_the_three_events(self):
        data = _load()
        self.assertEqual(
            sorted(data["hooks"]), ["SessionEnd", "SessionStart", "UserPromptSubmit"]
        )

    def test_every_command_points_at_a_script_that_exists(self):
        """A typo here fails silently: Claude Code runs the command, python3
        exits 2 on the missing file, and a fail-open hook is indistinguishable
        from a hook that is not there."""
        seen = 0
        for event in _load()["hooks"]:
            for _, hook in _commands(event):
                match = re.search(r"\$\{CLAUDE_PLUGIN_ROOT\}/hooks/([\w.-]+\.py)", hook["command"])
                self.assertIsNotNone(match, f"unrecognised command shape: {hook['command']}")
                script = os.path.join(_HOOKS_DIR, match.group(1))
                self.assertTrue(os.path.isfile(script), f"{event}: {script} does not exist")
                seen += 1
        self.assertGreaterEqual(seen, 3)


class TestSessionEndBudget(unittest.TestCase):
    def test_every_session_end_hook_declares_the_sixty_second_timeout(self):
        hooks = _commands("SessionEnd")
        self.assertTrue(hooks, "no SessionEnd command hook registered")
        for _, hook in hooks:
            # `is`-style check on the type: JSON `60.0` or `"60"` would be a
            # different thing to whatever reads this next.
            self.assertIs(type(hook.get("timeout")), int, hook)
            self.assertEqual(hook["timeout"], 60, hook)

    def test_the_capture_hook_is_one_of_them(self):
        commands = [hook["command"] for _, hook in _commands("SessionEnd")]
        self.assertTrue(any("session_capture.py" in c for c in commands), commands)


class TestSessionStartBound(unittest.TestCase):
    def _inject_module(self):
        spec = importlib.util.spec_from_file_location(
            "session_inject_for_manifest_test", os.path.join(_HOOKS_DIR, "session_inject.py")
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_the_timeout_clears_the_network_budget_and_stays_bounded(self):
        mod = self._inject_module()
        network = mod._TIER1_TIMEOUT_SECONDS + mod._TIER2_TIMEOUT_SECONDS
        hooks = _commands("SessionStart")
        self.assertTrue(hooks, "no SessionStart command hook registered")
        for _, hook in hooks:
            self.assertIs(type(hook.get("timeout")), int, hook)
            # Below the network budget and a slow success gets killed; the
            # margin covers the three 5 s git calls around it.
            self.assertGreaterEqual(hook["timeout"], network + 15, hook)
            self.assertLessEqual(hook["timeout"], 60, hook)


class TestSessionStartMatcher(unittest.TestCase):
    def test_injection_is_scoped_to_startup_and_resume(self):
        entries = [entry for entry, _ in _commands("SessionStart")]
        self.assertTrue(entries, "no SessionStart command hook registered")
        for entry in entries:
            self.assertEqual(entry.get("matcher"), "startup|resume", entry)

    def test_the_matcher_means_what_it_says_as_a_regex(self):
        """Claude Code evaluates a matcher containing `|` as a regular
        expression, so pin the semantics and not only the spelling."""
        for entry, _ in _commands("SessionStart"):
            fires = {s for s in _SESSION_START_SOURCES if re.fullmatch(entry["matcher"], s)}
            self.assertEqual(fires, {"startup", "resume"})


if __name__ == "__main__":
    unittest.main()
