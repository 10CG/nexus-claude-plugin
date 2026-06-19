#!/usr/bin/env python3
"""Adversarial test suite for hooks/memory_capture.py.

Runnable as: python3 hooks/test_memory_capture.py
Requires no third-party dependencies (stdlib unittest only).
Drives the hook as a real subprocess (echo JSON | python3 memory_capture.py)
so tests assert on actual stdout + exit code — not on rule re-evaluation.

Coverage mapping to T4 acceptance criteria (detailed-tasks.yaml):
  AC-Parity   (D5/AC2)  : Class 1 — parity_* tests
  AC-Recall   (D7)      : Class 2 — recall_floor_* tests
  AC-Precision          : Class 3 — precision_no_fire_* tests
  AC-Contract (D6)      : Class 4 — output_contract_* tests
  AC-FailOpen (D3/AC3)  : Class 5 — failopen_* tests
"""

import json
import os
import subprocess
import sys
import unittest

# ── Path helpers ────────────────────────────────────────────────────────────────

_HOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
_HOOK_SCRIPT = os.path.join(_HOOKS_DIR, "memory_capture.py")
_RULES_PATH = os.path.join(_HOOKS_DIR, "rules.json")


def _run_hook(stdin_text: str):
    """Drive the hook as a subprocess, return (stdout_bytes, exit_code)."""
    result = subprocess.run(
        [sys.executable, _HOOK_SCRIPT],
        input=stdin_text.encode(),
        capture_output=True,
        timeout=10,
    )
    return result.stdout, result.returncode


def _hook_json(prompt: str, key: str = "prompt"):
    """Build the minimal UserPromptSubmit JSON for a given prompt key."""
    return json.dumps({key: prompt})


# ── Load rules.json once for the parity / recall-floor test generators ──────────

def _load_examples():
    with open(_RULES_PATH, encoding="utf-8") as fh:
        rules = json.load(fh)
    return rules.get("examples", [])


_EXAMPLES = _load_examples()
_CAPTURE_EXAMPLES = [e for e in _EXAMPLES if e["expected"] == "capture"]
_IGNORE_EXAMPLES  = [e for e in _EXAMPLES if e["expected"] == "ignore"]


# ════════════════════════════════════════════════════════════════════════════════
# Class 1 — Parity (D5/AC2): hook runtime output == rules.json examples verbatim
# ════════════════════════════════════════════════════════════════════════════════

class TestParitySingleRuleSet(unittest.TestCase):
    """For EVERY example in rules.json, assert the hook's live subprocess output
    matches the declared expected classification.  If hook logic and rules.json
    ever diverge, these tests fail — enforcing the single-rule-set invariant (D2/D5).
    This is the 'honored-by-convention' guard: a vacuous test that only re-reads
    rules.json without running the hook would give false assurance."""

    def _assert_capture(self, prompt, note=""):
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, f"Non-zero exit for capture prompt: {prompt!r}")
        self.assertTrue(
            len(stdout) > 0,
            f"Expected nudge output (capture) but got empty stdout for: {prompt!r} {note}",
        )

    def _assert_ignore(self, prompt, note=""):
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, f"Non-zero exit for ignore prompt: {prompt!r}")
        self.assertEqual(
            stdout, b"",
            f"Expected empty stdout (ignore) but got output for: {prompt!r} {note} | stdout={stdout!r}",
        )

    def test_all_examples_parity(self):
        """Each example in rules.json must match the hook's live classification."""
        failures = []
        for ex in _EXAMPLES:
            prompt = ex["prompt"]
            expected = ex["expected"]
            note = ex.get("_note", "")
            stdin = _hook_json(prompt)
            stdout, code = _run_hook(stdin)
            if code != 0:
                failures.append(f"NON-ZERO EXIT [{expected}] {prompt!r}")
                continue
            has_output = len(stdout) > 0
            if expected == "capture" and not has_output:
                failures.append(f"MISS (expected capture, got silent) | {note!r} | {prompt!r}")
            elif expected == "ignore" and has_output:
                failures.append(f"OVER-FIRE (expected ignore, got nudge) | {note!r} | {prompt!r}")
        if failures:
            self.fail(
                f"Parity failures ({len(failures)}/{len(_EXAMPLES)} examples):\n"
                + "\n".join(f"  {f}" for f in failures)
            )


# ════════════════════════════════════════════════════════════════════════════════
# Class 2 — Recall floor (D7/AC-recall)
# Each of the 5 dogfood positive scenarios must produce a nudge.
# These are independent of LLM sampling — this is the deterministic gate.
# ════════════════════════════════════════════════════════════════════════════════

class TestRecallFloor(unittest.TestCase):
    """D7 requirement: ALL examples with expected=='capture' must fire the nudge.
    Includes every dogfood write-intent scenario.  Failing here means the hook
    would structurally miss memory writes in a clean dogfood session."""

    def _assert_nudge(self, prompt, label=""):
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, f"Non-zero exit: {prompt!r}")
        self.assertGreater(
            len(stdout), 0,
            f"Recall miss — expected nudge but got silent stdout | {label} | {prompt!r}",
        )

    def test_dogfood_scenario1_pnpm_preference(self):
        """The primary dogfood write scenario must always fire (previously 1/5 in SKILL.md runs)."""
        self._assert_nudge(
            "Remember that I prefer pnpm over npm for new TypeScript projects.",
            label="dogfood-scenario-1",
        )

    def test_all_capture_examples_produce_nudge(self):
        """Every rules.json capture example must produce a nudge — deterministic recall floor.
        This catches any regression where a rules.json example is labeled 'capture'
        but the hook silently misclassifies it (D7 gate)."""
        failures = []
        for ex in _CAPTURE_EXAMPLES:
            prompt = ex["prompt"]
            note = ex.get("_note", "")
            stdin = _hook_json(prompt)
            stdout, code = _run_hook(stdin)
            if code != 0:
                failures.append(f"NON-ZERO EXIT | {note} | {prompt!r}")
            elif len(stdout) == 0:
                failures.append(f"SILENT (no nudge) | {note} | {prompt!r}")
        if failures:
            self.fail(
                f"Recall floor FAILED ({len(failures)}/{len(_CAPTURE_EXAMPLES)} capture examples):\n"
                + "\n".join(f"  {f}" for f in failures)
            )

    def test_first_person_preference_fires(self):
        self._assert_nudge("I prefer dark mode in all my editors.")

    def test_always_pattern_fires(self):
        self._assert_nudge("I always use 2-space indentation in Python.")

    def test_from_now_on_fires(self):
        self._assert_nudge("From now on, use British English spelling in my docs.")

    def test_team_fact_fires(self):
        self._assert_nudge("We use Postgres 15 in production.")

    def test_going_forward_fires(self):
        self._assert_nudge("Going forward, prefer composition over inheritance in my code.")

    def test_my_N_is_fires(self):
        self._assert_nudge("My timezone is Asia/Shanghai.")

    def test_remember_my_customer_fires(self):
        self._assert_nudge("Remember that my main customer is Kairos.")

    def test_standardize_fires(self):
        self._assert_nudge("We've decided to standardize on TypeScript for all new services.")

    def test_identity_fires(self):
        self._assert_nudge("I'm a backend engineer focused on distributed systems.")

    def test_preferred_tool_fires(self):
        self._assert_nudge("My preferred test framework is pytest.")

    def test_please_remember_fires(self):
        self._assert_nudge("Please remember I like concise commit messages.")

    def test_never_use_fires(self):
        self._assert_nudge("I never use class components in React anymore.")


# ════════════════════════════════════════════════════════════════════════════════
# Class 3 — Precision / over-fire bound (ai R2 note)
# All ignore examples must produce NO output (empty stdout).
# ════════════════════════════════════════════════════════════════════════════════

class TestPrecisionNoOverFire(unittest.TestCase):
    """All rules.json examples with expected=='ignore' must produce silent stdout.
    An over-firing hook floods the model context with spurious nudges, degrading
    response quality and trust in the classification signal."""

    def _assert_silent(self, prompt, label=""):
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, f"Non-zero exit: {prompt!r}")
        self.assertEqual(
            stdout, b"",
            f"Over-fire — expected silent but got nudge | {label} | {prompt!r} | stdout={stdout!r}",
        )

    def test_all_ignore_examples_are_silent(self):
        """Every rules.json ignore example must produce empty stdout."""
        failures = []
        for ex in _IGNORE_EXAMPLES:
            prompt = ex["prompt"]
            note = ex.get("_note", "")
            stdin = _hook_json(prompt)
            stdout, code = _run_hook(stdin)
            if code != 0:
                failures.append(f"NON-ZERO EXIT | {note} | {prompt!r}")
            elif len(stdout) > 0:
                failures.append(f"OVER-FIRE (got nudge) | {note} | {prompt!r}")
        if failures:
            self.fail(
                f"Precision failures ({len(failures)}/{len(_IGNORE_EXAMPLES)} ignore examples):\n"
                + "\n".join(f"  {f}" for f in failures)
            )

    def test_dogfood_scenario4_within_session_summary(self):
        """Dogfood scenario 4 must be silent — within-session summary is transient."""
        self._assert_silent(
            "Summarize what we discussed in the last 5 minutes of this conversation.",
            label="dogfood-scenario-4",
        )

    def test_dogfood_scenario2_recall_query(self):
        """Read/recall queries must never fire the write nudge."""
        self._assert_silent(
            "What did I say about TypeScript tooling preferences?",
            label="dogfood-scenario-2",
        )

    def test_dogfood_scenario3_feedback(self):
        """Feedback prompts must be silent — route to feedback, not memory_create."""
        self._assert_silent(
            "That was helpful, thanks. Rate that retrieval 5 stars.",
            label="dogfood-scenario-3",
        )

    def test_dogfood_scenario5_recall_with_time(self):
        """Named-time recall must be silent."""
        self._assert_silent(
            "Three months ago I had a customer requirement about export formats. What was it?",
            label="dogfood-scenario-5",
        )

    def test_transient_explicit_for_now(self):
        """'for now, keep answers short in this chat' is the canonical transient case."""
        self._assert_silent("for now, keep answers short in this chat")

    def test_for_the_rest_of_this_chat(self):
        self._assert_silent("For the rest of this chat, just give me code with no explanation.")

    def test_remind_me_to(self):
        """'remind me to' signals a within-session reminder, not a durable fact."""
        self._assert_silent("Remind me to commit before I close this.")

    def test_just_for_today(self):
        self._assert_silent("Just for today, skip the tests.")

    def test_recap(self):
        self._assert_silent("Recap what we covered so far.")

    def test_question_about_preference(self):
        """A question about a preference is a read (context_retrieve), not a write."""
        self._assert_silent("What is my preferred package manager?")

    def test_do_you_remember(self):
        self._assert_silent("Do you remember what I told you earlier?")

    def test_transient_negative_overrides_positive(self):
        """A prompt that has BOTH a capture_positive match AND a transient_negative
        match must classify as ignore — the negative overrides (over-capture bound, D7).
        This directly tests the 'has_negative -> not capture' branch in _classify()."""
        # "I prefer short answers for now" — 'i prefer' is capture_positive,
        # 'for now' is transient_negative; negative must win.
        self._assert_silent("I prefer short answers for now")

    def test_purely_neutral_question(self):
        self._assert_silent("Explain how Python decorators work.")

    def test_coding_task_no_preference(self):
        self._assert_silent("Write a function to reverse a linked list.")


# ════════════════════════════════════════════════════════════════════════════════
# Class 4 — Output contract (D6)
# On a capture: JSON structure, hookEventName, additionalContext content.
# ════════════════════════════════════════════════════════════════════════════════

class TestOutputContract(unittest.TestCase):
    """When the hook fires, the JSON output must satisfy the full D6 contract:
      - Valid JSON (parseable)
      - hookSpecificOutput.hookEventName == "UserPromptSubmit"
      - additionalContext is a non-empty string
      - additionalContext mentions nexus.memory_create (explicit tool name)
      - additionalContext does NOT contain a bare 'remember' imperative
        (guard against re-arming Claude's built-in auto-memory routing, D6/D2)
    """

    _CANONICAL_CAPTURE = "Remember that I prefer pnpm over npm for new TypeScript projects."

    def _get_nudge_output(self, prompt):
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertGreater(len(stdout), 0, f"Expected nudge for: {prompt!r}")
        return stdout

    def test_output_is_valid_json(self):
        stdout = self._get_nudge_output(self._CANONICAL_CAPTURE)
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            self.fail(f"Hook stdout is not valid JSON: {exc}\nstdout={stdout!r}")
        self.assertIsInstance(parsed, dict)

    def test_hook_specific_output_key_present(self):
        stdout = self._get_nudge_output(self._CANONICAL_CAPTURE)
        parsed = json.loads(stdout)
        self.assertIn("hookSpecificOutput", parsed, "Missing top-level 'hookSpecificOutput' key")

    def test_hook_event_name_is_UserPromptSubmit(self):
        stdout = self._get_nudge_output(self._CANONICAL_CAPTURE)
        parsed = json.loads(stdout)
        hso = parsed["hookSpecificOutput"]
        self.assertEqual(
            hso.get("hookEventName"), "UserPromptSubmit",
            f"hookEventName must be 'UserPromptSubmit', got: {hso.get('hookEventName')!r}",
        )

    def test_additional_context_is_non_empty_string(self):
        stdout = self._get_nudge_output(self._CANONICAL_CAPTURE)
        parsed = json.loads(stdout)
        ctx = parsed["hookSpecificOutput"].get("additionalContext")
        self.assertIsInstance(ctx, str, "additionalContext must be a string")
        self.assertTrue(ctx.strip(), "additionalContext must not be empty or whitespace-only")

    def test_additional_context_names_nexus_memory_create_tool(self):
        """D6: nudge must explicitly name the nexus.memory_create MCP tool."""
        stdout = self._get_nudge_output(self._CANONICAL_CAPTURE)
        parsed = json.loads(stdout)
        ctx = parsed["hookSpecificOutput"]["additionalContext"]
        self.assertIn(
            "nexus.memory_create", ctx,
            "additionalContext must explicitly name 'nexus.memory_create' (D6 tool-name guard).\n"
            f"Got: {ctx!r}",
        )

    def test_additional_context_no_bare_remember_imperative(self):
        """D6: nudge must NOT contain a bare lowercase 'remember' imperative
        that could re-arm Claude's built-in auto-memory routing (the structural
        problem this hook was built to fix).  Any occurrence of the word 'remember'
        in an imperative/verb position is the forbidden pattern.

        Note: proposal D6 says 'avoid bare "remember"/"save to memory" imperatives'.
        We check for 'remember' as a standalone word (case-insensitive) anywhere in
        the nudge text, which is the conservative guard."""
        import re as _re
        stdout = self._get_nudge_output(self._CANONICAL_CAPTURE)
        parsed = json.loads(stdout)
        ctx = parsed["hookSpecificOutput"]["additionalContext"]
        match = _re.search(r"\bremember\b", ctx, _re.IGNORECASE)
        self.assertIsNone(
            match,
            f"additionalContext must not contain bare 'remember' verb (re-arms built-in routing).\n"
            f"Found at position {match.start() if match else 'n/a'} in: {ctx!r}",
        )

    def test_output_contract_multiple_capture_prompts(self):
        """Contract must hold for several capture prompts, not just the dogfood example."""
        capture_prompts = [
            "I always use 2-space indentation in Python.",
            "We use Postgres 15 in production.",
            "My preferred test framework is pytest.",
            "I'm a backend engineer focused on distributed systems.",
        ]
        import re as _re
        for prompt in capture_prompts:
            with self.subTest(prompt=prompt):
                stdout = self._get_nudge_output(prompt)
                parsed = json.loads(stdout)
                hso = parsed.get("hookSpecificOutput", {})
                self.assertEqual(hso.get("hookEventName"), "UserPromptSubmit")
                ctx = hso.get("additionalContext", "")
                self.assertIn("nexus.memory_create", ctx)
                self.assertIsNone(_re.search(r"\bremember\b", ctx, _re.IGNORECASE))

    def test_no_extra_stdout_on_ignore(self):
        """Ignore path must produce zero bytes on stdout — no partial JSON, no newlines."""
        stdin = _hook_json("Explain how Python decorators work.")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertEqual(stdout, b"", f"Expected exactly empty bytes, got: {stdout!r}")


# ════════════════════════════════════════════════════════════════════════════════
# Class 5 — Fail-open (D3/AC3)
# Malformed input must produce exit 0 AND empty stdout — never block the prompt.
# ════════════════════════════════════════════════════════════════════════════════

class TestFailOpen(unittest.TestCase):
    """D3/AC3: a non-zero exit from a UserPromptSubmit hook BLOCKS the user's prompt.
    Every malformed / edge-case input MUST produce exit 0 with empty stdout.
    We assert exit code EXPLICITLY — 'no output' alone is insufficient because
    a silent non-zero exit would still block the user."""

    def _assert_failopen(self, stdin_text, label):
        stdout, code = _run_hook(stdin_text)
        self.assertEqual(
            code, 0,
            f"FAIL-OPEN VIOLATION: got exit {code} (non-zero BLOCKS prompt) for {label}\n"
            f"stdin={stdin_text!r}",
        )
        self.assertEqual(
            stdout, b"",
            f"FAIL-OPEN VIOLATION: got non-empty stdout for {label}\nstdout={stdout!r}",
        )

    def test_empty_stdin(self):
        self._assert_failopen("", label="empty stdin")

    def test_whitespace_only_stdin(self):
        self._assert_failopen("   \n\t  ", label="whitespace-only stdin")

    def test_non_json_stdin(self):
        self._assert_failopen("not json at all", label="non-JSON plain text")

    def test_json_list_not_dict(self):
        """JSON that is a list (not a dict) must fail-open — _extract_prompt expects dict."""
        self._assert_failopen("[]", label="JSON list (not dict)")

    def test_json_list_of_strings(self):
        self._assert_failopen('["remember", "this"]', label="JSON list of strings")

    def test_json_missing_prompt_key(self):
        self._assert_failopen('{"other_key": "value"}', label="JSON missing prompt key")

    def test_json_prompt_is_null(self):
        self._assert_failopen('{"prompt": null}', label="prompt value null")

    def test_json_prompt_is_number(self):
        self._assert_failopen('{"prompt": 42}', label="prompt value is number")

    def test_json_prompt_is_empty_string(self):
        """An empty string prompt has no positive match, so must produce no output.
        Also tests that it doesn't crash."""
        self._assert_failopen('{"prompt": ""}', label="prompt value is empty string")

    def test_json_prompt_is_whitespace_only(self):
        self._assert_failopen('{"prompt": "   "}', label="prompt value is whitespace-only")

    def test_json_prompt_is_list(self):
        self._assert_failopen('{"prompt": ["remember", "this"]}', label="prompt value is list")

    def test_json_prompt_is_dict(self):
        self._assert_failopen('{"prompt": {"nested": "value"}}', label="prompt value is dict")

    def test_json_prompt_is_false(self):
        self._assert_failopen('{"prompt": false}', label="prompt value is bool false")

    def test_truncated_json(self):
        self._assert_failopen('{"prompt": "I prefer dark mode', label="truncated/partial JSON")

    def test_null_json_root(self):
        self._assert_failopen("null", label="JSON root is null")

    def test_json_number_root(self):
        self._assert_failopen("42", label="JSON root is a number")

    def test_json_string_root(self):
        self._assert_failopen('"just a string"', label="JSON root is a bare string")


# ════════════════════════════════════════════════════════════════════════════════
# Class 6 — Prompt key variants
# The hook tolerates 'prompt', 'user_prompt', and 'userPrompt' key names.
# ════════════════════════════════════════════════════════════════════════════════

class TestPromptKeyVariants(unittest.TestCase):
    """_extract_prompt() accepts 3 key names; verify hook fires for each."""

    _CAPTURE_PROMPT = "I prefer pnpm over npm."

    def test_key_prompt(self):
        stdin = _hook_json(self._CAPTURE_PROMPT, key="prompt")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertGreater(len(stdout), 0, "Key 'prompt' must fire nudge")

    def test_key_user_prompt(self):
        stdin = _hook_json(self._CAPTURE_PROMPT, key="user_prompt")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertGreater(len(stdout), 0, "Key 'user_prompt' must fire nudge")

    def test_key_userPrompt(self):
        stdin = _hook_json(self._CAPTURE_PROMPT, key="userPrompt")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertGreater(len(stdout), 0, "Key 'userPrompt' must fire nudge")

    def test_unknown_key_is_failopen(self):
        """An event with none of the 3 expected keys must fail-open (no nudge, exit 0)."""
        stdin = json.dumps({"content": self._CAPTURE_PROMPT})
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertEqual(stdout, b"", "Unknown key must produce empty stdout")


# ════════════════════════════════════════════════════════════════════════════════
# Class 7 — Non-vacuity self-check
# Prove the suite can go red — a green suite that can't fail is worthless.
# (Project lesson: feedback_type_annotation_vacuous_if_file_not_typechecked)
# ════════════════════════════════════════════════════════════════════════════════

class TestNonVacuityProof(unittest.TestCase):
    """This class proves the suite is non-vacuous by asserting that a
    deliberately WRONG expectation against a known-capture prompt produces a
    test failure.  We catch that AssertionError and confirm it happened —
    demonstrating the underlying test infrastructure can and does go red.

    This is a meta-test: it PASSES if and only if the underlying assertion FAILS
    as expected.  A meta-test that doesn't catch an AssertionError would itself
    fail, surfacing the bug in the test design."""

    def test_suite_can_go_red_on_capture(self):
        """Feed the known-good dogfood capture prompt and assert INCORRECTLY that
        stdout should be empty.  Catch the resulting AssertionError.  If no error
        is raised, the test suite is vacuous (hook produced empty stdout for a
        capture prompt — that's a real defect, not a non-vacuity proof)."""
        prompt = "Remember that I prefer pnpm over npm for new TypeScript projects."
        stdin = _hook_json(prompt)
        stdout, _ = _run_hook(stdin)

        # The hook must have produced output for this capture prompt.
        # If it did not, there is a real defect in the hook — report it clearly.
        if len(stdout) == 0:
            self.fail(
                "NON-VACUITY PROOF FAILED: the hook produced empty stdout for a known-capture "
                "prompt — this is a real defect in memory_capture.py, not a test infrastructure "
                f"issue. Prompt: {prompt!r}"
            )

        # Now assert the WRONG thing on purpose to prove the assertion can fail.
        intentionally_wrong_assertion_triggered = False
        try:
            self.assertEqual(
                stdout, b"",
                "This is an intentionally wrong assertion to prove the test can go red."
            )
        except AssertionError:
            intentionally_wrong_assertion_triggered = True

        self.assertTrue(
            intentionally_wrong_assertion_triggered,
            "Non-vacuity self-check failed: the deliberately wrong assertion did not raise "
            "AssertionError.  The test infrastructure itself may be broken.",
        )

    def test_suite_can_go_red_on_ignore(self):
        """Feed a known-good ignore prompt and assert INCORRECTLY that stdout should
        be non-empty.  Catch the AssertionError to prove the suite can go red."""
        prompt = "Summarize what we discussed in the last 5 minutes of this conversation."
        stdin = _hook_json(prompt)
        stdout, _ = _run_hook(stdin)

        # The hook must have produced empty stdout for this ignore prompt.
        if len(stdout) > 0:
            self.fail(
                "NON-VACUITY PROOF FAILED: the hook produced non-empty stdout for a known-ignore "
                f"prompt — this is a real precision defect. Prompt: {prompt!r} stdout={stdout!r}"
            )

        # Assert the WRONG thing: expect non-empty stdout when it should be empty.
        intentionally_wrong_assertion_triggered = False
        try:
            self.assertGreater(
                len(stdout), 0,
                "This is an intentionally wrong assertion to prove the test can go red.",
            )
        except AssertionError:
            intentionally_wrong_assertion_triggered = True

        self.assertTrue(
            intentionally_wrong_assertion_triggered,
            "Non-vacuity self-check failed: the deliberately wrong assertion did not raise "
            "AssertionError.",
        )


# ════════════════════════════════════════════════════════════════════════════════
# Runner
# ════════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Verbose output so CI logs show individual test names and results.
    loader = unittest.TestLoader()
    # Sort test classes in the order defined (parity → recall → precision → contract → failopen).
    suite = unittest.TestSuite()
    for cls in [
        TestParitySingleRuleSet,
        TestRecallFloor,
        TestPrecisionNoOverFire,
        TestOutputContract,
        TestFailOpen,
        TestPromptKeyVariants,
        TestNonVacuityProof,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
