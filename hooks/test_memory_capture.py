#!/usr/bin/env python3
"""Adversarial test suite for hooks/memory_capture.py.

Runnable as: python3 hooks/test_memory_capture.py
Requires no third-party dependencies (stdlib unittest only).
Drives the hook as a real subprocess (echo JSON | python3 memory_capture.py)
so tests assert on actual stdout + exit code — not on rule re-evaluation.

Coverage mapping to T4 acceptance criteria (detailed-tasks.yaml):
  AC-Parity   (D5/AC2)  : Class 1 — parity_* tests (3-state: capture/recall/ignore)
  AC-Recall   (D7)      : Class 2 — recall_floor_* tests (write-path positives)
  AC-Precision          : Class 3 — precision_no_fire_* tests
  AC-Contract (D6)      : Class 4 — output_contract_* tests
  AC-FailOpen (D3/AC3)  : Class 5 — failopen_* tests
  AC-ReadParity (D1)    : Class 8  — read_parity_* tests (read_examples parity)
  AC-ReadRecall (D1)    : Class 9  — read_recall_floor_* tests
  AC-OverFire (AC1/D4)  : Class 10 — over_fire_gate_* tests (held-out corpus ≥12)
  AC-ReadContract (D5)  : Class 11 — read_output_contract_* tests
  AC-MutualExcl (D3)    : Class 12 — mutual_exclusion_* tests
  AC-ReadNonVacuity     : Class 13 — read_non_vacuity_* tests
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


def _load_read_examples():
    with open(_RULES_PATH, encoding="utf-8") as fh:
        rules = json.load(fh)
    return rules.get("read_examples", [])


def _nudge_intent(stdout_bytes):
    """Classify which intent a hook output belongs to, or None if silent."""
    if not stdout_bytes:
        return None
    try:
        parsed = json.loads(stdout_bytes)
        ctx = parsed["hookSpecificOutput"]["additionalContext"]
    except (json.JSONDecodeError, KeyError):
        return "PARSE_ERROR"
    if "nexus.memory_create" in ctx:
        return "write"
    if "nexus.context_retrieve" in ctx:
        # timetravel nudge contains 'as_of' instruction; recall nudge does not
        if "as_of" in ctx and "90 days" in ctx:
            return "timetravel"
        return "recall"
    return "UNKNOWN"


_EXAMPLES = _load_examples()
_CAPTURE_EXAMPLES = [e for e in _EXAMPLES if e["expected"] == "capture"]
_IGNORE_EXAMPLES  = [e for e in _EXAMPLES if e["expected"] == "ignore"]
_READ_EXAMPLES    = _load_read_examples()


# ════════════════════════════════════════════════════════════════════════════════
# Class 1 — Parity (D5/AC2): hook runtime output == rules.json examples verbatim
#
# examples[] uses a 3-state label system after the read-path refactor:
#   "capture"   → hook must produce a WRITE nudge (nexus.memory_create)
#   "recall"    → hook must produce a READ nudge (nexus.context_retrieve)
#   "ignore"    → hook must produce empty stdout (no nudge of any kind)
#
# Note: the old examples[] only had "capture"/"ignore" labels. After the read-path
# was added, some prompts that were "ignore" (no write nudge) now correctly route
# to the read path ("recall"). The labels in examples[] were updated to reflect
# the new 3-state contract. The parity test here enforces that the hook's actual
# behavior matches each label — not a static re-read of the rules.
# ════════════════════════════════════════════════════════════════════════════════

class TestParitySingleRuleSet(unittest.TestCase):
    """For EVERY example in rules.json examples[], assert the hook's live subprocess
    output matches the declared expected classification (3-state: capture/recall/ignore).
    If hook logic and rules.json ever diverge, these tests fail — enforcing the
    single-rule-set invariant (D2/D5).  This is the 'honored-by-convention' guard:
    a vacuous test that only re-reads rules.json without running the hook would give
    false assurance."""

    def _assert_capture(self, prompt, note=""):
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, f"Non-zero exit for capture prompt: {prompt!r}")
        intent = _nudge_intent(stdout)
        self.assertEqual(
            intent, "write",
            f"Expected WRITE nudge (capture) but got intent={intent!r} for: {prompt!r} {note}",
        )

    def _assert_recall(self, prompt, note=""):
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, f"Non-zero exit for recall prompt: {prompt!r}")
        intent = _nudge_intent(stdout)
        self.assertIn(
            intent, ("recall", "timetravel"),
            f"Expected READ nudge (recall or timetravel) but got intent={intent!r} for: {prompt!r} {note}",
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
        """Each example in rules.json examples[] must match the hook's live classification.
        3-state: capture->write nudge, recall->read nudge, ignore->empty stdout."""
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
            intent = _nudge_intent(stdout)
            if expected == "capture" and intent != "write":
                failures.append(f"MISS (expected write nudge, got intent={intent!r}) | {note!r} | {prompt!r}")
            elif expected == "recall" and intent not in ("recall", "timetravel"):
                failures.append(f"MISS (expected read nudge, got intent={intent!r}) | {note!r} | {prompt!r}")
            elif expected == "ignore" and stdout != b"":
                failures.append(f"OVER-FIRE (expected ignore, got intent={intent!r}) | {note!r} | {prompt!r}")
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
        """Dogfood scenario 2: cross-session recall query must fire a READ nudge
        (nexus.context_retrieve), NOT a write nudge (nexus.memory_create).
        Previously asserted silence, but with the read path added this prompt
        correctly routes to recall — updated assertion to match new contract."""
        stdin = _hook_json("What did I say about TypeScript tooling preferences?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, "Non-zero exit (dogfood-scenario-2)")
        intent = _nudge_intent(stdout)
        # Must fire a READ nudge, not a write nudge, not silence
        self.assertIn(
            intent, ("recall", "timetravel"),
            f"Expected READ nudge for cross-session recall query, got intent={intent!r} | "
            f"stdout={stdout!r}",
        )
        # Explicitly guard: must NOT be a write nudge
        self.assertNotEqual(
            intent, "write",
            "Recall query must never produce a WRITE nudge (nexus.memory_create)",
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
        """A question about a preference is a READ (context_retrieve), not a write.
        With the read path added, this prompt correctly fires a RECALL nudge.
        The test now asserts READ routing (not silence) — ensuring no WRITE nudge fires."""
        stdin = _hook_json("What is my preferred package manager?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0, "Non-zero exit for preference question")
        intent = _nudge_intent(stdout)
        self.assertIn(
            intent, ("recall", "timetravel"),
            f"Expected READ nudge for preference question, got intent={intent!r}",
        )
        self.assertNotEqual(
            intent, "write",
            "Preference question must never produce a WRITE nudge",
        )

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
# Class 8 — Read parity (D1/AC1): hook runtime matches read_examples[] verbatim
#
# For EVERY entry in rules.json read_examples[], run the hook as a subprocess
# and assert:
#   expected=="recall"      → stdout names nexus.context_retrieve WITHOUT as_of
#   expected=="timetravel"  → stdout names nexus.context_retrieve WITH as_of
#   expected=="ignore"      → stdout is empty
#
# This is the single-source enforcement for the read partition: the hook's
# runtime classification must match the shared rules.json artifact (D1).
# ════════════════════════════════════════════════════════════════════════════════

class TestReadParitySingleRuleSet(unittest.TestCase):
    """Parity check: every read_examples[] entry must match hook's live classification.
    Enforces the single-rule-set invariant (D1) for the read partition."""

    def test_all_read_examples_parity(self):
        """Drive the hook for every read_examples[] entry; assert live output matches label."""
        failures = []
        for ex in _READ_EXAMPLES:
            prompt = ex["prompt"]
            expected = ex["expected"]
            note = ex.get("_note", "")
            stdin = _hook_json(prompt)
            stdout, code = _run_hook(stdin)
            if code != 0:
                failures.append(f"NON-ZERO EXIT [{expected}] {prompt!r}")
                continue
            intent = _nudge_intent(stdout)
            if expected == "recall":
                # Must produce a READ nudge — not a write nudge, not silence
                if intent not in ("recall", "timetravel"):
                    failures.append(
                        f"MISS (expected recall nudge, got intent={intent!r}) | {note!r} | {prompt!r}"
                    )
                # Specifically: recall expected means no as_of instruction
                if intent == "recall":
                    try:
                        ctx = json.loads(stdout)["hookSpecificOutput"]["additionalContext"]
                    except (KeyError, json.JSONDecodeError):
                        ctx = ""
                    if "Do NOT pass an as_of" not in ctx and "as_of" in ctx and "90 days" in ctx:
                        failures.append(
                            f"RECALL expected but got timetravel (as_of+90d present) | {note!r} | {prompt!r}"
                        )
            elif expected == "timetravel":
                if intent != "timetravel":
                    failures.append(
                        f"MISS (expected timetravel nudge, got intent={intent!r}) | {note!r} | {prompt!r}"
                    )
            elif expected == "ignore":
                if stdout != b"":
                    failures.append(
                        f"OVER-FIRE (expected ignore, got intent={intent!r}) | {note!r} | {prompt!r}"
                    )
            else:
                failures.append(f"UNKNOWN expected label {expected!r} | {prompt!r}")

        if failures:
            total = len(_READ_EXAMPLES)
            self.fail(
                f"Read parity failures ({len(failures)}/{total} read_examples):\n"
                + "\n".join(f"  {f}" for f in failures)
            )

    def test_dogfood_s2_recall_no_as_of(self):
        """S2 (dogfood rewrite): 'What's my preferred unit-testing framework?' →
        recall nudge naming nexus.context_retrieve WITHOUT an as_of instruction."""
        prompt = "What's my preferred unit-testing framework?"
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        intent = _nudge_intent(stdout)
        self.assertEqual(
            intent, "recall",
            f"S2 must route to recall, got intent={intent!r} | stdout={stdout!r}",
        )
        ctx = json.loads(stdout)["hookSpecificOutput"]["additionalContext"]
        self.assertIn("nexus.context_retrieve", ctx, "Recall nudge must name nexus.context_retrieve")
        # Strict: recall must NOT have as_of+90d instruction (that is timetravel territory)
        self.assertIn(
            "Do NOT pass an as_of", ctx,
            "Recall nudge must instruct model NOT to pass an as_of (no past time named)",
        )

    def test_dogfood_s5_timetravel_has_as_of(self):
        """S5 (dogfood rewrite): 'A few weeks ago I set a preference about testing tools —
        what was it?' → timetravel nudge naming nexus.context_retrieve WITH as_of."""
        prompt = "A few weeks ago I set a preference about testing tools — what was it?"
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        intent = _nudge_intent(stdout)
        self.assertEqual(
            intent, "timetravel",
            f"S5 must route to timetravel, got intent={intent!r} | stdout={stdout!r}",
        )
        ctx = json.loads(stdout)["hookSpecificOutput"]["additionalContext"]
        self.assertIn("nexus.context_retrieve", ctx, "Timetravel nudge must name nexus.context_retrieve")
        self.assertIn("as_of", ctx, "Timetravel nudge must mention as_of")


# ════════════════════════════════════════════════════════════════════════════════
# Class 9 — Read recall floor (D1/AC1)
# ALL recall + timetravel positives in read_examples[] must produce the correct nudge.
# ════════════════════════════════════════════════════════════════════════════════

class TestReadRecallFloor(unittest.TestCase):
    """Recall floor for the read path: every read_examples[] positive (recall or
    timetravel) must produce a nudge.  A miss here means the hook would silently
    skip routing cross-session recall to nexus.context_retrieve."""

    def test_all_recall_positives_produce_nudge(self):
        """Every read_examples[] entry with expected recall or timetravel must produce output."""
        positives = [e for e in _READ_EXAMPLES if e["expected"] in ("recall", "timetravel")]
        failures = []
        for ex in positives:
            prompt = ex["prompt"]
            note = ex.get("_note", "")
            expected = ex["expected"]
            stdin = _hook_json(prompt)
            stdout, code = _run_hook(stdin)
            if code != 0:
                failures.append(f"NON-ZERO EXIT | {note} | {prompt!r}")
            elif not stdout:
                failures.append(f"SILENT (no nudge, expected {expected}) | {note} | {prompt!r}")
            else:
                intent = _nudge_intent(stdout)
                if intent not in ("recall", "timetravel"):
                    failures.append(f"WRONG INTENT={intent!r} expected {expected} | {note} | {prompt!r}")
        if failures:
            self.fail(
                f"Read recall floor FAILED ({len(failures)}/{len(positives)} positives):\n"
                + "\n".join(f"  {f}" for f in failures)
            )

    def test_dogfood_s2_recall_fires(self):
        """Dogfood S2 'What's my preferred unit-testing framework?' must fire recall."""
        stdin = _hook_json("What's my preferred unit-testing framework?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIn(_nudge_intent(stdout), ("recall", "timetravel"))

    def test_dogfood_s5_timetravel_fires(self):
        """Dogfood S5 'A few weeks ago I set a preference...' must fire timetravel."""
        stdin = _hook_json(
            "A few weeks ago I set a preference about testing tools — what was it?"
        )
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertEqual(_nudge_intent(stdout), "timetravel")

    def test_cross_session_recall_fires(self):
        """Direct cross-session recall pattern must fire."""
        stdin = _hook_json("What's my usual database for new services?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIn(_nudge_intent(stdout), ("recall", "timetravel"))

    def test_we_decided_recall_fires(self):
        """Team decision recall must fire."""
        stdin = _hook_json("What did we decide about the API versioning scheme?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIn(_nudge_intent(stdout), ("recall", "timetravel"))

    def test_last_week_timetravel_fires(self):
        """Named 'last week' must fire timetravel nudge."""
        stdin = _hook_json("Last week I set a preference about my editor — what was it?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertEqual(_nudge_intent(stdout), "timetravel")

    def test_date_timetravel_fires(self):
        """Explicit ISO date must fire timetravel nudge."""
        stdin = _hook_json("On 2026-05-30 I mentioned a tooling preference — what was it?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertEqual(_nudge_intent(stdout), "timetravel")


# ════════════════════════════════════════════════════════════════════════════════
# Class 10 — Numeric over-fire gate (AC1/D4)
#
# HELD-OUT paraphrases NOT present in rules.json read_examples[] (D4 requirement:
# anti teaching-to-the-test). The corpus must have ≥ 12 entries. Each must produce
# ZERO read nudges.  If someone broadens read_allowlist to over-fire, these go red.
#
# Categories covered (T4 spec):
#   A. Imperative coding tasks
#   B. Third-person / general knowledge questions
#   C. Project-static lookups
#   D. Within-session referents
#   E. Ambiguous-but-blocklisted by read_blocklist
#
# Held-out = paraphrase not verbatim in read_examples[]. Verified below.
# ════════════════════════════════════════════════════════════════════════════════

# The held-out negative corpus — D4: must NOT be verbatim in read_examples[]
_OVER_FIRE_CORPUS = [
    # A. Imperative coding (4 prompts) — read_blocklist matches (write|fix|refactor|...)
    ("rename this variable to user_id",
     "imperative-coding: rename"),
    ("add a docstring to this function",
     "imperative-coding: add docstring"),
    ("refactor this class to use dependency injection",
     "imperative-coding: refactor"),
    ("generate a migration for the users table",
     "imperative-coding: generate migration"),

    # B. Third-person / general knowledge (4 prompts) — no first-person recall
    ("what's the difference between a list and a tuple in Python",
     "general-knowledge: list vs tuple"),
    ("when was Python 3.12 released",
     "general-knowledge: Python 3.12 release date"),
    ("what are the main features of PostgreSQL 16",
     "general-knowledge: postgres features"),
    ("how does garbage collection work in CPython",
     "general-knowledge: cpython gc"),

    # C. Project-static lookups (2 prompts) — project facts, no personal recall
    ("which port does the dev server run on",
     "project-static: dev server port"),
    ("what does this regex match",
     "project-static: regex explanation"),

    # D. Within-session referents (4 prompts) — read_blocklist within-session patterns
    ("summarize what you just told me",
     "within-session: summarize you just told me"),
    ("what did you say two messages ago",
     "within-session: two messages ago"),
    ("can you repeat what you explained earlier in this chat",
     "within-session: earlier in this chat"),
    ("what were we discussing five minutes ago",
     "within-session: five minutes ago"),
]

_OVER_FIRE_CORPUS_SIZE = len(_OVER_FIRE_CORPUS)


class TestOverFireGate(unittest.TestCase):
    """AC1 (T4 spec): numeric over-fire gate on held-out paraphrases (D4).

    Corpus size: _OVER_FIRE_CORPUS_SIZE prompts (must be ≥ 12).
    Every entry must produce ZERO nudge output.  These are held-out paraphrases
    that do NOT appear verbatim in rules.json read_examples[] — anti teaching-to-
    the-test (D4).  If read_allowlist is broadened to over-fire, these go red.
    """

    @classmethod
    def setUpClass(cls):
        """Enforce corpus size ≥ 12 at class setup, not inside test methods,
        so the constraint appears as a setup error (not a hidden test pass)."""
        assert _OVER_FIRE_CORPUS_SIZE >= 12, (
            f"Over-fire corpus must have ≥ 12 entries (AC1), got {_OVER_FIRE_CORPUS_SIZE}"
        )
        # Verify D4: none of the held-out corpus prompts appear verbatim in read_examples[]
        read_prompt_set = {ex["prompt"] for ex in _READ_EXAMPLES}
        verbatim_hits = [
            (p, label) for p, label in _OVER_FIRE_CORPUS if p in read_prompt_set
        ]
        if verbatim_hits:
            raise AssertionError(
                f"D4 violated: {len(verbatim_hits)} held-out corpus entries are verbatim in "
                f"read_examples[] (teaching-to-the-test): "
                + ", ".join(f"{p!r}" for p, _ in verbatim_hits)
            )

    def test_corpus_size_at_least_12(self):
        """Assert corpus size ≥ 12 explicitly so it appears in test output."""
        self.assertGreaterEqual(
            _OVER_FIRE_CORPUS_SIZE, 12,
            f"Over-fire corpus must have ≥ 12 entries for AC1, got {_OVER_FIRE_CORPUS_SIZE}",
        )

    def test_d4_no_verbatim_in_read_examples(self):
        """Verify no held-out corpus entry appears verbatim in read_examples[] (D4)."""
        read_prompt_set = {ex["prompt"] for ex in _READ_EXAMPLES}
        verbatim = [p for p, _ in _OVER_FIRE_CORPUS if p in read_prompt_set]
        self.assertEqual(
            verbatim, [],
            f"D4 violation: these held-out prompts appear verbatim in read_examples[]: {verbatim!r}",
        )

    def test_zero_read_nudges_on_held_out_corpus(self):
        """ALL corpus entries must produce ZERO nudge output (recall or timetravel).
        Over-fires are reported collectively so the full failure set is visible."""
        failures = []
        for prompt, label in _OVER_FIRE_CORPUS:
            stdin = _hook_json(prompt)
            stdout, code = _run_hook(stdin)
            if code != 0:
                failures.append(f"NON-ZERO EXIT | {label} | {prompt!r}")
                continue
            intent = _nudge_intent(stdout)
            if intent is not None:
                failures.append(
                    f"OVER-FIRE (intent={intent!r}) | {label} | {prompt!r}"
                )
        if failures:
            self.fail(
                f"Over-fire gate FAILED: {len(failures)}/{_OVER_FIRE_CORPUS_SIZE} corpus "
                f"entries produced a nudge (ZERO expected):\n"
                + "\n".join(f"  {f}" for f in failures)
            )

    # Individual named tests for the T4 spec-cited categories (more actionable CI output)

    def test_imperative_rename_no_nudge(self):
        """Imperative 'rename this variable' must produce no nudge."""
        stdin = _hook_json("rename this variable to user_id")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout), "Imperative coding must not fire read nudge")

    def test_imperative_add_docstring_no_nudge(self):
        stdin = _hook_json("add a docstring to this function")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))

    def test_general_knowledge_list_tuple_no_nudge(self):
        """Third-person general knowledge must produce no nudge."""
        stdin = _hook_json("what's the difference between a list and a tuple in Python")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))

    def test_general_knowledge_python_release_no_nudge(self):
        stdin = _hook_json("when was Python 3.12 released")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))

    def test_project_static_port_no_nudge(self):
        """Project-static lookup must produce no nudge."""
        stdin = _hook_json("which port does the dev server run on")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))

    def test_project_static_regex_no_nudge(self):
        stdin = _hook_json("what does this regex match")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))

    def test_within_session_you_just_told_me_no_nudge(self):
        """Within-session referent must produce no nudge."""
        stdin = _hook_json("summarize what you just told me")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))

    def test_within_session_earlier_in_this_chat_no_nudge(self):
        stdin = _hook_json("can you repeat what you explained earlier in this chat")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))

    def test_within_session_minutes_ago_no_nudge(self):
        stdin = _hook_json("what were we discussing five minutes ago")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertIsNone(_nudge_intent(stdout))


# ════════════════════════════════════════════════════════════════════════════════
# Class 11 — Read output contract (D5)
# On a timetravel hit, the nudge must:
#   (a) name nexus.context_retrieve
#   (b) instruct a RELATIVE as_of (NOT a hardcoded literal YYYY-MM-DD date)
#   (c) mention the 90-day cap
# On a recall hit, assert it does NOT instruct an as_of.
# ════════════════════════════════════════════════════════════════════════════════

class TestReadOutputContract(unittest.TestCase):
    """D5 output contract: timetravel nudge uses relative as_of instruction,
    recall nudge explicitly instructs NOT to pass as_of."""

    _RECALL_PROMPT = "What's my preferred unit-testing framework?"
    _TIMETRAVEL_PROMPT = "A few weeks ago I set a preference about testing tools — what was it?"

    def _get_ctx(self, prompt):
        """Run hook and return additionalContext string (fails test if no output)."""
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        self.assertGreater(len(stdout), 0, f"Expected nudge for {prompt!r}, got empty stdout")
        parsed = json.loads(stdout)
        return parsed["hookSpecificOutput"]["additionalContext"]

    # — Timetravel contract —

    def test_timetravel_names_context_retrieve(self):
        """Timetravel nudge (a): must name nexus.context_retrieve."""
        ctx = self._get_ctx(self._TIMETRAVEL_PROMPT)
        self.assertIn(
            "nexus.context_retrieve", ctx,
            "Timetravel nudge must explicitly name 'nexus.context_retrieve'",
        )

    def test_timetravel_no_hardcoded_date(self):
        """Timetravel nudge (b): must NOT contain a literal YYYY-MM-DD date.
        The hook must instruct the MODEL to compute the concrete RFC3339 date,
        not embed a hardcoded value (D5 — relative as_of)."""
        import re as _re
        ctx = self._get_ctx(self._TIMETRAVEL_PROMPT)
        hardcoded = _re.search(r"\b\d{4}-\d{2}-\d{2}\b", ctx)
        self.assertIsNone(
            hardcoded,
            f"Timetravel nudge must NOT contain a hardcoded YYYY-MM-DD date (D5: Claude computes it). "
            f"Found {hardcoded.group()!r} in: {ctx!r}" if hardcoded else "",
        )

    def test_timetravel_mentions_90_day_cap(self):
        """Timetravel nudge (c): must mention the 90-day cap."""
        ctx = self._get_ctx(self._TIMETRAVEL_PROMPT)
        self.assertIn(
            "90", ctx,
            "Timetravel nudge must mention the 90-day cap (backend rejects older with HTTP 422)",
        )
        self.assertIn(
            "days", ctx,
            "Timetravel nudge must mention 'days' as part of the 90-day cap instruction",
        )

    def test_timetravel_instructs_as_of(self):
        """Timetravel nudge must mention as_of in an instructional context."""
        ctx = self._get_ctx(self._TIMETRAVEL_PROMPT)
        self.assertIn(
            "as_of", ctx,
            "Timetravel nudge must mention 'as_of' argument",
        )

    def test_timetravel_instructs_relative_resolution(self):
        """Timetravel nudge must instruct Claude to COMPUTE the date (relative resolution).
        The phrase 'you compute' or 'resolve' must appear — confirming it's a model instruction,
        not a pre-computed literal."""
        ctx = self._get_ctx(self._TIMETRAVEL_PROMPT)
        has_relative_instruction = "you compute" in ctx.lower() or "resolve" in ctx.lower()
        self.assertTrue(
            has_relative_instruction,
            f"Timetravel nudge must instruct Claude to compute the date (not embed it). "
            f"Expected 'you compute' or 'resolve' in: {ctx!r}",
        )

    # — Recall contract (no as_of) —

    def test_recall_names_context_retrieve(self):
        """Recall nudge must name nexus.context_retrieve."""
        ctx = self._get_ctx(self._RECALL_PROMPT)
        self.assertIn("nexus.context_retrieve", ctx)

    def test_recall_instructs_no_as_of(self):
        """Recall nudge must explicitly instruct the model NOT to pass an as_of.
        (User did not name a past time — no as_of should be computed.)"""
        ctx = self._get_ctx(self._RECALL_PROMPT)
        self.assertIn(
            "Do NOT pass an as_of", ctx,
            f"Recall nudge must instruct model to skip as_of. Got: {ctx!r}",
        )

    def test_recall_valid_json_structure(self):
        """Recall nudge must be valid JSON with correct hookSpecificOutput structure."""
        stdin = _hook_json(self._RECALL_PROMPT)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            self.fail(f"Recall output is not valid JSON: {exc}")
        hso = parsed.get("hookSpecificOutput", {})
        self.assertEqual(hso.get("hookEventName"), "UserPromptSubmit")
        self.assertIsInstance(hso.get("additionalContext"), str)

    def test_timetravel_surfaces_retrieve_id(self):
        """Timetravel nudge must mention retrieve_id (so the chain to S3/memory_feedback is set up)."""
        ctx = self._get_ctx(self._TIMETRAVEL_PROMPT)
        self.assertIn(
            "retrieve_id", ctx,
            "Timetravel nudge must instruct surfacing retrieve_id (needed for S3 feedback chain)",
        )

    def test_recall_surfaces_retrieve_id(self):
        """Recall nudge must mention retrieve_id (so the chain to S3/memory_feedback is set up)."""
        ctx = self._get_ctx(self._RECALL_PROMPT)
        self.assertIn(
            "retrieve_id", ctx,
            "Recall nudge must instruct surfacing retrieve_id (needed for S3 feedback chain)",
        )


# ════════════════════════════════════════════════════════════════════════════════
# Class 12 — Write/read mutual exclusion (D3)
# A pure write prompt → write nudge only; a pure recall prompt → recall nudge only.
# Write-first precedence when both signals might apply.
# ════════════════════════════════════════════════════════════════════════════════

class TestMutualExclusion(unittest.TestCase):
    """D3: write and read paths are mutually exclusive — the hook dispatches
    write FIRST, then read.  A prompt that fires write must NEVER also fire read.
    A prompt that fires read must NEVER also fire write."""

    def test_pure_write_prompt_fires_write_only(self):
        """A canonical durable-preference write prompt must fire write nudge, not read."""
        stdin = _hook_json("Remember that I prefer pnpm over npm for new TypeScript projects.")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        intent = _nudge_intent(stdout)
        self.assertEqual(
            intent, "write",
            f"Pure write prompt must fire write nudge, got intent={intent!r}",
        )

    def test_pure_recall_prompt_fires_recall_only(self):
        """A canonical cross-session recall query must fire recall nudge, not write."""
        stdin = _hook_json("What did we decide about the API versioning scheme?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        intent = _nudge_intent(stdout)
        self.assertIn(
            intent, ("recall", "timetravel"),
            f"Pure recall prompt must fire read nudge, got intent={intent!r}",
        )
        self.assertNotEqual(
            intent, "write",
            "Recall prompt must never produce a write nudge",
        )

    def test_pure_timetravel_prompt_fires_timetravel_only(self):
        """A canonical time-named recall must fire timetravel nudge, not write."""
        stdin = _hook_json("Last week I set a preference about my editor — what was it?")
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        intent = _nudge_intent(stdout)
        self.assertEqual(
            intent, "timetravel",
            f"Time-named recall must fire timetravel nudge, got intent={intent!r}",
        )

    def test_write_first_precedence_over_recall_shape(self):
        """D3 mutual-exclusion: write and read are mutually exclusive dispatches.
        A prompt with a write signal (capture_positive) that is also suppressed by
        transient_negative falls through to the read path — showing the paths are
        mutually exclusive and not both emitted.

        The design: transient_negative patterns include recall-shape phrases ('what did we',
        '?') so that ambiguous prompts cannot fire BOTH paths. A prompt with 'i prefer'
        (write) + 'what did we decide?' (read_allowlist) ALSO has 'what did we' in
        transient_negative → write is suppressed → read fires. Only ONE path wins.

        This test confirms the mutual exclusion: the hook emits exactly ONE nudge type."""
        # 'I prefer pytest — what did we decide about test frameworks?'
        # capture_positive: 'i prefer' matches
        # transient_negative: 'what did we' matches + '?' at end matches → write suppressed
        # read_allowlist: 'what did we decide' matches → read fires
        # BOTH paths are considered but only ONE wins (read wins here because write suppressed)
        prompt = "I prefer pytest — what did we decide about test frameworks?"
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        intent = _nudge_intent(stdout)
        # Must produce exactly one nudge type — not both, not none (since read fires)
        self.assertIn(
            intent, ("recall", "timetravel", "write"),
            f"Mutual exclusion (D3): must produce exactly one nudge type, got {intent!r}",
        )
        # Additional invariant: if write was suppressed, recall should fire
        # (transient_negative suppresses write + read_allowlist matches → recall)
        self.assertIn(
            intent, ("recall", "timetravel"),
            f"With 'what did we' in transient_negative (suppressing write), "
            f"read path should win, got intent={intent!r}",
        )

    def test_pure_write_is_not_also_recall(self):
        """A pure write prompt (no recall shape) must fire write ONLY — not recall.
        Confirms mutually-exclusive dispatch: write-path win never leaks into read."""
        # 'From now on, always use pytest as our test runner'
        # capture_positive: 'always use' matches; transient_negative: no match;
        # read_allowlist: no match → pure write, no read path considered
        prompt = "From now on, always use pytest as our test runner."
        stdin = _hook_json(prompt)
        stdout, code = _run_hook(stdin)
        self.assertEqual(code, 0)
        intent = _nudge_intent(stdout)
        self.assertEqual(
            intent, "write",
            f"Pure write prompt must fire write nudge only, got intent={intent!r}",
        )

    def test_no_double_nudge_on_any_example(self):
        """Paranoia check: the hook must never emit two JSON objects (double-nudge).
        The output must be exactly one valid JSON object or empty bytes."""
        all_examples = list(_EXAMPLES) + list(_READ_EXAMPLES)
        for ex in all_examples:
            prompt = ex["prompt"]
            stdin = _hook_json(prompt)
            stdout, _ = _run_hook(stdin)
            if not stdout:
                continue  # silent is fine
            with self.subTest(prompt=prompt):
                # If there were double output, json.loads would fail or produce first object only.
                # A stricter check: the bytes must contain exactly ONE JSON object.
                decoded = stdout.decode("utf-8")
                try:
                    json.loads(decoded)
                except json.JSONDecodeError as exc:
                    self.fail(f"Invalid JSON output for {prompt!r}: {exc}")
                # Attempt to parse remainder after the first object
                decoder = json.JSONDecoder()
                _, end_idx = decoder.raw_decode(decoded)
                remainder = decoded[end_idx:].strip()
                self.assertEqual(
                    remainder, "",
                    f"Double-nudge detected for {prompt!r}: extra content after first JSON: {remainder!r}",
                )


# ════════════════════════════════════════════════════════════════════════════════
# Class 13 — Read non-vacuity self-check
# Prove the READ suite can go red: feed a known recall prompt, assert the WRONG
# expectation, confirm AssertionError fires; restore.
# ════════════════════════════════════════════════════════════════════════════════

class TestReadNonVacuity(unittest.TestCase):
    """Non-vacuity proof for the READ test suite.  The suite is useless if it
    cannot fail.  These tests deliberately inject the WRONG expectation and assert
    that AssertionError is raised — proving the assertions are live."""

    _RECALL_PROMPT = "What's my preferred unit-testing framework?"
    _TIMETRAVEL_PROMPT = "A few weeks ago I set a preference about testing tools — what was it?"
    _IGNORE_PROMPT = "rename this variable to user_id"

    def test_read_suite_can_go_red_on_recall_miss(self):
        """Feed a known recall prompt, assert INCORRECTLY that it should be silent.
        Catch the AssertionError — proving the suite is non-vacuous for recall."""
        stdin = _hook_json(self._RECALL_PROMPT)
        stdout, _ = _run_hook(stdin)

        # First: confirm the hook actually fired (otherwise the defect is in the hook)
        if not stdout:
            self.fail(
                f"NON-VACUITY PROOF FAILED: hook was silent for known-recall prompt "
                f"{self._RECALL_PROMPT!r} — this is a real defect, not a test infrastructure issue."
            )

        # Now assert the WRONG thing deliberately
        intentionally_wrong_fired = False
        try:
            self.assertEqual(stdout, b"", "Deliberately wrong assertion to prove suite can go red.")
        except AssertionError:
            intentionally_wrong_fired = True

        self.assertTrue(
            intentionally_wrong_fired,
            "Non-vacuity self-check failed: deliberately wrong assertion did not raise AssertionError.",
        )

    def test_read_suite_can_go_red_on_timetravel_miss(self):
        """Feed a known timetravel prompt, assert INCORRECTLY it should be silent.
        Catch the AssertionError — proving the suite is non-vacuous for timetravel."""
        stdin = _hook_json(self._TIMETRAVEL_PROMPT)
        stdout, _ = _run_hook(stdin)

        if not stdout:
            self.fail(
                f"NON-VACUITY PROOF FAILED: hook was silent for known-timetravel prompt "
                f"{self._TIMETRAVEL_PROMPT!r} — real defect, not test infrastructure."
            )

        intentionally_wrong_fired = False
        try:
            self.assertEqual(stdout, b"", "Deliberately wrong assertion.")
        except AssertionError:
            intentionally_wrong_fired = True

        self.assertTrue(
            intentionally_wrong_fired,
            "Non-vacuity self-check failed: deliberately wrong assertion did not raise AssertionError.",
        )

    def test_read_suite_can_go_red_on_over_fire(self):
        """Feed a known held-out negative prompt (should be silent), assert INCORRECTLY
        that it should produce a nudge.  Catch AssertionError to prove over-fire gate
        is falsifiable."""
        stdin = _hook_json(self._IGNORE_PROMPT)
        stdout, _ = _run_hook(stdin)

        if stdout:
            self.fail(
                f"NON-VACUITY PROOF FAILED: hook fired for held-out negative prompt "
                f"{self._IGNORE_PROMPT!r} — this is a real over-fire defect."
            )

        intentionally_wrong_fired = False
        try:
            self.assertGreater(len(stdout), 0, "Deliberately wrong assertion.")
        except AssertionError:
            intentionally_wrong_fired = True

        self.assertTrue(
            intentionally_wrong_fired,
            "Non-vacuity self-check failed: deliberately wrong assertion did not raise AssertionError.",
        )

    def test_s3_scope_note(self):
        """S3 (retrieve_id → memory_feedback) is a dogfood-level (T6) behavior, NOT
        hook-unit-testable.  This test records that fact explicitly — a vacuous pass
        is intentional here.

        The hook's job is to ROUTE recall → nexus.context_retrieve so a retrieve_id
        EXISTS to chain to memory_feedback.  Whether the model then calls memory_feedback
        correctly is observable only in a live dogfood session (T6), not from hook stdout.
        Asserting the hook calls memory_feedback internally would be a false unit test."""
        # S3 chain is asserted at T6 (user-led dogfood), not T4 (hook unit tests).
        # The hook's contribution (routing recall→context_retrieve so retrieve_id exists)
        # is covered by test_recall_surfaces_retrieve_id in TestReadOutputContract.
        pass  # intentionally vacuous — see docstring


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
        # T4 read-path classes (mcp-read-path-routing FU)
        TestReadParitySingleRuleSet,
        TestReadRecallFloor,
        TestOverFireGate,
        TestReadOutputContract,
        TestMutualExclusion,
        TestReadNonVacuity,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
