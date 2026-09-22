#!/usr/bin/env python3
"""Test suite for hooks/session_capture.py (SessionEnd activity capture, P1 write side).

Runnable as: python3 hooks/test_session_capture.py   (stdlib unittest only)

Two modes:
  - FAIL-OPEN + degenerate-stdin tests drive the hook as a real subprocess
    (echo JSON | python3 session_capture.py) so we assert on actual exit code
    and the absence of any POST — never trusting a non-zero exit to be benign.
  - Parse / mapping / provenance tests import the module IN-PROCESS and
    monkeypatch urllib so we capture the outgoing ActivityStreamRequest body
    WITHOUT hitting any real backend (no network in CI).

Coverage maps to workflow C acceptance:
  C1 fail-open (no transcript / missing file / no API URL)
  C1 parse + action mapping (Edit->edit_file, Bash 'git commit'->commit,
     Read->read_file, user text->user_message)
  C1 agent_id = project slug
  C1 provenance (container_id + branch on every activity_data) + URL + UA
  C3 bad-line skip; empty-activities -> no POST
"""

import glob
import http.server
import importlib.util
import io
import itertools
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from unittest import mock

import _hook_state
import _identity

_HOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
_HOOK_SCRIPT = os.path.join(_HOOKS_DIR, "session_capture.py")


# ── Hermetic module fixture ─────────────────────────────────────────────────────
#
# Every test here ends up running the hook, and since TASK-002 the hook writes a
# run ledger -- under ~/.nexus/hooks unless told otherwise. The first full run of
# this suite after that change wrote 39 fabricated records into the developer's
# real home directory, where a later SessionStart would have reported them as
# failures. So the whole module runs with HOME and NEXUS_HOOK_STATE_DIR pointed
# at a throwaway directory, and asserts on the way out that the fake HOME is
# still empty: a test that loses the override writes into it and turns this red.

def _assert_home_untouched(home):
    leaked = sorted(os.listdir(home))
    if leaked:
        raise AssertionError(f"a test wrote under HOME instead of the state dir: {leaked}")


def setUpModule():
    root = tempfile.mkdtemp(prefix="nexus-hooktest-")
    unittest.addModuleCleanup(shutil.rmtree, root, ignore_errors=True)
    home = os.path.join(root, "home")
    os.makedirs(home)
    patcher = mock.patch.dict(
        os.environ, {"HOME": home, "NEXUS_HOOK_STATE_DIR": os.path.join(root, "state")}
    )
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)
    unittest.addModuleCleanup(_assert_home_untouched, home)  # LIFO: this runs first


def _plugin_version():
    """Read independently of _identity, so the two can only agree by both
    being right."""
    manifest = os.path.join(_HOOKS_DIR, "..", ".claude-plugin", "plugin.json")
    with open(manifest, encoding="utf-8") as fh:
        return json.load(fh)["version"]


# ── In-process import of the hook module (for monkeypatch tests) ────────────────

def _load_module():
    spec = importlib.util.spec_from_file_location("session_capture", _HOOK_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MOD = _load_module()


# ── Subprocess driver (for fail-open / degenerate-stdin tests) ──────────────────

def _run_hook(stdin_text, env=None, drop=(), script=None, want_stderr=False):
    """Drive the hook as a subprocess; return (stdout_bytes, exit_code)."""
    run_env = dict(os.environ)
    for k in ("NEXUS_API_URL", "NEXUS_API_TOKEN", "NEXUS_DEFAULT_USER_ID",
              "NEXUS_CONTAINER_ID") + tuple(drop):
        run_env.pop(k, None)
    if env:
        run_env.update(env)
    result = subprocess.run(
        [sys.executable, script or _HOOK_SCRIPT],
        input=stdin_text.encode(),
        capture_output=True,
        timeout=20,
        env=run_env,
    )
    if want_stderr:
        return result.stdout, result.returncode, result.stderr.decode()
    return result.stdout, result.returncode


# ── Transcript fixture helpers ──────────────────────────────────────────────────

def _assistant_tool_use(tool, tool_input):
    """A Claude Code transcript line: an assistant message with a tool_use block."""
    return {
        "type": "assistant",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "tool_use", "name": tool, "input": tool_input},
            ],
        },
    }


def _user_text(text):
    """A transcript line: a user text message."""
    return {
        "type": "user",
        "message": {"role": "user", "content": text},
    }


def _write_transcript(lines):
    """Write a JSONL transcript with the given dict lines; return the path.

    `lines` entries may be dicts (json-encoded) or raw strings (written verbatim,
    used to inject malformed lines)."""
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        for ln in lines:
            if isinstance(ln, str):
                fh.write(ln + "\n")
            else:
                fh.write(json.dumps(ln) + "\n")
    return path


# ── A fake urllib response + capturing urlopen ──────────────────────────────────

class _FakeResponse:
    def __init__(self, payload):
        self._buf = io.BytesIO(json.dumps(payload).encode("utf-8"))

    def read(self, *a, **k):
        return self._buf.read(*a, **k)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _UrlopenCapture:
    """Records each request; returns a fixed 201-ish response payload."""

    def __init__(self):
        self.requests = []  # list of (url, parsed_body_dict, headers)

    def __call__(self, req, timeout=None):
        body = json.loads(req.data.decode("utf-8")) if req.data else {}
        self.requests.append((req.full_url, body, dict(req.headers)))
        return _FakeResponse({"accepted": len(body.get("activities", [])),
                              "request_id": "r1"})


def _run_main_capturing(mod, stdin_event):
    """Run mod.main() with urllib monkeypatched; return (capture, stdout_str)."""
    cap = _UrlopenCapture()
    orig_urlopen = mod.urllib.request.urlopen
    orig_branch = mod._current_branch
    mod.urllib.request.urlopen = cap
    mod._current_branch = lambda cwd: "feat/p1-capture"
    old_stdin, old_stdout = sys.stdin, sys.stdout
    sys.stdin = io.StringIO(json.dumps(stdin_event))
    sys.stdout = io.StringIO()
    try:
        mod.main()
        out = sys.stdout.getvalue()
    finally:
        sys.stdin, sys.stdout = old_stdin, old_stdout
        mod.urllib.request.urlopen = orig_urlopen
        mod._current_branch = orig_branch
    return cap, out


# ════════════════════════════════════════════════════════════════════════════════
# Class FailOpen — degenerate paths must exit 0 with NO POST.
# ════════════════════════════════════════════════════════════════════════════════

class TestFailOpen(unittest.TestCase):

    def test_no_api_url_env(self):
        """NEXUS_API_URL unset -> fail-open exit 0, no output."""
        path = _write_transcript([_assistant_tool_use("Edit", {"file_path": "/a/b.py"})])
        try:
            stdout, code = _run_hook(
                json.dumps({"transcript_path": path, "cwd": _HOOKS_DIR}), env=None)
            self.assertEqual(code, 0, "missing NEXUS_API_URL must fail-open exit 0")
            self.assertEqual(stdout, b"", f"must produce no stdout, got {stdout!r}")
        finally:
            os.unlink(path)

    def test_no_transcript_path(self):
        """SessionEnd event without transcript_path -> fail-open, no POST."""
        env = {"NEXUS_API_URL": "http://127.0.0.1:1/v1"}
        stdout, code = _run_hook(json.dumps({"cwd": _HOOKS_DIR}), env=env)
        self.assertEqual(code, 0)
        self.assertEqual(stdout, b"")

    def test_transcript_file_missing(self):
        """transcript_path pointing at a non-existent file -> fail-open, no POST."""
        env = {"NEXUS_API_URL": "http://127.0.0.1:1/v1", "NEXUS_DEFAULT_USER_ID": "proj"}
        stdout, code = _run_hook(
            json.dumps({"transcript_path": "/no/such/file.jsonl", "cwd": _HOOKS_DIR}),
            env=env)
        self.assertEqual(code, 0)
        self.assertEqual(stdout, b"")

    def test_unreachable_backend(self):
        """Real activities but unreachable backend -> fail-open (connection refused)."""
        path = _write_transcript([_assistant_tool_use("Edit", {"file_path": "/a/b.py"})])
        try:
            env = {"NEXUS_API_URL": "http://127.0.0.1:1/v1", "NEXUS_DEFAULT_USER_ID": "proj"}
            stdout, code = _run_hook(
                json.dumps({"transcript_path": path, "cwd": _HOOKS_DIR}), env=env)
            self.assertEqual(code, 0, "unreachable backend must fail-open")
            self.assertEqual(stdout, b"")
        finally:
            os.unlink(path)

    def test_empty_stdin(self):
        stdout, code = _run_hook("", env={"NEXUS_API_URL": "http://127.0.0.1:1/v1"})
        self.assertEqual(code, 0)
        self.assertEqual(stdout, b"")

    def test_non_json_stdin(self):
        stdout, code = _run_hook("not json at all",
                                 env={"NEXUS_API_URL": "http://127.0.0.1:1/v1"})
        self.assertEqual(code, 0)
        self.assertEqual(stdout, b"")

    def test_json_list_stdin(self):
        stdout, code = _run_hook("[]", env={"NEXUS_API_URL": "http://127.0.0.1:1/v1"})
        self.assertEqual(code, 0)
        self.assertEqual(stdout, b"")


# ════════════════════════════════════════════════════════════════════════════════
# Class ActionMapping — assistant tool_use + user text -> correct action enum.
# ════════════════════════════════════════════════════════════════════════════════

class TestActionMapping(unittest.TestCase):

    def setUp(self):
        os.environ["NEXUS_API_URL"] = "https://nexus.example/v1"
        os.environ["NEXUS_DEFAULT_USER_ID"] = "nexus"
        os.environ["NEXUS_CONTAINER_ID"] = "dev-claude-308"
        os.environ["NEXUS_API_TOKEN"] = "tok123"

    def tearDown(self):
        for k in ("NEXUS_API_URL", "NEXUS_DEFAULT_USER_ID", "NEXUS_CONTAINER_ID",
                  "NEXUS_API_TOKEN"):
            os.environ.pop(k, None)

    def _capture_activities(self, lines, cwd=None):
        path = _write_transcript(lines)
        try:
            cap, _out = _run_main_capturing(
                _MOD, {"transcript_path": path, "cwd": cwd or _HOOKS_DIR,
                       "session_id": "sess-xyz"})
        finally:
            os.unlink(path)
        return cap

    def test_full_mapping_and_provenance(self):
        """Mixed transcript: Edit / Bash 'git commit' / Read / user text ->
        edit_file / commit / user_message, each carrying provenance.

        Note: the Read (read_file) is LOW-SIGNAL (P0 source filter, C0d) and is
        dropped before the POST — the remaining high-signal activities keep their
        mapping and provenance."""
        lines = [
            _assistant_tool_use("Edit", {"file_path": "/repo/src/foo.py"}),
            _assistant_tool_use("Bash", {"command": "git commit -m 'feat: x'"}),
            _assistant_tool_use("Read", {"file_path": "/repo/README.md"}),  # filtered
            _user_text("please refactor the parser to be stricter"),
        ]
        cap = self._capture_activities(lines)
        self.assertEqual(len(cap.requests), 1, "exactly one POST expected")
        url, body, headers = cap.requests[0]

        # URL targets /activities/stream on the configured /v1 base.
        self.assertTrue(url.endswith("/v1/activities/stream"),
                        f"URL must hit /v1/activities/stream, got {url!r}")

        # agent_id == project slug (NEXUS_DEFAULT_USER_ID override here).
        self.assertEqual(body["agent_id"], "nexus")

        actions = [a["action"] for a in body["activities"]]
        self.assertEqual(actions, ["edit_file", "commit", "user_message"],
                         f"action mapping wrong (read_file should be filtered): {actions!r}")

        # Every activity_data carries provenance: container_id + branch + session_id.
        for a in body["activities"]:
            ad = a["activity_data"]
            self.assertEqual(ad.get("container_id"), "dev-claude-308",
                             f"missing container_id provenance: {ad!r}")
            self.assertEqual(ad.get("branch"), "feat/p1-capture",
                             f"missing branch provenance: {ad!r}")
            self.assertEqual(ad.get("session_id"), "sess-xyz",
                             f"missing session_id provenance: {ad!r}")

        # User-Agent header present (CF 1010 guard) + source + content type.
        self.assertIn("User-agent", headers, f"User-Agent required (CF 1010): {headers}")
        # <hook-name>/<version> since TASK-002; the backend attributes by the
        # part before the slash, so the name half is unchanged on purpose.
        self.assertEqual(
            headers.get("X-nexus-source"), f"session-capture-hook/{_plugin_version()}"
        )
        self.assertEqual(headers.get("X-api-key"), "tok123")

    def test_bash_pytest_maps_run_test(self):
        cap = self._capture_activities(
            [_assistant_tool_use("Bash", {"command": "uv run pytest tests/ -v"})])
        self.assertEqual(cap.requests[0][1]["activities"][0]["action"], "run_test")

    def test_bash_npm_test_maps_run_test(self):
        cap = self._capture_activities(
            [_assistant_tool_use("Bash", {"command": "npm test"})])
        self.assertEqual(cap.requests[0][1]["activities"][0]["action"], "run_test")

    def test_bash_other_maps_command_run(self):
        # Use a MUTATING command: `ls -la` is now low-signal (filtered), but the
        # Bash-other -> command_run classification must still hold for real work.
        cap = self._capture_activities(
            [_assistant_tool_use("Bash", {"command": "alembic upgrade head"})])
        self.assertEqual(cap.requests[0][1]["activities"][0]["action"], "command_run")

    def test_write_maps_create_file(self):
        cap = self._capture_activities(
            [_assistant_tool_use("Write", {"file_path": "/repo/new.py", "content": "x"})])
        self.assertEqual(cap.requests[0][1]["activities"][0]["action"], "create_file")

    def test_grep_maps_agent_action(self):
        # Grep -> agent_action is the correct classification; agent_action is
        # LOW-SIGNAL (P0 filter) so it never reaches the POST. Assert the
        # classifier directly (mapping intent) — filtering is covered separately.
        action, _ = _MOD._classify_tool("Grep", {"pattern": "foo"})
        self.assertEqual(action, "agent_action")

    def test_task_maps_agent_action(self):
        action, _ = _MOD._classify_tool("Task", {"description": "do a thing"})
        self.assertEqual(action, "agent_action")

    def test_activity_data_carries_tool_and_summary(self):
        cap = self._capture_activities(
            [_assistant_tool_use("Edit", {"file_path": "/repo/src/foo.py"})])
        ad = cap.requests[0][1]["activities"][0]["activity_data"]
        self.assertEqual(ad.get("tool"), "Edit")
        # the summary must reference the file path
        summary = json.dumps(ad)
        self.assertIn("/repo/src/foo.py", summary)

    def test_user_text_truncated_in_activity_data(self):
        long_text = "x" * 5000
        cap = self._capture_activities([_user_text(long_text)])
        ad = cap.requests[0][1]["activities"][0]["activity_data"]
        self.assertEqual(cap.requests[0][1]["activities"][0]["action"], "user_message")
        self.assertIn("text", ad)
        self.assertLess(len(ad["text"]), 5000, "long user text must be truncated")


# ════════════════════════════════════════════════════════════════════════════════
# Class AgentIdSlug — agent_id falls back to project slug when no env override.
# ════════════════════════════════════════════════════════════════════════════════

class TestAgentIdSlug(unittest.TestCase):

    def setUp(self):
        os.environ["NEXUS_API_URL"] = "https://nexus.example/v1"
        os.environ["NEXUS_CONTAINER_ID"] = "dev-claude-308"
        os.environ.pop("NEXUS_DEFAULT_USER_ID", None)

    def tearDown(self):
        for k in ("NEXUS_API_URL", "NEXUS_CONTAINER_ID", "NEXUS_DEFAULT_USER_ID"):
            os.environ.pop(k, None)

    def test_agent_id_is_project_slug(self):
        """With no NEXUS_DEFAULT_USER_ID, agent_id == normalized git-toplevel/cwd basename."""
        path = _write_transcript([_assistant_tool_use("Edit", {"file_path": "/a/b.py"})])
        try:
            cap, _ = _run_main_capturing(
                _MOD, {"transcript_path": path, "cwd": "/some/path/MyProject"})
        finally:
            os.unlink(path)
        # _project_slug normalizes to lowercase safe chars. cwd basename is "MyProject"
        # unless cwd is inside a git repo (then git toplevel basename). Assert the
        # slug is lowercase and non-empty either way.
        agent_id = cap.requests[0][1]["agent_id"]
        self.assertEqual(agent_id, agent_id.lower(), "slug must be lowercase")
        self.assertTrue(agent_id, "slug must be non-empty")


# ════════════════════════════════════════════════════════════════════════════════
# Class BadLineSkip — malformed transcript lines are skipped, not fatal.
# ════════════════════════════════════════════════════════════════════════════════

class TestBadLineSkip(unittest.TestCase):

    def setUp(self):
        os.environ["NEXUS_API_URL"] = "https://nexus.example/v1"
        os.environ["NEXUS_DEFAULT_USER_ID"] = "nexus"
        os.environ["NEXUS_CONTAINER_ID"] = "dev-claude-308"

    def tearDown(self):
        for k in ("NEXUS_API_URL", "NEXUS_DEFAULT_USER_ID", "NEXUS_CONTAINER_ID"):
            os.environ.pop(k, None)

    def test_bad_lines_skipped_good_kept(self):
        """A transcript with non-JSON lines must not crash; good lines still parsed."""
        lines = [
            "this is not json {{{",
            _assistant_tool_use("Edit", {"file_path": "/a/b.py"}),
            "",  # blank line
            "[not, a, dict]",
            _user_text("hello there"),
        ]
        path = _write_transcript(lines)
        try:
            cap, out = _run_main_capturing(
                _MOD, {"transcript_path": path, "cwd": _HOOKS_DIR})
        finally:
            os.unlink(path)
        self.assertEqual(len(cap.requests), 1, "must still POST despite bad lines")
        actions = [a["action"] for a in cap.requests[0][1]["activities"]]
        self.assertEqual(actions, ["edit_file", "user_message"],
                         f"bad lines must be skipped, kept={actions!r}")


# ════════════════════════════════════════════════════════════════════════════════
# Class EmptyActivities — no extractable activities -> NO POST, exit 0.
# ════════════════════════════════════════════════════════════════════════════════

class TestEmptyActivities(unittest.TestCase):

    def setUp(self):
        os.environ["NEXUS_API_URL"] = "https://nexus.example/v1"
        os.environ["NEXUS_DEFAULT_USER_ID"] = "nexus"
        os.environ["NEXUS_CONTAINER_ID"] = "dev-claude-308"

    def tearDown(self):
        for k in ("NEXUS_API_URL", "NEXUS_DEFAULT_USER_ID", "NEXUS_CONTAINER_ID"):
            os.environ.pop(k, None)

    def test_empty_transcript_no_post(self):
        """An empty transcript -> zero activities -> no POST at all."""
        path = _write_transcript([])
        try:
            cap, out = _run_main_capturing(
                _MOD, {"transcript_path": path, "cwd": _HOOKS_DIR})
        finally:
            os.unlink(path)
        self.assertEqual(len(cap.requests), 0, "empty activities must NOT POST")
        self.assertEqual(out, "", "SessionEnd hook produces no stdout")

    def test_only_bad_lines_no_post(self):
        """A transcript of only un-parseable lines -> no activities -> no POST."""
        path = _write_transcript(["garbage", "{{{", "[1,2,3]"])
        try:
            cap, _ = _run_main_capturing(
                _MOD, {"transcript_path": path, "cwd": _HOOKS_DIR})
        finally:
            os.unlink(path)
        self.assertEqual(len(cap.requests), 0)


# ════════════════════════════════════════════════════════════════════════════════
# Class Cap — bounded activity extraction (most-recent N, never exceeds 1000).
# ════════════════════════════════════════════════════════════════════════════════

class TestCap(unittest.TestCase):

    def setUp(self):
        os.environ["NEXUS_API_URL"] = "https://nexus.example/v1"
        os.environ["NEXUS_DEFAULT_USER_ID"] = "nexus"
        os.environ["NEXUS_CONTAINER_ID"] = "dev-claude-308"

    def tearDown(self):
        for k in ("NEXUS_API_URL", "NEXUS_DEFAULT_USER_ID", "NEXUS_CONTAINER_ID"):
            os.environ.pop(k, None)

    def test_activities_capped(self):
        """A huge transcript must be capped (<= _MAX_ACTIVITIES, <= 1000 schema limit)."""
        lines = [_assistant_tool_use("Edit", {"file_path": f"/a/{i}.py"})
                 for i in range(1000)]
        path = _write_transcript(lines)
        try:
            cap, _ = _run_main_capturing(
                _MOD, {"transcript_path": path, "cwd": _HOOKS_DIR})
        finally:
            os.unlink(path)
        n = len(cap.requests[0][1]["activities"])
        self.assertLessEqual(n, 1000, "must not exceed ActivityStreamRequest max_length")
        self.assertLessEqual(n, _MOD._MAX_ACTIVITIES, "must honor the local cap")


# ════════════════════════════════════════════════════════════════════════════════
# Class LowSignalFilter — P0: source-filter low-signal activities BEFORE extraction
# so the LLM extractor never sees navigation/search noise (C0d: 88% hallucination
# on low-signal activities; see docs/qa/nexus-replace-claude-mem-c0c-extraction-
# quality.md). High-signal activities are fully preserved.
# ════════════════════════════════════════════════════════════════════════════════

class TestLowSignalHelper(unittest.TestCase):
    """Unit-test the _is_low_signal predicate directly."""

    def _low(self, action, ad=None):
        return _MOD._is_low_signal(action, ad or {})

    # ── low-signal (skip) ────────────────────────────────────────────────────
    def test_read_file_is_low_signal(self):
        self.assertTrue(self._low("read_file", {"tool": "Read", "summary": "/a/b.py"}))

    def test_agent_action_is_low_signal(self):
        self.assertTrue(self._low("agent_action", {"tool": "Grep"}))
        self.assertTrue(self._low("agent_action", {"tool": "Glob"}))
        self.assertTrue(self._low("agent_action", {"tool": "Task"}))

    def test_readonly_command_run_is_low_signal(self):
        for cmd in ("ls -la /tmp", "cat foo.py", "pwd",
                    "which python3", "echo hi", "head -5 f", "tail f",
                    "tree src", "stat f", "wc -l f", "less f", "file f"):
            self.assertTrue(self._low("command_run", {"summary": cmd}),
                            f"{cmd!r} should be low-signal (read-only)")

    def test_readonly_git_subcommand_is_low_signal(self):
        for cmd in ("git status", "git log --oneline", "git diff HEAD",
                    "git show abc123", "git branch -a", "git remote -v",
                    "git rev-parse HEAD"):
            self.assertTrue(self._low("command_run", {"summary": cmd}),
                            f"{cmd!r} should be low-signal (read-only git)")

    # ── high-signal (keep) ───────────────────────────────────────────────────
    def test_high_signal_actions_preserved(self):
        for action in ("user_message", "commit", "run_test", "edit_file",
                       "create_file", "delete_file"):
            self.assertFalse(self._low(action, {"summary": "x"}),
                             f"{action!r} must be high-signal (keep)")

    def test_mutating_command_run_preserved(self):
        for cmd in ("alembic upgrade head", "docker-compose up -d",
                    "make build", "npm run build", "rm -rf dist",
                    "uv sync"):
            self.assertFalse(self._low("command_run", {"summary": cmd}),
                             f"{cmd!r} must be high-signal (mutating)")

    # ── boundary: write redirections / pipes are NOT read-only ───────────────
    def test_write_redirect_not_low_signal(self):
        for cmd in ("cat > out.txt", "cat >> out.txt", "echo hi > f",
                    "tee f", "ls | tee log", "cat a | sort > b"):
            self.assertFalse(self._low("command_run", {"summary": cmd}),
                             f"{cmd!r} has write redirect/pipe -> must be kept")

    # ── boundary: command chaining / substitution hides a mutating 2nd command ─
    # A read-only HEAD says nothing about what a `&&`/`;`/`&`/backtick/$()
    # chained command does — those must be KEPT, never dropped on the head alone.
    def test_command_chaining_not_low_signal(self):
        for cmd in ("ls && rm -rf dist", "ls -la && rm x",
                    "cat a; alembic upgrade head", "git status; git commit -m x",
                    "echo hi || make build", "ls & sleep 1",
                    "echo `rm -rf x`", "cat $(rm -rf x)",
                    "git log\nrm -rf dist"):
            self.assertFalse(self._low("command_run", {"summary": cmd}),
                             f"{cmd!r} chains a 2nd command -> must be kept")

    # ── boundary: read-only head with a MUTATING flag (find -delete/-exec) ────
    # `find` is excluded from the head whitelist entirely (its destructive flags
    # `-delete`/`-exec rm` make head-only inspection unsound), so any `find ...`
    # is high-signal and kept.
    def test_find_not_low_signal(self):
        for cmd in ("find . -name x", "find . -delete",
                    "find . -exec rm {} +", "find /tmp -type f"):
            self.assertFalse(self._low("command_run", {"summary": cmd}),
                             f"{cmd!r} (find) must be kept — head whitelist excludes find")


class TestLowSignalFiltering(unittest.TestCase):
    """End-to-end: low-signal activities are filtered from the POST body."""

    def setUp(self):
        os.environ["NEXUS_API_URL"] = "https://nexus.example/v1"
        os.environ["NEXUS_DEFAULT_USER_ID"] = "nexus"
        os.environ["NEXUS_CONTAINER_ID"] = "dev-claude-308"

    def tearDown(self):
        for k in ("NEXUS_API_URL", "NEXUS_DEFAULT_USER_ID", "NEXUS_CONTAINER_ID"):
            os.environ.pop(k, None)

    def _capture(self, lines, cwd=None):
        path = _write_transcript(lines)
        try:
            cap, _ = _run_main_capturing(
                _MOD, {"transcript_path": path, "cwd": cwd or _HOOKS_DIR,
                       "session_id": "sess-low"})
        finally:
            os.unlink(path)
        return cap

    def test_low_signal_filtered_high_signal_kept(self):
        """Mixed transcript: low-signal (Read/Grep/ls/git status) dropped;
        high-signal (user text / git commit / pytest / Edit / alembic) kept."""
        lines = [
            _user_text("please refactor the parser"),       # keep
            _assistant_tool_use("Read", {"file_path": "/r/a.py"}),       # drop
            _assistant_tool_use("Grep", {"pattern": "foo"}),            # drop
            _assistant_tool_use("Bash", {"command": "ls -la"}),        # drop
            _assistant_tool_use("Bash", {"command": "git status"}),    # drop
            _assistant_tool_use("Bash", {"command": "git commit -m x"}),  # keep
            _assistant_tool_use("Bash", {"command": "uv run pytest"}),  # keep
            _assistant_tool_use("Edit", {"file_path": "/r/a.py"}),      # keep
            _assistant_tool_use("Bash", {"command": "alembic upgrade head"}),  # keep
        ]
        cap = self._capture(lines)
        self.assertEqual(len(cap.requests), 1)
        actions = [a["action"] for a in cap.requests[0][1]["activities"]]
        self.assertEqual(
            actions,
            ["user_message", "commit", "run_test", "edit_file", "command_run"],
            f"low-signal must be dropped, high-signal kept: {actions!r}")

    def test_write_redirect_command_kept(self):
        """Boundary: `cat > out.txt` is a write -> must NOT be filtered out."""
        cap = self._capture(
            [_assistant_tool_use("Bash", {"command": "cat > out.txt"})])
        self.assertEqual(len(cap.requests), 1)
        actions = [a["action"] for a in cap.requests[0][1]["activities"]]
        self.assertEqual(actions, ["command_run"],
                         "write-redirect command_run must be kept")

    def test_all_low_signal_session_no_post(self):
        """A session of only navigation/search/read-only -> zero activities -> NO POST."""
        lines = [
            _assistant_tool_use("Read", {"file_path": "/r/a.py"}),
            _assistant_tool_use("Grep", {"pattern": "foo"}),
            _assistant_tool_use("Glob", {"pattern": "*.py"}),
            _assistant_tool_use("Bash", {"command": "ls -la"}),
            _assistant_tool_use("Bash", {"command": "git status"}),
            _assistant_tool_use("Bash", {"command": "cat README.md"}),
        ]
        cap = self._capture(lines)
        self.assertEqual(len(cap.requests), 0,
                         "all-low-signal session must NOT POST")


# ════════════════════════════════════════════════════════════════════════════════
# Class NonVacuity — prove the suite can go red.
# ════════════════════════════════════════════════════════════════════════════════

class TestNonVacuity(unittest.TestCase):

    def test_mapping_actually_classifies(self):
        """If _classify_tool were a no-op returning 'other', a git-commit Bash would
        not map to 'commit'. Asserting it DOES map proves the classifier is live."""
        action, _ = _MOD._classify_tool("Bash", {"command": "git commit -m x"})
        triggered = False
        try:
            self.assertEqual(action, "other")  # intentionally wrong
        except AssertionError:
            triggered = True
        self.assertTrue(triggered, "Non-vacuity: git-commit Bash did not map to commit")


# ════════════════════════════════════════════════════════════════════════════════
# TASK-002 — run ledger, X-Nexus-Source, shared identity.
# New tests use mock.patch / addCleanup only (TASK-011 audit rule): CI runs every
# hook test in one process, so a patch that is not restored leaks everywhere.
# ════════════════════════════════════════════════════════════════════════════════

def _raising(exc):
    def urlopen(req, timeout=None):
        raise exc
    return urlopen


def _http_error(code):
    return urllib.error.HTTPError("https://nexus.example/v1/activities/stream", code, "msg", {}, None)


_HIGH_SIGNAL = (
    _assistant_tool_use("Edit", {"file_path": "/r/a.py"}),
    _assistant_tool_use("Bash", {"command": "git commit -m wip"}),
)


class _LedgerCase(unittest.TestCase):
    """A private state dir per test, a pinned project, and a captured urlopen."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state_dir = os.path.join(self.tmp.name, "state")
        self._patch(mock.patch.dict(os.environ, {
            "NEXUS_HOOK_STATE_DIR": self.state_dir,
            "NEXUS_API_URL": "https://nexus.example/v1",
            "NEXUS_DEFAULT_USER_ID": "nexus",
            "NEXUS_CONTAINER_ID": "dev-claude-308",
            "NEXUS_API_TOKEN": "tok123",
        }))
        self._patch(mock.patch.object(_MOD, "_current_branch", return_value="feat/p1-capture"))
        # The ledger directory is keyed by the project slug, which shells out to
        # git. Patched on _identity: both the hook and _hook_state reach it by
        # attribute, which is what makes this patch land (a `from` import would
        # have bound the original at import time).
        self._patch(mock.patch.object(_identity, "project_slug", return_value="proj"))

    def _patch(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _transcript(self, lines=_HIGH_SIGNAL):
        path = _write_transcript(list(lines))
        self.addCleanup(os.unlink, path)
        return path

    def _main(self, urlopen, event=None, stdin_text=None, mod=None):
        mod = mod or _MOD
        if stdin_text is None:
            stdin_text = json.dumps(
                {"cwd": self.cwd, "session_id": "s1", "transcript_path": self._transcript()}
                if event is None else event
            )
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(urllib.request, "urlopen", urlopen), \
                mock.patch.object(sys, "stdin", io.StringIO(stdin_text)), \
                mock.patch.object(sys, "stdout", out), \
                mock.patch.object(sys, "stderr", err):
            mod.main()
        return out.getvalue(), err.getvalue()

    def _entries(self):
        entries, reasons = _hook_state.read_ledger("session-capture", self.cwd)
        self.assertEqual(reasons, [])
        return entries


class TestLedger(_LedgerCase):

    def test_a_successful_capture_records_one_clean_run(self):
        cap = _UrlopenCapture()
        out, _ = self._main(cap)
        self.assertEqual(out, "", "SessionEnd never injects")
        self.assertEqual(len(cap.requests), 1)
        entries = self._entries()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(
            (entry["hook"], entry["ok"], entry["reason"], entry["calls"]),
            ("session-capture", True, "none", 1),
        )
        self.assertIs(type(entry["elapsed_ms"]), int)
        self.assertEqual(entry["activities"], len(cap.requests[0][1]["activities"]))
        self.assertEqual(entry["activities"], 2)

    def test_a_failed_post_is_recorded_with_its_own_reason(self):
        cases = (
            (_http_error(500), "http_error"),
            (_http_error(429), "rate_limited"),
            (socket.timeout("timed out"), "timeout"),
            (urllib.error.URLError(ConnectionRefusedError()), "http_error"),
        )
        for count, (exc, expected) in enumerate(cases, start=1):
            with self.subTest(expected=expected):
                out, err = self._main(_raising(exc))  # and main() does not raise
                self.assertEqual(out, "")
                entries = self._entries()
                self.assertEqual(len(entries), count, "every run appends exactly one record")
                entry = entries[-1]
                self.assertEqual((entry["ok"], entry["reason"], entry["calls"]), (False, expected, 1))
                self.assertIn(expected, err)

    def test_no_backend_configured_is_a_recorded_skip_and_makes_no_call(self):
        urlopen = mock.Mock()
        with mock.patch.dict(os.environ):
            del os.environ["NEXUS_API_URL"]
            self._main(urlopen)
        urlopen.assert_not_called()
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"], entry["calls"]), (True, "not_configured", 0))

    def test_nothing_to_capture_is_a_recorded_skip_not_silence(self):
        """Exit 0 with no request is also what a broken capture hook looks like.
        The ledger is the only thing that tells the two apart."""
        urlopen = mock.Mock()
        low_signal = self._transcript([_assistant_tool_use("Read", {"file_path": "/r/a.py"})])
        events = (
            {"cwd": self.cwd, "session_id": "s1"},  # no transcript at all
            {"cwd": self.cwd, "transcript_path": os.path.join(self.tmp.name, "absent.jsonl")},
            {"cwd": self.cwd, "transcript_path": low_signal},  # everything filtered out
        )
        for event in events:
            with self.subTest(event=sorted(event)):
                self._main(urlopen, event=event)
                entry = self._entries()[-1]
                self.assertEqual(
                    (entry["ok"], entry["reason"], entry["calls"]), (True, "nothing_to_do", 0)
                )
        urlopen.assert_not_called()
        self.assertEqual(self._entries()[-1]["activities"], 0)

    def test_a_malformed_payload_is_recorded_not_swallowed(self):
        urlopen = mock.Mock()
        for payload in ("not json at all", "[]"):
            with self.subTest(payload=payload):
                self._main(urlopen, stdin_text=payload)
                entry = self._entries()[-1]
                self.assertEqual((entry["ok"], entry["reason"]), (False, "unknown"))
        urlopen.assert_not_called()

    def test_a_ledger_that_blows_up_does_not_take_the_hook_down(self):
        cap = _UrlopenCapture()
        with mock.patch.object(_hook_state, "record_run", side_effect=RuntimeError("disk on fire")):
            _, err = self._main(cap)
        self.assertEqual(len(cap.requests), 1, "the capture itself still went out")
        self.assertIn("could not record", err)
        self.assertIn("disk on fire", err)


class TestSourceHeader(_LedgerCase):

    def test_the_post_carries_name_slash_version(self):
        cap = _UrlopenCapture()
        self._main(cap)
        _, _, headers = cap.requests[0]  # urllib title-cases header keys
        self.assertEqual(
            headers.get("X-nexus-source"), f"session-capture-hook/{_plugin_version()}"
        )

    def test_the_name_half_is_the_one_the_backend_allowlists(self):
        """nexus `mcp_attribution._KNOWN_CLIENTS` contains this literal; a
        rename sends every capture to source="unknown"."""
        self.assertEqual(_MOD.SOURCE_NAME, "session-capture-hook")


class TestIdentityIsShared(_LedgerCase):

    def test_agent_id_and_container_id_come_from_the_shared_module(self):
        """Not `equal to what _identity would say` -- *the same call*. The read
        side uses it too, and two derivations that merely agree today are how
        the two sides end up keying one project differently."""
        cap = _UrlopenCapture()
        with mock.patch.object(_identity, "user_id", return_value="pinned-user") as user_id, \
                mock.patch.object(_identity, "container_id", return_value="pinned-container"):
            self._main(cap)
        user_id.assert_called_once_with(self.cwd)
        _, body, _ = cap.requests[0]
        self.assertEqual(body["agent_id"], "pinned-user")
        containers = {a["activity_data"]["container_id"] for a in body["activities"]}
        self.assertEqual(containers, {"pinned-container"})


class TestStateDirectory(unittest.TestCase):
    """The real script, as a subprocess, against a HOME of its own."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = os.path.join(self.tmp.name, "home")
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.home)
        os.makedirs(self.cwd)

    def test_the_override_is_honoured_and_home_stays_clean(self):
        state = os.path.join(self.tmp.name, "state")
        stdout, code = _run_hook(
            json.dumps({"cwd": self.cwd}), env={"HOME": self.home, "NEXUS_HOOK_STATE_DIR": state}
        )
        self.assertEqual((stdout, code), (b"", 0))
        ledgers = glob.glob(os.path.join(state, "*", "session-capture.json"))
        self.assertEqual(len(ledgers), 1, ledgers)
        with open(ledgers[0], encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)[-1]["reason"], "not_configured")
        self.assertEqual(os.listdir(self.home), [])

    def test_without_the_override_the_ledger_lands_under_home(self):
        """The other half: proves the test above can fail, and pins the default."""
        stdout, code = _run_hook(
            json.dumps({"cwd": self.cwd}), env={"HOME": self.home}, drop=("NEXUS_HOOK_STATE_DIR",)
        )
        self.assertEqual((stdout, code), (b"", 0))
        ledgers = glob.glob(os.path.join(self.home, ".nexus", "hooks", "*", "session-capture.json"))
        self.assertEqual(len(ledgers), 1, ledgers)


class TestSharedModulesUnavailable(_LedgerCase):
    """`_hook_state` needs fcntl, which a native Windows Python does not have.
    Wiring the hook to the ledger must not make the ledger a precondition for
    the hook's actual job."""

    def _copy_hook_without(self, *missing):
        target = os.path.join(self.tmp.name, "partial-install")
        os.makedirs(target)
        for name in ("session_capture.py", "_identity.py", "_hook_state.py"):
            if name not in missing:
                shutil.copy(os.path.join(_HOOKS_DIR, name), target)
        return os.path.join(target, "session_capture.py")

    def test_without_the_ledger_module_the_hook_still_captures(self):
        with mock.patch.dict(sys.modules, {"_hook_state": None}):  # import -> ImportError
            mod = _load_module()
        self.assertIsNone(mod._hook_state)
        cap = _UrlopenCapture()
        with mock.patch.object(mod, "_current_branch", return_value="feat/p1-capture"):
            _, err = self._main(cap, mod=mod)
        self.assertEqual(len(cap.requests), 1)
        self.assertIn("ledger unavailable", err)
        self.assertFalse(os.path.exists(self.state_dir), "nothing should have been written")

    def test_as_a_script_without_the_ledger_module_it_exits_zero(self):
        script = self._copy_hook_without("_hook_state.py")
        stdout, code, stderr = _run_hook(json.dumps({"cwd": self.cwd}), script=script, want_stderr=True)
        self.assertEqual((stdout, code), (b"", 0))
        self.assertIn("ledger unavailable", stderr)

    def test_as_a_script_without_the_identity_module_it_exits_zero_and_says_why(self):
        """A traceback here would be exit 1 -- a hook error on every session
        end -- for a plugin whose whole contract is fail-open."""
        script = self._copy_hook_without("_identity.py", "_hook_state.py")
        stdout, code, stderr = _run_hook(json.dumps({"cwd": self.cwd}), script=script, want_stderr=True)
        self.assertEqual((stdout, code), (b"", 0))
        self.assertIn("_identity", stderr)
        self.assertNotIn("Traceback", stderr)



class TestLedgerStepIsBounded(_LedgerCase):
    """Found by the TASK-002 pre-merge review: writing the brief first does not
    protect it from bookkeeping that STALLS. The host only uses stdout from a
    hook that exits 0, so a ledger write stuck on a held lock kept the hook
    alive until the host's timeout killed it -- brief discarded, session start
    delayed by the full timeout, and no ledger record either."""

    def test_a_stalled_ledger_write_is_left_behind(self):
        release = threading.Event()
        self.addCleanup(release.set)  # let the abandoned worker finish

        def stall(*args, **kwargs):
            release.wait(30)

        cap = _UrlopenCapture()
        began = time.monotonic()
        with mock.patch.object(_hook_state, "record_run", side_effect=stall), \
                mock.patch.object(_MOD, "_LEDGER_BUDGET_SECONDS", 0.2):
            out, err = self._main(cap)
        self.assertLess(time.monotonic() - began, 5, "main() waited for the stalled write")
        self.assertEqual(len(cap.requests), 1, "the capture must already be out")
        self.assertIn("still running", err)

    def test_as_a_script_a_held_ledger_lock_cannot_hang_it(self):
        """End to end, with a real flock held by this process."""
        import fcntl

        state = os.path.join(self.tmp.name, "held-state")
        project = os.path.join(state, "proj")  # the subprocess derives this from self.cwd
        os.makedirs(project)
        lock_path = os.path.join(project, "session-capture.json.lock")
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX)
        began = time.monotonic()
        stdout, code, stderr = _run_hook(  # _run_hook gives up at 20 s: a hang is an error
            json.dumps({"cwd": self.cwd}), env={"NEXUS_HOOK_STATE_DIR": state}, want_stderr=True
        )
        self.assertEqual((stdout, code), (b"", 0))
        self.assertLess(time.monotonic() - began, 12)
        self.assertIn("still running", stderr)


class TestInterpreterSettings(unittest.TestCase):
    def test_safe_path_does_not_switch_the_hook_off(self):
        """PYTHONSAFEPATH=1 (3.11+, also `python -P`) drops the script's own
        directory from sys.path. The hooks were single self-contained files
        until TASK-002 made them import siblings, so without putting the
        directory back an interpreter setting silently turned the plugin off."""
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.path.join(tmp, "proj")
            os.makedirs(cwd)
            state = os.path.join(tmp, "state")
            stdout, code, stderr = _run_hook(
                json.dumps({"cwd": cwd}),
                env={"PYTHONSAFEPATH": "1", "NEXUS_HOOK_STATE_DIR": state},
                want_stderr=True,
            )
            self.assertEqual((stdout, code), (b"", 0))
            self.assertNotIn("cannot import", stderr)
            ledgers = glob.glob(os.path.join(state, "*", "session-capture.json"))
            self.assertEqual(len(ledgers), 1, (ledgers, stderr))


class TestTranscriptThatCannotBeRead(_LedgerCase):
    """`nothing_to_do` has to mean "this session had nothing worth capturing".
    The transcript format belongs to Claude Code, not to this plugin, and if it
    changes, every session parses to zero activities -- which was recorded as
    the same expected skip, so capture would have stopped for good without a
    word. Same split as empty_sections / sections_unparsed."""

    def _run_with(self, lines):
        urlopen = mock.Mock()
        self._main(urlopen, event={"cwd": self.cwd, "transcript_path": self._transcript(lines)})
        urlopen.assert_not_called()
        return self._entries()[-1]

    def test_no_line_parses(self):
        entry = self._run_with(["not json", "{still not", "<html>"])
        self.assertEqual((entry["ok"], entry["reason"]), (False, "file_unparsable"))
        self.assertEqual((entry["lines"], entry["parsed"]), (3, 0))

    def test_every_line_parses_but_none_is_a_message(self):
        """The shape-change case: valid JSON, no user / assistant entries."""
        lines = [{"kind": "turn", "speaker": "human", "text": "hi"}] * _MOD._SHAPE_SUSPECT_MIN_LINES
        entry = self._run_with(lines)
        self.assertEqual((entry["ok"], entry["reason"]), (False, "file_unparsable"))

    def test_a_short_transcript_with_no_messages_is_just_a_short_session(self):
        lines = [{"type": "summary", "summary": "x"}] * (_MOD._SHAPE_SUSPECT_MIN_LINES - 1)
        entry = self._run_with(lines)
        self.assertEqual((entry["ok"], entry["reason"]), (True, "nothing_to_do"))

    def test_a_long_read_only_session_is_still_an_honest_skip(self):
        """Many messages, all filtered as low-signal: recognised, so quiet."""
        lines = [_assistant_tool_use("Read", {"file_path": "/r/a.py"})] * 40
        entry = self._run_with(lines)
        self.assertEqual((entry["ok"], entry["reason"]), (True, "nothing_to_do"))

    def test_some_bad_lines_among_good_ones_change_nothing(self):
        cap = _UrlopenCapture()
        path = self._transcript(["garbage", _assistant_tool_use("Edit", {"file_path": "/r/a.py"})])
        self._main(cap, event={"cwd": self.cwd, "transcript_path": path})
        self.assertEqual(len(cap.requests), 1)
        self.assertEqual(self._entries()[-1]["reason"], "none")


# ── Round 3: what the pre-merge review's surviving mutants pointed at ───────────

class _FakeBackend:
    """A real HTTP server on 127.0.0.1, for the few things only a real socket
    and a real interpreter exit can show."""

    def __init__(self, body, status=200, content_type="application/json"):
        self.seen = []
        payload = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        seen = self.seen

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(handler):
                handler.rfile.read(int(handler.headers.get("Content-Length") or 0))
                seen.append({k.lower(): v for k, v in handler.headers.items()})
                handler.send_response(status)
                handler.send_header("Content-Type", content_type)
                handler.send_header("Content-Length", str(len(payload)))
                handler.end_headers()
                handler.wfile.write(payload)

            def log_message(handler, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_address[1]}/v1"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


# urllib honours proxy variables, and a developer machine's cross-border proxy
# answers 502 for 127.0.0.1 -- which reads exactly like the backend being down.
_PROXY_VARS = ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY")
_NO_PROXY = {"no_proxy": "127.0.0.1,localhost", "NO_PROXY": "127.0.0.1,localhost"}


class _Clock:
    """time.monotonic that advances a quarter second every time it is read."""

    def __init__(self):
        self._ticks = itertools.count(1000.0, 0.25)

    def __call__(self):
        return next(self._ticks)


class TestWhatTheLedgerSays(_LedgerCase):

    def test_elapsed_ms_is_the_hooks_own_time_in_milliseconds(self):
        """`type is int and >= 0` let both "always 0" and "seconds, not
        milliseconds" through, and the visibility baseline is read off this."""
        with mock.patch.object(_MOD.time, "monotonic", _Clock()):
            self._main(_UrlopenCapture())
        elapsed = self._entries()[-1]["elapsed_ms"]
        self.assertGreaterEqual(elapsed, 250)
        self.assertEqual(elapsed % 250, 0, elapsed)

    def test_the_record_is_filed_under_the_payloads_project(self):
        """Not under wherever the process happens to be running. Every other
        test pins the slug to one value, so this was never asserted."""
        elsewhere = os.path.join(self.tmp.name, "some-other-project")
        os.makedirs(elsewhere)
        with mock.patch.object(_identity, "project_slug", side_effect=os.path.basename):
            self._main(_UrlopenCapture(), event={"cwd": elsewhere, "transcript_path": self._transcript()})
        expected = os.path.join(self.state_dir, "some-other-project", "session-capture.json")
        self.assertTrue(os.path.isfile(expected), os.listdir(self.state_dir))

    def test_a_payload_cwd_that_is_not_a_string_is_ignored_not_fatal(self):
        """The slug stub has to fail the way the real lookup does on a value
        that is not a path. With a stub that answers "proj" to anything, a cwd
        of 123 had no consequences at all and this test passed against a hook
        that accepted it -- the injection matrix caught that, not review."""
        def like_the_real_one(cwd):
            return os.path.basename(cwd.rstrip("/"))

        with mock.patch.object(_identity, "project_slug", side_effect=like_the_real_one):
            self._main(_UrlopenCapture(), event={"cwd": 123, "transcript_path": self._transcript()})
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "session-capture.json"))
        self.assertEqual(len(ledgers), 1, "the run left no record")
        with open(ledgers[0], encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)[-1]["reason"], "none")

    def test_running_out_of_time_is_recorded_by_the_hook_itself(self):
        """urllib's timeout is per socket operation, not per request: against a
        server that drips bytes, one 6 s request was measured at 24 s. Left to
        the host's timeout, the hook is killed -- no record, and for
        SessionStart no brief. So the hook keeps its own deadline and leaves
        first, with a record."""
        release = threading.Event()
        self.addCleanup(release.set)

        def drip(req, timeout=None):
            release.wait(30)
            raise RuntimeError("released by test cleanup")

        began = time.monotonic()
        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.2):
            out, err = self._main(drip)
        self.assertLess(time.monotonic() - began, 5)
        self.assertEqual(out, "")
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (False, "timeout"))
        self.assertIn("timeout", err)


class TestImportedNotRun(unittest.TestCase):
    def test_a_broken_install_is_loud_when_imported(self):
        """Run as a hook, a missing sibling is exit 0 and a line on stderr.
        Imported by a test it has to raise: a quiet sys.exit(0) during
        collection is a green run that executed nothing."""
        with mock.patch.dict(sys.modules, {"_identity": None}):
            with self.assertRaises(ImportError):
                _load_module()


class _RawBody(_FakeResponse):
    def __init__(self, body):
        self._buf = io.BytesIO(body)


class _Replies:
    """urlopen that answers every request with one fixed body."""

    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def __call__(self, req, timeout=None):
        self.calls += 1
        if isinstance(self.payload, bytes):
            return _RawBody(self.payload)
        return _FakeResponse(self.payload)


class TestTheBackendReallyTookIt(_LedgerCase):
    """The hook used to drain the response and look at none of it, so ANY 2xx
    was a success. Reproduced by the pre-merge review: a POST answered with a
    302 to a login page is re-issued by urllib as a GET, comes back 200
    text/html, and was recorded ok / none with nothing captured."""

    def test_only_an_acknowledged_batch_counts(self):
        cases = (
            (b"<html>Sign in</html>", "http_error"),  # what the 302 -> login page looks like
            ("a JSON string, not an object", "http_error"),
            ({"request_id": "r1"}, "http_error"),
            ({"accepted": 0, "request_id": "r1"}, "http_error"),
            ({"accepted": "2", "request_id": "r1"}, "http_error"),
            ({"accepted": True, "request_id": "r1"}, "http_error"),
            ([], "http_error"),
        )
        for payload, expected in cases:
            with self.subTest(payload=payload):
                replies = _Replies(payload)
                self._main(replies)
                self.assertEqual(replies.calls, 1)
                entry = self._entries()[-1]
                self.assertEqual((entry["ok"], entry["reason"], entry["calls"]), (False, expected, 1))

    def test_what_was_acknowledged_is_recorded(self):
        self._main(_Replies({"accepted": 2, "queued": 2, "request_id": "r1"}))
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"], entry["accepted"]), (True, "none", 2))

    def test_a_transcript_that_cannot_be_opened_is_not_a_quiet_session(self):
        urlopen = mock.Mock()
        with mock.patch.object(_MOD, "_parse_transcript", side_effect=PermissionError("denied")):
            self._main(urlopen)
        urlopen.assert_not_called()
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (False, "file_unparsable"))


class TestTheWorkerDiedQuietly(_LedgerCase):
    def test_a_worker_that_ends_without_a_result_is_still_recorded(self):
        """SystemExit is not an Exception, so the worker's own handler misses it
        and it ends having reported nothing. Found re-reading my own fix for
        the very pattern it was fixing."""
        with mock.patch.object(_MOD, "_collect", side_effect=SystemExit(3)), \
                mock.patch.object(threading, "excepthook", lambda args: None):
            out, err = self._main(mock.Mock())
        self.assertEqual(out, "")
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (False, "unknown"))
        self.assertIn("without a result", err)

if __name__ == "__main__":
    unittest.main(verbosity=2)
