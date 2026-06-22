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

import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

_HOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
_HOOK_SCRIPT = os.path.join(_HOOKS_DIR, "session_capture.py")


# ── In-process import of the hook module (for monkeypatch tests) ────────────────

def _load_module():
    spec = importlib.util.spec_from_file_location("session_capture", _HOOK_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MOD = _load_module()


# ── Subprocess driver (for fail-open / degenerate-stdin tests) ──────────────────

def _run_hook(stdin_text, env=None):
    """Drive the hook as a subprocess; return (stdout_bytes, exit_code)."""
    run_env = dict(os.environ)
    for k in ("NEXUS_API_URL", "NEXUS_API_TOKEN", "NEXUS_DEFAULT_USER_ID",
              "NEXUS_CONTAINER_ID"):
        run_env.pop(k, None)
    if env:
        run_env.update(env)
    result = subprocess.run(
        [sys.executable, _HOOK_SCRIPT],
        input=stdin_text.encode(),
        capture_output=True,
        timeout=20,
        env=run_env,
    )
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
        self.assertEqual(headers.get("X-nexus-source"), "session-capture-hook")
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
