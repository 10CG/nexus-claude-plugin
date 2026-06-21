#!/usr/bin/env python3
"""Test suite for hooks/session_inject.py (SessionStart warm-start injection, P2).

Runnable as: python3 hooks/test_session_inject.py   (stdlib unittest only)

Two modes:
  - FAIL-OPEN + malformed-stdin tests drive the hook as a real subprocess
    (echo JSON | python3 session_inject.py) so we assert on actual stdout +
    exit code, never trusting a non-zero exit to be benign.
  - Render + request-parameter tests import the module IN-PROCESS and
    monkeypatch urllib so we can capture the outgoing request body WITHOUT
    hitting any real backend (no network in CI).

Coverage maps to workflow A acceptance:
  AC-A-01 (profile_limit honored, shape) : Class Render / Class RequestParams
  AC-A-02 (settled summary + provenance) : Class Render
  Fail-open (A3)                         : Class FailOpen
  Two-tier fallback (§6)                 : Class RequestParams
"""

import importlib.util
import io
import json
import os
import subprocess
import sys
import unittest

_HOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
_HOOK_SCRIPT = os.path.join(_HOOKS_DIR, "session_inject.py")


# ── In-process import of the hook module (for monkeypatch tests) ────────────────

def _load_module():
    spec = importlib.util.spec_from_file_location("session_inject", _HOOK_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MOD = _load_module()


# ── Subprocess driver (for fail-open / malformed-stdin tests) ───────────────────

def _run_hook(stdin_text, env=None):
    """Drive the hook as a subprocess; return (stdout_bytes, exit_code)."""
    run_env = dict(os.environ)
    # Strip any inherited Nexus env so 'unset' tests are deterministic.
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
    """Callable replacement for urllib.request.urlopen that records each request
    and returns queued responses in order."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []  # list of (url, parsed_body_dict, headers)

    def __call__(self, req, timeout=None):
        body = json.loads(req.data.decode("utf-8")) if req.data else {}
        self.requests.append((req.full_url, body, dict(req.headers)))
        if not self._responses:
            raise AssertionError("urlopen called more times than queued responses")
        return _FakeResponse(self._responses.pop(0))


def _profile_row(content, *, container_id="dev-claude-308", layer="summary",
                 branch="feat/x", valid_from="2026-06-20T10:00:00Z"):
    meta = {"container_id": container_id, "branch": branch, "valid_from": valid_from}
    if layer is not None:
        meta["layer"] = layer
    return {"memory_id": "m1", "content": content, "memory_type": "semantic",
            "metadata": meta}


# ════════════════════════════════════════════════════════════════════════════════
# Class FailOpen — every degenerate path must exit 0 with empty stdout.
# ════════════════════════════════════════════════════════════════════════════════

class TestFailOpen(unittest.TestCase):

    def _assert_failopen(self, stdin_text, env, label):
        stdout, code = _run_hook(stdin_text, env=env)
        self.assertEqual(code, 0, f"FAIL-OPEN VIOLATION (non-zero exit) for {label}")
        self.assertEqual(stdout, b"", f"FAIL-OPEN VIOLATION (non-empty stdout) for {label}: {stdout!r}")

    def test_no_api_url_env(self):
        """NEXUS_API_URL unset -> fail-open, no backend call."""
        self._assert_failopen(json.dumps({"cwd": _HOOKS_DIR}), env=None,
                              label="NEXUS_API_URL unset")

    def test_unreachable_backend(self):
        """A pointed-but-unreachable backend -> fail-open (connection refused)."""
        env = {"NEXUS_API_URL": "http://127.0.0.1:1/v1", "NEXUS_DEFAULT_USER_ID": "proj"}
        self._assert_failopen(json.dumps({"cwd": _HOOKS_DIR}), env=env,
                              label="unreachable backend")

    def test_malformed_stdin_not_json(self):
        env = {"NEXUS_API_URL": "http://127.0.0.1:1/v1"}
        self._assert_failopen("not json at all", env=env, label="non-JSON stdin")

    def test_empty_stdin(self):
        """Empty stdin -> event {} -> but no API URL means fail-open silent."""
        self._assert_failopen("", env=None, label="empty stdin")

    def test_json_list_stdin(self):
        env = {"NEXUS_API_URL": "http://127.0.0.1:1/v1"}
        self._assert_failopen("[]", env=env, label="JSON list (not dict)")

    def test_truncated_json_stdin(self):
        env = {"NEXUS_API_URL": "http://127.0.0.1:1/v1"}
        self._assert_failopen('{"cwd": "/tmp"', env=env, label="truncated JSON")


# ════════════════════════════════════════════════════════════════════════════════
# Class Render — settled-summary filtering + provenance annotation.
# ════════════════════════════════════════════════════════════════════════════════

class TestRender(unittest.TestCase):

    def test_only_summary_layer_kept(self):
        rows = [
            _profile_row("settled one", layer="summary"),
            _profile_row("raw obs", layer="observation"),
            _profile_row("settled two", layer="summary"),
        ]
        kept = _MOD._settled_rows(rows)
        contents = [r["content"] for r in kept]
        self.assertIn("settled one", contents)
        self.assertIn("settled two", contents)
        self.assertNotIn("raw obs", contents)

    def test_no_layer_key_takes_all(self):
        """If NO row carries a layer key, all rows are taken (nothing to filter on)."""
        rows = [_profile_row("a", layer=None), _profile_row("b", layer=None)]
        kept = _MOD._settled_rows(rows)
        self.assertEqual(len(kept), 2)

    def test_render_has_provenance(self):
        rows = [_profile_row("did the thing", container_id="dev-claude-308",
                             branch="feat/us-037")]
        brief = _MOD._render(_MOD._settled_rows(rows))
        self.assertIsNotNone(brief)
        self.assertIn("dev-claude-308", brief)
        self.assertIn("feat/us-037", brief)
        self.assertIn("did the thing", brief)
        # provenance bracket form [container · age · branch]
        self.assertIn("[dev-claude-308 ·", brief)

    def test_render_empty_is_none(self):
        self.assertIsNone(_MOD._render([]))

    def test_age_formatting(self):
        # ~2 hours ago -> "Nh"
        from datetime import datetime, timedelta, timezone
        two_h = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        meta = {"valid_from": two_h}
        self.assertTrue(_MOD._age(meta).endswith("h"))
        # missing -> '?'
        self.assertEqual(_MOD._age({}), "?")


# ════════════════════════════════════════════════════════════════════════════════
# Class RequestParams — capture the outgoing request body via monkeypatched urllib.
# ════════════════════════════════════════════════════════════════════════════════

class TestRequestParams(unittest.TestCase):

    def setUp(self):
        self._orig_urlopen = _MOD.urllib.request.urlopen
        self._orig_branch = _MOD._current_branch
        # Force a deterministic branch so metadata_filter is predictable.
        _MOD._current_branch = lambda cwd: "feat/inject"
        os.environ["NEXUS_API_URL"] = "https://nexus.example/v1"
        os.environ["NEXUS_DEFAULT_USER_ID"] = "nexus"
        os.environ["NEXUS_CONTAINER_ID"] = "dev-claude-308"
        os.environ["NEXUS_API_TOKEN"] = "tok123"

    def tearDown(self):
        _MOD.urllib.request.urlopen = self._orig_urlopen
        _MOD._current_branch = self._orig_branch
        for k in ("NEXUS_API_URL", "NEXUS_DEFAULT_USER_ID", "NEXUS_CONTAINER_ID",
                  "NEXUS_API_TOKEN"):
            os.environ.pop(k, None)

    def _run_main_capturing(self, responses, stdin_event):
        cap = _UrlopenCapture(responses)
        _MOD.urllib.request.urlopen = cap
        old_stdin, old_stdout = sys.stdin, sys.stdout
        sys.stdin = io.StringIO(json.dumps(stdin_event))
        sys.stdout = io.StringIO()
        try:
            _MOD.main()
            out = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout = old_stdin, old_stdout
        return cap, out

    def test_body_uses_profile_limit_not_limit(self):
        resp = {"profile": [_profile_row("hello world")], "total_latency_ms": 1}
        cap, out = self._run_main_capturing([resp], {"cwd": _HOOKS_DIR})
        self.assertEqual(len(cap.requests), 1)
        _, body, headers = cap.requests[0]
        self.assertIn("profile_limit", body, "request body must use profile_limit")
        self.assertEqual(body["profile_limit"], 10)
        self.assertNotIn("limit", body, "request body must NOT contain a bare `limit` field")
        self.assertEqual(body["ranking_strategy"], "quality_rerank")
        self.assertEqual(body["recent_hours"], 72)

    def test_user_agent_header_present(self):
        resp = {"profile": [_profile_row("hello")], "total_latency_ms": 1}
        cap, _ = self._run_main_capturing([resp], {"cwd": _HOOKS_DIR})
        _, _, headers = cap.requests[0]
        # urllib title-cases header keys.
        joined = " ".join(headers.keys())
        self.assertIn("User-agent", headers, f"User-Agent header required (CF 1010). Got: {headers}")
        self.assertIn("X-nexus-source", headers)

    def test_metadata_filter_has_branch_and_container(self):
        resp = {"profile": [_profile_row("hello")], "total_latency_ms": 1}
        cap, _ = self._run_main_capturing([resp], {"cwd": _HOOKS_DIR})
        _, body, _ = cap.requests[0]
        mf = body.get("metadata_filter")
        self.assertIsNotNone(mf, "tier-1 request must carry metadata_filter")
        self.assertEqual(mf.get("branch"), "feat/inject")
        self.assertEqual(mf.get("container_id"), "dev-claude-308")

    def test_empty_profile_triggers_second_unfiltered_request(self):
        """Tier 1 empty -> tier 2 (no metadata_filter) project-level fallback."""
        tier1 = {"profile": [], "total_latency_ms": 1}
        tier2 = {"profile": [_profile_row("project level hit")], "total_latency_ms": 1}
        cap, out = self._run_main_capturing([tier1, tier2], {"cwd": _HOOKS_DIR})
        self.assertEqual(len(cap.requests), 2, "empty tier-1 must trigger a second request")
        _, body1, _ = cap.requests[0]
        _, body2, _ = cap.requests[1]
        self.assertIn("metadata_filter", body1, "tier-1 carries metadata_filter")
        self.assertNotIn("metadata_filter", body2, "tier-2 fallback must omit metadata_filter")
        # And the tier-2 hit must be rendered.
        self.assertIn("project level hit", out)

    def test_output_shape_and_provenance(self):
        resp = {"profile": [_profile_row("did a refactor", container_id="dev-claude-308",
                                         branch="feat/inject")],
                "total_latency_ms": 1}
        cap, out = self._run_main_capturing([resp], {"cwd": _HOOKS_DIR})
        parsed = json.loads(out)
        hso = parsed["hookSpecificOutput"]
        self.assertEqual(hso["hookEventName"], "SessionStart")
        ctx = hso["additionalContext"]
        self.assertIn("did a refactor", ctx)
        self.assertIn("[dev-claude-308 ·", ctx)
        self.assertIn("feat/inject", ctx)

    def test_branch_omitted_when_no_branch(self):
        """Non-git dir (branch None) -> metadata_filter has container_id but no branch."""
        _MOD._current_branch = lambda cwd: None
        resp = {"profile": [_profile_row("hello")], "total_latency_ms": 1}
        cap, _ = self._run_main_capturing([resp], {"cwd": _HOOKS_DIR})
        _, body, _ = cap.requests[0]
        mf = body.get("metadata_filter")
        self.assertIsNotNone(mf)
        self.assertNotIn("branch", mf, "branch key must be omitted when branch is unknown")
        self.assertIn("container_id", mf)


# ════════════════════════════════════════════════════════════════════════════════
# Class NonVacuity — prove the suite can go red.
# ════════════════════════════════════════════════════════════════════════════════

class TestNonVacuity(unittest.TestCase):

    def test_render_actually_filters(self):
        """If _settled_rows were a no-op, this would keep the observation row.
        Asserting it is dropped proves the filter is live."""
        rows = [_profile_row("keep", layer="summary"),
                _profile_row("drop", layer="observation")]
        kept_contents = [r["content"] for r in _MOD._settled_rows(rows)]
        triggered = False
        try:
            self.assertIn("drop", kept_contents)  # intentionally wrong
        except AssertionError:
            triggered = True
        self.assertTrue(triggered, "Non-vacuity: filter did not drop the observation row")


if __name__ == "__main__":
    unittest.main(verbosity=2)
