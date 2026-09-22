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

import glob
import http.server
import importlib.util
import io
import itertools
import json
import os
import re
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
_HOOK_SCRIPT = os.path.join(_HOOKS_DIR, "session_inject.py")


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
    spec = importlib.util.spec_from_file_location("session_inject", _HOOK_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MOD = _load_module()


# ── Subprocess driver (for fail-open / malformed-stdin tests) ───────────────────

def _run_hook(stdin_text, env=None, drop=(), script=None, want_stderr=False):
    """Drive the hook as a subprocess; return (stdout_bytes, exit_code)."""
    run_env = dict(os.environ)
    # Strip any inherited Nexus env so 'unset' tests are deterministic.
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

    def setUp(self):
        # A state dir of this test's own. Since TASK-003 a start REPORTS the
        # previous run's failure on stdout, so a directory shared with the
        # other subprocess tests would make "no stdout" depend on test order.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        patcher = mock.patch.dict(os.environ, {"NEXUS_HOOK_STATE_DIR": os.path.join(tmp.name, "state")})
        patcher.start()
        self.addCleanup(patcher.stop)

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

    # ── 10CG/nexus-claude-plugin#32: the aggregator's layer is session_summary ──

    @staticmethod
    def _aggregated_row(content, *, branch="feat/x", container_id="dev-claude-308"):
        """A row as `workers/session_aggregator.py` writes it: the provenance
        keys only when the session's observations agree on one, and no
        timestamp in the metadata."""
        meta = {"layer": "session_summary", "session_id": "s-1", "source_activity_count": 3,
                "observation_ids": ["t::u::a"], "aggregation_hash": "h"}
        if branch is not None:
            meta["branch"] = branch
        if container_id is not None:
            meta["container_id"] = container_id
        return {"memory_id": "m2", "content": content, "memory_type": "episodic", "metadata": meta}

    def test_a_profile_of_aggregated_summaries_is_rendered(self):
        """The bug itself: a profile holding only session_summary rows used to
        render nothing, so the brief fell back to July's migrated summaries."""
        rows = [self._aggregated_row("episode one"), _profile_row("raw obs", layer="observation")]
        brief = _MOD._render(_MOD._settled_rows(rows))
        self.assertIsNotNone(brief)
        self.assertIn("episode one", brief)
        self.assertNotIn("raw obs", brief)

    def test_session_summary_goes_first_and_each_layer_keeps_the_backend_order(self):
        # Backend order runs against both content and id, so a secondary sort
        # key on either would show up as a reordering.
        rows = [
            _profile_row("migrated y", layer="summary"),
            _profile_row("raw obs", layer="observation"),
            self._aggregated_row("episode z"),
            _profile_row("migrated b", layer="summary"),
            self._aggregated_row("episode a"),
        ]
        for row, memory_id in zip(rows, ("m-4", "m-5", "m-3", "m-2", "m-1")):
            row["memory_id"] = memory_id
        kept = [r["content"] for r in _MOD._settled_rows(rows)]
        self.assertEqual(kept, ["episode z", "episode a", "migrated y", "migrated b"])

    def test_a_profile_of_only_observations_renders_nothing(self):
        """The rows carry a layer, just not a settled one: that is not the
        no-layer case, and observations must not fall through into the brief."""
        rows = [_profile_row("obs 1", layer="observation"), _profile_row("obs 2", layer="observation")]
        self.assertEqual(_MOD._settled_rows(rows), [])
        self.assertIsNone(_MOD._render(_MOD._settled_rows(rows)))

    def test_an_empty_branch_is_rendered_like_a_missing_one(self):
        migrated = _profile_row("old", layer="summary", branch="")
        episode = self._aggregated_row("new", branch="")
        lines = _MOD._render(_MOD._settled_rows([migrated, episode])).splitlines()
        self.assertTrue(lines[1].endswith("· ?] new"), lines[1])
        self.assertTrue(lines[2].endswith("· -] old"), lines[2])

    def test_a_blank_or_non_string_branch_is_rendered_like_a_missing_one(self):
        """Whitespace leaves the same empty slot an empty string did, and a
        branch that is not a string is not a branch."""
        for value in ("  ", "\t", 0, 7, ["main"]):
            with self.subTest(value=value):
                row = self._aggregated_row("x", branch=value)
                self.assertTrue(_MOD._render(_MOD._settled_rows([row])).endswith("· ?] x"))

    def test_a_layer_value_that_is_not_a_known_string_is_dropped_not_raised(self):
        """`in` on the rank table hashes the value: a list would raise and take
        every other row down with it."""
        rows = [self._aggregated_row("keep"), _profile_row("list", layer=["summary"]),
                _profile_row("number", layer=7), _profile_row("unknown", layer="fact")]
        self.assertEqual([r["content"] for r in _MOD._settled_rows(rows)], ["keep"])

    def test_branch_shows_dash_for_a_migrated_summary_and_question_mark_otherwise(self):
        migrated = _profile_row("old", layer="summary")
        del migrated["metadata"]["branch"]
        mixed_session = self._aggregated_row("new", branch=None)  # no unique branch
        brief = _MOD._render(_MOD._settled_rows([migrated, mixed_session]))
        lines = brief.splitlines()
        self.assertTrue(lines[1].endswith("· ?] new"), lines[1])
        self.assertTrue(lines[2].endswith("· -] old"), lines[2])

    def test_an_aggregated_row_renders_its_provenance_with_an_unknown_age(self):
        """Neither the aggregator's metadata nor a profile row carries a
        timestamp, so the age is '?' until workflow D (TASK-007) reads
        `created_at` from the list endpoint. Pinned so that change is seen."""
        brief = _MOD._render(_MOD._settled_rows([self._aggregated_row("did it", branch="feat/us-037")]))
        self.assertIn("[dev-claude-308 · ? · feat/us-037] did it", brief)

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

    def test_known_limit_own_episodes_in_tier_one_keep_tier_two_from_running(self):
        """Pinned known limit (10CG/nexus-claude-plugin#32 interim fix): this
        container's aggregated episode on the branch satisfies tier 1, so the
        project-level tier 2 -- the only source of the other container's rows
        and of the migrated summaries -- is never sent. Workflow D (change 2
        TASK-007) replaces the tiers; this test is expected to change then."""
        own = TestRender._aggregated_row("my own episode", branch="feat/inject")
        tier1 = {"profile": [own], "total_latency_ms": 1}
        tier2 = {"profile": [_profile_row("other container episode", container_id="dev-claude2")],
                 "total_latency_ms": 1}
        cap, out = self._run_main_capturing([tier1, tier2], {"cwd": _HOOKS_DIR})
        self.assertEqual(len(cap.requests), 1)
        self.assertIn("my own episode", out)
        self.assertNotIn("other container episode", out)

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


# ════════════════════════════════════════════════════════════════════════════════
# TASK-002 — run ledger, X-Nexus-Source, shared identity.
# New tests use mock.patch / addCleanup only (TASK-011 audit rule): CI runs every
# hook test in one process, so a patch that is not restored leaks everywhere.
# ════════════════════════════════════════════════════════════════════════════════

class _RawResponse:
    """A 2xx whose body is whatever bytes you hand it."""

    def __init__(self, body):
        self._buf = io.BytesIO(body)

    def read(self, *a, **k):
        return self._buf.read(*a, **k)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _raising(exc):
    def urlopen(req, timeout=None):
        raise exc
    return urlopen


def _http_error(code):
    return urllib.error.HTTPError("https://nexus.example/v1/context/retrieve", code, "msg", {}, None)


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
        self._patch(mock.patch.object(_MOD, "_current_branch", return_value="feat/inject"))
        # The ledger directory is keyed by the project slug, which shells out to
        # git. Patched on _identity: both the hook and _hook_state reach it by
        # attribute, which is what makes this patch land (a `from` import would
        # have bound the original at import time).
        self._patch(mock.patch.object(_identity, "project_slug", return_value="proj"))

    def _patch(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _main(self, urlopen, stdin_text=None, mod=None):
        mod = mod or _MOD
        payload = json.dumps({"cwd": self.cwd}) if stdin_text is None else stdin_text
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(urllib.request, "urlopen", urlopen), \
                mock.patch.object(sys, "stdin", io.StringIO(payload)), \
                mock.patch.object(sys, "stdout", out), \
                mock.patch.object(sys, "stderr", err):
            mod.main()
        return out.getvalue(), err.getvalue()

    def _entries(self):
        entries, reasons = _hook_state.read_ledger("session-inject", self.cwd)
        self.assertEqual(reasons, [])
        return entries


class TestLedger(_LedgerCase):

    def test_a_successful_injection_records_one_clean_run(self):
        out, _ = self._main(_UrlopenCapture([{"profile": [_profile_row("hello")]}]))
        self.assertIn("hello", out)
        entries = self._entries()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(
            (entry["hook"], entry["ok"], entry["reason"], entry["calls"]),
            ("session-inject", True, "none", 1),
        )
        self.assertIs(type(entry["elapsed_ms"]), int)
        self.assertGreaterEqual(entry["elapsed_ms"], 0)
        self.assertEqual((entry["tier"], entry["rows"]), (1, 1))

    def test_the_project_level_fallback_counts_both_calls(self):
        cap = _UrlopenCapture([{"profile": []}, {"profile": [_profile_row("tier two")]}])
        out, _ = self._main(cap)
        self.assertIn("tier two", out)
        entry = self._entries()[-1]
        self.assertEqual((entry["reason"], entry["calls"], entry["tier"]), ("none", 2, 2))

    def test_nothing_to_inject_is_a_recorded_skip_not_silence(self):
        """Exit 0 with no stdout is also what a broken hook looks like. The
        ledger is the only thing that tells the two apart."""
        out, _ = self._main(_UrlopenCapture([{"profile": []}, {"profile": []}]))
        self.assertEqual(out, "")
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"], entry["calls"]), (True, "nothing_to_do", 2))
        self.assertFalse(_hook_state.is_failure_reason(entry["reason"]))

    def test_no_backend_configured_is_a_recorded_skip_and_makes_no_call(self):
        urlopen = mock.Mock()
        with mock.patch.dict(os.environ):
            del os.environ["NEXUS_API_URL"]
            out, _ = self._main(urlopen)
        urlopen.assert_not_called()
        self.assertEqual(out, "")
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"], entry["calls"]), (True, "not_configured", 0))

    def test_a_failed_retrieval_is_recorded_with_its_own_reason(self):
        cases = (
            (_http_error(500), "http_error"),
            (_http_error(429), "rate_limited"),
            (socket.timeout("timed out"), "timeout"),
            (urllib.error.URLError(ConnectionRefusedError()), "http_error"),
        )
        for count, (exc, expected) in enumerate(cases, start=1):
            with self.subTest(expected=expected):
                out, err = self._main(_raising(exc))  # and main() does not raise
                # From the second case on, stdout carries the REPORT of the
                # previous failure (TASK-003) -- but never a brief.
                if out:
                    ctx = json.loads(out)["hookSpecificOutput"]["additionalContext"]
                    self.assertTrue(ctx.startswith("[nexus-memory]"), ctx[:80])
                    self.assertEqual(len(ctx.splitlines()), 1, "a failed run must not inject anything")
                entries = self._entries()
                self.assertEqual(len(entries), count, "every run appends exactly one record")
                entry = entries[-1]
                self.assertEqual((entry["ok"], entry["reason"], entry["calls"]), (False, expected, 1))
                self.assertIn(expected, err)

    def test_the_second_call_failing_still_counts_two_calls(self):
        """`calls` is attempts, not successes: a baseline that undercounts the
        calls made on the failing path is wrong exactly when it is read."""
        responses = iter([_FakeResponse({"profile": []})])

        def urlopen(req, timeout=None):
            try:
                return next(responses)
            except StopIteration:
                raise _http_error(503)

        self._main(urlopen)
        entry = self._entries()[-1]
        self.assertEqual((entry["reason"], entry["calls"]), ("http_error", 2))

    def test_a_two_hundred_that_is_not_json_is_an_http_error(self):
        """A proxy or challenge page. `unknown` would be true and useless."""
        self._main(lambda req, timeout=None: _RawResponse(b"<html>Just a moment...</html>"))
        self.assertEqual(self._entries()[-1]["reason"], "http_error")

    def test_a_malformed_payload_is_recorded_not_swallowed(self):
        urlopen = mock.Mock()
        for payload in ("not json at all", "[]"):
            with self.subTest(payload=payload):
                out, _ = self._main(urlopen, stdin_text=payload)
                self.assertEqual(out, "")
                entry = self._entries()[-1]
                self.assertEqual((entry["ok"], entry["reason"]), (False, "unknown"))
        urlopen.assert_not_called()

    def test_the_brief_is_on_stdout_before_the_ledger_is_touched(self):
        """Nothing in the bookkeeping may cost the session its injection --
        not an exception, and not a stall either, which is why this pins the
        ORDER rather than only the survival of an exception."""
        seen = {}

        def spy(*args, **kwargs):
            seen["stdout"] = sys.stdout.getvalue()
            return {}

        with mock.patch.object(_hook_state, "record_run", side_effect=spy):
            self._main(_UrlopenCapture([{"profile": [_profile_row("first things first")]}]))
        self.assertIn("first things first", seen["stdout"])

    def test_a_ledger_that_blows_up_does_not_take_the_hook_down(self):
        with mock.patch.object(_hook_state, "record_run", side_effect=RuntimeError("disk on fire")):
            out, err = self._main(_UrlopenCapture([{"profile": [_profile_row("still here")]}]))
        self.assertIn("still here", out)
        self.assertIn("could not record", err)
        self.assertIn("disk on fire", err)


class TestSourceHeader(_LedgerCase):

    def test_every_remote_call_carries_name_slash_version(self):
        cap = _UrlopenCapture([{"profile": []}, {"profile": [_profile_row("x")]}])
        self._main(cap)
        self.assertEqual(len(cap.requests), 2)
        for _, _, headers in cap.requests:  # urllib title-cases header keys
            self.assertEqual(headers.get("X-nexus-source"), f"sessionstart-hook/{_plugin_version()}")

    def test_the_name_half_is_the_one_the_backend_allowlists(self):
        """nexus `mcp_attribution._KNOWN_CLIENTS` contains this literal. It was
        missing from that list once, and 212 SessionStart calls on prod were
        attributed to "unknown" before anyone noticed."""
        self.assertEqual(_MOD.SOURCE_NAME, "sessionstart-hook")


class TestIdentityIsShared(_LedgerCase):

    def test_user_id_and_container_id_come_from_the_shared_module(self):
        """Not `equal to what _identity would say` -- *the same call*. The write
        side uses it too, and two derivations that merely agree today are how
        the two sides end up keying one project differently."""
        cap = _UrlopenCapture([{"profile": [_profile_row("x")]}])
        with mock.patch.object(_identity, "user_id", return_value="pinned-user") as user_id, \
                mock.patch.object(_identity, "container_id", return_value="pinned-container"):
            self._main(cap)
        user_id.assert_called_once_with(self.cwd)
        _, body, _ = cap.requests[0]
        self.assertEqual(body["user_id"], "pinned-user")
        self.assertEqual(body["metadata_filter"]["container_id"], "pinned-container")


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
        ledgers = glob.glob(os.path.join(state, "*", "session-inject.json"))
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
        ledgers = glob.glob(os.path.join(self.home, ".nexus", "hooks", "*", "session-inject.json"))
        self.assertEqual(len(ledgers), 1, ledgers)


class TestSharedModulesUnavailable(_LedgerCase):
    """`_hook_state` needs fcntl, which a native Windows Python does not have.
    Wiring the hook to the ledger must not make the ledger a precondition for
    the hook's actual job."""

    def _copy_hook_without(self, *missing):
        target = os.path.join(self.tmp.name, "partial-install")
        os.makedirs(target)
        for name in ("session_inject.py", "_identity.py", "_hook_state.py"):
            if name not in missing:
                shutil.copy(os.path.join(_HOOKS_DIR, name), target)
        return os.path.join(target, "session_inject.py")

    def test_without_the_ledger_module_the_hook_still_injects(self):
        with mock.patch.dict(sys.modules, {"_hook_state": None}):  # import -> ImportError
            mod = _load_module()
        self.assertIsNone(mod._hook_state)
        with mock.patch.object(mod, "_current_branch", return_value="feat/inject"):
            out, err = self._main(
                _UrlopenCapture([{"profile": [_profile_row("ledgerless")]}]), mod=mod
            )
        self.assertIn("ledgerless", out)
        self.assertIn("ledger unavailable", err)
        self.assertFalse(os.path.exists(self.state_dir), "nothing should have been written")

    def test_as_a_script_without_the_ledger_module_it_exits_zero(self):
        script = self._copy_hook_without("_hook_state.py")
        stdout, code, stderr = _run_hook(json.dumps({"cwd": self.cwd}), script=script, want_stderr=True)
        self.assertEqual((stdout, code), (b"", 0))
        self.assertIn("ledger unavailable", stderr)

    def test_as_a_script_without_the_identity_module_it_exits_zero_and_says_why(self):
        """A traceback here would be exit 1 -- a hook error on every session
        start -- for a plugin whose whole contract is fail-open."""
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

        began = time.monotonic()
        with mock.patch.object(_hook_state, "record_run", side_effect=stall), \
                mock.patch.object(_MOD, "_LEDGER_BUDGET_SECONDS", 0.2):
            out, err = self._main(_UrlopenCapture([{"profile": [_profile_row("kept")]}]))
        self.assertLess(time.monotonic() - began, 5, "main() waited for the stalled write")
        self.assertIn("kept", out, "the brief must already be out")
        self.assertIn("still running", err)

    def test_as_a_script_a_held_ledger_lock_cannot_hang_it(self):
        """End to end, with a real flock held by this process."""
        import fcntl

        state = os.path.join(self.tmp.name, "held-state")
        project = os.path.join(state, "proj")  # the subprocess derives this from self.cwd
        os.makedirs(project)
        lock_path = os.path.join(project, "session-inject.json.lock")
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
            ledgers = glob.glob(os.path.join(state, "*", "session-inject.json"))
            self.assertEqual(len(ledgers), 1, (ledgers, stderr))


class TestBackendSaidItFailed(_LedgerCase):
    """The backend degrades gracefully: when its memory lookup raises, the
    answer is still HTTP 200, with `profile: null` and the exception text under
    `errors`. Reading only `profile` turned that into `nothing_to_do` -- an
    expected skip, never reported. A backend outage looked exactly like a
    project with no memories."""

    def test_a_reported_profile_failure_is_not_a_clean_skip(self):
        failed = {"profile": None, "errors": {"profile": "connection pool exhausted"}}
        out, err = self._main(_UrlopenCapture([failed, failed]))
        self.assertEqual(out, "")
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"], entry["calls"]), (False, "http_error", 2))
        self.assertEqual(entry["backend_errors"], ["profile"])
        self.assertIn("profile", err)

    def test_what_did_come_back_is_still_injected(self):
        """`recent` and `profile` are merged server-side, so one can fail
        while the other returns rows. Inject them AND report the failure."""
        partial = {"profile": [_profile_row("half an answer")], "errors": {"recent": "timeout"}}
        out, _ = self._main(_UrlopenCapture([partial]))
        self.assertIn("half an answer", out)
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (False, "http_error"))
        self.assertEqual(entry["backend_errors"], ["recent"])

    def test_a_failure_in_a_layer_this_hook_does_not_read_is_not_its_problem(self):
        unrelated = {"profile": [_profile_row("fine")], "errors": {"graph": "neo4j down"}}
        self._main(_UrlopenCapture([unrelated]))
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (True, "none"))
        self.assertNotIn("backend_errors", entry)

    def test_a_body_of_the_wrong_shape_is_an_http_error_not_an_empty_result(self):
        for body in ([], "a string", {"profile": "not a list"}, {"profile": [1, 2]}):
            with self.subTest(body=body):
                self._main(_UrlopenCapture([body, body]))
                entry = self._entries()[-1]
                self.assertEqual((entry["ok"], entry["reason"]), (False, "http_error"))

    def test_an_honest_empty_profile_is_still_a_skip(self):
        """The other half: `profile: null` with no `errors` is what the backend
        sends when there is nothing, and must stay quiet."""
        self._main(_UrlopenCapture([{"profile": None}, {"profile": None, "errors": None}]))
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (True, "nothing_to_do"))


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
            self._main(_UrlopenCapture([{"profile": [_profile_row("x")]}]))
        elapsed = self._entries()[-1]["elapsed_ms"]
        self.assertGreaterEqual(elapsed, 250)
        self.assertEqual(elapsed % 250, 0, elapsed)

    def test_the_record_is_filed_under_the_payloads_project(self):
        """Not under wherever the process happens to be running. Every other
        test pins the slug to one value, so this was never asserted."""
        elsewhere = os.path.join(self.tmp.name, "some-other-project")
        os.makedirs(elsewhere)
        with mock.patch.object(_identity, "project_slug", side_effect=os.path.basename):
            self._main(_UrlopenCapture([{"profile": [_profile_row("x")]}]), stdin_text=json.dumps({"cwd": elsewhere}))
        expected = os.path.join(self.state_dir, "some-other-project", "session-inject.json")
        self.assertTrue(os.path.isfile(expected), os.listdir(self.state_dir))

    def test_a_payload_cwd_that_is_not_a_string_is_ignored_not_fatal(self):
        """The slug stub has to fail the way the real lookup does on a value
        that is not a path. With a stub that answers "proj" to anything, a cwd
        of 123 had no consequences at all and this test passed against a hook
        that accepted it -- the injection matrix caught that, not review."""
        def like_the_real_one(cwd):
            return os.path.basename(cwd.rstrip("/"))

        with mock.patch.object(_identity, "project_slug", side_effect=like_the_real_one):
            self._main(_UrlopenCapture([{"profile": [_profile_row("x")]}]), stdin_text=json.dumps({"cwd": 123}))
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "session-inject.json"))
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


class _EventfulStdout(io.StringIO):
    def __init__(self, events):
        super().__init__()
        self._events = events

    def write(self, text):
        self._events.append("write")
        return super().write(text)

    def flush(self):
        self._events.append("flush")
        return super().flush()


class TestTheBriefIsReallyOut(_LedgerCase):

    def test_stdout_is_flushed_before_the_ledger_is_touched(self):
        """The ordering test reads a StringIO, which has no buffer to forget to
        flush. With the ledger step able to be abandoned, an unflushed brief is
        a lost brief."""
        events = []
        out = _EventfulStdout(events)
        with mock.patch.object(_hook_state, "record_run", side_effect=lambda *a, **k: events.append("record")), \
                mock.patch.object(urllib.request, "urlopen", _UrlopenCapture([{"profile": [_profile_row("x")]}])), \
                mock.patch.object(sys, "stdin", io.StringIO(json.dumps({"cwd": self.cwd}))), \
                mock.patch.object(sys, "stdout", out), mock.patch.object(sys, "stderr", io.StringIO()):
            _MOD.main()
        self.assertIn("record", events)
        before = events[: events.index("record")]
        self.assertIn("write", before)
        self.assertIn("flush", before)
        self.assertLess(before.index("write"), len(before) - 1 - before[::-1].index("flush"))

    def test_a_brief_that_could_not_be_written_is_not_recorded_as_delivered(self):
        class Broken(io.StringIO):
            def write(self, text):
                raise BrokenPipeError("reader went away")

        with mock.patch.object(urllib.request, "urlopen", _UrlopenCapture([{"profile": [_profile_row("x")]}])), \
                mock.patch.object(sys, "stdin", io.StringIO(json.dumps({"cwd": self.cwd}))), \
                mock.patch.object(sys, "stdout", Broken()), mock.patch.object(sys, "stderr", io.StringIO()):
            _MOD.main()
        entry = self._entries()[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (False, "unknown"))

    def test_one_unencodable_row_does_not_cost_the_whole_brief(self):
        """A lone surrogate in one row raised UnicodeEncodeError on the write,
        and every other row went with it."""
        rows = [_profile_row("good row"), _profile_row("bad \ud800 row")]
        out, _ = self._main(_UrlopenCapture([{"profile": rows}]))
        self.assertIn("good row", out)
        json.loads(out).get("hookSpecificOutput")["additionalContext"].encode("utf-8")
        self.assertEqual(self._entries()[-1]["reason"], "none")


class TestAsARealProcess(unittest.TestCase):
    """The script, a socket, and a real interpreter exit."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state = os.path.join(self.tmp.name, "state")
        self.backend = _FakeBackend({"profile": [_profile_row("from a real socket")]})
        self.addCleanup(self.backend.close)
        self.env = dict(_NO_PROXY, NEXUS_API_URL=self.backend.url, NEXUS_HOOK_STATE_DIR=self.state,
                        NEXUS_DEFAULT_USER_ID="nexus")

    def _last_entry(self):
        (ledger,) = glob.glob(os.path.join(self.state, "*", "session-inject.json"))
        with open(ledger, encoding="utf-8") as fh:
            return json.load(fh)[-1]

    def test_end_to_end(self):
        stdout, code = _run_hook(json.dumps({"cwd": self.cwd}), env=self.env, drop=_PROXY_VARS)
        self.assertEqual(code, 0)
        self.assertIn("from a real socket", json.loads(stdout)["hookSpecificOutput"]["additionalContext"])
        self.assertEqual(self.backend.seen[0]["x-nexus-source"], f"sessionstart-hook/{_plugin_version()}")
        self.assertEqual(self._last_entry()["reason"], "none")

    def test_a_stdout_nobody_is_reading_still_exits_zero(self):
        """Python retries the flush at interpreter exit; against a closed pipe
        that fails again and the process exits 120 -- a hook error on a plugin
        whose contract is exit 0, always."""
        run_env = {k: v for k, v in os.environ.items()
                   if k not in _PROXY_VARS and not k.startswith("NEXUS_")}
        run_env.update(self.env)
        read_end, write_end = os.pipe()
        os.close(read_end)
        try:
            proc = subprocess.run(
                [sys.executable, _HOOK_SCRIPT], input=json.dumps({"cwd": self.cwd}).encode(),
                stdout=write_end, stderr=subprocess.PIPE, env=run_env, timeout=20,
            )
        finally:
            os.close(write_end)
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        self.assertEqual(self._last_entry()["reason"], "unknown")


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


# ════════════════════════════════════════════════════════════════════════════════
# TASK-003 — SessionStart reads every hook's ledger and reports failures once.
# ════════════════════════════════════════════════════════════════════════════════

def _entry(reason, ts, ok=None, hook="session-capture"):
    if ok is None:
        ok = not _hook_state.is_failure_reason(reason)
    return {"hook": hook, "ts": ts, "ok": ok, "reason": reason, "elapsed_ms": 5, "calls": 1}


class _ReportCase(_LedgerCase):
    """A project whose session-inject ledger already holds one clean run, so
    this is not the very first session start (a missing capture ledger is only
    news after a SessionEnd has had the chance to fire)."""

    def setUp(self):
        super().setUp()
        self._write_ledger("session-inject", [_entry("none", "2026-09-20T10:00:00Z", hook="session-inject")])

    def _write_ledger(self, hook, entries):
        path = _hook_state.ledger_path(hook, self.cwd)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(entries if isinstance(entries, str) else json.dumps(entries))

    def _start(self, urlopen=None):
        out, err = self._main(urlopen or _UrlopenCapture([{"profile": [_profile_row("the brief")]}]))
        return (json.loads(out) if out else None), err

    def _system_message(self, urlopen=None):
        parsed, _ = self._start(urlopen)
        return (parsed or {}).get("systemMessage")


class TestFailureReport(_ReportCase):

    def test_a_failed_capture_run_is_reported_and_the_brief_survives(self):
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        parsed, _ = self._start()
        message = parsed["systemMessage"]
        self.assertIn("session-capture", message)
        self.assertIn("http_error", message)
        ctx = parsed["hookSpecificOutput"]["additionalContext"]
        self.assertTrue(ctx.startswith("[nexus-memory]"), ctx[:80])
        self.assertIn("session-capture", ctx.splitlines()[0])
        self.assertIn("the brief", ctx)  # the injection is intact underneath
        self.assertEqual(parsed["hookSpecificOutput"]["hookEventName"], "SessionStart")

    def test_the_same_failure_is_reported_once(self):
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        self.assertIsNotNone(self._system_message())
        self.assertIsNone(self._system_message(), "the same entry was reported twice")
        parsed, _ = self._start()
        self.assertFalse(parsed["hookSpecificOutput"]["additionalContext"].startswith("[nexus-memory]"))

    def test_a_newer_failure_is_reported_again(self):
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        self._system_message()
        self._write_ledger(
            "session-capture",
            [_entry("http_error", "2026-09-21T09:00:00Z"), _entry("timeout", "2026-09-21T18:00:00Z")],
        )
        message = self._system_message()
        self.assertIsNotNone(message)
        self.assertIn("timeout", message)

    def test_the_same_reason_failing_again_later_is_news(self):
        """The marker is the entry (its timestamp), not the reason: a hook
        that keeps failing the same way every session keeps being reported."""
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        self._system_message()
        self._write_ledger(
            "session-capture",
            [_entry("http_error", "2026-09-21T09:00:00Z"), _entry("http_error", "2026-09-21T18:00:00Z")],
        )
        self.assertIsNotNone(self._system_message())

    def test_a_skip_class_latest_entry_is_quiet_even_after_an_older_failure(self):
        self._write_ledger(
            "session-capture",
            [_entry("http_error", "2026-09-21T09:00:00Z"), _entry("not_owner", "2026-09-21T18:00:00Z")],
        )
        self.assertIsNone(self._system_message())

    def test_a_clean_latest_entry_is_quiet(self):
        self._write_ledger("session-capture", [_entry("none", "2026-09-21T18:00:00Z")])
        self.assertIsNone(self._system_message())

    def test_unknown_is_a_failure(self):
        self._write_ledger("session-capture", [_entry("unknown", "2026-09-21T18:00:00Z")])
        self.assertIn("unknown", self._system_message())

    def test_this_hooks_own_previous_failure_is_reported_too(self):
        """The reader reads its own ledger before this run appends to it, so a
        session start that timed out last time is reported this time."""
        self._write_ledger(
            "session-inject",
            [_entry("none", "2026-09-20T10:00:00Z", hook="session-inject"),
             _entry("timeout", "2026-09-21T10:00:00Z", hook="session-inject")],
        )
        message = self._system_message()
        self.assertIn("session-inject", message)
        self.assertIn("timeout", message)

    def test_every_failing_hook_is_named(self):
        self._write_ledger("session-capture", [_entry("rate_limited", "2026-09-21T09:00:00Z")])
        self._write_ledger("memory-sync", [_entry("orphan_guard", "2026-09-21T09:00:00Z", hook="memory-sync")])
        message = self._system_message()
        for needle in ("session-capture", "rate_limited", "memory-sync", "orphan_guard"):
            self.assertIn(needle, message)

    def test_the_report_names_where_the_ledgers_are(self):
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        parsed, _ = self._start()
        self.assertIn(_hook_state.project_dir(self.cwd), parsed["hookSpecificOutput"]["additionalContext"].splitlines()[0])


class TestUnreadableAndMissingLedgers(_ReportCase):

    def test_a_corrupt_ledger_is_reported_once_and_forgotten_when_it_recovers(self):
        self._write_ledger("session-capture", "{not json")
        message = self._system_message()
        self.assertIn("session-capture", message)
        self.assertIn("unreadable", message)
        self.assertIsNone(self._system_message(), "corruption reported twice")
        # It recovers (the hook rewrote it) with a clean run: quiet...
        self._write_ledger("session-capture", [_entry("none", "2026-09-21T20:00:00Z")])
        self.assertIsNone(self._system_message())
        # ...and a later corruption is news again.
        self._write_ledger("session-capture", "[1, 2")
        self.assertIsNotNone(self._system_message())

    def test_a_missing_capture_ledger_is_quiet_on_the_very_first_start(self):
        """Fresh install: SessionEnd has not had a chance to fire yet."""
        os.remove(_hook_state.ledger_path("session-inject", self.cwd))
        self.assertIsNone(self._system_message())

    def test_a_missing_capture_ledger_after_a_previous_session_is_reported_once(self):
        """A session start already happened and still no SessionEnd ever
        recorded anything: the capture hook is not firing (killed at the 1.5 s
        shared budget, hooks.json not loaded, ...). This is the stall the
        baseline exists for, and it is invisible in every other way."""
        message = self._system_message()
        self.assertIsNotNone(message)
        self.assertIn("session-capture", message)
        self.assertIn("never", message)
        self.assertIsNone(self._system_message(), "missing ledger reported twice")

    def test_a_ledger_that_appears_clears_the_missing_marker(self):
        self._system_message()  # reports missing
        self._write_ledger("session-capture", [_entry("none", "2026-09-21T20:00:00Z")])
        self.assertIsNone(self._system_message())
        os.remove(_hook_state.ledger_path("session-capture", self.cwd))
        self.assertIsNotNone(self._system_message(), "gone again is news again")

    def test_temp_and_state_files_are_not_ledgers(self):
        project = _hook_state.project_dir(self.cwd)
        os.makedirs(project, exist_ok=True)
        with open(os.path.join(project, ".tmp-abc.json"), "w") as fh:
            fh.write("{garbage")  # an abandoned atomic write
        with open(os.path.join(project, "memory-sync.state.json"), "w") as fh:
            fh.write("[1, 2")  # a state file, not a ledger
        self._write_ledger("session-capture", [_entry("none", "2026-09-21T20:00:00Z")])
        self.assertIsNone(self._system_message())


class TestReportDelivery(_ReportCase):

    def test_a_report_with_nothing_to_inject_still_goes_out(self):
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        parsed, _ = self._start(_UrlopenCapture([{"profile": []}, {"profile": []}]))
        self.assertIn("session-capture", parsed["systemMessage"])
        ctx = parsed["hookSpecificOutput"]["additionalContext"]
        self.assertTrue(ctx.startswith("[nexus-memory]"))
        self.assertEqual(len(ctx.splitlines()), 1)
        # and this run's own ledger entry is still a clean skip
        self.assertEqual(self._entries()[-1]["reason"], "nothing_to_do")

    def test_a_report_goes_out_even_when_this_runs_retrieval_fails(self):
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        parsed, _ = self._start(_raising(_http_error(503)))
        self.assertIn("session-capture", parsed["systemMessage"])
        self.assertEqual(self._entries()[-1]["reason"], "http_error")

    def test_a_report_goes_out_when_no_backend_is_configured(self):
        """The failures of other hooks are worth knowing regardless of this
        hook's own configuration, and exit 0 always."""
        self._write_ledger("session-capture", [_entry("timeout", "2026-09-21T09:00:00Z")])
        with mock.patch.dict(os.environ):
            del os.environ["NEXUS_API_URL"]
            parsed, _ = self._start(mock.Mock())
        self.assertIn("timeout", parsed["systemMessage"])

    def test_nothing_to_report_and_nothing_to_inject_is_still_silent(self):
        self._write_ledger("session-capture", [_entry("none", "2026-09-21T20:00:00Z")])
        out, _ = self._main(_UrlopenCapture([{"profile": []}, {"profile": []}]))
        self.assertEqual(out, "")

    def test_the_reported_marker_lives_in_state_next_to_the_container_id(self):
        """TASK-007 will keep its marker in the same state file and run
        identity_drift over it; a state written without container_id makes
        every later run report unknown."""
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        self._start()
        state, reasons = _hook_state.read_state("session-inject", self.cwd)
        self.assertEqual(reasons, [])
        self.assertIn("session-capture", state["reported"])
        self.assertEqual(state["container_id"], _identity.container_id())

    def test_as_a_script_it_reports_and_exits_zero(self):
        self._write_ledger("session-capture", [_entry("http_error", "2026-09-21T09:00:00Z")])
        stdout, code, stderr = _run_hook(
            json.dumps({"cwd": self.cwd}),
            env={"NEXUS_HOOK_STATE_DIR": self.state_dir},
            want_stderr=True,
        )
        self.assertEqual(code, 0, stderr)
        parsed = json.loads(stdout)
        self.assertIn("http_error", parsed["systemMessage"])


class TestExpectedLedgersMatchTheManifest(unittest.TestCase):
    def test_every_registered_session_end_hook_is_expected_to_leave_a_ledger(self):
        """The "never recorded a run" report only covers hooks named in
        _EXPECTED_LEDGERS. A SessionEnd hook registered in hooks.json but not
        named here could stop firing and never be missed -- which is the exact
        silence the baseline exists to break. New hooks (TASK-005 / 006) add
        themselves here, and this test is what reminds them."""
        with open(os.path.join(_HOOKS_DIR, "hooks.json"), encoding="utf-8") as fh:
            manifest = json.load(fh)
        commands = [
            h["command"]
            for entry in manifest["hooks"].get("SessionEnd", [])
            for h in entry.get("hooks", [])
            if h.get("type") == "command"
        ]
        self.assertTrue(commands)
        for command in commands:
            script = re.search(r"hooks/([\w.-]+\.py)", command).group(1)
            spec = importlib.util.spec_from_file_location(
                f"expected_ledgers_{script[:-3]}", os.path.join(_HOOKS_DIR, script)
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            self.assertIn(mod.HOOK, _MOD._EXPECTED_LEDGERS, f"{script} leaves ledger {mod.HOOK!r}")

if __name__ == "__main__":
    unittest.main(verbosity=2)
