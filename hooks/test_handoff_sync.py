#!/usr/bin/env python3
"""Tests for hooks/handoff_sync.py (change 2 TASK-005, workflow B: handoff ->
session_summary episode).

Runnable as: python3 hooks/test_handoff_sync.py   (stdlib unittest only)

Three layers, matching how the hook itself is layered:
  - TestLocate / TestContent call the pure filesystem/string functions
    (_locate, _build_content, _split_frontmatter) directly -- no project
    root, no identity, no network.
  - TestOwnerAndOptOut / TestWritePath / TestPointerUnresolvedIsReported
    drive the hook end to end (mod.main(), in-process) against a REAL
    scripted loopback HTTP backend (like test_ingest_client.py's own
    _Backend): nothing about _ingest_client is mocked, so every assertion
    about what gets POSTed/PATCHed is on the actual wire.
  - TestRuns / TestSubprocess exercise the two things that need a real
    project root (a subdirectory cwd, a degraded identity) and a real
    subprocess (exit code, ledger written, garbage stdin).

Hermetic per the plugin's project lessons: the whole module runs under a
throwaway HOME + NEXUS_HOOK_STATE_DIR (setUpModule), proxy env vars are
scrubbed so loopback requests never go near a developer machine's
cross-border proxy, and the module asserts the fake HOME is still empty on
the way out.
"""

import glob
import http.server
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.parse
from unittest import mock

import _hook_state
import _identity
import _ingest_client
import _redact

_HOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
_HOOK_SCRIPT = os.path.join(_HOOKS_DIR, "handoff_sync.py")

_PROXY_VARS = ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY")
_NO_PROXY = {"no_proxy": "127.0.0.1,localhost", "NO_PROXY": "127.0.0.1,localhost"}


def _assert_home_untouched(home):
    leaked = sorted(os.listdir(home))
    if leaked:
        raise AssertionError(f"a test wrote under HOME instead of the state dir: {leaked}")


def setUpModule():
    root = tempfile.mkdtemp(prefix="nexus-hooktest-")
    unittest.addModuleCleanup(shutil.rmtree, root, ignore_errors=True)
    home = os.path.join(root, "home")
    os.makedirs(home)
    env = {k: v for k, v in os.environ.items() if k not in _PROXY_VARS}
    env.update({"HOME": home, "NEXUS_HOOK_STATE_DIR": os.path.join(root, "state")})
    env.update(_NO_PROXY)
    patcher = mock.patch.dict(os.environ, env, clear=True)
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)
    unittest.addModuleCleanup(_assert_home_untouched, home)  # LIFO: this runs first


def _load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MOD = _load_module(_HOOK_SCRIPT, "handoff_sync")
_INJECT_MOD = _load_module(os.path.join(_HOOKS_DIR, "session_inject.py"), "session_inject_for_handoff_test")


# ── in-process runner ────────────────────────────────────────────────────

# What a call through _run_main may keep from the ambient environment: HOME
# (the module fixture's fake one) and PATH (TestRuns drives real git repos,
# which need it to find the binary). Everything else -- most of all a
# developer shell's own NEXUS_API_URL -- must never leak into a test that
# means to exercise `not_configured` or a specific backend.
_BASE_ENV_KEYS = ("HOME", "NEXUS_HOOK_STATE_DIR", "PATH")


def _run_main(mod, event, extra_env):
    """Run ``mod.main()`` in-process: stdin is ``event`` as JSON, the
    environment is reset to just ``_BASE_ENV_KEYS`` plus ``extra_env`` plus
    NO_PROXY. Never touches ``__main__``'s ``sys.exit`` / ``os._exit`` --
    those live only in the hook's own ``if __name__ == "__main__":`` block.
    """
    clean = {k: os.environ[k] for k in _BASE_ENV_KEYS if k in os.environ}
    clean.update(_NO_PROXY)
    clean.update(extra_env)
    old_stdin = sys.stdin
    sys.stdin = io.StringIO(json.dumps(event))
    try:
        with mock.patch.dict(os.environ, clean, clear=True):
            mod.main()
    finally:
        sys.stdin = old_stdin


def _run_hook(stdin_text, env=None, want_stderr=False):
    """Drive the real script as a subprocess; return (stdout_bytes, exit_code)."""
    run_env = {k: v for k, v in os.environ.items() if not k.startswith("NEXUS_")}
    if env:
        run_env.update(env)
    result = subprocess.run(
        [sys.executable, _HOOK_SCRIPT],
        input=stdin_text.encode(),
        capture_output=True,
        timeout=20,
        env=run_env,
    )
    if want_stderr:
        return result.stdout, result.returncode, result.stderr.decode()
    return result.stdout, result.returncode


# ── scripted loopback backend (mirrors test_ingest_client.py's _Backend) ──

class _Backend:
    """A scripted fake of the memory endpoints on a real loopback socket."""

    def __init__(self):
        self.script = []
        self.requests = []
        backend = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _handle(handler):
                length = int(handler.headers.get("Content-Length") or 0)
                raw = handler.rfile.read(length) if length else b""
                parsed = urllib.parse.urlsplit(handler.path)
                backend.requests.append(
                    {
                        "method": handler.command,
                        "path": parsed.path,
                        "query": dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)),
                        "headers": {k.lower(): v for k, v in handler.headers.items()},
                        "json": json.loads(raw) if raw else None,
                    }
                )
                if backend.script:
                    status, body = backend.script.pop(0)
                else:
                    status, body = 599, {"detail": "unscripted request"}
                payload = json.dumps(body).encode("utf-8") if body is not None else b""
                handler.send_response(status)
                handler.send_header("Content-Type", "application/json")
                handler.send_header("Content-Length", str(len(payload)))
                handler.end_headers()
                if payload:
                    try:
                        handler.wfile.write(payload)
                    except BrokenPipeError:
                        pass

            do_GET = do_POST = do_PATCH = do_DELETE = _handle

            def log_message(handler, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_address[1]}/v1"

    def reply(self, status, body=None):
        self.script.append((status, body))
        return self

    def close(self):
        self.server.shutdown()
        self.server.server_close()


CONTAINER = "dev-box-a"
LOCAL_UUID = "aaaaaaaa"
OTHER_UUID = "bbbbbbbb"
USER = "proj"
BRANCH = "feat/handoff-sync-test"


def _row(external_id, *, content_hash, layer="session_summary", container_id=CONTAINER,
         created_at="2026-09-20T10:00:00.000001Z", row_id="11111111-1111-4111-8111-111111111111", extra=None):
    meta = {"layer": layer, "external_id": external_id, "container_id": container_id, "content_hash": content_hash}
    meta.update(extra or {})
    return {
        "memory_id": f"t1::{USER}::{row_id}",
        "id": row_id,
        "user_id": USER,
        "content": "stored",
        "metadata": meta,
        "created_at": created_at,
        "updated_at": created_at,
    }


def _page(*rows):
    return {"memories": list(rows), "total_count": len(rows), "limit": 5, "offset": 0, "has_next": False}


# ── handoff document fixtures ───────────────────────────────────────────

def _write_handoff(
    handoff_dir, name, *, track_id="carry-x", owner="owner/aaaaaaaa", phase="B", status="active",
    updated_at="2026-09-20T10:00:00Z", nexus_ingest=None, h1="# Handoff Title",
    section6="## §6 Next session 入口 + 优先级建议\n\nsix body",
    section2="## §2 未完成 / Carry-forward 清单\n\ntwo body",
    frontmatter_lines=None, body=None,
):
    """Write a fixture handoff document. ``frontmatter_lines``, given,
    REPLACES the whole frontmatter block verbatim (malformed-frontmatter
    fixtures); ``body``, given, replaces the whole assembled body (empty /
    renamed-heading fixtures). A field passed as ``None`` (``owner``
    included) is OMITTED from the frontmatter rather than written empty."""
    if frontmatter_lines is None:
        fields = [
            ("track-id", track_id), ("owner-container", owner), ("phase", phase),
            ("status", status), ("updated-at", updated_at), ("nexus-ingest", nexus_ingest),
        ]
        frontmatter_lines = ["---"] + [f"{k}: {v}" for k, v in fields if v is not None] + ["---"]
    if body is None:
        body = "\n\n".join(p for p in (h1, section6, section2) if p)
    text = "\n".join(frontmatter_lines) + "\n\n" + body + "\n"
    path = os.path.join(handoff_dir, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


def _write_latest_pointer(handoff_dir, target_name, style="pointer"):
    """``style``: ``pointer`` (the Aria collector format) / ``arrow`` (the
    deprecated arrow style) / ``banner`` (a multi-track deprecation banner,
    no pointer line at all)."""
    if style == "pointer":
        text = f"# Latest\n\n**Latest**: [{target_name}](./{target_name})\n"
    elif style == "arrow":
        text = f"# Latest\n\n→ [{target_name}]({target_name})\n"
    elif style == "banner":
        text = "# Latest\n\n> Multiple active tracks; see the dashboard, not this file.\n"
    else:
        raise ValueError(style)
    with open(os.path.join(handoff_dir, "latest.md"), "w", encoding="utf-8") as fh:
        fh.write(text)


# ════════════════════════════════════════════════════════════════════════
# locate() -- pure filesystem scanning, no identity / network involved
# ════════════════════════════════════════════════════════════════════════

class _HandoffDirCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.handoff_dir = os.path.join(self.cwd, "docs", "handoff")

    def _mkdir(self):
        os.makedirs(self.handoff_dir, exist_ok=True)
        return self.handoff_dir


class TestLocate(_HandoffDirCase):

    def test_pointer_hit_selects_the_pointed_to_file(self):
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")  # newer, not pointed to
        _write_latest_pointer(self.handoff_dir, "a.md")
        self.assertEqual(_MOD._locate(self.handoff_dir), ("a.md", None))

    def test_pointer_missing_falls_back_to_newest_updated_at(self):
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        self.assertEqual(_MOD._locate(self.handoff_dir), ("b.md", None))

    def test_multi_track_banner_without_a_pointer_falls_back(self):
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        _write_latest_pointer(self.handoff_dir, "a.md", style="banner")
        self.assertEqual(_MOD._locate(self.handoff_dir), ("b.md", None))

    def test_arrow_style_pointer_falls_back(self):
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        _write_latest_pointer(self.handoff_dir, "a.md", style="arrow")  # points at a, the OLDER one
        self.assertEqual(_MOD._locate(self.handoff_dir), ("b.md", None))  # arrow ignored -> newest wins

    def test_pointer_to_a_non_candidate_falls_back(self):
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        _write_latest_pointer(self.handoff_dir, "deleted-file.md")
        self.assertEqual(_MOD._locate(self.handoff_dir), ("b.md", None))

    def test_no_dir_is_no_handoff(self):
        self.assertFalse(os.path.isdir(self.handoff_dir))
        self.assertEqual(_MOD._locate(self.handoff_dir), (None, "no_handoff"))

    def test_only_latest_and_readme_is_no_handoff(self):
        self._mkdir()
        _write_latest_pointer(self.handoff_dir, "irrelevant.md")
        with open(os.path.join(self.handoff_dir, "README.md"), "w", encoding="utf-8") as fh:
            fh.write("# About this directory\n")
        self.assertEqual(_MOD._locate(self.handoff_dir), (None, "no_handoff"))

    # -- Amendment A4-5 pair: the SAME directory shape, contents-only diff --

    def test_a4_5_leg_empty_dir_is_no_handoff(self):
        self._mkdir()
        self.assertEqual(_MOD._locate(self.handoff_dir), (None, "no_handoff"))

    def test_a4_5_leg_broken_frontmatter_and_unmatched_pointer_is_pointer_unresolved(self):
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", frontmatter_lines=["---", "not: a-known-key", "---"])
        _write_latest_pointer(self.handoff_dir, "does-not-exist.md")
        self.assertEqual(_MOD._locate(self.handoff_dir), (None, "pointer_unresolved"))


# ════════════════════════════════════════════════════════════════════════
# content: H1 -> §6 -> §2, capped; empty_sections / sections_unparsed
# ════════════════════════════════════════════════════════════════════════

class TestContent(unittest.TestCase):

    def test_order_is_h1_then_section_six_then_section_two(self):
        body = "\n".join((
            "# My Title", "",
            "## §1 已完成", "", "stuff nobody cares about here", "",
            "## §6 Next session 入口", "", "six body", "",
            "### §6.1 a subsection", "", "still six", "",
            "## §2 未完成 / Carry-forward 清单", "", "two body", "",
            "## §7 提交清单", "", "not included",
        ))
        content, reason = _MOD._build_content(body)
        self.assertIsNone(reason)
        self.assertTrue(content.startswith("# My Title"))
        self.assertLess(content.index("# My Title"), content.index("## §6"))
        self.assertLess(content.index("## §6"), content.index("## §2"))
        self.assertIn("still six", content)  # a subsection is swept up
        self.assertNotIn("已完成", content)   # §1 excluded
        self.assertNotIn("提交清单", content)  # §7 excluded

    def test_section_sixty_is_not_mistaken_for_section_six(self):
        body = "# T\n\n## §60 Something Else\n\nnope\n\n## §6 Next session\n\nreal six\n"
        content, reason = _MOD._build_content(body)
        self.assertIsNone(reason)
        self.assertIn("real six", content)
        self.assertNotIn("Something Else", content)

    def test_cap_cuts_section_two_first(self):
        section6 = "## §6 Next session\n\n" + ("A" * 3000)
        section2 = "## §2 Carry\n\n" + ("B" * 3000)
        content, reason = _MOD._build_content(f"# T\n\n{section6}\n\n{section2}\n")
        self.assertIsNone(reason)
        self.assertLessEqual(len(content), _MOD._CONTENT_CAP)
        self.assertIn("A" * 100, content)       # section 6 survives whole
        self.assertNotIn("B" * 3000, content)   # section 2's tail was cut
        self.assertIn("truncated", content)

    def test_cap_cuts_section_six_too_when_h1_and_six_alone_exceed_it(self):
        section6 = "## §6 Next session\n\n" + ("A" * 5000)
        section2 = "## §2 Carry\n\n" + ("B" * 500)
        content, reason = _MOD._build_content(f"# T\n\n{section6}\n\n{section2}\n")
        self.assertIsNone(reason)
        self.assertLessEqual(len(content), _MOD._CONTENT_CAP)
        self.assertNotIn("B" * 500, content)    # section 2 dropped entirely
        self.assertIn("truncated", content)

    def test_empty_body_is_empty_sections(self):
        self.assertEqual(_MOD._build_content("   \n\n  "), (None, "empty_sections"))

    def test_a_short_body_with_no_sections_is_still_empty_sections(self):
        content, reason = _MOD._build_content("# Title\n\nshort.")
        self.assertEqual((content, reason), (None, "empty_sections"))

    def test_renamed_headings_on_a_nontrivial_body_is_sections_unparsed(self):
        body = "# Title\n\n" + ("prose with no known section headings at all. " * 10)
        self.assertGreaterEqual(len(body.strip()), _MOD._MIN_NONTRIVIAL_BODY)
        content, reason = _MOD._build_content(body)
        self.assertIsNone(content)
        self.assertEqual(reason, "sections_unparsed")

    def test_frontmatter_values_containing_a_colon(self):
        text = "---\nupdated-at: 2026-05-19T22:31:13Z\nowner-container: simonfish/bfe8285d\n---\nbody"
        frontmatter, body = _MOD._split_frontmatter(text)
        self.assertEqual(frontmatter["updated-at"], "2026-05-19T22:31:13Z")
        self.assertEqual(frontmatter["owner-container"], "simonfish/bfe8285d")
        self.assertEqual(body, "body")

    def test_a_bom_is_tolerated(self):
        text = "﻿---\ntrack-id: t\n---\n# H\n\n## §6 X\n\nbody\n"
        frontmatter, body = _MOD._split_frontmatter(text)
        self.assertEqual(frontmatter["track-id"], "t")
        self.assertIn("## §6 X", body)


# ════════════════════════════════════════════════════════════════════════
# end-to-end through the REAL IngestClient (scripted loopback backend)
# ════════════════════════════════════════════════════════════════════════

class _WriteCase(unittest.TestCase):
    """A throwaway project + a scripted loopback backend, driving the hook
    through its real IngestClient -- nothing about _ingest_client itself is
    mocked, only identity primitives that would otherwise shell out to git
    or read the real ~/.aria file."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        self.handoff_dir = os.path.join(self.cwd, "docs", "handoff")
        os.makedirs(self.handoff_dir)
        self.state_dir = os.path.join(self.tmp.name, "state")
        self.backend = _Backend()
        self.addCleanup(self.backend.close)
        # Persistent (not just scoped inside _run_main's own call): a test's
        # OWN post-run assertions (_last_entry, _failure_report) call
        # _hook_state directly, outside _run_main's transient env patch, and
        # must see the same NEXUS_HOOK_STATE_DIR the hook itself just wrote
        # under -- not whatever setUpModule's module-wide default is.
        self._patch(mock.patch.dict(os.environ, {"NEXUS_HOOK_STATE_DIR": self.state_dir}))
        self._patch(mock.patch.object(_identity, "project_root", return_value=(self.cwd, False)))
        self._patch(mock.patch.object(_identity, "container_id", return_value=CONTAINER))
        self._patch(mock.patch.object(_MOD, "_current_branch", return_value=BRANCH))
        aria_file = os.path.join(self.tmp.name, "container-id")
        with open(aria_file, "w", encoding="utf-8") as fh:
            fh.write(f"uuid: {LOCAL_UUID}\nlabel:\n")
        self._patch(mock.patch.object(_identity, "ARIA_CONTAINER_ID_FILE", aria_file))

    def _patch(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _write(self, name, **kw):
        kw.setdefault("owner", f"owner/{LOCAL_UUID}")
        return _write_handoff(self.handoff_dir, name, **kw)

    def _expected_metadata(self, path, session_id):
        """What the hook would send as its `metadata` argument for the
        document at `path`, computed the same way the hook itself does --
        so a fixture's stored `aria.*` values can be built without hand-
        duplicating them (and drifting from the implementation)."""
        with open(path, encoding="utf-8") as fh:
            frontmatter, _body = _MOD._split_frontmatter(fh.read())
        return _MOD._build_metadata(session_id, BRANCH, frontmatter, LOCAL_UUID)

    def _run(self, session_id="sess-1", **extra_env):
        env = {"NEXUS_API_URL": self.backend.url, "NEXUS_HOOK_STATE_DIR": self.state_dir,
               "NEXUS_DEFAULT_USER_ID": USER}
        env.update(extra_env)
        _run_main(_MOD, {"cwd": self.cwd, "session_id": session_id}, env)

    @property
    def requests(self):
        return self.backend.requests

    def _last_entry(self):
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertTrue(entries, "no ledger entry was written")
        return entries[-1]


class TestOwnerAndOptOut(_WriteCase):

    def test_owner_match_ingests(self):
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET", "POST"])
        self.assertEqual(self._last_entry()["reason"], "none")

    def test_a_different_owner_uuid_is_not_owner_with_zero_requests(self):
        self._write("2026-09-20-1000-x.md", owner=f"owner/{OTHER_UUID}")
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "not_owner")

    def test_a_missing_aria_file_is_identity_unresolved_with_zero_requests(self):
        self._write("2026-09-20-1000-x.md")
        with mock.patch.object(_identity, "ARIA_CONTAINER_ID_FILE", os.path.join(self.tmp.name, "no-such-file")):
            self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "identity_unresolved")

    def test_a_hostname_shaped_owner_container_is_identity_unresolved(self):
        self._write("2026-09-20-1000-x.md", owner="simonfish/dev-claude2")
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "identity_unresolved")

    def test_nexus_ingest_skip_is_opted_out_with_zero_requests(self):
        self._write("2026-09-20-1000-x.md", nexus_ingest="skip")
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "opted_out")

    def test_nexus_ingest_skip_is_checked_before_the_owner_check(self):
        """A document that is BOTH someone else's AND opted out reports the
        opt-out, not not_owner -- the digest's explicit ordering."""
        self._write("2026-09-20-1000-x.md", owner=f"owner/{OTHER_UUID}", nexus_ingest="skip")
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "opted_out")

    def test_a_pointer_resolved_candidate_missing_owner_container_falls_through(self):
        """A4-5 note: it does not silently skip -- it reaches the owner
        check, which reports identity_unresolved (there is no value to
        compare against at all), never not_owner."""
        self._write(
            "2026-09-20-1000-x.md",
            frontmatter_lines=[
                "---", "track-id: t1", "phase: B", "status: active",
                "updated-at: 2026-09-20T10:00:00Z", "---",
            ],
        )
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "identity_unresolved")


class TestPointerUnresolvedIsReported(_WriteCase):
    """Amendment A4-5's other direction: the failure-class leg must surface
    at the next SessionStart; the quiet leg must not."""

    def test_pointer_unresolved_is_reported_next_session_start(self):
        with open(os.path.join(self.handoff_dir, "a.md"), "w", encoding="utf-8") as fh:
            fh.write("---\nnot: a-known-key\n---\nbody\n")  # no parseable updated-at
        _write_latest_pointer(self.handoff_dir, "does-not-exist.md")
        self._run()
        self.assertEqual(self._last_entry()["reason"], "pointer_unresolved")
        findings, _marks = _INJECT_MOD._failure_report(self.cwd)
        self.assertTrue(any("handoff-sync" in f and "pointer_unresolved" in f for f in findings), findings)

    def test_no_handoff_is_not_reported_next_session_start(self):
        self._run()  # handoff_dir exists (created in setUp) but is empty
        self.assertEqual(self._last_entry()["reason"], "no_handoff")
        findings, _marks = _INJECT_MOD._failure_report(self.cwd)
        self.assertFalse(any("handoff-sync" in f for f in findings), findings)


class TestWritePath(_WriteCase):
    """Through the real IngestClient: every fixture is asserted on the wire."""

    def test_first_run_posts_with_the_full_metadata(self):
        self._write(
            "2026-09-20-1000-x.md", track_id="carry-x", phase="B", status="active",
            updated_at="2026-09-20T10:00:00Z",
        )
        self.backend.reply(200, _page()).reply(201, {"memory_id": "t1::proj::m1"})
        self._run(session_id="sess-1")
        self.assertEqual([r["method"] for r in self.requests], ["GET", "POST"])
        get = self.requests[0]
        self.assertEqual(get["path"], "/v1/memories")
        self.assertEqual(
            get["query"],
            {"user_id": USER, "layer": "session_summary", "container_id": CONTAINER,
             "external_id": "docs/handoff/2026-09-20-1000-x.md", "limit": "5"},
        )
        post = self.requests[1]["json"]
        self.assertEqual(post["user_id"], USER)
        self.assertTrue(post["content"].startswith("# Handoff Title"))
        meta = post["metadata"]
        self.assertEqual(meta["session_id"], "sess-1")
        self.assertEqual(meta["branch"], BRANCH)
        self.assertEqual(meta["aria.source"], "handoff")
        self.assertEqual(meta["aria.track_id"], "carry-x")
        self.assertEqual(meta["aria.owner_container"], LOCAL_UUID)
        self.assertEqual(meta["aria.phase"], "B")
        self.assertEqual(meta["aria.status"], "active")
        self.assertEqual(meta["aria.updated_at"], "2026-09-20T10:00:00Z")
        self.assertNotIn("aggregation_hash", meta)
        self.assertEqual(meta["layer"], "session_summary")
        self.assertEqual(meta["external_id"], "docs/handoff/2026-09-20-1000-x.md")
        self.assertEqual(meta["container_id"], CONTAINER)
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "none")
        self.assertTrue(entry["ok"])
        self.assertEqual(entry["action"], "created")

    def test_second_identical_run_is_unchanged_get_only(self):
        path = self._write("2026-09-20-1000-x.md", updated_at="2026-09-20T10:00:00Z")
        with open(path, encoding="utf-8") as fh:
            _fm, body = _MOD._split_frontmatter(fh.read())
        content, _reason = _MOD._build_content(body)
        digest = _ingest_client.content_hash(content)
        stored = {k: v for k, v in self._expected_metadata(path, "sess-1").items() if k.startswith("aria.")}
        self.backend.reply(200, _page(_row(
            "docs/handoff/2026-09-20-1000-x.md", content_hash=digest, extra=stored,
        )))
        self._run(session_id="sess-1")
        self.assertEqual([r["method"] for r in self.requests], ["GET"])
        self.assertEqual(self._last_entry()["reason"], "unchanged")

    def test_a_body_revision_patches_without_identity_keys(self):
        self._write("2026-09-20-1000-x.md", section6="## §6 Next session 入口\n\noriginal six")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        created_meta = self.requests[1]["json"]["metadata"]

        path = self._write(
            "2026-09-20-1000-x.md", section6="## §6 Next session 入口\n\nCHANGED six",
            updated_at="2026-09-20T11:00:00Z",
        )
        self.backend.reply(200, _page(_row(
            "docs/handoff/2026-09-20-1000-x.md", content_hash=created_meta["content_hash"],
            row_id="11111111-1111-4111-8111-111111111111",
            extra={k: v for k, v in created_meta.items() if k.startswith("aria.")},
        ))).reply(200, {"memory_id": "t1::proj::11111111-1111-4111-8111-111111111111"})
        self._run(session_id="sess-1")

        patch = self.requests[3]["json"]
        self.assertIn("CHANGED six", patch["content"])
        for key in _ingest_client.IDENTITY_KEYS:
            self.assertNotIn(key, patch["metadata"])
        self.assertEqual(self._last_entry()["action"], "updated")

    def test_a_metadata_only_revision_patches_without_content(self):
        """status active -> done, §6/§2 unchanged: PATCH must carry no
        `content` key at all (A8-1)."""
        self._write("2026-09-20-1000-x.md", status="active")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        created_meta = self.requests[1]["json"]["metadata"]

        self._write("2026-09-20-1000-x.md", status="done", updated_at="2026-09-20T11:00:00Z")
        self.backend.reply(200, _page(_row(
            "docs/handoff/2026-09-20-1000-x.md", content_hash=created_meta["content_hash"],
            row_id="22222222-1111-4111-8111-111111111111",
            extra={k: v for k, v in created_meta.items() if k.startswith("aria.")},
        ))).reply(200, {"memory_id": "m1"})
        self._run(session_id="sess-1")

        patch = self.requests[3]["json"]
        self.assertEqual(set(patch), {"metadata"})  # no `content`: the backend would re-embed for nothing
        self.assertEqual(patch["metadata"]["aria.status"], "done")
        self.assertEqual(self._last_entry()["action"], "updated")

    def test_an_older_local_updated_at_is_stale_local(self):
        self._write("2026-09-20-1000-x.md", updated_at="2026-09-20T10:00:00Z")
        self.backend.reply(200, _page(_row(
            "docs/handoff/2026-09-20-1000-x.md", content_hash="something-else",
            extra={"aria.updated_at": "2026-09-21T00:00:00Z"},
        )))
        self._run(session_id="sess-1")
        self.assertEqual([r["method"] for r in self.requests], ["GET"])
        self.assertEqual(self._last_entry()["reason"], "stale_local")

    def test_a_filter_suspect_page_writes_nothing(self):
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page(_row("some-unrelated-external-id", content_hash="x")))
        self._run(session_id="sess-1")
        self.assertEqual([r["method"] for r in self.requests], ["GET"])
        self.assertEqual(self._last_entry()["reason"], "filter_suspect")

    def test_two_duplicate_rows_dedup_with_the_reported_reason(self):
        path = self._write("2026-09-20-1000-x.md", updated_at="2026-09-20T10:00:00Z")
        with open(path, encoding="utf-8") as fh:
            _fm, body = _MOD._split_frontmatter(fh.read())
        content, _reason = _MOD._build_content(body)
        digest = _ingest_client.content_hash(content)
        stored = {k: v for k, v in self._expected_metadata(path, "sess-1").items() if k.startswith("aria.")}
        older = _row(
            "docs/handoff/2026-09-20-1000-x.md", content_hash=digest, extra=stored,
            created_at="2026-09-19T00:00:00.000001Z", row_id="aaaaaaaa-1111-4111-8111-111111111111",
        )
        newer = _row(
            "docs/handoff/2026-09-20-1000-x.md", content_hash=digest, extra=stored,
            created_at="2026-09-19T01:00:00.000001Z", row_id="bbbbbbbb-1111-4111-8111-111111111111",
        )
        self.backend.reply(200, _page(newer, older)).reply(204, None)
        self._run(session_id="sess-1")
        self.assertEqual([r["method"] for r in self.requests], ["GET", "DELETE"])
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "dedup_merged")  # A4-4: not `unchanged`
        self.assertEqual(entry["dedup_merged"], 1)
        findings, _marks = _INJECT_MOD._failure_report(self.cwd)
        self.assertTrue(any("handoff-sync" in f and "dedup_merged" in f for f in findings), findings)

    def test_403_structured_ingest_disabled(self):
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(
            403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "tenant off"}}
        )
        self._run(session_id="sess-1")
        self.assertEqual([r["method"] for r in self.requests], ["GET", "POST"])
        self.assertEqual(self._last_entry()["reason"], "ingest_disabled")

    def test_a_forged_userinfo_in_section_six_is_redacted_and_counted(self):
        secret_line = "See db: postgresql://nexus:s3cretpassword1@db-host:5432/nexus"
        self.assertEqual(len(_redact.find(secret_line)), 1)  # the fixture must actually be caught
        self._write("2026-09-20-1000-x.md", section6=f"## §6 Next session 入口\n\n{secret_line}")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        sent_content = self.requests[1]["json"]["content"]
        self.assertNotIn("s3cretpassword1", sent_content)
        self.assertIn("[redacted:url-userinfo]", sent_content)
        self.assertEqual(self._last_entry()["redacted"], 1)

    def test_session_id_missing_is_loud_and_writes_nothing(self):
        self._write("2026-09-20-1000-x.md")
        with mock.patch("sys.stderr"):
            self._run(session_id=None)
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "unknown")

    def test_identity_drift_reports_once(self):
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "none")

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "identity_changed")

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m3"})
            self._run(session_id="sess-1")
        self.assertNotEqual(self._last_entry()["reason"], "identity_changed")  # reported once, not every run


# ════════════════════════════════════════════════════════════════════════
# runs: real project roots (subdirectory / degraded), not_configured, timing
# ════════════════════════════════════════════════════════════════════════

class TestRuns(unittest.TestCase):
    """Uses a REAL git repository (unlike _WriteCase, which pins
    project_root): the point of these tests is _identity.project_root's own
    wiring into the hook, not the write protocol."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = os.path.join(self.tmp.name, "repo")
        os.makedirs(self.repo)
        subprocess.run(["git", "init", "-q", self.repo], check=True, capture_output=True)
        self.handoff_dir = os.path.join(self.repo, "docs", "handoff")
        os.makedirs(self.handoff_dir)
        self.state_dir = os.path.join(self.tmp.name, "state")
        self.backend = _Backend()
        self.addCleanup(self.backend.close)
        # Persistent, for the same reason as _WriteCase's identical patch:
        # this class's own post-run ledger assertions run outside
        # _run_main's transient env scope.
        self._patch(mock.patch.dict(os.environ, {"NEXUS_HOOK_STATE_DIR": self.state_dir}))
        aria_file = os.path.join(self.tmp.name, "container-id")
        with open(aria_file, "w", encoding="utf-8") as fh:
            fh.write(f"uuid: {LOCAL_UUID}\n")
        self._patch(mock.patch.object(_identity, "ARIA_CONTAINER_ID_FILE", aria_file))
        self._patch(mock.patch.object(_identity, "container_id", return_value=CONTAINER))
        self._patch(mock.patch.object(_MOD, "_current_branch", return_value=BRANCH))

    def _patch(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, cwd, backend=None, session_id="sess-1", **extra_env):
        env = {
            "NEXUS_API_URL": (backend or self.backend).url,
            "NEXUS_HOOK_STATE_DIR": self.state_dir,
            "NEXUS_DEFAULT_USER_ID": USER,
        }
        env.update(extra_env)
        _run_main(_MOD, {"cwd": cwd, "session_id": session_id}, env)

    def test_subdirectory_cwd_gives_the_same_external_id(self):
        _write_handoff(self.handoff_dir, "2026-09-20-1000-x.md", owner=f"owner/{LOCAL_UUID}")
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")

        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(self.repo)
        from_root = self.backend.requests[1]["json"]["metadata"]["external_id"]

        sub = os.path.join(self.repo, "some", "nested", "dir")
        os.makedirs(sub)
        backend2 = _Backend()
        self.addCleanup(backend2.close)
        backend2.reply(200, _page()).reply(201, {"memory_id": "m2"})
        self._run(sub, backend=backend2, session_id="sess-2")
        from_sub = backend2.requests[1]["json"]["metadata"]["external_id"]

        self.assertEqual(from_root, "docs/handoff/2026-09-20-1000-x.md")
        self.assertEqual(from_root, from_sub)

    def test_degraded_identity_refuses_to_write(self):
        _write_handoff(self.handoff_dir, "2026-09-20-1000-x.md", owner=f"owner/{LOCAL_UUID}")
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")
        with mock.patch.object(_identity, "project_root", return_value=(self.repo, True)):
            self._run(self.repo)
        self.assertEqual(self.backend.requests, [])
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.repo)
        self.assertEqual(entries[-1]["reason"], "identity_unresolved")

    def test_not_configured_makes_no_request_when_no_api_url(self):
        _run_main(_MOD, {"cwd": self.repo, "session_id": "s"}, {"NEXUS_HOOK_STATE_DIR": self.state_dir})
        self.assertEqual(self.backend.requests, [])
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.repo)
        self.assertEqual(entries[-1]["reason"], "not_configured")

    def test_elapsed_ms_is_recorded(self):
        _run_main(_MOD, {"cwd": self.repo, "session_id": "s"}, {"NEXUS_HOOK_STATE_DIR": self.state_dir})
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.repo)
        self.assertIs(type(entries[-1]["elapsed_ms"]), int)
        self.assertGreaterEqual(entries[-1]["elapsed_ms"], 0)


class TestSubprocess(unittest.TestCase):
    """The real script, as a subprocess, against a HOME/state dir of its own."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state_dir = os.path.join(self.tmp.name, "state")

    def test_a_real_run_exits_zero_and_writes_a_ledger(self):
        stdout, code = _run_hook(
            json.dumps({"cwd": self.cwd}), env={"NEXUS_HOOK_STATE_DIR": self.state_dir}
        )
        self.assertEqual((stdout, code), (b"", 0))
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "handoff-sync.json"))
        self.assertEqual(len(ledgers), 1, ledgers)
        with open(ledgers[0], encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)[-1]["reason"], "not_configured")

    def test_garbage_stdin_exits_zero_and_records_a_failure(self):
        stdout, code = _run_hook("{not json", env={"NEXUS_HOOK_STATE_DIR": self.state_dir})
        self.assertEqual((stdout, code), (b"", 0))
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "handoff-sync.json"))
        self.assertEqual(len(ledgers), 1, ledgers)
        with open(ledgers[0], encoding="utf-8") as fh:
            entry = json.load(fh)[-1]
        self.assertEqual(entry["reason"], "unknown")
        self.assertFalse(entry["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
