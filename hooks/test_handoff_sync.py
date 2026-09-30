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


class _BrokenStderr:
    """A stand-in for ``sys.stderr`` whose ``write`` always raises
    ``BrokenPipeError`` -- what a real closed pipe (the host process has
    already exited) looks like to a ``print(..., file=sys.stderr)`` call
    (R2-c05)."""

    def write(self, *args, **kwargs):
        raise BrokenPipeError("stderr closed")

    def flush(self):
        raise BrokenPipeError("stderr closed")


def _run_hook(stdin_text, env=None, want_stderr=False, script=None):
    """Drive the real script as a subprocess; return (stdout_bytes, exit_code).

    ``script`` (R1-c31) overrides which file is run -- a partial-install
    copy in a temp dir, for the import-guard tests, instead of the real
    ``_HOOK_SCRIPT``.
    """
    run_env = {k: v for k, v in os.environ.items() if not k.startswith("NEXUS_")}
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

    def test_a_lowercase_readme_is_excluded_case_insensitively(self):
        """Ruling 6 (TASK-005 R1 fix round, R1-c12): the exclusion of
        latest.md / README.md must not be a literal-case match -- a project
        that only keeps a lowercase readme.md is the same "no handoff kept
        here" shape as one with README.md, not a broken one reported every
        session.

        The latest.md leg uses ``LATEST.md`` (uppercase NAME, lowercase
        extension) rather than ``Latest.MD`` (R2-c14): the candidate filter
        first requires ``name.endswith(".md")``, which is itself a literal,
        case-SENSITIVE match -- ``Latest.MD`` fails that check before the
        name-exclusion comparison this test means to exercise is ever
        reached, so it would report ``no_handoff`` even if the exclusion's
        own case-insensitivity regressed to a literal match."""
        self._mkdir()
        with open(os.path.join(self.handoff_dir, "readme.md"), "w", encoding="utf-8") as fh:
            fh.write("# About this directory\n")
        with open(os.path.join(self.handoff_dir, "LATEST.md"), "w", encoding="utf-8") as fh:
            fh.write("nothing useful\n")
        self.assertEqual(_MOD._locate(self.handoff_dir), (None, "no_handoff"))

    def test_pointer_target_with_a_directory_prefix_resolves_via_basename(self):
        """R1-c28: the Aria collector pattern's target group is normalised
        to its basename, so a pointer written as a relative PATH (not just a
        bare filename) still resolves. A SECOND, newer candidate is present
        specifically so a basename-normalisation regression is visible: with
        only one candidate, a failure to normalise would still "work" by
        accident, via the newest-updated-at FALLBACK selecting the same file
        the pointer meant to name -- indistinguishable from the pointer
        actually being followed (caught by mutation: dropping
        `os.path.basename` here left this test green until this fixture
        gained a second, newer file)."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")  # newer, not pointed to
        with open(os.path.join(self.handoff_dir, "latest.md"), "w", encoding="utf-8") as fh:
            fh.write("# Latest\n\n**Latest**: [a.md](docs/handoff/a.md)\n")
        self.assertEqual(_MOD._locate(self.handoff_dir), ("a.md", None))

    def test_an_unreadable_directory_is_pointer_unresolved_not_quiet(self):
        """Ruling 5 (TASK-005 R1 fix round, R1-c06): a docs/handoff directory
        that exists but could not be LISTED (permission denied here) is not
        the same as one that does not exist -- "cannot tell" must surface as
        the existing failure-class reason, with a stderr line naming the
        error, rather than the quiet no_handoff most projects hit."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md")
        os.chmod(self.handoff_dir, 0o000)
        self.addCleanup(os.chmod, self.handoff_dir, 0o755)  # so TemporaryDirectory cleanup can remove it
        try:
            os.listdir(self.handoff_dir)
        except PermissionError:
            pass
        else:
            self.skipTest("running as a user unaffected by chmod 000 (e.g. root)")
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("docs/handoff", stderr.getvalue())

    def test_an_unreadable_directory_is_pointer_unresolved_even_as_root(self):
        """R2-c09: the chmod-000 test above SKIPS when the suite runs as
        root (root ignores the permission bits, so `os.listdir` succeeds
        anyway) -- and this plugin's CI image declares no non-root `user:`,
        so that test never actually RUNS there. Injecting the error via
        mock exercises the exact same `_locate` branch regardless of who
        is running the suite; the chmod version is kept alongside it as a
        real-shape control for when the suite does run unprivileged."""
        self._mkdir()
        stderr = io.StringIO()
        with mock.patch.object(_MOD.os, "listdir", side_effect=PermissionError(13, "denied")), \
                mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("docs/handoff", stderr.getvalue())

    def test_an_unstattable_candidate_entry_is_pointer_unresolved_not_quiet(self):
        """R2-c03: the per-entry filter used to call `os.path.isfile()`,
        which CATCHES OSError internally and returns False -- so a listed
        name that could not be STATTED (typically the directory itself
        lacking the execute bit needed to traverse into it, the usual
        result of a recursive `chmod 644` over a whole `docs/` tree) read
        exactly like "not a regular file", silently dropping every
        candidate and landing on the quiet no_handoff most projects hit,
        rather than the loud "cannot tell" ruling 5 requires. `os.stat`
        (not `isfile`) is mocked here so the injection is independent of
        which underlying primitive the fix ends up calling being isfile-
        shaped or stat-shaped, as long as SOME per-entry OSError surfaces."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md")
        stderr = io.StringIO()
        with mock.patch.object(_MOD.os, "stat", side_effect=PermissionError(13, "denied")), \
                mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("docs/handoff", stderr.getvalue())

    def test_a_dangling_symlink_handoff_dir_is_pointer_unresolved_not_quiet(self):
        """R2-c03, second leg: `docs/handoff` ITSELF being a dangling
        symlink (a broken mount, an unlinked shared volume) makes
        `os.listdir` raise `FileNotFoundError` -- indistinguishable, by
        exception type alone, from "the directory does not exist" at all.
        But something WAS configured here; that is "cannot tell", not the
        ordinary quiet case."""
        os.makedirs(os.path.dirname(self.handoff_dir), exist_ok=True)  # docs/, NOT docs/handoff itself
        os.symlink(os.path.join(self.tmp.name, "does-not-exist"), self.handoff_dir)
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))

    def test_pointer_unresolved_records_a_detail_for_an_unlistable_directory(self):
        """R2-c04: the two `pointer_unresolved` origins (a directory that
        could not be listed at all, vs. one whose candidates all failed to
        resolve) used to be indistinguishable on the ledger -- the only
        channel a user can actually inspect; stderr from a SessionEnd hook
        is not read by anyone. `extra`, when given, must end up with a
        `detail` naming which origin this was."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md")
        extra = {}
        with mock.patch.object(_MOD, "_candidates", side_effect=OSError("denied")), \
                mock.patch.object(sys, "stderr", io.StringIO()):
            result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("could not be listed", extra["detail"])

    def test_pointer_unresolved_records_a_detail_when_no_candidate_resolves(self):
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", frontmatter_lines=["---", "not: a-known-key", "---"])
        _write_latest_pointer(self.handoff_dir, "does-not-exist.md")
        extra = {}
        result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("candidate", extra["detail"])

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
        # R2-c12: every other assertion here is an ordering/substring check
        # that would still pass if `_join` separated parts with a single
        # newline instead of a blank line -- pin the exact separator too, so
        # adjacent sections do not visually run together in the rendered
        # Markdown.
        self.assertIn("# My Title\n\n## §6 Next session 入口", content)
        self.assertIn("still six\n\n## §2 未完成", content)

    def test_section_sixty_is_not_mistaken_for_section_six(self):
        body = "# T\n\n## §60 Something Else\n\nnope\n\n## §6 Next session\n\nreal six\n"
        content, reason = _MOD._build_content(body)
        self.assertIsNone(reason)
        self.assertIn("real six", content)
        self.assertNotIn("Something Else", content)

    def test_cap_cuts_section_two_first(self):
        """R1-c24: strengthened past "does not contain the tail" (a mutant
        that drops §2 wholesale once it does not fit whole would still pass
        THAT alone) to also require §2's own EARLIEST lines survive -- a
        truncated carry-forward list must lose its tail, not its entirety
        or its head. Realistic multi-line content (not one 3000-char line):
        R1-c02/c11 changed `_cap` to retreat to a line boundary rather than
        cut mid-line, and a single giant unbroken line has no boundary to
        retreat to short of dropping the whole thing."""
        section6 = "## §6 Next session\n\n" + "\n".join(
            f"six line {i} " + "A" * 40 for i in range(60)
        )
        section2 = "## §2 Carry\n\n" + "\n".join(
            f"two line {i} " + "B" * 40 for i in range(80)
        )
        content, reason = _MOD._build_content(f"# T\n\n{section6}\n\n{section2}\n")
        self.assertIsNone(reason)
        self.assertLessEqual(len(content), _MOD._CONTENT_CAP)
        self.assertIn("six line 0 " + "A" * 40, content)    # section 6 survives whole...
        self.assertIn("six line 59 " + "A" * 40, content)   # ...every line of it
        self.assertIn("## §2 Carry", content)                # section 2's heading survives
        self.assertIn("two line 0 " + "B" * 40, content)     # ...and its earliest lines
        self.assertNotIn("two line 79 " + "B" * 40, content)  # but not its tail
        self.assertIn("truncated", content)

    def test_cap_cuts_section_six_too_when_h1_and_six_alone_exceed_it(self):
        section6 = "## §6 Next session\n\n" + "\n".join(
            f"six line {i} " + "A" * 40 for i in range(120)
        )
        section2 = "## §2 Carry\n\n" + "\n".join(
            f"two line {i} " + "B" * 40 for i in range(10)
        )
        content, reason = _MOD._build_content(f"# T\n\n{section6}\n\n{section2}\n")
        self.assertIsNone(reason)
        self.assertLessEqual(len(content), _MOD._CONTENT_CAP)
        self.assertIn("six line 0 " + "A" * 40, content)     # section 6's own earliest lines survive
        self.assertNotIn("two line 0 " + "B" * 40, content)  # section 2 dropped entirely
        self.assertIn("truncated", content)

    def test_cap_holds_when_h1_alone_meets_the_cap(self):
        """Ruling 9 (TASK-005 R1 fix round, R1-c11): an H1 line long enough
        by itself to reach _CONTENT_CAP used to come back verbatim,
        uncapped -- there was nothing left in the section6 budget to even
        fit the truncation marker."""
        h1 = "# " + ("T" * (_MOD._CONTENT_CAP + 200))
        section6 = "## §6 Next session\n\nsix body"
        section2 = "## §2 Carry\n\ntwo body"
        content, reason = _MOD._build_content(f"{h1}\n\n{section6}\n\n{section2}\n")
        self.assertIsNone(reason)
        self.assertLessEqual(len(content), _MOD._CONTENT_CAP)
        self.assertIn("truncated", content)

    def test_cap_never_returns_a_partial_truncation_marker(self):
        """R2-c07: a remaining budget of 1..13 chars (less than the marker's
        OWN length) used to return a slice of the marker itself, e.g.
        "\\n\\n…[t" -- an unreadable fragment that looks like real content
        got cut mid-marker, not a deliberate truncation notice. Too little
        room for even the complete marker must come back empty; exactly
        enough room (14, the marker's own length) must come back as the
        whole marker, uncut."""
        long_text = "x" * 100
        for limit in range(1, len(_MOD._TRUNCATION_MARKER)):
            with self.subTest(limit=limit):
                result = _MOD._cap(long_text, limit)
                self.assertEqual(result, "", f"limit={limit} produced a fragment: {result!r}")
        self.assertEqual(_MOD._cap(long_text, len(_MOD._TRUNCATION_MARKER)), _MOD._TRUNCATION_MARKER)

    def test_cap_never_leaves_a_partial_truncation_marker_in_assembled_content(self):
        """R2-c07, end to end: engineer H1 + section6 so section2's
        remaining budget lands at exactly 7 (inside the dangerous 1..13
        band) and confirm the assembled content carries no marker
        fragment -- section2 is dropped whole instead."""
        h1 = "# T"
        section6 = "A" * (_MOD._CONTENT_CAP - 9 - len(h1) - len("\n\n"))
        section2 = "B" * 500
        content = _MOD._assemble_content(h1, section6, section2)
        self.assertLessEqual(len(content), _MOD._CONTENT_CAP)
        self.assertNotIn("…[", content)  # no partial marker fragment anywhere
        self.assertNotIn("B", content)  # section2 dropped whole, not partially

    def test_a_credential_spanning_the_cut_point_does_not_reach_the_wire(self):
        """R1-c02/c11: the OLD hard mid-line cut could land inside a value
        _redact would otherwise catch whole -- the fragment before the cut
        then matches no rule at all. The padding count below (14 lines
        before the secret line) is not arbitrary: computed once, offline,
        from the OLD `_cap`'s own cut arithmetic for this exact fixture, it
        is the count that puts the OLD hard-cut boundary strictly inside
        the userinfo password -- confirmed by reproducing this test against
        the pre-fix `_cap` (see the R1 fix round's verification), where it
        left a live fragment of the secret on the wire. The assertion below
        does not hardcode that mechanism, only the OUTCOME any correct `_cap`
        must uphold: the credential is either intact (so _ingest_client's
        redaction can catch it whole) or entirely absent, never a partial
        fragment on either side of the truncation marker.
        """
        secret = "hunter2-genuinely-secret-value-0123456789"
        padding_line = lambda i: f"two line {i} " + "B" * 40  # noqa: E731 - local, single use
        before = [padding_line(i) for i in range(14)]  # see docstring: lands the OLD cut mid-secret
        after = [padding_line(i) for i in range(14, 24)]
        secret_line = f"See db: postgresql://nexus:{secret}@db-host:5432/nexus"
        section2 = "## §2 Carry\n\n" + "\n".join(before + [secret_line] + after)
        section6 = "## §6 Next session\n\n" + "\n".join(
            f"six line {i} " + "A" * 40 for i in range(60)
        )
        content, reason = _MOD._build_content(f"# T\n\n{section6}\n\n{section2}\n")
        self.assertIsNone(reason)
        self.assertLessEqual(len(content), _MOD._CONTENT_CAP)
        if secret_line in content:
            self.assertEqual(len(_redact.find(secret_line)), 1)  # whole line: redaction can still catch it
        else:
            # Dropped -- but it must be dropped WHOLE. A fragment of the
            # secret with no closing delimiter for _redact to match against
            # is exactly what the OLD hard mid-line cut used to leave on
            # the wire (confirmed against the pre-fix `_cap`).
            self.assertNotIn(secret[:15], content)

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
        text = "\ufeff---\ntrack-id: t\n---\n# H\n\n## §6 X\n\nbody\n"
        frontmatter, body = _MOD._split_frontmatter(text)
        self.assertEqual(frontmatter["track-id"], "t")
        self.assertIn("## §6 X", body)

    def test_the_bom_constant_is_the_real_codepoint(self):
        """R1-c32: pins the escape to the codepoint it must decode to, so a
        tool that silently strips/mangles it again is caught here even if
        the source diff itself looks fine."""
        self.assertEqual(_MOD._BOM, "\ufeff")
        self.assertEqual(ord(_MOD._BOM), 0xFEFF)

    def test_quoted_frontmatter_values_are_unquoted(self):
        """R1-c28: one level of matching quotes is stripped from a value."""
        text = "---\nowner-container: \"simonfish/bfe8285d\"\nstatus: 'active'\n---\nbody"
        frontmatter, _body = _MOD._split_frontmatter(text)
        self.assertEqual(frontmatter["owner-container"], "simonfish/bfe8285d")
        self.assertEqual(frontmatter["status"], "active")

    def test_frontmatter_must_start_on_the_very_first_line(self):
        """R1-c28: a leading blank line before the opening ``---`` means the
        document has no frontmatter at all -- a legacy handoff predating the
        convention, not a malformed one."""
        text = "\n---\ntrack-id: t\n---\nbody"
        frontmatter, body = _MOD._split_frontmatter(text)
        self.assertEqual(frontmatter, {})
        self.assertEqual(body, text)

    def test_the_200_char_floor_between_empty_and_unparsed(self):
        """R1-c29: the digest's ``>= _MIN_NONTRIVIAL_BODY`` split, pinned
        exactly at the boundary so a `>` vs `>=` typo would be caught."""
        at_floor = "x" * _MOD._MIN_NONTRIVIAL_BODY
        self.assertEqual(len(at_floor.strip()), _MOD._MIN_NONTRIVIAL_BODY)
        self.assertEqual(_MOD._build_content(at_floor), (None, "sections_unparsed"))
        just_under = "x" * (_MOD._MIN_NONTRIVIAL_BODY - 1)
        self.assertEqual(_MOD._build_content(just_under), (None, "empty_sections"))

    def test_a_heading_only_section_six_is_empty(self):
        """Ruling 3 (TASK-005 R1 fix round, R1-c01): a section heading with
        nothing but blank lines under it is not content -- a freshly created
        template (only heading-only §6/§2) must land on empty_sections, a
        skip, exactly like a wholly blank body."""
        body = "# T\n\n## §6 Next session 入口 + 优先级建议\n\n## §2 未完成 / Carry-forward 清单\n"
        self.assertEqual(_MOD._build_content(body), (None, "empty_sections"))

    def test_a_heading_only_section_six_on_a_long_body_is_sections_unparsed(self):
        """The same heading-only emptiness, but with enough OTHER prose in
        the body to cross _MIN_NONTRIVIAL_BODY: a broken/renamed template,
        not a quiet empty one -- must be reported, not skipped."""
        filler = "prose that is not under either known heading. " * 6
        body = f"# T\n\n{filler}\n\n## §6 Next session\n\n## §2 unfinished\n\n"
        self.assertGreaterEqual(len(body.strip()), _MOD._MIN_NONTRIVIAL_BODY)
        self.assertEqual(_MOD._build_content(body), (None, "sections_unparsed"))

    def test_a_subsection_line_under_section_six_still_counts_as_content(self):
        """The presence check (ruling 3) must not regress `test_order_is_h1_
        then_section_six_then_section_two`'s subsection sweep-up: a bare
        `### §6.1` heading line under §6, with nothing else, is still a
        non-blank line and must keep the section present."""
        body = "# T\n\n## §6 Next session\n\n### §6.1 a subsection\n\n## §2 Carry\n\ntwo body\n"
        content, reason = _MOD._build_content(body)
        self.assertIsNone(reason)
        self.assertIn("§6.1", content)


class TestOptOut(unittest.TestCase):
    """R1-c05: `nexus-ingest: skip` recognised past exact-lowercase-match."""

    def test_exact_lowercase_skip_opts_out(self):
        self.assertTrue(_MOD._opted_out({"nexus-ingest": "skip"}))

    def test_mixed_and_upper_case_skip_opts_out(self):
        self.assertTrue(_MOD._opted_out({"nexus-ingest": "Skip"}))
        self.assertTrue(_MOD._opted_out({"nexus-ingest": "SKIP"}))

    def test_an_inline_comment_after_skip_still_opts_out(self):
        self.assertTrue(_MOD._opted_out({"nexus-ingest": "skip  # has a secret in §6"}))
        self.assertTrue(_MOD._opted_out({"nexus-ingest": "skip # reason"}))

    def test_missing_key_does_not_opt_out(self):
        self.assertFalse(_MOD._opted_out({}))

    def test_unrecognised_values_do_not_opt_out(self):
        """false/no/off are left ingesting -- an explicitly OPEN question
        (Amendment A9), not a guess made by this fix."""
        for value in ("false", "no", "off", "true", "none"):
            with self.subTest(value=value):
                self.assertFalse(_MOD._opted_out({"nexus-ingest": value}))

    def test_a_quoted_value_with_an_inline_comment_still_opts_out(self):
        """R2-c02: _split_frontmatter only strips a matching pair of quotes
        when they sit at the very start AND end of the WHOLE value -- a
        trailing inline comment means the closing quote is no longer the
        last character, so a quoted `"skip"  # reason` / `'skip' # reason`
        value still carries its quotes by the time it reaches here. Both
        quote styles must still be recognised once the comment is gone."""
        self.assertTrue(_MOD._opted_out({"nexus-ingest": '"skip"  # leaked a token'}))
        self.assertTrue(_MOD._opted_out({"nexus-ingest": "'skip' # leaked a token"}))

    def test_a_value_merely_containing_skip_does_not_opt_out(self):
        """R2-c15: guards the exact-match comparison against a future
        widening to a substring check (e.g. `"skip" in value`), which would
        silently opt out documents whose value merely mentions "skip"."""
        for value in ("skipped", "no-skip-please", "unskippable"):
            with self.subTest(value=value):
                self.assertFalse(_MOD._opted_out({"nexus-ingest": value}))


class TestMetadata(unittest.TestCase):
    """_build_metadata: aria.* keys and `branch` are OMITTED (not sent as an
    explicit null) when the source value is absent -- a PATCH is a shallow
    merge, and an explicit null would clobber a value already on the server
    (R1-c25: these negative branches had no fixture at all)."""

    def test_branch_is_omitted_when_none(self):
        meta = _MOD._build_metadata("s1", None, {}, "aaaaaaaa")
        self.assertNotIn("branch", meta)

    def test_branch_is_present_when_known(self):
        meta = _MOD._build_metadata("s1", "main", {}, "aaaaaaaa")
        self.assertEqual(meta["branch"], "main")

    def test_each_aria_key_is_omitted_when_its_frontmatter_value_is_absent(self):
        meta = _MOD._build_metadata("s1", "main", {}, "aaaaaaaa")
        for key in ("aria.track_id", "aria.phase", "aria.status", "aria.updated_at"):
            self.assertNotIn(key, meta)
        # aria.owner_container is keyed off the uuid ARGUMENT, not frontmatter
        # (see the function's own docstring) -- its own omission case is
        # exercised separately below.
        self.assertEqual(meta["aria.owner_container"], "aaaaaaaa")

    def test_owner_container_is_omitted_when_the_uuid_argument_is_falsy(self):
        meta = _MOD._build_metadata("s1", "main", {}, None)
        self.assertNotIn("aria.owner_container", meta)

    def test_each_aria_key_is_present_when_its_frontmatter_value_is_given(self):
        frontmatter = {
            "track-id": "t1", "phase": "B", "status": "active",
            "updated-at": "2026-09-20T10:00:00Z",
        }
        meta = _MOD._build_metadata("s1", "main", frontmatter, "aaaaaaaa")
        self.assertEqual(meta["aria.track_id"], "t1")
        self.assertEqual(meta["aria.phase"], "B")
        self.assertEqual(meta["aria.status"], "active")
        self.assertEqual(meta["aria.updated_at"], "2026-09-20T10:00:00Z")


class TestPointerTargetReadSafety(_HandoffDirCase):
    """R1-c13: latest.md is checked with os.path.isfile before it is
    opened, and its read is capped -- a FIFO or a huge file must not block
    the hook or exhaust memory just to find one pointer line."""

    def test_a_non_regular_latest_md_reads_as_no_pointer(self):
        """R2-c13: ``_pointer_target`` runs on a watchdog thread of THIS
        test's own, with its own 1 s bound (the ``test_hook_runner.py``
        pattern, ``test_a_budget_that_stopped_working_fails_fast_not_
        slow``) rather than being called directly on the main test
        thread. ``unittest`` has NO per-test default timeout -- an
        earlier revision of this docstring claimed there was one -- so if
        the ``os.path.isfile`` guard this test means to pin ever
        regressed, ``open()`` on a FIFO with no writer blocks forever,
        and calling it directly here would hang this test, and the whole
        suite behind it, rather than failing fast."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        fifo_path = os.path.join(self.handoff_dir, "latest.md")
        os.mkfifo(fifo_path)  # a directory would also fail isfile(); a FIFO is the risk this guards
        self.addCleanup(os.remove, fifo_path)

        result = {}

        def call_it():
            result["pointer"] = _MOD._pointer_target(self.handoff_dir)

        watchdog = threading.Thread(target=call_it, daemon=True)
        watchdog.start()
        watchdog.join(1.0)
        self.assertFalse(
            watchdog.is_alive(), "_pointer_target did not return within 1s (FIFO open() blocked?)"
        )
        # Reads as "no pointer" (falls back to newest updated-at), NOT a
        # blocking open().
        self.assertIsNone(result.get("pointer"))
        self.assertEqual(_MOD._locate(self.handoff_dir), ("a.md", None))


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

    def test_nexus_ingest_quoted_skip_with_inline_comment_is_opted_out(self):
        """R2-c02, end to end: the recovery path in memory-layers.md §3.2
        writes `nexus-ingest: "skip"  # <reason>` (quoted, commented) after
        a server-side row was soft-deleted; the opt-out must still be
        honoured, or the row gets recreated on the very next SessionEnd."""
        self._write("2026-09-20-1000-x.md", nexus_ingest='"skip"  # leaked a token in §6')
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


class TestChosenDocumentReadCap(_WriteCase):
    """R2-c18: R1-c13 added ``_MAX_DOCUMENT_BYTES`` for TWO reads -- the
    ``latest.md`` pointer probe (``_pointer_target``, fixture in
    ``TestPointerTargetReadSafety``) and the CHOSEN document's own read
    inside ``_collect`` (``fh.read(_MAX_DOCUMENT_BYTES)``) -- but only the
    first of the two had a fixture. An abnormally huge candidate would
    otherwise be read into memory in full before anything here gets a
    chance to cap it."""

    def test_the_chosen_documents_read_is_bounded(self):
        # Patched well ABOVE the frontmatter block (~114 chars for this
        # fixture's defaults) so parsing still succeeds, but well BELOW
        # where the marker sits -- and the marker itself sits well below
        # `_CONTENT_CAP` (4000), so if the read cap were the only thing
        # missing, nothing downstream would ALSO have trimmed it away.
        marker = "MARKERBEYONDCAP"
        section6 = "## §6 Next session 入口 + 优先级建议\n\n" + ("A" * 300) + marker
        with mock.patch.object(_MOD, "_MAX_DOCUMENT_BYTES", 200):
            self._write("2026-09-20-1000-x.md", section6=section6)
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
            self._run()
        sent = self.requests[1]["json"]["content"]
        self.assertLess(len(sent), _MOD._CONTENT_CAP)  # nowhere near the OTHER cap
        self.assertNotIn(marker, sent)


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


class TestContentReasonsEndToEnd(_WriteCase):
    """R2-c10: the content-level split (ruling 3 / R1-c01) -- empty_sections
    quiet, sections_unparsed a failure reported at the next SessionStart --
    was previously exercised only at the pure-function level
    (``_build_content`` in ``TestContent``). This drives it through a real
    file, ``main()``, and the ledger: the missing link was ``_collect``'s
    own wiring from ``_build_content``'s ``content_reason`` to the run's
    ``reason`` (:content_reason: return content_reason` a few lines below
    the call), not the pure function itself."""

    def test_an_empty_handoff_body_is_quiet(self):
        self._write("2026-09-20-1000-x.md", body="   \n\n  ")
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "empty_sections")
        findings, _marks = _INJECT_MOD._failure_report(self.cwd)
        self.assertFalse(any("handoff-sync" in f for f in findings), findings)

    def test_renamed_headings_on_a_real_file_is_reported_next_session_start(self):
        body = "# Title\n\n" + ("prose with no known section headings at all. " * 10)
        self.assertGreaterEqual(len(body.strip()), _MOD._MIN_NONTRIVIAL_BODY)
        self._write("2026-09-20-1000-x.md", body=body)
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "sections_unparsed")
        findings, _marks = _INJECT_MOD._failure_report(self.cwd)
        self.assertTrue(
            any("handoff-sync" in f and "sections_unparsed" in f for f in findings), findings
        )


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

    def test_redaction_can_grow_content_past_the_4000_char_cap(self):
        """R2-c08: `_build_content` caps at `_CONTENT_CAP` BEFORE
        `_ingest_client` redacts -- and a redaction marker is LONGER than a
        short secret it replaces (a 4-char URL password becomes the
        23-char "[redacted:url-userinfo]" marker, +19 net per hit).
        Content this hook built at EXACTLY the cap, honouring its own
        invariant, can still leave the process longer than the cap by the
        time it reaches the wire -- `filler` below was sized (empirically,
        against the current H1/section2 defaults) so the built content
        lands exactly at `_CONTENT_CAP` with the secret intact."""
        secret_line = "See db: postgresql://nexus:abcd@db-host:5432/nexus"
        self.assertEqual(len(_redact.find(secret_line)), 1)  # the fixture must actually be caught
        filler = "A" * 3869
        section6 = "## §6 Next session 入口\n\n" + filler + "\n" + secret_line
        path = self._write("2026-09-20-1000-x.md", section6=section6)

        with open(path, encoding="utf-8") as fh:
            _fm, body = _MOD._split_frontmatter(fh.read())
        content, reason = _MOD._build_content(body)
        self.assertIsNone(reason)
        self.assertEqual(len(content), _MOD._CONTENT_CAP)  # the hook's own cap invariant holds
        self.assertIn(secret_line, content)  # intact, uncut -- not a cap-boundary artefact

        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        sent_content = self.requests[1]["json"]["content"]
        self.assertLessEqual(len(sent_content), _MOD._CONTENT_CAP)  # the WIRE content must not exceed it either

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
        self.assertNotIn("identity_changed", self._last_entry())  # no drift this run: key absent

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            self._run(session_id="sess-1")
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "identity_changed")
        self.assertTrue(entry["identity_changed"])
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-b")  # a clean write: persisted

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m3"})
            self._run(session_id="sess-1")
        self.assertNotEqual(self._last_entry()["reason"], "identity_changed")  # reported once, not every run

    def _drift_then_fail(self, act):
        """Shared setup for the three ruling-2 (R1-c04) tests below: a clean
        first run under CONTAINER, then ``act()`` -- which must patch
        container_id to something else itself, queue whatever backend
        response(s) it needs, and drive the second (failing) run. Returns
        that second run's ledger entry. The container_id / deadline patches
        live inside ``act`` rather than around this whole method so they
        never touch the FIRST (deliberately clean) run."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "none")
        act()
        return self._last_entry()

    def test_identity_drift_with_a_500_does_not_persist_the_new_id(self):
        """Ruling 2 (TASK-005 R1 fix round, R1-c04): http_error aborts the
        round, so the drift must stay unresolved for the NEXT run too --
        and, since http_error outranks identity_changed in worst_reason,
        the drift signal must still show up in the ledger `extra`."""

        def act():
            with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
                self.backend.reply(200, _page()).reply(500, {"detail": "boom"})
                self._run(session_id="sess-1")

        entry = self._drift_then_fail(act)
        self.assertEqual(entry["reason"], "http_error")
        self.assertTrue(entry["identity_changed"])
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), CONTAINER)  # NOT persisted

        # Next run, still dev-box-b, backend healthy: the drift must still
        # be detected -- it was never actually recorded as resolved.
        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m3"})
            self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "identity_changed")

    def test_identity_drift_with_403_ingest_disabled_does_not_persist_the_new_id(self):
        def act():
            with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
                self.backend.reply(200, _page()).reply(
                    403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "tenant off"}}
                )
                self._run(session_id="sess-1")

        entry = self._drift_then_fail(act)
        self.assertEqual(entry["reason"], "ingest_disabled")
        self.assertTrue(entry["identity_changed"])
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), CONTAINER)

    def test_identity_drift_with_a_client_timeout_does_not_persist_the_new_id(self):
        """The client's OWN deadline refusal (no request even sent), not the
        outer work-budget abandonment -- see test_a_deadline_already_
        exhausted_refuses_without_a_request for that half of R1-c26."""

        def act():
            with mock.patch.object(_identity, "container_id", return_value="dev-box-b"), \
                    mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 1_000_000.0):
                self._run(session_id="sess-1")  # deadline already exhausted: no reply need be queued

        entry = self._drift_then_fail(act)
        self.assertEqual(entry["reason"], "timeout")
        self.assertTrue(entry["identity_changed"])
        self.assertEqual(len(self.requests), 2)  # only the first (successful) run's GET+POST
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), CONTAINER)

    def test_identity_drift_row_survives_a_persist_that_stalls_past_the_ledger_budget(self):
        """R2-c01: the row for THIS run must exist independently of how long
        the container_id persist (which follows it) takes. The earlier
        ordering wrote the persist FIRST, inside the same budgeted write();
        a persist slow/contended enough to eat the whole ledger budget could
        commit the new container_id to disk and then run out of budget
        before record_run ever ran at all -- silently and PERMANENTLY
        losing identity_changed (the next run's drift check would then find
        the state already at the new id and detect nothing). Reordering
        alone fixes this: record_run must complete before the persist
        attempt even starts.

        The persist step is stalled here (not the ledger write itself) via
        a released Event, the same pattern as
        test_calls_is_recorded_as_unknown_not_zero_when_the_work_is_abandoned
        above -- a real subprocess kill is exercised separately by
        TestSubprocess; what matters at this level is the ORDER of the two
        writes, not the abandonment mechanics themselves.
        """
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "none")

        release = threading.Event()
        self.addCleanup(release.set)  # let the stalled persist finish so it does not leak into later tests

        def stalled_update_state(name, cwd, mutate):
            release.wait(30)
            return dict(mutate({})), []

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"), \
                mock.patch.object(_hook_state, "update_state", side_effect=stalled_update_state), \
                mock.patch.object(_MOD, "_LEDGER_BUDGET_SECONDS", 0.2):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            with mock.patch("sys.stderr"):  # the abandoned-write warning is expected here
                self._run(session_id="sess-1")

        entry = self._last_entry()
        self.assertEqual(entry["reason"], "identity_changed")
        self.assertTrue(entry["identity_changed"])
        # The persist itself has not gone through yet (still stalled): proof
        # that the row above was NOT gated on it finishing first.
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), CONTAINER)

    def test_state_write_failed_from_persisting_does_not_vanish(self):
        """Ruling 2 / R2-c11: a persist failure discovered AFTER the run's
        own row is already written must still show up in the ledger, and
        the drift itself must still be detected on the NEXT run (the old
        container_id was never actually replaced on disk, so it is not a
        one-time report that then falls silent)."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")

        def failing_update_state(name, cwd, mutate):
            return {}, ["state_write_failed"]

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"), \
                mock.patch.object(_hook_state, "update_state", side_effect=failing_update_state):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            with mock.patch("sys.stderr"):
                self._run(session_id="sess-1")

        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertGreaterEqual(len(entries), 2)
        identity_row, persist_row = entries[-2], entries[-1]
        self.assertEqual(identity_row["reason"], "identity_changed")
        self.assertTrue(identity_row["identity_changed"])
        self.assertEqual(persist_row["reason"], "state_write_failed")
        self.assertFalse(persist_row["ok"])

        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), CONTAINER)  # never actually persisted

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m3"})
            self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "identity_changed")  # still unresolved

    def test_identity_drift_unknown_does_not_set_identity_changed(self):
        """R2-c06: `_hook_state.identity_drift` returns `['unknown']` --
        NOT `['identity_changed']` -- when the previous identity is
        UNKNOWABLE (state exists but carries no `container_id`, e.g. a
        corrupted or pre-migration state file): a genuinely different
        condition from "the identity changed". The old code treated ANY
        non-empty `drift_reasons` as `identity_changed=True`, conflating
        the two and sending a debugger chasing a container swap that never
        happened."""
        self._write("2026-09-20-1000-x.md")
        _hook_state.update_state(_MOD.HOOK, self.cwd, lambda s: {**s, "some_other_key": "x"})
        self.assertTrue(_hook_state.state_exists(_MOD.HOOK, self.cwd))
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertNotIn("container_id", state)

        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        with mock.patch("sys.stderr"):
            self._run(session_id="sess-1")
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "unknown")
        self.assertNotIn("identity_changed", entry)

    def test_calls_is_recorded_as_unknown_not_zero_when_the_work_is_abandoned(self):
        """R1-c09: `run["calls"]` is only assigned AFTER `upsert` returns --
        a thread abandoned while still INSIDE it (the outer work budget, not
        the client's own deadline refusal) never reaches that line, and the
        ledger must not report a bare 0 there: that reads as "definitely no
        requests were made", when in truth a real request may already be in
        flight or even answered."""
        self._write("2026-09-20-1000-x.md")
        release = threading.Event()
        self.addCleanup(release.set)  # let the abandoned worker finish and exit

        def stall(*args, **kwargs):
            release.wait(30)
            return _ingest_client.Outcome()

        with mock.patch.object(_ingest_client.IngestClient, "upsert", stall), \
                mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.2):
            self._run(session_id="sess-1")
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertIsNone(entry["calls"])

    def test_a_broken_stderr_pipe_does_not_cost_the_ledger_row(self):
        """R2-c05: the timeout/exception branches in `main()` used to print
        their diagnostic line to stderr BEFORE calling `_record` -- so a
        stderr write that fails (the host has already closed the pipe, on
        its own way out) raised OUT of `main()` before the ledger row was
        ever written, silently losing the record of this run entirely
        (and, via `_hook_runner.finish`'s own interpreter-shutdown flush,
        risked the "always exit 0" contract too, on the real subprocess
        path). The ledger write must not depend on stderr succeeding."""
        self._write("2026-09-20-1000-x.md")
        release = threading.Event()
        self.addCleanup(release.set)  # let the abandoned worker finish and exit

        def stall(*args, **kwargs):
            release.wait(30)
            return _ingest_client.Outcome()

        with mock.patch.object(_ingest_client.IngestClient, "upsert", stall), \
                mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.2), \
                mock.patch("sys.stderr", _BrokenStderr()):
            self._run(session_id="sess-1")  # must not raise BrokenPipeError
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")

    def test_source_name_is_the_backend_allowlisted_value(self):
        """R1-c27: SOURCE_NAME is a literal string the backend attributes by
        exact match (mcp_attribution._KNOWN_CLIENTS); a typo here sends
        every write to source=unknown with nothing local to catch it."""
        self.assertEqual(_MOD.SOURCE_NAME, "handoff-sync-hook")

    def test_requests_carry_the_x_nexus_source_header(self):
        """R1-c27: the wire header itself, not just the constant's value."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        expected = f"{_MOD.SOURCE_NAME}/{_identity.plugin_version()}"
        self.assertTrue(self.requests)
        for req in self.requests:
            self.assertEqual(req["headers"]["x-nexus-source"], expected)

    def test_the_ingest_client_deadline_is_wired_from_the_work_budget(self):
        """R1-c26: nothing previously pinned that `deadline=run["deadline"]`
        (main's own absolute time.monotonic() budget) actually reaches
        IngestClient -- passing `deadline=None` through left every other
        test green."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        captured = {}
        real_init = _ingest_client.IngestClient.__init__

        def spy_init(self_, *args, **kwargs):
            captured.update(kwargs)
            real_init(self_, *args, **kwargs)

        with mock.patch.object(_ingest_client.IngestClient, "__init__", spy_init), \
                mock.patch.object(_MOD.time, "monotonic", return_value=1_000_000.0):
            self._run(session_id="sess-1")
        expected = 1_000_000.0 + _MOD._WORK_BUDGET_SECONDS - _MOD._DEADLINE_SLACK_SECONDS
        self.assertEqual(captured.get("deadline"), expected)

    def test_a_deadline_already_exhausted_refuses_without_a_request(self):
        """The second half of R1-c26: with the deadline already in the past
        by the time IngestClient makes its first call, the client refuses
        instead of trying against the clock -- zero requests."""
        self._write("2026-09-20-1000-x.md")
        with mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 1_000_000.0):
            self._run(session_id="sess-1")
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "timeout")

    def test_the_ledger_records_which_document_a_write_was_about(self):
        """R1-c30: pins external_id as its OWN ledger field (not only inside
        the metadata the wire request happens to carry)."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["external_id"], "docs/handoff/2026-09-20-1000-x.md")

    def test_the_ledger_detail_is_truncated_to_200_chars(self):
        """R1-c30: `_short`'s cap, pinned end to end through the ledger."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(422, {"detail": "x" * 1000})
        self._run(session_id="sess-1")
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "rejected_422")
        self.assertLessEqual(len(entry["detail"]), 200)

    def test_locate_does_not_filter_candidates_by_owner(self):
        """Ruling 1 (TASK-005 R1 fix round, R1-c07): `_locate` picks the
        newest `updated-at` across ALL candidates in the directory, never
        checking frontmatter `owner-container` -- the owner check only runs
        AFTER a single document has already been chosen. A newer handoff
        written by a DIFFERENT container therefore wins over an older one
        THIS container actually owns, and the older one is never even
        considered: this run ends in `not_owner`, not a successful
        ingestion of the older document. Deliberate (see Amendment A9): a
        change here must be a deliberate decision, not an incidental
        refactor -- this test exists so that decision shows up as a test
        change, not a silent behaviour change.
        """
        self._write("2026-09-19-old-mine.md", owner=f"owner/{LOCAL_UUID}", updated_at="2026-09-19T00:00:00Z")
        self._write("2026-09-20-new-theirs.md", owner=f"owner/{OTHER_UUID}", updated_at="2026-09-20T00:00:00Z")
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "not_owner")

    def test_session_attribution_is_the_ingesting_session_not_the_documents_own(self):
        """Ruling 1 (TASK-005 R1 fix round, R1-c03): metadata.session_id is
        always the CURRENT SessionEnd's session_id, never anything derived
        from the handoff document itself -- there is nothing in a handoff
        document that could tell us which session originally wrote it. This
        is deliberate, not an oversight (see Amendment A9); a change here
        must be deliberate, not incidental.
        """
        self._write("2026-09-20-1000-x.md", updated_at="2020-01-01T00:00:00Z")  # written long "ago"
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="this-very-session")
        meta = self.requests[1]["json"]["metadata"]
        self.assertEqual(meta["session_id"], "this-very-session")

    def test_owner_container_frontmatter_value_is_recorded_on_identity_unresolved(self):
        """R1-c10: the ledger `extra` used to stay `{}` on every reason that
        returns before the write path -- a SessionStart failure report for
        `identity_unresolved` could not even say which frontmatter value
        was the problem."""
        self._write("2026-09-20-1000-x.md", owner="simonfish/dev-claude2")  # hostname-shaped
        self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "identity_unresolved")
        self.assertIn("simonfish/dev-claude2", entry["detail"])
        self.assertEqual(entry["external_id"], "docs/handoff/2026-09-20-1000-x.md")

    def test_not_owner_and_opted_out_also_record_external_id(self):
        """R1-c10, the other non-write branches: `not_owner` and `opted_out`
        likewise say which document the run was about."""
        self._write("2026-09-20-1000-x.md", owner=f"owner/{OTHER_UUID}")
        self._run()
        self.assertEqual(self._last_entry()["reason"], "not_owner")
        self.assertEqual(self._last_entry()["external_id"], "docs/handoff/2026-09-20-1000-x.md")


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
        """R1-c33: `_identity._resolved_root` only ever sets `degraded=True`
        alongside `toplevel=None` (never a real path) -- `(self.repo, True)`
        is a shape the real code never produces. `(None, True)` is the real
        one; `root = toplevel or cwd` still resolves to `self.repo` here
        because this call runs from the repo root, so the rest of the test
        is unchanged."""
        _write_handoff(self.handoff_dir, "2026-09-20-1000-x.md", owner=f"owner/{LOCAL_UUID}")
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")
        with mock.patch.object(_identity, "project_root", return_value=(None, True)):
            self._run(self.repo)
        self.assertEqual(self.backend.requests, [])
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.repo)
        self.assertEqual(entries[-1]["reason"], "identity_unresolved")

    def test_outside_a_repository_locates_and_writes_under_cwd(self):
        """R1-c33: the OTHER real shape `_resolved_root` returns -- not a
        repository at all (`degraded=False`, `toplevel=None`) -- as opposed
        to the degraded case above. This one is not refused: `root = cwd`
        and the write proceeds normally."""
        non_repo = os.path.join(self.tmp.name, "not-a-repo")
        sub_handoff = os.path.join(non_repo, "docs", "handoff")
        os.makedirs(sub_handoff)
        _write_handoff(sub_handoff, "2026-09-20-1000-x.md", owner=f"owner/{LOCAL_UUID}")
        _write_latest_pointer(sub_handoff, "2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        with mock.patch.object(_identity, "project_root", return_value=(None, False)):
            self._run(non_repo)
        self.assertEqual([r["method"] for r in self.backend.requests], ["GET", "POST"])
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, non_repo)
        self.assertEqual(entries[-1]["reason"], "none")

    def test_a_non_string_cwd_value_is_ignored_and_falls_back_to_process_cwd(self):
        """R2-c19: the type/non-empty guard on the SessionEnd event's `cwd`
        exists for exactly this shape -- a non-string but still TRUTHY
        value (a list, say) must be IGNORED, falling back to the hook's own
        process cwd, the same as an absent `cwd` -- not surfaced as an
        `unknown` failure. The host's own contract guarantees `cwd` is a
        string; this pins what happens if that is ever not true, rather
        than trusting the contract blindly and letting a non-string value
        flow into `_identity`/`_hook_state`, where it would eventually
        raise (a `subprocess.run` argument list cannot contain a list) and
        surface as a loud but unhelpful `unknown`."""
        _write_handoff(self.handoff_dir, "2026-09-20-1000-x.md", owner=f"owner/{LOCAL_UUID}")
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        with mock.patch.object(_MOD.os, "getcwd", return_value=self.repo):
            _run_main(
                _MOD,
                {"cwd": ["not", "a", "string"], "session_id": "s"},
                {
                    "NEXUS_API_URL": self.backend.url,
                    "NEXUS_HOOK_STATE_DIR": self.state_dir,
                    "NEXUS_DEFAULT_USER_ID": USER,
                },
            )
        self.assertEqual([r["method"] for r in self.backend.requests], ["GET", "POST"])
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.repo)
        self.assertEqual(entries[-1]["reason"], "none")

    def test_not_configured_makes_no_request_when_no_api_url(self):
        _run_main(_MOD, {"cwd": self.repo, "session_id": "s"}, {"NEXUS_HOOK_STATE_DIR": self.state_dir})
        self.assertEqual(self.backend.requests, [])
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.repo)
        self.assertEqual(entries[-1]["reason"], "not_configured")

    def test_elapsed_ms_is_recorded(self):
        """R1-c30: strengthened past "an int >= 0" (a frozen `elapsed_ms =
        0` constant would also satisfy that) to a real, strictly positive
        value, using a fake clock that advances on every call."""
        counter = {"n": 0}

        def fake_monotonic():
            counter["n"] += 1
            return counter["n"] * 0.001

        with mock.patch.object(_MOD.time, "monotonic", side_effect=fake_monotonic):
            _run_main(_MOD, {"cwd": self.repo, "session_id": "s"}, {"NEXUS_HOOK_STATE_DIR": self.state_dir})
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.repo)
        self.assertIs(type(entries[-1]["elapsed_ms"]), int)
        self.assertGreater(entries[-1]["elapsed_ms"], 0)


class TestImportGuards(unittest.TestCase):
    """handoff_sync.py's own three import guards (R1-c31): none of them had
    a test of its own, unlike session_capture.py's / session_inject.py's
    equivalent `TestSharedModulesUnavailable` (which this mirrors). One
    difference from those two (module docstring, A5-3): `_ingest_client`
    is REQUIRED here, not peripheral bookkeeping."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _copy_hook_without(self, *missing):
        target = os.path.join(self.tmp.name, "partial-install")
        os.makedirs(target)
        names = (
            "handoff_sync.py", "_identity.py", "_hook_runner.py",
            "_ingest_client.py", "_hook_state.py", "_redact.py",
        )
        for name in names:
            if name not in missing:
                shutil.copy(os.path.join(_HOOKS_DIR, name), target)
        return os.path.join(target, "handoff_sync.py")

    def test_without_identity_it_exits_zero_and_says_why(self):
        script = self._copy_hook_without("_identity.py")
        stdout, code, stderr = _run_hook("{}", script=script, want_stderr=True)
        self.assertEqual((stdout, code), (b"", 0))
        self.assertIn("_identity", stderr)
        self.assertNotIn("Traceback", stderr)

    def test_without_hook_runner_it_exits_zero_and_says_why(self):
        script = self._copy_hook_without("_hook_runner.py")
        stdout, code, stderr = _run_hook("{}", script=script, want_stderr=True)
        self.assertEqual((stdout, code), (b"", 0))
        self.assertIn("_hook_runner", stderr)
        self.assertNotIn("Traceback", stderr)

    def test_without_ingest_client_it_exits_zero_and_says_why(self):
        """The one guard that is unique to this hook (contrast the other two
        SessionEnd hooks, which treat their ledger as optional)."""
        script = self._copy_hook_without("_ingest_client.py")
        stdout, code, stderr = _run_hook("{}", script=script, want_stderr=True)
        self.assertEqual((stdout, code), (b"", 0))
        self.assertIn("_ingest_client", stderr)
        self.assertNotIn("Traceback", stderr)


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
