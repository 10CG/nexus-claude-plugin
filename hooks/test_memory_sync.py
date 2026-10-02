#!/usr/bin/env python3
"""Tests for hooks/memory_sync.py (change 2 TASK-006, workflow C: Claude
Code memory files -> layer=fact rows).

Runnable as: python3 hooks/test_memory_sync.py   (stdlib unittest only)

Layers, matching how the hook itself is layered:
  - TestFrontmatterParsing / TestCapBody / TestListMemoryFiles / TestDirtyCheck
    call the pure filesystem/string functions directly -- no identity, no
    network.
  - TestSingleFileSync / TestBatchingAndCursor / TestTruncation /
    TestDeletion / TestOrphanReconciliation / TestX1PrefixIsolation /
    TestAbortReasons / TestAlsoFailed drive the hook end to end
    (mod.main(), in-process) against a REAL scripted loopback HTTP backend
    (like test_handoff_sync.py's own _Backend): nothing about
    _ingest_client is mocked, so every assertion about what gets
    POSTed/PATCHed/DELETEd is on the actual wire.
  - TestNoDriftCode / TestRequiredModules are static source-text checks for
    the two A9 rulings that are structural absences, not behaviour a
    scripted backend could observe.
  - TestImportGuards / TestStderrAndFd2Hygiene / TestSubprocess exercise a
    real subprocess (exit code, ledger written, broken stdio).

Hermetic per the plugin's project lessons: the whole module runs under a
throwaway HOME + NEXUS_HOOK_STATE_DIR (setUpModule), proxy env vars are
scrubbed so loopback requests never go near a developer machine's
cross-border proxy, and the module asserts the fake HOME is still empty on
the way out.
"""

import glob
import hashlib
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
import time
import unittest
import urllib.parse
from unittest import mock

import _hook_runner
import _hook_state
import _identity
import _ingest_client
import _redact

_HOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
_HOOK_SCRIPT = os.path.join(_HOOKS_DIR, "memory_sync.py")

_PROXY_VARS = ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY")
_NO_PROXY = {"no_proxy": "127.0.0.1,localhost", "NO_PROXY": "127.0.0.1,localhost"}


def _assert_home_untouched(home):
    leaked = sorted(os.listdir(home))
    if leaked:
        raise AssertionError(f"a test wrote under HOME instead of the state dir: {leaked}")


def _assert_stderr_not_left_wrapped():
    """Mirrors test_handoff_sync.py's / test_session_capture.py's own
    backstop: mod.main() permanently replaces the GLOBAL sys.stderr with a
    _StderrGuard (_hook_runner.guard_stderr(), called before the work
    thread starts) the first time it runs in this process; _run_main
    (below) saves and restores sys.stderr around each call for exactly this
    reason, and this is the module-wide backstop for a future test that
    calls mod.main() directly instead."""
    if isinstance(sys.stderr, _hook_runner._StderrGuard):
        raise AssertionError(
            "a test left sys.stderr wrapped in _StderrGuard -- every OTHER "
            "test_*.py module run in this same `unittest discover` process "
            "would inherit it"
        )


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
    # Registration order matters (LIFO, see test_handoff_sync.py's own
    # TestModuleCleanupOrdering for the mechanism): the stderr check is
    # registered BEFORE the home check, so the home-leak report -- the more
    # actionable of the two -- is the one that survives if both ever fail on
    # the same run.
    unittest.addModuleCleanup(_assert_stderr_not_left_wrapped)
    unittest.addModuleCleanup(_assert_home_untouched, home)  # LIFO: this runs first


def _load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MOD = _load_module(_HOOK_SCRIPT, "memory_sync")


# ── in-process runner ────────────────────────────────────────────────────

_BASE_ENV_KEYS = ("HOME", "NEXUS_HOOK_STATE_DIR", "PATH")


def _run_main(mod, event, extra_env):
    """Run ``mod.main()`` in-process: stdin is ``event`` as JSON, the
    environment is reset to just ``_BASE_ENV_KEYS`` plus ``extra_env`` plus
    NO_PROXY -- never a developer shell's own NEXUS_API_URL. Mirrors
    test_handoff_sync.py's own ``_run_main``; see there for why ``sys.
    stderr`` is patched to ITSELF rather than left alone."""
    clean = {k: os.environ[k] for k in _BASE_ENV_KEYS if k in os.environ}
    clean.update(_NO_PROXY)
    clean.update(extra_env)
    old_stdin = sys.stdin
    sys.stdin = io.StringIO(json.dumps(event))
    try:
        with mock.patch.dict(os.environ, clean, clear=True), \
                mock.patch.object(sys, "stderr", sys.stderr):
            return mod.main()
    finally:
        sys.stdin = old_stdin


_PYTHON_PATH_VARS = ("PYTHONPATH", "PYTHONHOME")


def _scrub_subprocess_env(extra=None):
    run_env = {
        k: v for k, v in os.environ.items()
        if not k.startswith("NEXUS_") and k not in _PYTHON_PATH_VARS
    }
    if extra:
        run_env.update(extra)
    return run_env


def _run_hook(stdin_text, env=None, want_stderr=False, script=None):
    run_env = _scrub_subprocess_env(env)
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


def _communicate_kill_on_timeout(proc, payload, timeout):
    """Mirrors test_handoff_sync.py's own copy -- see there for why a bare
    try/finally communicate(timeout=) is not safe for these closed-pipe
    tests."""
    try:
        return proc.communicate(payload, timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)
        raise
    finally:
        proc.wait(timeout=timeout)


# ── scripted loopback backend (mirrors test_handoff_sync.py's own _Backend) ─

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
                    status, body, delay = backend.script.pop(0)
                else:
                    status, body, delay = 599, {"detail": "unscripted request"}, 0
                if delay:
                    time.sleep(delay)
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

    def reply(self, status, body=None, delay=0):
        """``delay`` (seconds) is slept on the SERVER thread right before
        answering this one scripted request -- used to force a REAL, small
        amount of wall-clock time to elapse between two requests so a
        per-file budget check can be pinned deterministically without
        mocking the global ``time`` module (which a worker-thread/
        ``threading.Thread.join`` budget mechanism also reads)."""
        self.script.append((status, body, delay))
        return self

    def close(self):
        self.server.shutdown()
        self.server.server_close()


CONTAINER = "mem-sync-box"
USER = "proj"


def _row(external_id, *, content_hash="sha256:" + "0" * 64, layer="fact", container_id=CONTAINER,
         created_at="2026-10-01T10:00:00.000001Z", row_id="11111111-1111-4111-8111-111111111111",
         extra=None):
    meta = {"layer": layer, "external_id": external_id, "container_id": container_id,
            "content_hash": content_hash}
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
    return {"memories": list(rows), "total_count": len(rows), "limit": 100, "offset": 0, "has_next": False}


def _empty_lookup():
    return 200, _page()


def _created(memory_id="m1"):
    return 201, {"memory_id": memory_id}


def _updated(memory_id="m1"):
    return 200, {"memory_id": memory_id}


def _write_memory_file(memory_dir, slug, *, style="flat", description="a description",
                        mtype="feedback", modified=None, origin_session=None,
                        body="body paragraph one.\n\nbody paragraph two.", name=None):
    """Write one memory-file fixture. ``style``: "flat" (every key
    top-level, matching ~42% of the real corpus) or "nested" (type /
    modified / originSessionId under a `metadata:` block, description still
    top-level -- matching the other ~60%). ``None`` for a field omits it."""
    lines = ["---", f"name: {name or slug}"]
    if description is not None:
        lines.append(f"description: {description}")
    if style == "flat":
        if mtype is not None:
            lines.append(f"type: {mtype}")
        if origin_session is not None:
            lines.append(f"originSessionId: {origin_session}")
        if modified is not None:
            lines.append(f"modified: {modified}")
    elif style == "nested":
        lines.append("metadata: ")
        lines.append("  node_type: memory")
        if mtype is not None:
            lines.append(f"  type: {mtype}")
        if origin_session is not None:
            lines.append(f"  originSessionId: {origin_session}")
        if modified is not None:
            lines.append(f"  modified: {modified}")
    else:
        raise ValueError(style)
    lines.append("---")
    text = "\n".join(lines) + "\n\n" + body + "\n"
    path = os.path.join(memory_dir, f"{slug}.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


# ════════════════════════════════════════════════════════════════════════
# Pure functions -- no identity, no network
# ════════════════════════════════════════════════════════════════════════

class TestFrontmatterParsing(unittest.TestCase):
    def test_flat_structure_reads_every_known_key(self):
        text = (
            "---\nname: x\ndescription: hello world\ntype: feedback\n"
            "originSessionId: sess-1\nmodified: 2026-08-13T17:32:45.325Z\n---\n\nbody"
        )
        fm, body = _MOD._split_memory_frontmatter(text)
        self.assertEqual(fm, {
            "description": "hello world", "type": "feedback",
            "originSessionId": "sess-1", "modified": "2026-08-13T17:32:45.325Z",
        })
        self.assertEqual(body, "body")

    def test_nested_metadata_block_reads_the_same_keys(self):
        """The other real structure: description stays top-level,
        type/modified/originSessionId move under an indented `metadata:`
        block (~60% of this machine's real corpus)."""
        text = (
            "---\nname: x\ndescription: \"a: description with a colon\"\n"
            "metadata: \n  node_type: memory\n  type: project\n"
            "  originSessionId: sess-2\n  modified: 2026-07-21T16:37:39.354Z\n---\n\nbody"
        )
        fm, body = _MOD._split_memory_frontmatter(text)
        self.assertEqual(fm, {
            "description": "a: description with a colon", "type": "project",
            "originSessionId": "sess-2", "modified": "2026-07-21T16:37:39.354Z",
        })
        self.assertEqual(body, "body")

    def test_description_with_an_internal_colon_is_not_truncated(self):
        """partition(":") splits on the FIRST colon only; a value with more
        colons must survive whole, quoted or not."""
        text = '---\ndescription: "v4.11.0 (JobVersion 24, 2026-09-13 16:36Z)"\n---\n\nb'
        fm, _ = _MOD._split_memory_frontmatter(text)
        self.assertEqual(fm["description"], "v4.11.0 (JobVersion 24, 2026-09-13 16:36Z)")

    def test_missing_type_is_simply_absent(self):
        text = "---\ndescription: d\n---\n\nbody"
        fm, _ = _MOD._split_memory_frontmatter(text)
        self.assertNotIn("type", fm)

    def test_no_frontmatter_block_is_empty_and_whole_text_is_body(self):
        fm, body = _MOD._split_memory_frontmatter("just a plain file\nwith no frontmatter\n")
        self.assertEqual(fm, {})
        self.assertEqual(body, "just a plain file\nwith no frontmatter\n")

    def test_unterminated_block_is_unparsable_not_empty(self):
        """K07: an opened-but-never-closed frontmatter block is a LOCAL
        deterministic error (``file_unparsable``), not the same as "this
        file genuinely has no frontmatter" -- the two must be
        distinguishable so the caller does not upload the open fence's own
        ``name:``/``description:`` lines as plain content with no
        ``aria.description`` at all."""
        fm, body = _MOD._split_memory_frontmatter("---\ndescription: d\nno closing fence")
        self.assertIsNone(fm)
        self.assertEqual(body, "---\ndescription: d\nno closing fence")

    def test_a_bom_is_tolerated(self):
        text = "﻿---\ndescription: d\n---\n\nbody"
        fm, body = _MOD._split_memory_frontmatter(text)
        self.assertEqual(fm["description"], "d")
        self.assertEqual(body, "body")

    def test_an_indented_line_outside_a_metadata_block_is_not_a_top_level_key(self):
        """A line that starts with whitespace but is not immediately inside
        a recognised `metadata:` block must not be stripped and read as a
        top-level key -- it is simply skipped."""
        text = "---\ndescription: d\n  modified: 2026-01-01T00:00:00Z\n---\n\nbody"
        fm, _ = _MOD._split_memory_frontmatter(text)
        self.assertNotIn("modified", fm)

    def test_unknown_keys_are_ignored_both_structures(self):
        text = "---\nname: x\ndescription: d\nunknown_key: zzz\n---\n\nbody"
        fm, _ = _MOD._split_memory_frontmatter(text)
        self.assertNotIn("unknown_key", fm)
        self.assertNotIn("name", fm)  # not read by this hook -- slug comes from the filename


class TestCapBody(unittest.TestCase):
    def test_short_body_is_unchanged(self):
        content, truncated = _MOD._cap_body("short", 100)
        self.assertEqual((content, truncated), ("short", False))

    def test_cuts_at_the_last_paragraph_boundary(self):
        # budget = limit - len(marker) must comfortably clear 50 so the
        # paragraph break itself (at position 50) falls inside the cut.
        body = "a" * 50 + "\n\n" + "b" * 50
        content, truncated = _MOD._cap_body(body, 80)
        self.assertTrue(truncated)
        self.assertEqual(content, "a" * 50 + _MOD._TRUNCATION_MARKER)

    def test_falls_back_to_a_line_boundary_when_no_paragraph_fits(self):
        body = "a" * 50 + "\n" + "b" * 50  # one newline, no blank-line paragraph break
        content, truncated = _MOD._cap_body(body, 80)
        self.assertTrue(truncated)
        self.assertEqual(content, "a" * 50 + _MOD._TRUNCATION_MARKER)

    def test_hard_cuts_when_not_even_one_line_fits(self):
        body = "a" * 200  # one giant line, no newline at all
        content, truncated = _MOD._cap_body(body, 60)
        self.assertTrue(truncated)
        self.assertLessEqual(len(content), 60)
        self.assertTrue(content.endswith(_MOD._TRUNCATION_MARKER))


class TestListMemoryFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.memory_dir = os.path.join(self.tmp.name, "memory")
        os.makedirs(self.memory_dir)

    def _touch(self, name, text="x"):
        with open(os.path.join(self.memory_dir, name), "w", encoding="utf-8") as fh:
            fh.write(text)

    def test_missing_directory_is_empty_not_an_error(self):
        out, indeterminate, reason = _MOD._list_memory_files(os.path.join(self.tmp.name, "nope"))
        self.assertEqual((out, indeterminate, reason), ({}, set(), None))

    def test_memory_index_is_excluded_case_insensitively(self):
        self._touch("MEMORY.md")
        self._touch("memory.md")  # a project that happens to use lowercase
        self._touch("feedback_x.md")
        out, _indeterminate, _reason = _MOD._list_memory_files(self.memory_dir)
        self.assertEqual(out, {"feedback_x": os.path.join(self.memory_dir, "feedback_x.md")})

    def test_non_md_files_are_ignored(self):
        self._touch("notes.txt")
        self._touch("a.md")
        out, _indeterminate, _reason = _MOD._list_memory_files(self.memory_dir)
        self.assertEqual(list(out), ["a"])

    def test_slug_is_the_filename_without_the_extension(self):
        self._touch("project_nexus_prod_deployed.md")
        out, _indeterminate, _reason = _MOD._list_memory_files(self.memory_dir)
        self.assertEqual(list(out), ["project_nexus_prod_deployed"])

    def test_a_subdirectory_is_not_a_candidate(self):
        os.makedirs(os.path.join(self.memory_dir, "sub.md"))
        self._touch("real.md")
        out, _indeterminate, _reason = _MOD._list_memory_files(self.memory_dir)
        self.assertEqual(list(out), ["real"])


class TestDirtyCheck(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "f.md")
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write("---\ndescription: d\n---\n\nbody")

    def _stored(self, **overrides):
        st = os.stat(self.path)
        entry = {
            "mtime": st.st_mtime, "size": st.st_size,
            "ctime": getattr(st, "st_ctime_ns", None),  # K21
            "file_hash": _MOD._whole_file_hash(self.path),
            "redaction_fingerprint": _MOD._current_fingerprint(),
        }
        entry.update(overrides)
        return entry

    def test_untouched_file_is_not_dirty_and_the_hash_is_not_recomputed(self):
        stored = self._stored()
        with mock.patch.object(_MOD, "_whole_file_hash") as hash_fn:
            dirty, file_hash = _MOD._dirty_check(self.path, stored, stored["redaction_fingerprint"])
        self.assertFalse(dirty)
        hash_fn.assert_not_called()  # mtime+size fast path: no recompute at all
        self.assertEqual(file_hash, stored["file_hash"])

    def test_content_rewrite_that_changes_size_is_dirty_even_with_mtime_rolled_back(self):
        """os.utime rollback: a script rewrites the content (necessarily a
        different size for any realistic edit) and then resets mtime to the
        OLD value. The fast path alone would miss this; size differing is
        what forces the recompute that catches it."""
        stored = self._stored()
        old_mtime = os.stat(self.path).st_mtime
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write("\n\nmore content that changes the size")
        os.utime(self.path, (old_mtime, old_mtime))
        dirty, _ = _MOD._dirty_check(self.path, stored, stored["redaction_fingerprint"])
        self.assertTrue(dirty)

    def test_frontmatter_only_edit_is_dirty_via_the_whole_file_hash(self):
        """Amendment A8 follow-up: the LOCAL dirty-check hashes the whole
        file including frontmatter, so an edit confined to the description
        (body untouched) is still caught."""
        stored = self._stored()
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write("---\ndescription: a DIFFERENT description\n---\n\nbody")
        dirty, new_hash = _MOD._dirty_check(self.path, stored, stored["redaction_fingerprint"])
        self.assertTrue(dirty)
        self.assertNotEqual(new_hash, stored["file_hash"])

    def test_a_fingerprint_mismatch_dirties_an_untouched_file(self):
        """Amendment A8-2: the file's bytes have not changed; the
        redaction rule that will be applied to them has."""
        stored = self._stored(redaction_fingerprint="sha256:" + "f" * 64)
        dirty, _ = _MOD._dirty_check(self.path, stored, _MOD._current_fingerprint())
        self.assertTrue(dirty)

    def test_matching_fingerprint_and_unchanged_content_is_clean(self):
        stored = self._stored()
        dirty, _ = _MOD._dirty_check(self.path, stored, stored["redaction_fingerprint"])
        self.assertFalse(dirty)


class TestMetadataBuilding(unittest.TestCase):
    def test_includes_x1_keys_and_slug_and_modified_always(self):
        meta = _MOD._build_memory_metadata("-home-dev-nexus", "nexus", "my-slug", {}, "2026-01-01T00:00:00Z")
        self.assertEqual(meta["aria.memory_dir"], "-home-dev-nexus")
        self.assertEqual(meta["aria.project"], "nexus")
        self.assertEqual(meta["aria.memory_slug"], "my-slug")
        self.assertEqual(meta["aria.modified"], "2026-01-01T00:00:00Z")
        self.assertNotIn("aria.memory_type", meta)
        self.assertNotIn("aria.description", meta)
        self.assertNotIn("aria.origin_session", meta)

    def test_optional_keys_are_included_only_when_present(self):
        fm = {"type": "feedback", "description": "d", "originSessionId": "s1"}
        meta = _MOD._build_memory_metadata("k", "p", "slug", fm, "2026-01-01T00:00:00Z")
        self.assertEqual(meta["aria.memory_type"], "feedback")
        self.assertEqual(meta["aria.description"], "d")
        self.assertEqual(meta["aria.origin_session"], "s1")


# ════════════════════════════════════════════════════════════════════════
# End-to-end against a real loopback backend
# ════════════════════════════════════════════════════════════════════════

class _WriteCase(unittest.TestCase):
    """A throwaway project + a scripted loopback backend, driving the hook
    through its real IngestClient -- nothing about _ingest_client itself is
    mocked, only the git-derived identity (so tests do not depend on a real
    git binary or a real ~/.aria file)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state_dir = os.path.join(self.tmp.name, "state")
        self.config_dir = os.path.join(self.tmp.name, "claude-config")
        self._patch(mock.patch.object(_identity, "_resolved_root", return_value=(self.cwd, False)))
        self.key, _ = _identity.memory_dir_key(self.cwd)
        self.memory_dir = os.path.join(self.config_dir, "projects", self.key, "memory")
        os.makedirs(self.memory_dir)
        self.backend = _Backend()
        self.addCleanup(self.backend.close)
        self._patch(mock.patch.dict(os.environ, {"NEXUS_HOOK_STATE_DIR": self.state_dir}))
        self._patch(mock.patch.object(_identity, "container_id", return_value=CONTAINER))

    def _patch(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _write(self, slug, **kw):
        return _write_memory_file(self.memory_dir, slug, **kw)

    def _run(self, **extra_env):
        env = {
            "NEXUS_API_URL": self.backend.url, "NEXUS_HOOK_STATE_DIR": self.state_dir,
            "NEXUS_DEFAULT_USER_ID": USER, "CLAUDE_CONFIG_DIR": self.config_dir,
        }
        env.update(extra_env)
        return _run_main(_MOD, {"cwd": self.cwd}, env)

    @property
    def requests(self):
        return self.backend.requests

    def _last_entry(self):
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertTrue(entries, "no ledger entry was written")
        return entries[-1]

    def _state(self):
        return _hook_state.read_state_at(_MOD._memory_state_path(self.key))[0]

    def _ext(self, slug):
        return f"{self.key}/{slug}"


class TestSingleFileSync(_WriteCase):
    def test_a_new_file_is_posted_with_the_slug_and_prefix(self):
        self._write("my-fact")
        self.backend.reply(*_empty_lookup())  # orphan reconciliation: nothing to see
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        posts = [r for r in self.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 1, self.requests)
        self.assertEqual(posts[0]["json"]["metadata"]["external_id"], self._ext("my-fact"))
        self.assertEqual(posts[0]["json"]["metadata"]["layer"], "fact")
        self.assertEqual(self._last_entry()["reason"], "none")

    def test_the_bulk_import_header_is_sent_on_every_write(self):
        self._write("f1")
        self.backend.reply(*_empty_lookup()).reply(*_empty_lookup()).reply(*_created())
        self._run()
        writes = [r for r in self.requests if r["method"] in ("POST", "PATCH", "DELETE")]
        self.assertTrue(writes)
        for r in writes:
            self.assertEqual(r["headers"].get("x-bulk-import"), "true", r)

    def test_a_second_run_with_unchanged_content_is_unchanged_and_does_not_patch(self):
        path = self._write("f1")
        with open(path, encoding="utf-8") as fh:
            _, raw_body = _MOD._split_memory_frontmatter(fh.read())
        content, _ = _MOD._cap_body(raw_body, 10000)
        redacted, _ = _redact.redact_text(content)
        digest = _ingest_client.content_hash(redacted)
        meta = {"layer": "fact", "external_id": self._ext("f1"), "container_id": CONTAINER,
                "content_hash": digest, "aria.memory_slug": "f1", "aria.project": "proj",
                "aria.memory_dir": self.key, "aria.truncated": False,
                "aria.description": "a description", "aria.memory_type": "feedback"}
        st = os.stat(path)
        meta["aria.modified"] = _MOD._mtime_iso(st)
        self.backend.reply(*_empty_lookup())  # orphan reconciliation
        self.backend.reply(200, _page(_row(self._ext("f1"), content_hash=digest, extra=meta)))
        self._run()
        self.assertEqual([r["method"] for r in self.requests[1:]], ["GET"])
        self.assertEqual(self._last_entry()["reason"], "unchanged")

    def test_editing_the_body_patches_with_new_content(self):
        self._write("f1", body="version one")
        self.backend.reply(*_empty_lookup())  # reconciliation, round 1 only
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self._run()
        self.assertTrue(self._state().get("reconciled"))  # round 2 must not re-reconcile
        self._write("f1", body="version two, now longer")
        self.backend.reply(
            200, _page(_row(self._ext("f1"), content_hash="sha256:" + "0" * 64,
                             extra={"aria.memory_slug": "f1"})),
        ).reply(*_updated("m1"))
        self._run()
        patches = [r for r in self.requests if r["method"] == "PATCH"]
        self.assertEqual(len(patches), 1, self.requests)
        self.assertIn("version two", patches[0]["json"]["content"])

    def test_editing_only_the_description_patches_metadata_only_not_content(self):
        """Amendment A8-1 / A8 follow-up: a description-only edit must not
        be judged unchanged (which would never PATCH at all), and the PATCH
        it does send must omit `content` (body hash is unchanged)."""
        path = self._write("f1", description="first description", body="same body always")
        with open(path, encoding="utf-8") as fh:
            _, body = _MOD._split_memory_frontmatter(fh.read())
        content, _ = _MOD._cap_body(body, 10000)
        redacted, _ = _redact.redact_text(content)
        digest = _ingest_client.content_hash(redacted)
        st = os.stat(path)
        stored_meta = {
            "layer": "fact", "external_id": self._ext("f1"), "container_id": CONTAINER,
            "content_hash": digest, "aria.memory_slug": "f1", "aria.project": "proj",
            "aria.memory_dir": self.key, "aria.truncated": False,
            "aria.description": "first description", "aria.memory_type": "feedback",
            "aria.modified": _MOD._mtime_iso(st),
        }
        self.backend.reply(*_empty_lookup())
        self.backend.reply(200, _page(_row(self._ext("f1"), content_hash=digest, extra=stored_meta)))
        self.backend.reply(*_updated("m1"))
        self._write("f1", description="a NEW description", body="same body always")
        self._run()
        patches = [r for r in self.requests if r["method"] == "PATCH"]
        self.assertEqual(len(patches), 1, self.requests)
        self.assertNotIn("content", patches[0]["json"], "a metadata-only edit must not resend content")
        self.assertEqual(patches[0]["json"]["metadata"]["aria.description"], "a NEW description")
        self.assertNotEqual(self._last_entry()["reason"], "unchanged")

    def test_a_server_422_is_a_skip_not_an_abort_and_advances(self):
        """R3-T03: a deterministic per-file error (422) does not abort the
        round -- every file in the batch is still attempted, and only the
        rejected one is left out of state.

        R4-C5 (fix round 4, test_gap, corrects the R3 commit's own
        characterization of this test): this test does NOT also pin "the
        cursor advances past the 422" -- the docstring and the trailing
        ``cursor == 0`` assertion that used to claim it here were vacuous.
        With exactly 3 files that are ALL examined in the same round, the
        cursor wraps to 0 regardless of whether "bad"'s own advance-cursor-
        only write (inside ``_advance_cursor_only``) ever actually ran:
        verified by mutation on a scratch copy (gutting
        ``_advance_cursor_only`` into a true no-op still passed the old
        assertion here). That acceptance item is pinned for real by
        ``TestR3T03CursorAdvanceAndPlaceholderPersistAreCovered`` below
        (whose own tests DO fail under the same mutation), via the shape
        that actually discriminates: a 422 file in the MIDDLE of a batch
        that then ABORTS on a later file, so the on-disk cursor has
        nowhere else it could be except where the 422 file's own advance
        left it."""
        self._write("bad")
        self._write("zzz-good")
        self._write("zzz-last")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(422, {"detail": "nope"})
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        methods = [r["method"] for r in self.requests]
        # All three files are ATTEMPTED (a 422 does not abort the round);
        # only "zzz-good" and "zzz-last" actually land.
        self.assertEqual(methods.count("POST"), 3, methods)
        self.assertEqual(self._last_entry()["reason"], "rejected_422")
        state = self._state()
        self.assertNotIn("bad", state.get("files", {}))
        self.assertIn("zzz-good", state.get("files", {}))
        self.assertIn("zzz-last", state.get("files", {}))

    def test_a_locally_unparsable_file_is_skipped_and_the_next_one_still_runs(self):
        bad_path = self._write("bad")
        with open(bad_path, "wb") as fh:
            fh.write(b"\xff\xfe\x00not valid utf8 \x80\x81")
        self._write("zzz-good")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        posts = [r for r in self.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 1, self.requests)
        self.assertEqual(self._last_entry()["reason"], "file_unparsable")


class TestStaleLocal(_WriteCase):
    """Amendment A8-2 / TASK-006 verification bullet 6: ``upsert`` returning
    ``stale_local`` (the server's copy is newer than this local file) must
    be treated as PROCESSED this round -- the file's state entry is written
    (moving it out of the dirty set) so it does not occupy the NEXT round's
    N slot by being retried every time for no reason."""

    _OLD = "2020-01-01T00:00:00.000Z"
    _NEW = "2030-01-01T00:00:00.000Z"

    def test_stale_local_updates_state_and_is_not_retried_next_round(self):
        self._write("f1", modified=self._OLD, body="version one")
        self.backend.reply(*_empty_lookup())  # reconciliation, round 1 only
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self._run()
        self.assertTrue(self._state().get("reconciled"))  # round 2 must not re-reconcile

        # Round 2: the body changes locally (so content_changed is True and
        # the "unchanged" skip is never reached), but the server's row
        # claims a NEWER aria.modified than this local file's -- the local
        # edit must not clobber it.
        path = self._write("f1", modified=self._OLD, body="version two, now longer")
        self.requests.clear()
        self.backend.reply(
            200,
            _page(_row(
                self._ext("f1"), content_hash="sha256:" + "9" * 64,
                extra={"aria.modified": self._NEW},
            )),
        )
        self._run()
        # stale_local never PATCHes: the server's copy already wins.
        self.assertEqual([r["method"] for r in self.requests], ["GET"])
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "stale_local")
        self.assertTrue(entry["ok"])  # a skip, not a failure

        # Processed, not left dirty: the state entry now matches the
        # CURRENT (round 2) file, so a later round does not re-attempt it.
        state_after = self._state()
        self.assertEqual(state_after["files"]["f1"]["file_hash"], _MOD._whole_file_hash(path))

        self.requests.clear()
        self._run()
        self.assertEqual(self.requests, [], "a stale_local file must not occupy the next round's N slot")
        self.assertEqual(self._last_entry()["reason"], "none")


class TestTruncation(_WriteCase):
    def test_a_long_file_is_truncated_and_marked(self):
        body = ("x" * 9000 + "\n\n") * 3  # well over the 10000 cap
        self._write("big", body=body)
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        posts = [r for r in self.requests if r["method"] == "POST"]
        self.assertEqual(posts[0]["json"]["metadata"]["aria.truncated"], True)
        self.assertLessEqual(len(posts[0]["json"]["content"]), 10000)

    def test_shrinking_back_under_the_cap_explicitly_clears_the_flag(self):
        """PATCH is a shallow merge: a flag that CAN turn off must be sent
        as an explicit `false`, or the stored `true` never clears."""
        self._write("shrink", body=("x" * 9000 + "\n\n") * 3)
        self.backend.reply(*_empty_lookup())  # reconciliation, round 1 only
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self._run()
        self._write("shrink", body="now short")
        self.backend.reply(
            200, _page(_row(self._ext("shrink"), content_hash="sha256:" + "0" * 64,
                             extra={"aria.truncated": True})),
        ).reply(*_updated("m1"))
        self._run()
        patches = [r for r in self.requests if r["method"] == "PATCH"]
        self.assertEqual(patches[-1]["json"]["metadata"]["aria.truncated"], False)


class TestBatchingAndCursor(_WriteCase):
    def _write_n(self, n, prefix="file"):
        for i in range(n):
            self._write(f"{prefix}{i}")

    def test_the_batch_cap_is_an_actual_ceiling_not_a_fixture_coincidence(self):
        """``_BATCH_SIZE = 5`` must cap the round even when MORE than 5
        files are eligible -- every other fixture in this class happens to
        use exactly 5 (or fewer) files, which would pass just the same if
        the cap were raised or removed outright. Seven brand-new files:
        only the first five (cursor/sorted order) are POSTed, and the
        cursor stops at 5 -- not 6, and not wrapped to 0."""
        self._write_n(7)
        self.backend.reply(*_empty_lookup())  # reconciliation
        for i in range(5):
            self.backend.reply(*_empty_lookup()).reply(*_created(f"m{i}"))
        self._run()
        posted_ids = [
            r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"
        ]
        self.assertEqual(posted_ids, [self._ext(f"file{i}") for i in range(5)])
        self.assertEqual(self._state().get("cursor"), 5)
        # The two files the cap left untouched this round are not in state
        # at all yet -- they are genuinely deferred, not silently dropped.
        self.assertEqual(sorted(self._state().get("files", {})), [f"file{i}" for i in range(5)])

    def test_abort_on_the_third_file_stops_the_round_and_records_the_first_two(self):
        self._write_n(5)
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self.backend.reply(*_empty_lookup()).reply(403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "off"}})
        self._run()
        self.assertEqual(self._last_entry()["reason"], "ingest_disabled")
        state = self._state()
        self.assertEqual(sorted(state.get("files", {})), ["file0", "file1"])
        self.assertEqual(state.get("cursor"), 2)  # resumes AT file2 next time

    def test_429_stops_the_round_the_same_way(self):
        self._write_n(5)
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self.backend.reply(*_empty_lookup()).reply(429, {"detail": "slow down"})
        self._run()
        self.assertEqual(self._last_entry()["reason"], "rate_limited")
        self.assertEqual(self._state().get("cursor"), 2)

    def test_an_unscripted_response_mid_batch_stops_the_round_the_same_way(self):
        """A round-abort need not be 403/429 specifically -- ANY non-2xx,
        non-structured answer on the third file (here: the backend's own
        "unscripted request" fallback, 599) stops the round at that file
        exactly like a real server error would."""
        self._write_n(5)
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        # Nothing scripted for file2: its lookup GET falls through to the
        # backend's own 599 "unscripted request" fallback.
        self._run()
        self.assertEqual(self._last_entry()["reason"], "http_error")
        self.assertEqual(sorted(self._state().get("files", {})), ["file0", "file1"])

    def test_a_stalled_third_file_stops_the_round_via_a_real_socket_timeout(self):
        """Same 3-of-5-files shape as the 403 / 429 / generic-599 siblings
        above, but for ``reason == "timeout"`` specifically (gate finding,
        2026-10-02: detailed-tasks.yaml's TASK-006 verification bullet 1
        names all three round-abort reasons -- 403 / 429 / timeout -- for
        this exact fixture shape, and only the first two had a literal
        sibling; the two existing "timeout" tests, TestK02 and R2C07,
        exercise whole-round WORKER-THREAD abandonment via a slow LOCAL
        call, not a per-file network timeout mid-batch, and say in their
        own docstrings why: every network call is itself deadline-aware,
        so a slow SERVER can never produce a worker-thread-abandonment
        timeout in production). This is a REAL per-call socket timeout
        instead, using the exact client-timeout/server-delay pair already
        proven in test_ingest_client.py (``test_a_stalled_reply_is_
        timeout`` / ``test_a_dripping_body_is_cut_at_the_deadline``, both
        ``timeout=0.2`` against roughly a one-second server stall) rather
        than inventing new, untested timing constants here. file2's own
        lookup GET is the request that stalls -- ``upsert`` never reaches
        the POST once its own lookup has already failed (``_ingest_
        client.py``'s ``rows is None: return outcome``), so no third
        scripted reply is needed for it."""
        self._write_n(5)
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self.backend.reply(200, _page(), delay=1.0)  # file2's lookup stalls past the 0.2s client timeout
        with mock.patch.object(_MOD, "_HTTP_TIMEOUT_SECONDS", 0.2):
            self._run()
        self.assertEqual(self._last_entry()["reason"], "timeout")
        state = self._state()
        self.assertEqual(sorted(state.get("files", {})), ["file0", "file1"])
        self.assertEqual(state.get("cursor"), 2)  # resumes AT file2 next time

    def test_the_cursor_resumes_at_the_failed_file_next_round(self):
        self._write_n(5)
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self.backend.reply(*_empty_lookup()).reply(429, {"detail": "slow"})
        self._run()
        self.requests.clear()
        self.backend.reply(*_empty_lookup()).reply(*_created("m2"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m3"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m4"))
        self._run()
        posted_ids = [
            r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"
        ]
        self.assertEqual(posted_ids, [self._ext("file2"), self._ext("file3"), self._ext("file4")])

    def test_the_cursor_wraps_to_zero_once_every_file_is_synced(self):
        self._write_n(2)
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self._run()
        self.assertEqual(self._state().get("cursor"), 0)

    def test_steady_state_with_nothing_changed_makes_zero_calls(self):
        """No dirty files, nothing new: the round must be a pure local
        scan, zero HTTP."""
        path = self._write("f1")
        with open(path, encoding="utf-8") as fh:
            _, body = _MOD._split_memory_frontmatter(fh.read())
        content, _ = _MOD._cap_body(body, 10000)
        redacted, _ = _redact.redact_text(content)
        digest = _ingest_client.content_hash(redacted)
        st = os.stat(path)
        fingerprint = _MOD._current_fingerprint()
        state = {
            "cursor": 0, "reconciled": True,
            "files": {"f1": {"mtime": st.st_mtime, "size": st.st_size,
                              "file_hash": _MOD._whole_file_hash(path),
                              "synced_at": "2026-01-01T00:00:00Z",
                              "redaction_fingerprint": fingerprint}},
        }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "none")

    def test_dirty_set_takes_priority_over_the_cursor_within_the_same_round(self):
        """A mix of one dirty (already-synced, edited) file and several
        brand-new files: the dirty one is sent first, regardless of
        alphabetical position."""
        fingerprint = _MOD._current_fingerprint()
        dirty_path = self._write("zz-dirty", body="original")
        state = {
            "cursor": 0, "reconciled": True,
            # mtime/size deliberately stale (not the file's real stat), so
            # the fast path fails and the hash is recomputed -- matching a
            # stored file_hash exactly here would wrongly be "clean".
            "files": {"zz-dirty": {"mtime": 1.0, "size": -1,
                                    "file_hash": "sha256:" + "0" * 64,
                                    "synced_at": "2026-01-01T00:00:00Z",
                                    "redaction_fingerprint": fingerprint}},
        }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        self._write("aa-new")
        # `reconciled: True` is already in the seeded state, so no
        # reconciliation GET happens this round -- the first real request is
        # the dirty file's own lookup.
        self.backend.reply(200, _page(_row(self._ext("zz-dirty"), content_hash="sha256:" + "1" * 64))).reply(*_updated("m1"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m2"))
        self._run()
        writes = [r for r in self.requests if r["method"] in ("PATCH", "POST")]
        self.assertEqual([w["method"] for w in writes], ["PATCH", "POST"])


class TestDeletion(_WriteCase):
    def _seed_synced_state(self, slug, path):
        """Merges one more synced entry into state (preserving any other
        slug a previous call already seeded), rather than replacing the
        whole file -- several tests below seed more than one slug."""
        st = os.stat(path)
        entry = {
            "mtime": st.st_mtime, "size": st.st_size,
            "ctime": getattr(st, "st_ctime_ns", None),
            "file_hash": _MOD._whole_file_hash(path),
            "synced_at": "2026-01-01T00:00:00Z",
            "redaction_fingerprint": _MOD._current_fingerprint(),
        }
        _hook_state.update_state_at(
            _MOD._memory_state_path(self.key),
            lambda s, slug=slug, entry=entry: {
                **s, "cursor": s.get("cursor", 0), "reconciled": True,
                "files": {**(s.get("files") or {}), slug: entry},
            },
        )

    def test_a_vanished_file_is_deleted_and_the_mapping_clears(self):
        # K01 (post_implementation R1): zero local files no longer deletes
        # anything on its own (see TestZeroLocalFilesDuringPendingDelete) --
        # "kept" stays on disk so this test's own "gone" is an ordinary
        # single vanished file, not the "did resolution break" signal.
        self._seed_synced_state("kept", self._write("kept"))
        path = self._write("gone")
        self._seed_synced_state("gone", path)
        os.remove(path)
        self.backend.reply(200, _page(_row(self._ext("gone")))).reply(204, None)
        self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(len(deletes), 1, self.requests)
        self.assertNotIn("gone", self._state().get("files", {}))
        self.assertEqual(self._last_entry()["reason"], "none")

    def test_a_failed_delete_leaves_the_mapping_for_retry(self):
        self._seed_synced_state("kept", self._write("kept"))  # K01: keep local_files non-empty
        path = self._write("gone")
        self._seed_synced_state("gone", path)
        os.remove(path)
        self.backend.reply(200, _page(_row(self._ext("gone")))).reply(500, {"detail": "boom"})
        self._run()
        self.assertIn("gone", self._state().get("files", {}))
        self.assertEqual(self._last_entry()["reason"], "http_error")
        self.requests.clear()
        self.backend.reply(200, _page(_row(self._ext("gone")))).reply(204, None)
        self._run()
        self.assertNotIn("gone", self._state().get("files", {}))

    def test_deletes_are_retried_before_the_regular_batch_each_round(self):
        gone_path = self._write("gone")
        self._seed_synced_state("gone", gone_path)
        os.remove(gone_path)
        self._write("new-one")
        # delete("fact", ...) does its own lookup GET before the DELETE verb.
        self.backend.reply(200, _page(_row(self._ext("gone")))).reply(204, None)
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        methods = [r["method"] for r in self.requests]
        self.assertEqual(methods[:2], ["GET", "DELETE"], methods)
        delete_index = methods.index("DELETE")
        post_index = methods.index("POST")
        self.assertLess(delete_index, post_index, "the delete retry must run before the regular batch")


class TestOrphanReconciliation(_WriteCase):
    def test_collects_every_page_before_the_first_delete(self):
        """Contract §6.2: three pages of results must be fully collected --
        with the second and third requests carrying the previous page's
        own (created_at, id) and no `offset` -- before any DELETE is sent.
        One local file ("kept") keeps the guard's "zero local files" leg
        from tripping; 3 orphans <= max(5, 20% of 4 synced rows) so the
        ceiling leg does not trip either."""
        self._write("kept")
        row_a = _row(self._ext("a"), row_id="aaaaaaaa-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000003Z")
        row_b = _row(self._ext("b"), row_id="bbbbbbbb-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000002Z")
        row_c = _row(self._ext("c"), row_id="cccccccc-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000001Z")
        self.backend.reply(200, _page(row_a))
        self.backend.reply(200, _page(row_b))
        self.backend.reply(200, _page(row_c))
        self.backend.reply(200, _page())  # empty page ends the scan
        # Each orphan delete: client.delete() does its OWN lookup GET (by
        # external_id) before the DELETE verb -- three pairs.
        self.backend.reply(200, _page(row_a)).reply(204, None)
        self.backend.reply(200, _page(row_b)).reply(204, None)
        self.backend.reply(200, _page(row_c)).reply(204, None)
        self.backend.reply(*_empty_lookup()).reply(*_created())  # "kept" itself, a new file
        self._run()
        listing_gets = [
            r for r in self.requests if r["method"] == "GET" and "external_id" not in r["query"]
        ]
        self.assertEqual(len(listing_gets), 4, self.requests)
        self.assertNotIn("before_created_at", listing_gets[0]["query"])
        self.assertNotIn("offset", listing_gets[1]["query"])
        self.assertEqual(listing_gets[1]["query"]["before_created_at"], row_a["created_at"])
        self.assertEqual(listing_gets[1]["query"]["before_id"], row_a["id"])
        self.assertEqual(listing_gets[2]["query"]["before_created_at"], row_b["created_at"])
        last_listing_index = max(
            i for i, r in enumerate(self.requests)
            if r["method"] == "GET" and "external_id" not in r["query"]
        )
        first_delete_index = next(i for i, r in enumerate(self.requests) if r["method"] == "DELETE")
        self.assertGreater(first_delete_index, last_listing_index)
        self.assertEqual(self._last_entry()["reason"], "orphans_deleted")

    def test_zero_local_files_refuses_to_delete_anything(self):
        """The subdirectory / misconfigured-directory safety net (TASK-006
        verification): with zero local files, any row at all looks like an
        orphan, so the guard refuses to touch anything."""
        self.backend.reply(200, _page(_row(self._ext("a"))))
        self.backend.reply(200, _page())
        self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET", "GET"])
        self.assertEqual(self._last_entry()["reason"], "orphan_guard")

    def test_more_than_the_guard_ceiling_refuses_to_delete_anything(self):
        """local file count 1, six orphan rows found under THIS project's
        own prefix: max(5, 20% of 6) = 5, 6 > 5 -- the whole batch is
        refused. Reconciliation's own refusal does not stop the REGULAR
        batch (independent phases), so "kept" -- a brand new local file --
        still gets its own upsert attempt this same round."""
        self._write("kept")
        rows = [_row(self._ext(f"gone{i}"), row_id=f"{i:08d}-1111-4111-8111-111111111111")
                for i in range(6)]
        self.backend.reply(200, _page(*rows))
        self.backend.reply(200, _page())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET", "GET", "GET", "POST"])
        self.assertEqual(self._last_entry()["reason"], "orphan_guard")

    def test_reconciliation_runs_only_once_per_state_lifetime(self):
        self._write("a")
        self.backend.reply(*_empty_lookup())  # reconciliation, first run
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        self.requests.clear()
        self._write("b")
        self.backend.reply(*_empty_lookup()).reply(*_created())  # no reconciliation GET this time
        self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET", "POST"])

    def test_a_degraded_identity_skips_reconciliation_with_zero_requests(self):
        """Mirrors IngestClient's own identity_degraded guard on upsert/
        delete: this hook's own reconciliation listing is not a method of
        that class, so it does not inherit that protection for free, and a
        bulk-delete decision must never be driven by a guessed user_id/key."""
        self._write("f1")
        with mock.patch.object(_identity, "_resolved_root", return_value=(self.cwd, True)):
            self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "identity_unresolved")
        self.assertFalse(self._state().get("reconciled"))

    def test_a_guard_trip_does_not_mark_state_reconciled(self):
        """A safety refusal must be retried on a later run, not accepted as
        settled -- otherwise a transient misconfiguration permanently
        disables the one mechanism meant to clean up after it."""
        self.backend.reply(200, _page(_row(self._ext("a"))))
        self.backend.reply(200, _page())
        self._run()
        self.assertFalse(self._state().get("reconciled"))


class TestR2C01IndeterminateFilesNeverOrphaned(_WriteCase):
    """R2-C01: a slug ``_list_memory_files`` could not conclusively
    resolve (``indeterminate`` -- a dangling symlink, an lstat failure)
    must be treated as "still present" everywhere a vanished-file or
    orphan decision is made -- including orphan RECONCILIATION, which the
    previous version left OUT of the candidate set it passed as "local
    slugs" (only ``set(local_files)``): an indeterminate file's server
    row then looked exactly like every other orphan and got soft-deleted
    the moment it fell inside the guard's ceiling, with no trace in the
    ledger that it was ever anything other than a normal cleanup."""

    def test_a_dangling_symlink_is_never_orphan_deleted_during_reconciliation(self):
        self._write("kept")
        linked_target = os.path.join(self.tmp.name, "nonexistent-target.md")
        linked_path = os.path.join(self.memory_dir, "linked.md")
        os.symlink(linked_target, linked_path)
        linked_row = _row(self._ext("linked"))
        self.backend.reply(200, _page(linked_row))
        self.backend.reply(200, _page())  # ends the listing scan
        self.backend.reply(*_empty_lookup()).reply(*_created())  # "kept": new file
        self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(deletes, [], self.requests)
        self.assertEqual(len(self.requests), 4, self.requests)  # the 2 listing GETs + kept's own 2
        posts = [r for r in self.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 1, self.requests)
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "unknown")
        self.assertIn("linked", entry.get("unresolved_files", []))

    def test_an_lstat_failure_mock_twin_is_never_orphan_deleted(self):
        """Mock twin of the real dangling symlink above, pinned under
        root / any platform by failing ``os.lstat`` for the one entry."""
        self._write("kept")
        linked_path = os.path.join(self.memory_dir, "linked.md")
        with open(linked_path, "w", encoding="utf-8") as fh:
            fh.write("placeholder")
        real_lstat = os.lstat

        def flaky_lstat(path, *a, **kw):
            if path == linked_path:
                raise PermissionError(13, "Permission denied")
            return real_lstat(path, *a, **kw)

        linked_row = _row(self._ext("linked"))
        self.backend.reply(200, _page(linked_row))
        self.backend.reply(200, _page())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        with mock.patch.object(os, "lstat", side_effect=flaky_lstat):
            self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(deletes, [], self.requests)
        self.assertEqual(len(self.requests), 4, self.requests)
        posts = [r for r in self.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 1, self.requests)
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "unknown")
        self.assertIn("linked", entry.get("unresolved_files", []))

    def test_steady_state_with_an_unresolvable_file_is_reported_every_round(self):
        """Once a project IS reconciled, a permanently-indeterminate file
        (a dangling symlink nobody ever fixes) must keep being reported
        every round -- not silently and permanently ignored just because
        there happens to be no network call to make for it specifically."""
        kept_path = self._write("kept")
        st = os.stat(kept_path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"kept": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(kept_path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        linked_target = os.path.join(self.tmp.name, "nonexistent-target.md")
        os.symlink(linked_target, os.path.join(self.memory_dir, "linked.md"))
        self._run()
        self.assertEqual(self.requests, [])
        entry = self._last_entry()
        self.assertFalse(entry["ok"])
        self.assertEqual(entry["reason"], "unknown")
        self.assertIn("linked", entry.get("unresolved_files", []))
        self.requests.clear()
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "unknown")

    def test_the_pending_delete_path_also_never_treats_an_indeterminate_slug_as_vanished(self):
        """Not itself a regression (the pending-delete walk already
        excluded ``indeterminate`` before this fix) -- locked in
        alongside the reconciliation fix so the two paths cannot drift
        apart again."""
        path = self._write("was-synced")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"was-synced": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        # Replaced by a dangling symlink of the SAME name -- still
        # "there" (indeterminate), not genuinely gone.
        os.remove(path)
        os.symlink(os.path.join(self.tmp.name, "nonexistent.md"), path)
        self._run()
        self.assertEqual([r["method"] for r in self.requests if r["method"] == "DELETE"], [])
        self.assertIn("was-synced", self._state().get("files", {}))


class TestX1PrefixIsolation(_WriteCase):
    """X1 (owner 2026-10-01): required tests (a)-(c) from the owner ruling."""

    def test_a_shared_user_and_container_never_cross_contaminates_two_projects(self):
        """(a): two memory directories sharing user_id and container_id
        must never overwrite or orphan-delete each other's rows."""
        other_cwd = os.path.join(self.tmp.name, "other-proj")
        os.makedirs(other_cwd)
        with mock.patch.object(_identity, "_resolved_root", return_value=(other_cwd, False)):
            other_key, _ = _identity.memory_dir_key(other_cwd)
        self.assertNotEqual(self.key, other_key)
        self._write("shared-slug-name")
        # The reconciliation GET returns a row belonging to the OTHER
        # project (same user/container, different prefix) -- it must be
        # left alone.
        other_row = _row(f"{other_key}/shared-slug-name")
        self.backend.reply(200, _page(other_row))
        self.backend.reply(200, _page())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(deletes, [])
        posts = [r for r in self.requests if r["method"] == "POST"]
        self.assertEqual(posts[0]["json"]["metadata"]["external_id"], self._ext("shared-slug-name"))

    def test_orphan_listing_ignores_rows_without_the_prefix(self):
        """(b): the orphan listing ignores rows without the prefix -- they
        are never counted as orphans and never deleted."""
        self._write("mine")
        foreign_row = _row("completely-unrelated/slug")
        self.backend.reply(200, _page(foreign_row))
        self.backend.reply(200, _page())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET", "GET", "GET", "POST"])
        self.assertEqual(self._last_entry()["reason"], "none")

    def test_a_string_prefix_collision_does_not_cross_contaminate(self):
        """(c): a memory dir key that is a plain string prefix of another
        (this machine has several real pairs, e.g. -home-dev-nexus vs
        -home-dev-nexus-packages-nexus-claude-plugin) must not have the
        longer key's rows counted as the shorter key's orphans."""
        inner_cwd = os.path.join(self.cwd, "packages", "nexus-claude-plugin")
        os.makedirs(inner_cwd)
        with mock.patch.object(_identity, "_resolved_root", return_value=(inner_cwd, False)):
            inner_key, _ = _identity.memory_dir_key(inner_cwd)
        self.assertTrue(inner_key.startswith(self.key))
        self.assertNotEqual(inner_key, self.key)
        inner_row = _row(f"{inner_key}/some-slug")
        self.backend.reply(200, _page(inner_row))
        self.backend.reply(200, _page())
        self._run()
        # The inner key's row must NEVER be treated as this (outer) project's
        # row: external_id.startswith(outer_key + "/") is false here (the
        # character right after the shared prefix is "-", not "/"), so
        # reconciliation finds ZERO orphans under ITS OWN prefix -- not a
        # guard trip (which would mean "found some, refused to touch them"),
        # a clean no-op.
        self.assertEqual([r["method"] for r in self.requests], ["GET", "GET"])
        self.assertEqual(self._last_entry()["reason"], "none")
        self.assertEqual([r for r in self.requests if r["method"] == "DELETE"], [])


class TestX1StateFileKeying(unittest.TestCase):
    """(d): two working directories with the same basename but different
    memory dir keys keep separate state files and never schedule each
    other's rows for deletion."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_dir = os.path.join(self.tmp.name, "state")
        patcher = mock.patch.dict(os.environ, {"NEXUS_HOOK_STATE_DIR": self.state_dir})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_two_cwds_with_the_same_basename_get_distinct_state_paths(self):
        cwd_a = os.path.join(self.tmp.name, "worktree-a", "nexus")
        cwd_b = os.path.join(self.tmp.name, "worktree-b", "nexus")
        os.makedirs(cwd_a)
        os.makedirs(cwd_b)
        self.assertEqual(os.path.basename(cwd_a), os.path.basename(cwd_b))
        with mock.patch.object(_identity, "_resolved_root", return_value=(cwd_a, False)):
            key_a, _ = _identity.memory_dir_key(cwd_a)
        with mock.patch.object(_identity, "_resolved_root", return_value=(cwd_b, False)):
            key_b, _ = _identity.memory_dir_key(cwd_b)
        self.assertNotEqual(key_a, key_b)
        path_a = _MOD._memory_state_path(key_a)
        path_b = _MOD._memory_state_path(key_b)
        self.assertNotEqual(path_a, path_b)
        _hook_state.write_state_at(path_a, {"files": {"f1": "a-owns-this"}})
        self.assertEqual(_hook_state.read_state_at(path_b)[0], {})  # b sees nothing of a's

    def test_the_state_path_is_not_the_basename_keyed_project_dir_path(self):
        cwd = os.path.join(self.tmp.name, "nexus")
        os.makedirs(cwd)
        with mock.patch.object(_identity, "_resolved_root", return_value=(cwd, False)):
            key, _ = _identity.memory_dir_key(cwd)
        memory_sync_path = _MOD._memory_state_path(key)
        basename_keyed_path = _hook_state.state_path(_MOD.HOOK, cwd)
        self.assertNotEqual(memory_sync_path, basename_keyed_path)


class TestAbortReasons(_WriteCase):
    def test_structured_ingest_disabled_is_ingest_disabled(self):
        self._write("f1")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(
            403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "tenant off"}}
        )
        self._run()
        self.assertEqual(self._last_entry()["reason"], "ingest_disabled")

    def test_budget_exhausted_stops_before_a_call_is_even_made(self):
        self._write("f1")
        self._write("f2")
        with mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 1e9):  # always "too little left"
            self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "budget_exhausted")


class TestAlsoFailed(_WriteCase):
    def test_dedup_merged_buried_by_a_later_failure_is_in_also_failed(self):
        self._write("dup")
        self._write("zz-fails")
        dup_rows = _page(
            _row(self._ext("dup"), row_id="11111111-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000001Z"),
            _row(self._ext("dup"), row_id="22222222-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000002Z"),
        )
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(200, dup_rows).reply(204, None).reply(*_updated("m1"))
        self.backend.reply(*_empty_lookup()).reply(500, {"detail": "boom"})
        self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "http_error")
        self.assertIn("dedup_merged", entry.get("also_failed", []))


class TestPerFileStateWriteFailure(_WriteCase):
    """CRITICAL (independent gate finding): a genuine state-write failure on
    a PER-FILE persist (``_sync_file`` / ``_delete_file``) must be folded
    into this round's accumulated reasons -- and hence into
    ``worst_reason`` / the ledger row -- exactly like the round-end
    cursor/reconciled persist already does (memory_sync.py's own module
    docstring, A9-7, documents this; these two tests pin it as actual
    behaviour instead of just a claim in a comment). Both call sites
    (``_sync_file`` line ~625, ``_delete_file`` line ~641) discarded the
    ``(state, reasons)`` tuple ``_hook_state.update_state_at`` returns
    before this fix, so a failing write there silently vanished -- the run
    was recorded clean even though nothing was actually persisted."""

    def _fail_update_for_slug(self, slug):
        """Patches ``_hook_state.update_state_at`` so the ONE call whose
        mutate closure is specifically for ``slug`` -- identified by
        ``slug`` appearing among that closure's own default-argument
        values, since every per-file merge/drop write closes over the
        slug it is for -- returns a genuine failure without writing
        anything, while every OTHER call (reconciliation's own
        reconciled/placeholder persist, a cursor-only advance, a
        DIFFERENT file's own write) runs for real. Identifying the call
        this way (R2-C03), rather than by ordinal position, survives the
        reconciliation block now ALSO persisting (reconciled=True) before
        a brand-new file's own cursor-walk write -- call ORDER is no
        longer a stable way to pick out one specific file's write."""
        real = _hook_state.update_state_at

        def side_effect(path, mutate):
            try:
                is_for_slug = slug in (mutate.__defaults__ or ())
            except Exception:
                is_for_slug = False
            if is_for_slug:
                current, _ = _hook_state.read_state_at(path)
                return current, ["state_write_failed"]
            return real(path, mutate)

        return mock.patch.object(_hook_state, "update_state_at", side_effect=side_effect)

    def test_a_failed_persist_right_after_a_successful_post_is_reported(self):
        """Reproduces the independent gate's finding directly: a
        201-Created POST succeeds, but persisting that file's OWN state
        entry fails -- the run must not come back clean."""
        self._write("f1")
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        with self._fail_update_for_slug("f1"):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "state_write_failed")
        self.assertFalse(entry["ok"])
        # The write genuinely never landed -- self-healing (f1 is retried
        # next round) is fine; vanishing from the LEDGER is the bug.
        self.assertNotIn("f1", self._state().get("files", {}))

    def test_a_failed_persist_right_after_a_confirmed_delete_is_reported(self):
        kept_path = self._write("kept")  # K01: zero local files alone no longer deletes anything
        path = self._write("gone")
        st = os.stat(path)
        state = {
            "cursor": 0, "reconciled": True,
            "files": {
                "gone": {"mtime": st.st_mtime, "size": st.st_size,
                         "ctime": getattr(st, "st_ctime_ns", None),
                         "file_hash": _MOD._whole_file_hash(path),
                         "synced_at": "2026-01-01T00:00:00Z",
                         "redaction_fingerprint": _MOD._current_fingerprint()},
                "kept": {"mtime": os.stat(kept_path).st_mtime, "size": os.stat(kept_path).st_size,
                         "ctime": getattr(os.stat(kept_path), "st_ctime_ns", None),
                         "file_hash": _MOD._whole_file_hash(kept_path),
                         "synced_at": "2026-01-01T00:00:00Z",
                         "redaction_fingerprint": _MOD._current_fingerprint()},
            },
        }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        os.remove(path)
        self.backend.reply(200, _page(_row(self._ext("gone")))).reply(204, None)
        with self._fail_update_for_slug("gone"):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "state_write_failed")
        self.assertFalse(entry["ok"])
        # The DELETE genuinely succeeded server-side; only the local
        # bookkeeping failed to persist, so "gone" is retried (a harmless
        # re-delete) next round rather than the failure vanishing.
        self.assertIn("gone", self._state().get("files", {}))


# ════════════════════════════════════════════════════════════════════════
# Structural (source-text) checks for the A9 rulings
# ════════════════════════════════════════════════════════════════════════

def _source():
    with open(_HOOK_SCRIPT, encoding="utf-8") as fh:
        return fh.read()


class TestNoDriftCode(_WriteCase):
    """A9-11 (owner 2026-10-01): container_id drift detection and reporting
    belongs to session_inject alone (TASK-007); memory_sync must carry
    none of it and must not persist a container_id for that purpose."""

    def test_identity_drift_is_never_called(self):
        self.assertNotIn("identity_drift", _source())

    def test_no_identity_changed_reason_is_ever_produced(self):
        self.assertNotIn("identity_changed", _source())

    def test_written_state_never_carries_a_container_id_key(self):
        """Behavioural, not a source grep: container_id legitimately
        appears throughout the file as a query parameter / constructor
        argument (provenance on every row); what A9-11 actually forbids is
        persisting it in THIS hook's own state for a drift comparison."""
        self._write("f1")
        self.backend.reply(*_empty_lookup()).reply(*_empty_lookup()).reply(*_created())
        self._run()
        state = self._state()
        self.assertNotIn("container_id", state)
        for entry in state.get("files", {}).values():
            self.assertNotIn("container_id", entry)


class TestRequiredModules(unittest.TestCase):
    """A9-13 (owner 2026-10-01): the required modules are {_identity,
    _hook_runner, _ingest_client} -- no import degradation for any of the
    three (contrast session_capture.py / session_inject.py, which treat
    `_hook_state` as optional bookkeeping)."""

    def test_all_three_import_guards_exit_on_failure_as_a_script(self):
        source = _source()
        for module in ("_identity", "_hook_runner", "_ingest_client"):
            self.assertIn(f"import {module}", source)
        # The _hook_state-is-optional pattern from session_capture.py /
        # session_inject.py must not reappear here.
        self.assertNotIn("_LEDGER_IMPORT_ERROR", source)


class TestAlsoFailedIsTheSharedHelper(unittest.TestCase):
    def test_memory_sync_calls_the_shared_hook_state_helper(self):
        self.assertIn("_hook_state.also_failed(", _source())


# ════════════════════════════════════════════════════════════════════════
# Import guards + broken-stdio hygiene (Amendment A9-21 / TASK-012 shape)
# ════════════════════════════════════════════════════════════════════════

class TestImportGuards(unittest.TestCase):
    """memory_sync.py's own three import guards -- mirrors
    test_handoff_sync.py's TestImportGuards, with the same three required
    modules session_capture.py's equivalent class covers for its two."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _copy_hook_without(self, *missing):
        target = os.path.join(self.tmp.name, "partial-install")
        os.makedirs(target)
        names = (
            "memory_sync.py", "_identity.py", "_hook_runner.py",
            "_ingest_client.py", "_hook_state.py", "_redact.py",
        )
        for name in names:
            if name not in missing:
                shutil.copy(os.path.join(_HOOKS_DIR, name), target)
        return os.path.join(target, "memory_sync.py")

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
        script = self._copy_hook_without("_ingest_client.py")
        stdout, code, stderr = _run_hook("{}", script=script, want_stderr=True)
        self.assertEqual((stdout, code), (b"", 0))
        self.assertIn("_ingest_client", stderr)
        self.assertNotIn("Traceback", stderr)

    def _run_with_closed_stderr(self, script):
        r, w = os.pipe()
        os.close(r)
        run_env = _scrub_subprocess_env()
        proc = subprocess.Popen(
            [sys.executable, script],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=w, env=run_env,
        )
        os.close(w)
        stdout, _stderr = _communicate_kill_on_timeout(proc, b"{}", 20)
        return stdout, proc.returncode

    def test_without_identity_and_a_closed_stderr_pipe_still_exits_zero(self):
        script = self._copy_hook_without("_identity.py")
        stdout, code = self._run_with_closed_stderr(script)
        self.assertEqual((stdout, code), (b"", 0))

    def test_without_hook_runner_and_a_closed_stderr_pipe_still_exits_zero(self):
        script = self._copy_hook_without("_hook_runner.py")
        stdout, code = self._run_with_closed_stderr(script)
        self.assertEqual((stdout, code), (b"", 0))

    def test_without_ingest_client_and_a_closed_stderr_pipe_still_exits_zero(self):
        script = self._copy_hook_without("_ingest_client.py")
        stdout, code = self._run_with_closed_stderr(script)
        self.assertEqual((stdout, code), (b"", 0))

    def test_an_import_guard_with_fd_2_closed_before_the_interpreter_starts_puts_nothing_on_stdout(self):
        """fd 2 closed BEFORE the interpreter even starts: sys.stderr is
        None (not a stream), and a bare print(msg, file=None) would
        silently fall back to stdout -- the one channel a SessionEnd
        hook's contract requires to stay empty."""
        for missing in ("_identity.py", "_hook_runner.py", "_ingest_client.py"):
            with self.subTest(missing=missing):
                iter_tmp = tempfile.TemporaryDirectory()
                self.addCleanup(iter_tmp.cleanup)
                target = os.path.join(iter_tmp.name, "partial-install")
                os.makedirs(target)
                for name in (
                    "memory_sync.py", "_identity.py", "_hook_runner.py",
                    "_ingest_client.py", "_hook_state.py", "_redact.py",
                ):
                    if name != missing:
                        shutil.copy(os.path.join(_HOOKS_DIR, name), target)
                script = os.path.join(target, "memory_sync.py")
                run_env = _scrub_subprocess_env()
                proc = subprocess.Popen(
                    [sys.executable, script],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None,
                    env=run_env, preexec_fn=lambda: os.close(2),
                )
                stdout, _stderr = _communicate_kill_on_timeout(proc, b"{}", 20)
                self.assertEqual((stdout, proc.returncode), (b"", 0))


class TestStderrAndFd2Hygiene(unittest.TestCase):
    """Amendment A9-21: ``_hook_runner.guard_stderr()`` (installed at the
    top of ``main()``, before the work thread starts) must protect a
    worker-thread stderr write -- here, specifically, one made through
    ``_ingest_client`` itself (the lookup-page-full diagnostic) -- from
    turning into an unrecorded ``http_error``/exit 120, mirroring
    test_handoff_sync.py's own ``TestSubprocess`` closed-pipe tests."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state_dir = os.path.join(self.tmp.name, "state")

    def _popen_env(self, extra=None):
        run_env = _scrub_subprocess_env(extra)
        run_env["NEXUS_HOOK_STATE_DIR"] = self.state_dir
        return run_env

    def test_fd_2_closed_before_the_interpreter_starts_still_exits_zero_with_no_stdout(self):
        proc = subprocess.Popen(
            [sys.executable, _HOOK_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None, env=self._popen_env(),
            cwd=self.cwd, preexec_fn=lambda: os.close(2),
        )
        stdout, _stderr = _communicate_kill_on_timeout(proc, b"{not json", 20)
        self.assertEqual((stdout, proc.returncode), (b"", 0))

    def test_garbage_stdin_with_a_closed_stderr_pipe_still_exits_zero_and_records_the_run(self):
        r, w = os.pipe()
        os.close(r)
        proc = subprocess.Popen(
            [sys.executable, _HOOK_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=w, env=self._popen_env(),
            cwd=self.cwd,
        )
        os.close(w)
        stdout, _stderr = _communicate_kill_on_timeout(proc, b"{not json", 20)
        self.assertEqual((stdout, proc.returncode), (b"", 0))
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "memory-sync.json"))
        self.assertEqual(len(ledgers), 1, ledgers)
        with open(ledgers[0], encoding="utf-8") as fh:
            entry = json.load(fh)[-1]
        self.assertEqual((entry["ok"], entry["reason"]), (False, "unknown"))

    def test_a_real_run_with_stdout_read_end_closed_still_exits_zero(self):
        read_end, write_end = os.pipe()
        os.close(read_end)
        try:
            proc = subprocess.Popen(
                [sys.executable, _HOOK_SCRIPT],
                stdin=subprocess.PIPE, stdout=write_end, stderr=None,
                env=self._popen_env(), cwd=self.cwd, preexec_fn=lambda: os.close(2),
            )
            _stdout, _stderr = _communicate_kill_on_timeout(proc, b"{not json", 20)
        finally:
            os.close(write_end)
        self.assertEqual(proc.returncode, 0)
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "memory-sync.json"))
        self.assertEqual(len(ledgers), 1, ledgers)


class TestSubprocess(unittest.TestCase):
    """The real script, as a subprocess, against a HOME/state dir of its own."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state_dir = os.path.join(self.tmp.name, "state")

    def test_a_real_run_exits_zero_and_writes_not_configured(self):
        stdout, code = _run_hook(
            json.dumps({"cwd": self.cwd}), env={"NEXUS_HOOK_STATE_DIR": self.state_dir}
        )
        self.assertEqual((stdout, code), (b"", 0))
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "memory-sync.json"))
        self.assertEqual(len(ledgers), 1, ledgers)
        with open(ledgers[0], encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)[-1]["reason"], "not_configured")

    def test_garbage_stdin_exits_zero_and_records_a_failure(self):
        stdout, code = _run_hook("{not json", env={"NEXUS_HOOK_STATE_DIR": self.state_dir})
        self.assertEqual((stdout, code), (b"", 0))
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "memory-sync.json"))
        with open(ledgers[0], encoding="utf-8") as fh:
            entry = json.load(fh)[-1]
        self.assertEqual(entry["reason"], "unknown")
        self.assertFalse(entry["ok"])


# ════════════════════════════════════════════════════════════════════════
# post_implementation R1 -- per-cluster regression tests (K01..K27)
# ════════════════════════════════════════════════════════════════════════

class TestK01DirectoryListingFailure(_WriteCase):
    """A non-ENOENT directory-listing failure must never be treated as
    "every file was deleted"."""

    def test_a_listdir_permission_error_deletes_nothing_this_round(self):
        kept_path = self._write("kept")
        gone_path = self._write("gone")
        for slug, path in (("kept", kept_path), ("gone", gone_path)):
            st = os.stat(path)
            _hook_state.update_state_at(
                _MOD._memory_state_path(self.key),
                lambda s, slug=slug, path=path, st=st: {
                    **s, "cursor": 0, "reconciled": True,
                    "files": {**(s.get("files") or {}), slug: {
                        "mtime": st.st_mtime, "size": st.st_size,
                        "ctime": getattr(st, "st_ctime_ns", None),
                        "file_hash": _MOD._whole_file_hash(path),
                        "synced_at": "2026-01-01T00:00:00Z",
                        "redaction_fingerprint": _MOD._current_fingerprint(),
                    }},
                },
            )
        real_listdir = os.listdir

        def flaky_listdir(path):
            if path == self.memory_dir:
                raise PermissionError(13, "Permission denied")
            return real_listdir(path)

        with mock.patch.object(os, "listdir", side_effect=flaky_listdir):
            self._run()
        self.assertEqual([r["method"] for r in self.requests], [], self.requests)
        self.assertIn("kept", self._state().get("files", {}))
        self.assertIn("gone", self._state().get("files", {}))
        self.assertFalse(self._last_entry()["ok"])

    def test_a_listdir_io_error_under_root_also_deletes_nothing(self):
        """Mock twin of the chmod test above, so the behaviour is pinned
        under a root CI runner too (chmod 000 is a no-op for root)."""
        path = self._write("gone")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"gone": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path),
                "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        real_listdir = os.listdir

        def flaky_listdir(path):
            if path == self.memory_dir:
                raise OSError(5, "Input/output error")
            return real_listdir(path)

        with mock.patch.object(os, "listdir", side_effect=flaky_listdir):
            self._run()
        self.assertEqual(self.requests, [])
        self.assertIn("gone", self._state().get("files", {}))


class TestK01ZeroLocalFilesPendingDelete(_WriteCase):
    def test_zero_local_files_with_synced_entries_refuses_to_delete(self):
        """K01 point 3: a GENUINELY successful listing that comes back
        empty while state remembers synced entries is the same
        "directory resolution might be wrong" signal _reconcile_orphans
        already guards -- the pending-delete phase must refuse too, not
        only orphan reconciliation."""
        path = self._write("only")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"only": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path),
                "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        os.remove(path)
        self._run()
        self.assertEqual(self.requests, [])
        self.assertIn("only", self._state().get("files", {}))
        self.assertEqual(self._last_entry()["reason"], "orphan_guard")


class TestK02RoundFactsSurviveTheRecord(_WriteCase):
    def test_also_failed_is_present_even_empty_on_a_timeout_row(self):
        """Ruling item 6 (A9-20): main()'s own also_failed computation
        (R2-C12: now the ONLY computation, unconditional for every path,
        not just this abnormal-exit one) always sets the key (even to
        []), since _collect's own per-round reasons list may be abandoned
        mid-round. A REAL worker-thread abandonment -- every
        network call is itself deadline-aware (by design: that is what
        stops a slow SERVER from ever producing this scenario in
        production), so this pins it the only way left: a LOCAL,
        non-network call that blocks past the work budget, same shape as a
        pathological filesystem stall (NFS)."""
        self._write("f1")
        real_list = _MOD._list_memory_files

        def slow_list(memory_dir):
            time.sleep(2.0)
            return real_list(memory_dir)

        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.3), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_list_memory_files", side_effect=slow_list):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertIn("also_failed", entry)


class TestR2C03PerFileCursorPersistence(_WriteCase):
    """R2-C03 (A9-7 as actually ruled, post_implementation R2): the
    round's cursor and reconciled flag are persisted the INSTANT they are
    known -- per file for the cursor (merged into that file's own state
    entry write, or a cursor-only write for a deterministic local skip),
    and at reconciliation's own conclusion for the flag -- never deferred
    to a round-end write that runs AFTER the ledger row. The previous
    version did exactly the round-end persist the binding ruling's own
    text excludes (handoff_sync's own shape), which could starve a
    confirmed-synced file's cursor advance behind an unrelated, slow
    ledger write."""

    def test_the_cursor_is_on_disk_for_every_file_this_round_synced_before_the_ledger_row(self):
        """Seven new files, one more than ``_BATCH_SIZE`` -- the cursor
        stops at 5 (not wrapped to 0, which it WOULD be after a full lap
        syncs every file -- TestBatchingAndCursor's own wrap test already
        covers that case) -- so a cursor already correctly on disk is
        unambiguous."""
        for i in range(7):
            self._write(f"file{i}")
        self.backend.reply(*_empty_lookup())  # reconciliation
        for i in range(5):
            self.backend.reply(*_empty_lookup()).reply(*_created(f"m{i}"))
        seen = {}
        real_record_run = _hook_state.record_run

        def spy(*a, **kw):
            on_disk = _hook_state.read_state_at(_MOD._memory_state_path(self.key))[0]
            seen["cursor"] = on_disk.get("cursor")
            seen["files"] = sorted((on_disk.get("files") or {}).keys())
            return real_record_run(*a, **kw)

        with mock.patch.object(_hook_state, "record_run", side_effect=spy):
            self._run()
        self.assertEqual(seen.get("cursor"), 5, "by the time record_run runs, the cursor must already be on disk")
        self.assertEqual(seen.get("files"), [f"file{i}" for i in range(5)])

    def test_a_zero_file_round_makes_no_state_write_at_all(self):
        """The other half of the same fix (and K02's original motivation,
        post_implementation R1 finding K02): since reconciled/cursor are
        now written as they happen inside _collect rather than
        unconditionally at round end, a round that touches NOTHING does
        not call update_state_at even once -- the exact write that used
        to be able to take a contended lock and burn the whole work
        budget on a round that would not have changed a single byte."""
        path = self._write("f1")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"f1": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        calls = []
        real = _hook_state.update_state_at

        def spy(path, mutate):
            calls.append(path)
            return real(path, mutate)

        with mock.patch.object(_hook_state, "update_state_at", side_effect=spy):
            self._run()
        self.assertEqual(calls, [])
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "none")


class TestR2C05ReconciledPersistFailureFoldsIntoTheMainRow(_WriteCase):
    def test_a_reconciled_persist_failure_is_one_more_reason_not_a_followup_row(self):
        """R2-C05 (as redesigned by R2-C03): since reconciled is now
        persisted the instant it is known -- inside _collect, strictly
        before _record ever runs -- a genuine failure to persist it is
        simply one more reason this round produced, folded into the SAME
        row via _fold_persist_reasons; there is no longer a separate
        end-of-round write for it to fail AFTER the ledger row, so no
        more follow-up row either. orphans_deleted (a one-time,
        destructive fact) survives in also_failed regardless of which
        reason wins the scalar slot."""
        self._write("kept")
        row = _row(self._ext("gone"))
        self.backend.reply(200, _page(row))
        self.backend.reply(200, _page())  # ends the listing scan
        self.backend.reply(200, _page(row)).reply(204, None)  # the orphan delete itself
        self.backend.reply(*_empty_lookup()).reply(*_created())  # "kept": a new file

        real = _hook_state.update_state_at

        def selective(path, mutate):
            try:
                is_reconciled_write = (
                    "to_register" in mutate.__code__.co_varnames
                    and "reconciled" in mutate.__code__.co_consts
                )
            except Exception:
                is_reconciled_write = False
            if is_reconciled_write:
                current, _ = _hook_state.read_state_at(path)
                return current, ["state_write_failed"]
            return real(path, mutate)

        with mock.patch.object(_hook_state, "update_state_at", side_effect=selective):
            self._run()
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertEqual(len(entries), 1, entries)
        entry = entries[-1]
        self.assertEqual(entry["reason"], "state_write_failed")
        self.assertFalse(entry["ok"])
        self.assertIn("orphans_deleted", entry.get("also_failed", []))
        self.assertEqual(entry.get("orphans_deleted"), 1)
        # The disk write genuinely did not land: reconciled stays unset
        # (or False), so the next round retries reconciliation.
        self.assertFalse(self._state().get("reconciled"))


class TestR2C07OrphanDeletionCountSurvivesAbandonment(_WriteCase):
    def test_a_mid_batch_hang_during_orphan_deletion_still_reports_the_first_deletion(self):
        """R2-C07: each orphan deletion's count/reason is written straight
        into ``run`` the INSTANT it succeeds (mirroring ``_ingest_client.
        _dedup``'s own A8-6 pattern), not accumulated locally and reported
        only once ``_reconcile_orphans`` returns -- a worker thread
        abandoned mid-batch (a local stall unrelated to any one network
        call, e.g. a pathological filesystem) must not lose a deletion
        that had ALREADY happened before the stall."""
        self._write("kept")
        row_a = _row(self._ext("a"), row_id="aaaaaaaa-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000002Z")
        row_b = _row(self._ext("b"), row_id="bbbbbbbb-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000001Z")
        self.backend.reply(200, _page(row_a, row_b))
        self.backend.reply(200, _page())
        self.backend.reply(200, _page(row_a)).reply(204, None)  # a: deleted cleanly

        real_delete = _ingest_client.IngestClient.delete

        def hanging_delete(self_client, layer, external_id):
            if external_id == self._ext("b"):
                time.sleep(2.0)  # exceeds the shortened work budget below
            return real_delete(self_client, layer, external_id)

        # _MIN_REMAINING_SECONDS (3.0s) must also shrink: otherwise the
        # per-item budget check trips on the very FIRST orphan (there is
        # no scenario where a work budget is both > 3.0s, so the check
        # passes, and < 2.0s, so the hang still exceeds it).
        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.5), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 0.05), \
                mock.patch.object(_ingest_client.IngestClient, "delete", hanging_delete):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertEqual(entry.get("orphans_deleted"), 1, entry)
        self.assertIn("orphans_deleted", entry.get("also_failed", []))


class TestR2C10ReconcileAttributionAndForwardProgress(_WriteCase):
    def test_a_listing_failure_is_attributed_to_the_reconcile_stage(self):
        self._write("kept")
        self.backend.reply(500, {"detail": "boom"})
        self.backend.reply(*_empty_lookup()).reply(*_created())  # "kept": new file, independent phase
        self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "http_error")
        self.assertEqual(entry.get("reconcile", {}).get("reason"), "http_error")

    def test_the_pending_delete_zero_files_guard_records_how_many_were_blocked(self):
        path = self._write("only")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"only": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        os.remove(path)
        self._run()
        self.assertEqual(self._last_entry().get("pending_delete_guard"), 1)

    def test_a_server_that_ignores_the_cursor_and_repeats_the_same_page_is_caught(self):
        """Contract §6.2: a before_created_at/before_id pair the backend
        silently ignores would otherwise re-serve the identical first page
        forever -- without a forward-progress check, this hangs the round
        in an ever-growing, never-terminating list loop (bounded only by
        _RECONCILE_MAX_PAGES/the deadline) instead of being reported."""
        self._write("kept")
        same_row = _row(self._ext("a"))
        # The backend ignores the cursor and returns the identical page
        # every single time -- scripted twice is enough to prove the
        # SECOND page is where this stops, not the thousandth.
        self.backend.reply(200, _page(same_row))
        self.backend.reply(200, _page(same_row))
        # filter_suspect (like the existing per-row verification failure,
        # K04) does not abort the WHOLE round -- the regular sync batch is
        # an independent phase and still gets its own turn.
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "filter_suspect")
        listing_gets = [r for r in self.requests if r["method"] == "GET" and "external_id" not in r["query"]]
        self.assertEqual(len(listing_gets), 2, self.requests)
        self.assertFalse(self._state().get("reconciled"))


class TestR2C12AlsoFailedIsComputedForEveryLedgerRow(_WriteCase):
    """R2-C12: also_failed's computation lives in ONE place (main(), after
    `reason` is known, before `_record` runs) and applies to EVERY path
    that writes a ledger row -- not just the abnormal-exit branch. The
    previous version only computed it at the very end of _collect's own
    normal return, so a round that exited through an EARLIER return (not_
    configured, the K01 directory-listing failure, ...) wrote a row with
    no also_failed key at all."""

    def test_a_directory_listing_failure_still_carries_also_failed(self):
        kept_path = self._write("kept")
        gone_path = self._write("gone")
        for slug, path in (("kept", kept_path), ("gone", gone_path)):
            st = os.stat(path)
            _hook_state.update_state_at(
                _MOD._memory_state_path(self.key),
                lambda s, slug=slug, path=path, st=st: {
                    **s, "cursor": 0, "reconciled": True,
                    "files": {**(s.get("files") or {}), slug: {
                        "mtime": st.st_mtime, "size": st.st_size,
                        "ctime": getattr(st, "st_ctime_ns", None),
                        "file_hash": _MOD._whole_file_hash(path),
                        "synced_at": "2026-01-01T00:00:00Z",
                        "redaction_fingerprint": _MOD._current_fingerprint(),
                    }},
                },
            )
        real_listdir = os.listdir

        def flaky_listdir(path):
            if path == self.memory_dir:
                raise PermissionError(13, "Permission denied")
            return real_listdir(path)

        with mock.patch.object(os, "listdir", side_effect=flaky_listdir):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "unknown")
        self.assertIn("also_failed", entry)

    def test_not_configured_carries_also_failed_too(self):
        self._write("f1")
        self._run(NEXUS_API_URL="")
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "not_configured")
        self.assertIn("also_failed", entry)
        self.assertEqual(entry["also_failed"], [])


class TestK03AbortPropagation(_WriteCase):
    def test_a_pending_delete_abort_stops_reconciliation_from_starting(self):
        path = self._write("gone")
        self._write("kept")
        for slug, p in (("gone", path),):
            st = os.stat(p)
            _hook_state.write_state_at(
                _MOD._memory_state_path(self.key),
                {"cursor": 0, "reconciled": False, "files": {slug: {
                    "mtime": st.st_mtime, "size": st.st_size,
                    "ctime": getattr(st, "st_ctime_ns", None),
                    "file_hash": _MOD._whole_file_hash(p),
                    "synced_at": "2026-01-01T00:00:00Z",
                    "redaction_fingerprint": _MOD._current_fingerprint(),
                }}},
            )
        os.remove(path)
        # The pending-delete's own lookup GET is rate-limited: the round
        # must stop there, never reaching the reconciliation listing GET.
        self.backend.reply(429, {"detail": "slow down"})
        self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET"], self.requests)
        self.assertEqual(self._last_entry()["reason"], "rate_limited")

    def test_an_orphan_delete_failure_mid_batch_is_not_dropped_by_a_later_success(self):
        """The previous ``elif`` only ever kept ONE of "some rows got
        deleted" or "one reason why a row did not" -- both must survive.
        "kept" is seeded as ALREADY synced (own matching state entry) so
        neither the dirty scan nor the cursor walk has anything left to do
        with it -- an unscripted request for it would otherwise answer the
        backend's generic 599 fallback (also an http_error), masking
        whether THIS assertion is actually about the orphan delete path."""
        kept_path = self._write("kept")
        st = os.stat(kept_path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": False, "files": {"kept": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(kept_path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        row_a = _row(self._ext("a"), row_id="aaaaaaaa-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000002Z")
        row_b = _row(self._ext("b"), row_id="bbbbbbbb-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000001Z")
        self.backend.reply(200, _page(row_a, row_b))
        self.backend.reply(200, _page())
        self.backend.reply(200, _page(row_a)).reply(204, None)  # a: deleted
        self.backend.reply(500, {"detail": "boom"})  # b: lookup fails -> http_error
        self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "http_error")
        self.assertIn("orphans_deleted", entry.get("also_failed", []))
        self.assertEqual(entry.get("orphans_deleted"), 1)
        self.assertFalse(self._state().get("reconciled"))


class TestK04OrphanRowVerification(_WriteCase):
    def test_a_row_sharing_the_prefix_but_a_different_layer_is_not_deleted(self):
        self._write("mine")
        wrong_layer = _row(self._ext("sneaky"), extra={"layer": "session_summary"})
        self.backend.reply(200, _page(wrong_layer))
        self.backend.reply(200, _page())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(deletes, [])
        self.assertEqual(self._last_entry()["reason"], "filter_suspect")

    def test_a_row_sharing_the_prefix_but_a_different_container_is_not_deleted(self):
        self._write("mine")
        wrong_container = _row(self._ext("sneaky"), container_id="someone-elses-box")
        self.backend.reply(200, _page(wrong_container))
        self.backend.reply(200, _page())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        self.assertEqual([r for r in self.requests if r["method"] == "DELETE"], [])
        self.assertEqual(self._last_entry()["reason"], "filter_suspect")

    def test_a_non_abort_orphan_rejection_does_not_block_the_rest_of_the_batch(self):
        self._write("kept")
        row_a = _row(self._ext("a"), row_id="aaaaaaaa-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000002Z")
        row_b = _row(self._ext("b"), row_id="bbbbbbbb-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000001Z")
        self.backend.reply(200, _page(row_a, row_b))
        self.backend.reply(200, _page())
        self.backend.reply(200, _page(row_a)).reply(422, {"detail": "nope"})  # a: rejected, non-abort
        self.backend.reply(200, _page(row_b)).reply(204, None)  # b: still attempted and deleted
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        # Both orphans get a DELETE ATTEMPT (a's is rejected 422, b's
        # succeeds 204) -- the rejection must not stop b's own turn.
        delete_paths = [r["path"] for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(len(delete_paths), 2, self.requests)
        self.assertTrue(any("aaaaaaaa" in p for p in delete_paths), delete_paths)
        self.assertTrue(any("bbbbbbbb" in p for p in delete_paths), delete_paths)
        self.assertEqual(self._last_entry().get("orphans_deleted"), 1)
        self.assertFalse(self._state().get("reconciled"))


@unittest.skipIf(os.geteuid() == 0, "root ignores chmod 0; see the mock twin below")
class TestK05UnreadableFileDuringDirtyScan(_WriteCase):
    def test_an_unreadable_dirty_candidate_is_skipped_not_fatal(self):
        bad_path = self._write("bad", body="original bad body")
        good_path = self._write("good", body="original good body")
        for slug, path in (("bad", bad_path), ("good", good_path)):
            st = os.stat(path)
            _hook_state.update_state_at(
                _MOD._memory_state_path(self.key),
                lambda s, slug=slug, path=path, st=st: {
                    **s, "cursor": 0, "reconciled": True,
                    "files": {**(s.get("files") or {}), slug: {
                        "mtime": st.st_mtime, "size": st.st_size,
                        "ctime": getattr(st, "st_ctime_ns", None),
                        "file_hash": _MOD._whole_file_hash(path),
                        "synced_at": "2026-01-01T00:00:00Z",
                        "redaction_fingerprint": _MOD._current_fingerprint(),
                    }},
                },
            )
        # Both files are edited (so both are dirty-check CANDIDATES); "bad"
        # is then made unreadable so its own os.stat() fails mid-scan.
        with open(bad_path, "a", encoding="utf-8") as fh:
            fh.write("\n\nmore")
        with open(good_path, "a", encoding="utf-8") as fh:
            fh.write("\n\nmore")
        os.chmod(bad_path, 0)
        self.addCleanup(os.chmod, bad_path, 0o644)
        self.backend.reply(200, _page(_row(self._ext("good"), content_hash="sha256:" + "1" * 64))).reply(*_updated("m1"))
        self._run()
        self.assertFalse(self._last_entry()["ok"])
        self.assertEqual(self._last_entry()["reason"], "unknown")
        patches = [r for r in self.requests if r["method"] == "PATCH"]
        self.assertEqual(len(patches), 1, self.requests)
        self.assertIn("good", patches[0]["json"].get("content", "") or str(patches[0]["json"]))


class TestK05UnreadableFileMockTwin(_WriteCase):
    """Same scenario as TestK05UnreadableFileDuringDirtyScan, pinned under a
    root CI runner (chmod 000 is a no-op for root).

    R2-C14 (post_implementation R2): the twin mocks ``open()`` for the one
    candidate file, not ``os.stat()``. Non-root ``chmod 0`` on a regular
    file does not block ``os.stat`` at all (only directory SEARCH
    permission matters for stat-ing an entry inside it) -- the real
    failure, under both chmod-000 and root-equivalent conditions, is
    ``_whole_file_hash``'s own ``open(path, "rb")``. The previous twin
    mocked ``os.stat`` instead, which happened to land in the SAME
    ``except OSError:`` in ``_collect`` today (``_dirty_check`` calls
    ``os.stat`` first) but pins the WRONG call: a mutant that moves the
    hash computation's exception handling so it no longer shares that
    guard (verified: the previous twin stays falsely GREEN under exactly
    that mutant, while this one correctly goes red) would have shipped
    with the old twin never noticing."""

    def test_an_open_failure_during_the_dirty_scan_is_skipped_not_fatal(self):
        bad_path = self._write("bad", body="original bad body")
        good_path = self._write("good", body="original good body")
        for slug, path in (("bad", bad_path), ("good", good_path)):
            st = os.stat(path)
            _hook_state.update_state_at(
                _MOD._memory_state_path(self.key),
                lambda s, slug=slug, path=path, st=st: {
                    **s, "cursor": 0, "reconciled": True,
                    "files": {**(s.get("files") or {}), slug: {
                        "mtime": st.st_mtime, "size": st.st_size,
                        "ctime": getattr(st, "st_ctime_ns", None),
                        "file_hash": _MOD._whole_file_hash(path),
                        "synced_at": "2026-01-01T00:00:00Z",
                        "redaction_fingerprint": _MOD._current_fingerprint(),
                    }},
                },
            )
        with open(bad_path, "a", encoding="utf-8") as fh:
            fh.write("\n\nmore")
        with open(good_path, "a", encoding="utf-8") as fh:
            fh.write("\n\nmore")
        real_open = open

        def flaky_open(target, *a, **kw):
            if target == bad_path:
                raise PermissionError(13, "Permission denied")
            return real_open(target, *a, **kw)

        self.backend.reply(200, _page(_row(self._ext("good"), content_hash="sha256:" + "1" * 64))).reply(*_updated("m1"))
        with mock.patch.object(_MOD, "open", create=True, side_effect=flaky_open):
            self._run()
        self.assertFalse(self._last_entry()["ok"])
        self.assertEqual(self._last_entry()["reason"], "unknown")
        self.assertEqual(len([r for r in self.requests if r["method"] == "PATCH"]), 1, self.requests)


class TestK05CorruptStateShape(_WriteCase):
    def test_files_as_a_list_is_treated_as_absent_and_self_heals(self):
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key), {"cursor": 0, "reconciled": True, "files": ["not-a-dict"]},
        )
        self._write("f1")
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self._run()
        self.assertFalse(self._last_entry()["ok"])
        self.assertEqual(self._last_entry()["reason"], "unknown")
        self.assertIn("f1", self._state().get("files", {}))
        self.assertIsInstance(self._state()["files"], dict)

    def test_a_non_dict_entry_for_a_vanished_slug_is_tracked_not_dropped(self):
        """R2-C08: a per-entry shape defect (``files[slug]`` itself not a
        dict -- a hand edit, a partial write a crash interrupted) used to
        be silently DROPPED from ``files_state``, which made a ghost slug
        with no local file invisible to the vanished-file computation: it
        was never queued for DELETE, the server row survived forever, and
        every round kept reporting ``unknown`` for a shape that could
        never actually self-heal. Normalizing the bad entry to the SAME
        empty placeholder shape K22 already uses keeps the slug tracked:
        round 1 queues it for DELETE (closing the server row and healing
        the shape via `_drop_file_entry`), and round 2 is a clean run."""
        kept_path = self._write("kept")
        st = os.stat(kept_path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {
                "ghost": "corrupted-entry",
                "kept": {
                    "mtime": st.st_mtime, "size": st.st_size,
                    "ctime": getattr(st, "st_ctime_ns", None),
                    "file_hash": _MOD._whole_file_hash(kept_path),
                    "synced_at": "2026-01-01T00:00:00Z",
                    "redaction_fingerprint": _MOD._current_fingerprint(),
                },
            }},
        )
        self.backend.reply(200, _page(_row(self._ext("ghost")))).reply(204, None)
        self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET", "DELETE"], self.requests)
        self.assertFalse(self._last_entry()["ok"])
        self.assertEqual(self._last_entry()["reason"], "unknown")
        self.assertNotIn("ghost", self._state().get("files", {}))

        self.requests.clear()
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "none")


def _redact_text_that_always_grows_by(n):
    """A fake ``_redact.redact_text`` that grows whatever it is given by
    ``n`` characters, standing in for a real rule whose replacement MARKER
    is longer than the secret it replaces -- deterministic and independent
    of ``_redact.py``'s actual rule set (which the K06 cap-accounting fix
    does not depend on in any way), and without embedding anything
    secret-shaped in this test file."""

    def fake(text):
        return text + ("Y" * n), 1

    return fake


class TestK06ContentCapAccountsForRedaction(_WriteCase):
    def test_redaction_growth_near_the_cap_does_not_overflow_the_wire_limit(self):
        """A body that fits ``_CONTENT_CAP`` RAW but whose redacted form
        would exceed it (a real short secret replaced by a longer marker,
        simulated here with a fake grow-by-20 redactor) must still land
        within the backend's own ``content`` ``max_length=10000``."""
        body = "x" * 9995  # under _CONTENT_CAP raw; only the simulated post-redaction growth overflows it
        self._write("big", body=body)
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        with mock.patch.object(_redact, "redact_text", side_effect=_redact_text_that_always_grows_by(20)):
            self._run()
        posts = [r for r in self.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 1, self.requests)
        self.assertLessEqual(len(posts[0]["json"]["content"]), 10000)


class TestK07UnparsableFrontmatterEndToEnd(_WriteCase):
    def test_third_file_broken_frontmatter_fourth_still_processed(self):
        self._write("a1")
        self._write("a2")
        broken_path = os.path.join(self.memory_dir, "a3.md")
        with open(broken_path, "w", encoding="utf-8") as fh:
            fh.write("---\nname: a3\ndescription: unterminated\n\nbody with no closing fence\n")
        self._write("a4")
        self.backend.reply(*_empty_lookup())
        for _ in range(3):  # a1, a2, a4 each get a lookup+create; a3 makes none
            self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        posts = [r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"]
        self.assertEqual(sorted(posts), sorted(self._ext(s) for s in ("a1", "a2", "a4")))
        self.assertEqual(self._last_entry()["reason"], "file_unparsable")
        self.assertNotIn("a3", self._state().get("files", {}))
        for slug in ("a1", "a2", "a4"):
            self.assertIn(slug, self._state().get("files", {}))


class TestK08LedgerCountsAndFailedFiles(_WriteCase):
    def test_redaction_hit_count_reaches_the_ledger(self):
        self._write("plain", body="nothing secret here")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        with mock.patch.object(_redact, "redact_text", side_effect=_redact_text_that_always_grows_by(0)):
            self._run()
        entry = self._last_entry()
        self.assertGreaterEqual(entry.get("redacted") or 0, 1, entry)

    def test_dedup_merged_count_reaches_the_ledger(self):
        self._write("dup")
        dup_rows = _page(
            _row(self._ext("dup"), row_id="11111111-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000001Z"),
            _row(self._ext("dup"), row_id="22222222-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000002Z"),
        )
        self.backend.reply(*_empty_lookup())
        self.backend.reply(200, dup_rows).reply(204, None).reply(*_updated("m1"))
        self._run()
        entry = self._last_entry()
        self.assertEqual(entry.get("dedup_merged"), 1, entry)

    def test_an_unparsable_file_is_named_in_the_ledger(self):
        broken_path = os.path.join(self.memory_dir, "broken.md")
        with open(broken_path, "w", encoding="utf-8") as fh:
            fh.write("---\nunterminated\n\nbody")
        self.backend.reply(*_empty_lookup())
        self._run()
        entry = self._last_entry()
        failed = entry.get("failed") or []
        self.assertTrue(any(f.get("slug") == "broken" for f in failed), entry)

    def test_a_422_row_names_the_file_and_status(self):
        self._write("bad")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(422, {"detail": "nope"})
        self._run()
        entry = self._last_entry()
        failed = entry.get("failed") or []
        self.assertTrue(
            any(f.get("slug") == "bad" and f.get("status") == 422 for f in failed), entry
        )


class TestK09ConcurrentRunLock(_WriteCase):
    def test_a_concurrent_run_for_the_same_key_does_no_network_work(self):
        lock_fd = _MOD._acquire_run_lock(_MOD._memory_run_lock_path(self.key))
        self.assertIsNotNone(lock_fd)
        self.addCleanup(_MOD._release_run_lock, lock_fd)
        self._write("f1")
        self._run()
        self.assertEqual(self.requests, [])

    def test_a_peer_running_round_writes_no_ledger_row_of_its_own(self):
        """R2-C13: the loser's own ledger write and the winner's race --
        on a round where the WINNER did only LOCAL work (no network call
        at all, e.g. an orphan_guard trip), the two can land in either
        order, and since the reporter reads only the LAST row, the
        loser's harmless "nothing_to_do"/peer_running row could overwrite
        and bury the winner's real failure. A losing run writes NOTHING:
        a prior run's row (seeded here, standing in for the winner's)
        already covers this round, and there is no "stuck peer" to
        report either -- flock releases the instant that process exits."""
        _hook_state.record_run(_MOD.HOOK, ok=False, reason="orphan_guard", cwd=self.cwd)
        before, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        lock_fd = _MOD._acquire_run_lock(_MOD._memory_run_lock_path(self.key))
        self.addCleanup(_MOD._release_run_lock, lock_fd)
        self._write("f1")
        self._run()
        self.assertEqual(self.requests, [])
        after, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertEqual(after, before, "a peer-running round must not append its own ledger row")

    def test_the_lock_is_released_so_the_next_round_proceeds_normally(self):
        lock_fd = _MOD._acquire_run_lock(_MOD._memory_run_lock_path(self.key))
        self._write("f1")
        self._run()
        _MOD._release_run_lock(lock_fd)
        self.requests.clear()
        self.backend.reply(*_empty_lookup()).reply(*_empty_lookup()).reply(*_created())
        self._run()
        self.assertTrue(any(r["method"] == "POST" for r in self.requests))


class TestR2C02RunLockNonContentionOSError(unittest.TestCase):
    """R2-C02: ``_acquire_run_lock`` must treat only a REAL contention
    signal (``BlockingIOError``/EAGAIN/EWOULDBLOCK, what ``flock(...,
    LOCK_NB)`` actually raises when a peer holds the lock) as "a peer is
    running" -- any OTHER ``OSError`` (``ENOLCK``, ``EOPNOTSUPP``, a test
    double's ``EACCES``, ...) means the filesystem cannot do ``flock`` at
    all, which the function's own docstring says must degrade to
    proceeding WITHOUT a lock, not be mistaken for contention."""

    def test_enolck_degrades_to_the_no_run_lock_sentinel(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.run.lock")

            def raise_enolck(fd, op):
                raise OSError(_MOD.errno.ENOLCK, "No locks available")

            with mock.patch.object(_MOD.fcntl, "flock", side_effect=raise_enolck):
                result = _MOD._acquire_run_lock(path)
        self.assertEqual(result, _MOD._NO_RUN_LOCK)

    def test_a_real_blockingioerror_still_means_a_peer_holds_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.run.lock")

            def raise_blocking(fd, op):
                raise BlockingIOError(_MOD.errno.EAGAIN, "Resource temporarily unavailable")

            with mock.patch.object(_MOD.fcntl, "flock", side_effect=raise_blocking):
                result = _MOD._acquire_run_lock(path)
        self.assertIsNone(result)


class TestR2C02RunLockEndToEnd(_WriteCase):
    """The behavioural guarantee end to end: a filesystem that cannot
    flock at all must not silently stop every future round the way
    "contended" would. The injected failure is scoped to the RUN-LOCK fd
    specifically (by its own path, via /proc/self/fd) so the state
    file's own unrelated flock use (_hook_state._locked) is untouched."""

    def test_a_non_contention_flock_error_still_lets_the_round_do_its_work(self):
        self._write("f1")
        self.backend.reply(*_empty_lookup()).reply(*_empty_lookup()).reply(*_created())
        real_flock = _MOD.fcntl.flock

        def selective_enolck(fd, op):
            try:
                target = os.readlink(f"/proc/self/fd/{fd}")
            except OSError:
                target = ""
            if target.endswith(".run.lock") and (op & _MOD.fcntl.LOCK_NB):
                raise OSError(_MOD.errno.ENOLCK, "No locks available")
            return real_flock(fd, op)

        with mock.patch.object(_MOD.fcntl, "flock", side_effect=selective_enolck):
            self._run()
        self.assertTrue(any(r["method"] == "POST" for r in self.requests), self.requests)
        entry = self._last_entry()
        self.assertFalse(entry.get("peer_running"))
        self.assertEqual(entry.get("run_lock"), "unavailable")


class TestK10WorkerThreadStderrGuard(unittest.TestCase):
    """A5-21 / K10: ``guard_stderr()`` must protect a stderr write made ON
    THE WORKER THREAD itself -- specifically, through ``_ingest_client``'s
    own "lookup page full" diagnostic -- not only the main thread's own
    diagnostic prints (which is all ``TestStderrAndFd2Hygiene``'s existing
    garbage-stdin fixtures ever reach, since they fail before the worker
    thread calls into ``_ingest_client`` at all)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state_dir = os.path.join(self.tmp.name, "state")
        self.config_dir = os.path.join(self.tmp.name, "claude-config")
        self.backend = _Backend()
        self.addCleanup(self.backend.close)
        with mock.patch.object(_identity, "_resolved_root", return_value=(self.cwd, False)):
            self.key, _ = _identity.memory_dir_key(self.cwd)
        self.memory_dir = os.path.join(self.config_dir, "projects", self.key, "memory")
        os.makedirs(self.memory_dir)
        _write_memory_file(self.memory_dir, "dup")

    def _popen_env(self):
        run_env = _scrub_subprocess_env({
            "NEXUS_API_URL": self.backend.url, "NEXUS_HOOK_STATE_DIR": self.state_dir,
            "NEXUS_DEFAULT_USER_ID": USER, "CLAUDE_CONFIG_DIR": self.config_dir,
            "NEXUS_CONTAINER_ID": CONTAINER,  # a real subprocess: must match via env, not a same-process mock
        })
        return run_env

    def test_lookup_page_full_diagnostic_with_closed_stderr_does_not_become_http_error(self):
        full_page = _page(*[
            _row(f"{self.key}/dup", row_id=f"{i:08d}-1111-4111-8111-111111111111",
                 container_id=CONTAINER, created_at=f"2026-10-01T10:00:00.00000{i}Z")
            for i in range(5)
        ])
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(200, full_page)
        for _ in range(4):
            self.backend.reply(204, None)  # dedup deletes
        self.backend.reply(*_updated("m1"))  # final PATCH against the canonical row

        r, w = os.pipe()
        os.close(r)
        proc = subprocess.Popen(
            [sys.executable, _HOOK_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=w,
            env=self._popen_env(), cwd=self.cwd,
        )
        os.close(w)
        stdout, _stderr = _communicate_kill_on_timeout(
            proc, json.dumps({"cwd": self.cwd}).encode(), 20,
        )
        self.assertEqual((stdout, proc.returncode), (b"", 0))
        methods = [r["method"] for r in self.backend.requests]
        self.assertEqual(methods.count("DELETE"), 4, methods)
        self.assertEqual(methods.count("PATCH"), 1, methods)
        ledgers = glob.glob(os.path.join(self.state_dir, "*", "memory-sync.json"))
        self.assertEqual(len(ledgers), 1, ledgers)
        with open(ledgers[0], encoding="utf-8") as fh:
            entry = json.load(fh)[-1]
        self.assertNotEqual(entry["reason"], "http_error", entry)
        self.assertEqual(entry["reason"], "dedup_merged", entry)


class TestK11FingerprintChangeDirtiesEndToEnd(_WriteCase):
    def _matching_row(self, slug):
        path = os.path.join(self.memory_dir, f"{slug}.md")
        with open(path, encoding="utf-8") as fh:
            _, body = _MOD._split_memory_frontmatter(fh.read())
        content, _ = _MOD._cap_for_wire(body, 10000)
        redacted, _ = _redact.redact_text(content)
        digest = _ingest_client.content_hash(redacted)
        st = os.stat(path)
        meta = {
            "layer": "fact", "external_id": self._ext(slug), "container_id": CONTAINER,
            "content_hash": digest, "aria.memory_slug": slug, "aria.project": "proj",
            "aria.memory_dir": self.key, "aria.truncated": False,
            "aria.description": "a description", "aria.memory_type": "feedback",
            "aria.modified": _MOD._mtime_iso(st),
        }
        return _row(self._ext(slug), content_hash=digest, extra=meta)

    def test_a_redact_rule_change_dirties_every_synced_file_next_round(self):
        self._write("f1")
        self._write("f2")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self.backend.reply(*_empty_lookup()).reply(*_created("m2"))
        self._run()
        self.assertTrue(self._state().get("reconciled"))  # round 2 must not re-reconcile
        old_fp = self._state()["files"]["f1"]["redaction_fingerprint"]
        self.requests.clear()
        fake_redact_file = os.path.join(self.tmp.name, "fake_redact.py")
        with open(_redact.__file__, "rb") as fh:
            original = fh.read()
        with open(fake_redact_file, "wb") as fh:
            fh.write(original + b"\n# a deliberate marker comment\n")
        with mock.patch.object(_redact, "__file__", fake_redact_file):
            self.backend.reply(200, _page(self._matching_row("f1")))
            self.backend.reply(200, _page(self._matching_row("f2")))
            self._run()
            new_fp = _MOD._current_fingerprint()
        methods = [r["method"] for r in self.requests]
        self.assertEqual(methods, ["GET", "GET"], methods)  # re-verified, no PATCH needed
        self.assertEqual(self._last_entry()["reason"], "unchanged")
        state = self._state()
        self.assertEqual(state["files"]["f1"]["redaction_fingerprint"], new_fp)
        self.assertEqual(state["files"]["f2"]["redaction_fingerprint"], new_fp)
        self.assertNotEqual(new_fp, old_fp)
        # Round 3, SAME (fake) fingerprint still active -- nothing changed
        # since round 2's own recorded state: zero calls.
        self.requests.clear()
        with mock.patch.object(_redact, "__file__", fake_redact_file):
            self._run()
        self.assertEqual(self.requests, [])


class TestK12PerFileBudgetChecks(_WriteCase):
    """The 4 per-file budget-check sites, using a REAL small work budget
    plus a scripted server-side reply delay (``_Backend.reply(...,
    delay=...)``) so the budget runs out exactly between file N and file
    N+1 of a given phase -- ``test_budget_exhausted_stops_before_a_call_is_
    even_made`` only ever exercises the reconciliation-ENTRY check (the
    FIRST of the four). A real sleep, not a mocked clock: the budget
    mechanism also drives ``threading.Thread.join``'s own real-time
    deadline (``_hook_runner.run_with_deadline``), so mocking the global
    ``time.monotonic`` would desynchronise the two."""

    def test_dirty_set_budget_check_before_the_second_dirty_file(self):
        fp = _MOD._current_fingerprint()
        paths = {}
        for slug in ("a-dirty", "b-dirty"):
            paths[slug] = self._write(slug, body="original")
        state = {"cursor": 0, "reconciled": True, "files": {}}
        for slug, path in paths.items():
            state["files"][slug] = {
                "mtime": 1.0, "size": -1, "ctime": -1,  # force the fast path to miss -> recompute
                "file_hash": "sha256:" + "0" * 64, "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fp,
            }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        self.backend.reply(200, _page(_row(self._ext("a-dirty"), content_hash="sha256:" + "1" * 64)))
        self.backend.reply(200, {"memory_id": "m1"}, delay=1.6)
        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 2.0), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 0.5):
            self._run()
        self.assertEqual(self._last_entry()["reason"], "budget_exhausted")
        self.assertEqual(len([r for r in self.requests if r["method"] == "PATCH"]), 1, self.requests)
        # a-dirty's entry advanced (a real hash, not the seeded placeholder);
        # b-dirty's is untouched -- its own turn never started this round.
        self.assertNotEqual(self._state()["files"]["a-dirty"]["file_hash"], "sha256:" + "0" * 64)
        self.assertEqual(self._state()["files"]["b-dirty"]["file_hash"], "sha256:" + "0" * 64)

    def test_orphan_delete_budget_check_before_the_second_orphan(self):
        """R2-C07: ``orphans_deleted`` is now recorded the INSTANT row_a's
        delete succeeds -- BEFORE the budget check for row_b even runs --
        so by A8-6's own precedent (one-time destructive facts win a
        same-run tie against a later, non-priority-table failure)
        ``orphans_deleted`` is the scalar ``reason`` here, not
        ``budget_exhausted``; the budget boundary itself (exactly one
        delete attempted, the round stops there) is unchanged, and
        ``budget_exhausted`` still reaches the ledger via ``also_failed``."""
        self._write("kept")
        row_a = _row(self._ext("a"), row_id="aaaaaaaa-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000002Z")
        row_b = _row(self._ext("b"), row_id="bbbbbbbb-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000001Z")
        self.backend.reply(200, _page(row_a, row_b))
        self.backend.reply(200, _page())
        self.backend.reply(200, _page(row_a))
        self.backend.reply(204, None, delay=1.6)
        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 2.0), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 0.5):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "orphans_deleted")
        self.assertIn("budget_exhausted", entry.get("also_failed", []))
        self.assertEqual(entry.get("orphans_deleted"), 1)
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(len(deletes), 1, self.requests)
        self.assertFalse(self._state().get("reconciled"))


class TestK13OrphanGuardBoundary(_WriteCase):
    """Pure server-side orphans -- NO local file and NO prior state entry
    for any "gone" slug (mirrors the existing ``test_more_than_the_guard_
    ceiling_refuses_to_delete_anything`` pattern exactly): seeding local
    STATE for a slug with no local FILE would make it "vanished", pulling
    it into the unrelated pending-delete phase instead of orphan
    reconciliation -- a prior revision of these tests did that by mistake
    (seeded + then removed the same path) and every delete-lookup ended up
    answered by a stale, misaligned reply from a LATER phase's own script."""

    def _write_and_register(self, n, prefix="matched"):
        """``n`` files that are BOTH local (on disk) and already synced
        (own state entry matching real stat) -- i.e. correctly NOT orphans,
        used only to grow ``synced_count`` (the 20% ratio's denominator)
        without being vanished, dirty, or new."""
        fp = _MOD._current_fingerprint()
        slugs = [f"{prefix}{i:03d}" for i in range(n)]
        files = {}
        for slug in slugs:
            path = self._write(slug)
            st = os.stat(path)
            files[slug] = {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fp,
            }
        _hook_state.update_state_at(
            _MOD._memory_state_path(self.key),
            lambda s, files=files: {
                **s, "cursor": s.get("cursor", 0), "reconciled": s.get("reconciled", False),
                "files": {**(s.get("files") or {}), **files},
            },
        )
        return [_row(self._ext(slug)) for slug in slugs]

    def _orphan_rows(self, n, prefix="gone"):
        """``n`` rows for slugs with NO local file and NO state entry --
        pure server-side orphans. ``client.delete()`` does its OWN lookup
        before the DELETE verb, so each one needs its OWN scripted
        lookup+204 pair when the guard is expected to let it through."""
        return [
            _row(self._ext(f"{prefix}{i:03d}"), row_id=f"{i:08d}-1111-4111-8111-111111111111",
                 created_at=f"2026-10-01T10:00:00.{i:06d}Z")
            for i in range(n)
        ]

    def _script_deletes_for(self, n, prefix="gone"):
        for i in range(n):
            self.backend.reply(200, _page(_row(self._ext(f"{prefix}{i:03d}"))))
            self.backend.reply(204, None)

    def test_exactly_at_the_ceiling_deletes_everything(self):
        matched = self._write_and_register(80)
        orphans = self._orphan_rows(20)
        self.backend.reply(200, _page(*(matched + orphans)))
        self.backend.reply(200, _page())
        self._script_deletes_for(20)
        self._run()
        self.assertEqual(self._last_entry()["reason"], "orphans_deleted")
        self.assertEqual(self._last_entry().get("orphans_deleted"), 20)

    def test_one_more_than_the_ceiling_refuses_everything(self):
        matched = self._write_and_register(80)
        orphans = self._orphan_rows(21)
        self.backend.reply(200, _page(*(matched + orphans)))
        self.backend.reply(200, _page())
        self._run()
        self.assertEqual(self._last_entry()["reason"], "orphan_guard")
        self.assertEqual([r["method"] for r in self.requests if r["method"] == "DELETE"], [])

    def test_the_floor_of_5_decides_a_small_project_alone(self):
        """No "matched" rows at all (a small project): the 20% term never
        gets a chance to matter here -- the floor of 5 decides alone, and 5
        orphans (not 6, which the existing ceiling test already covers) is
        the other side of that same boundary."""
        self._write("kept")  # keeps the "zero local files" leg from tripping
        orphans = self._orphan_rows(5)
        self.backend.reply(200, _page(*orphans))
        self.backend.reply(200, _page())
        self._script_deletes_for(5)
        self.backend.reply(*_empty_lookup()).reply(*_created())  # "kept" itself: a brand-new local file
        self._run()
        self.assertEqual(self._last_entry()["reason"], "orphans_deleted")

    def test_foreign_prefixed_rows_never_dilute_the_denominator(self):
        """A large number of ANOTHER project's rows sharing no prefix with
        this one must not inflate ``synced_count`` and let a real ceiling
        breach through."""
        matched = self._write_and_register(1)
        orphans = self._orphan_rows(6)
        foreign_rows = [_row(f"some-other-project/f{i}") for i in range(30)]
        self.backend.reply(200, _page(*(foreign_rows + matched + orphans)))
        self.backend.reply(200, _page())
        self._run()
        self.assertEqual(self._last_entry()["reason"], "orphan_guard")
        self.assertEqual([r["method"] for r in self.requests if r["method"] == "DELETE"], [])


class TestK14SubdirectoryStart(unittest.TestCase):
    def test_starting_from_a_subdirectory_resolves_the_toplevels_key(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        repo = os.path.join(tmp.name, "repo")
        os.makedirs(repo)
        subprocess.run(["git", "init", "-q", repo], check=True)
        sub = os.path.join(repo, "a", "b")
        os.makedirs(sub)
        state_dir = os.path.join(tmp.name, "state")
        config_dir = os.path.join(tmp.name, "claude-config")
        key, degraded = _identity.memory_dir_key(repo)
        self.assertFalse(degraded)
        memory_dir = os.path.join(config_dir, "projects", key, "memory")
        os.makedirs(memory_dir)
        _write_memory_file(memory_dir, "f1")
        backend = _Backend()
        self.addCleanup(backend.close)
        backend.reply(*_empty_lookup()).reply(*_empty_lookup()).reply(*_created())
        env = {
            "NEXUS_API_URL": backend.url, "NEXUS_HOOK_STATE_DIR": state_dir,
            "NEXUS_DEFAULT_USER_ID": USER, "CLAUDE_CONFIG_DIR": config_dir,
        }
        _run_main(_MOD, {"cwd": sub}, env)
        posts = [r for r in backend.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 1, backend.requests)
        self.assertEqual(posts[0]["json"]["metadata"]["external_id"], f"{key}/f1")


class TestK18DirtySetRotation(_WriteCase):
    def test_a_real_content_edit_is_not_queued_behind_fingerprint_only_churn(self):
        fp_old = "sha256:" + "a" * 64
        files = {}
        paths = {}
        for i in range(4):
            slug = f"fp-only-{i}"
            paths[slug] = self._write(slug)
        real_slug = "zz-real-edit"
        paths[real_slug] = self._write(real_slug, body="original body")
        for slug, path in paths.items():
            st = os.stat(path)
            files[slug] = {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fp_old,
            }
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key), {"cursor": 0, "reconciled": True, "files": files},
        )
        self._write(real_slug, body="a genuinely different body now")
        # Only the real edit should get a lookup+PATCH within this round's
        # N=5 budget -- the 4 fingerprint-only files would otherwise eat
        # the whole batch alphabetically ("fp-only-*" sorts before
        # "zz-real-edit").
        self.backend.reply(
            200, _page(_row(self._ext(real_slug), content_hash="sha256:" + "0" * 64))
        ).reply(*_updated("m1"))
        for _ in range(4):
            self.backend.reply(*_empty_lookup())  # fp-only-N: unchanged after recompute
        self._run()
        patches = [r for r in self.requests if r["method"] == "PATCH"]
        self.assertEqual(len(patches), 1, self.requests)
        self.assertIn(real_slug, patches[0]["path"] + str(patches[0]["json"]))

    def test_a_persistently_failing_dirty_backlog_does_not_starve_new_files(self):
        """R2-C09: the previous version reserved one of this round's N
        slots for a new file whenever the dirty set alone would consume
        the whole batch -- which violates the BINDING TASK-006 acceptance
        list ("dirty set first", C row) and A8-2's "spread by N per round"
        exactly as written: a round with >= N genuinely dirty files must
        spend its whole batch on them, same as any other round. Five
        ALREADY-SYNCED files are genuinely edited (ordinary dirty, not a
        permanent rejection) alongside one brand-new file: round 1 is the
        dirty set's full batch (all five PATCHed, "aa-new" untouched);
        "aa-new" gets its own first try on round 2, once the backlog it
        was never entitled to cut in front of has cleared."""
        fp = _MOD._current_fingerprint()
        old_paths = {}
        for i in range(5):
            slug = f"dirty-{i}"
            old_paths[slug] = self._write(slug, body="original")
        files = {}
        for slug, path in old_paths.items():
            files[slug] = {
                "mtime": 1.0, "size": -1, "ctime": -1,  # force the fast path to miss every round
                "file_hash": "sha256:" + "0" * 64, "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fp,
            }
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key), {"cursor": 0, "reconciled": True, "files": files},
        )
        for slug in old_paths:
            self._write(slug, body="edited for real")
        self._write("aa-new")
        for i in range(5):
            self.backend.reply(
                200, _page(_row(self._ext(f"dirty-{i}"), content_hash="sha256:" + "0" * 64)),
            ).reply(*_updated(f"m{i}"))
        self._run()
        # A PATCH never carries `external_id` (an IDENTITY_KEY, §4) -- the
        # lookup GETs right before each one do, and are what pin WHICH
        # five files this round touched.
        looked_up = [r["query"]["external_id"] for r in self.requests if r["method"] == "GET"]
        self.assertEqual(sorted(looked_up), sorted(self._ext(f"dirty-{i}") for i in range(5)), self.requests)
        patched = [r for r in self.requests if r["method"] == "PATCH"]
        self.assertEqual(len(patched), 5, self.requests)
        posted = [r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"]
        self.assertEqual(posted, [], "the whole round 1 batch belongs to the dirty set, not the new file")
        self.assertNotIn("aa-new", self._state().get("files", {}))

        self.requests.clear()
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        posted_round2 = [r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"]
        self.assertEqual(posted_round2, [self._ext("aa-new")], self.requests)


class TestK19SingleFileRead(_WriteCase):
    def test_sync_file_opens_the_candidate_exactly_once(self):
        self._write("f1")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        real_open = open
        opens_of_f1 = []

        def counting_open(path, *a, **kw):
            if isinstance(path, str) and path.endswith("f1.md"):
                opens_of_f1.append(path)
            return real_open(path, *a, **kw)

        with mock.patch("builtins.open", side_effect=counting_open):
            self._run()
        self.assertEqual(len(opens_of_f1), 1, opens_of_f1)

    def test_synced_at_is_stamped_before_the_upsert_round_trip(self):
        before = _MOD._now_iso()
        self._write("f1")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        after = _MOD._now_iso()
        synced_at = self._state()["files"]["f1"]["synced_at"]
        self.assertGreaterEqual(synced_at, before)
        self.assertLessEqual(synced_at, after)


class TestK20PartialDeletePreservesMapping(_WriteCase):
    def test_a_partial_delete_failure_does_not_clear_the_local_mapping(self):
        path = self._write("gone")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"gone": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        os.remove(path)
        two_rows = _page(
            _row(self._ext("gone"), row_id="11111111-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000001Z"),
            _row(self._ext("gone"), row_id="22222222-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000002Z"),
        )
        self.backend.reply(200, two_rows).reply(204, None).reply(422, {"detail": "nope"})
        self._run()
        self.assertIn("gone", self._state().get("files", {}), "a partial delete must be retried, not forgotten")
        self.assertFalse(self._last_entry()["ok"])


class TestK21SameSizeContentRewrite(_WriteCase):
    def test_a_same_length_edit_with_mtime_rolled_back_is_still_dirty(self):
        path = self._write("f1", body="version-one-same-length-text")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": True, "files": {"f1": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        old_mtime = st.st_mtime
        with open(path, "r+", encoding="utf-8") as fh:
            text = fh.read()
            fh.seek(0)
            fh.write(text.replace("version-one", "version-TWO"))  # identical length
        os.utime(path, (old_mtime, old_mtime))
        new_st = os.stat(path)
        self.assertEqual(new_st.st_size, st.st_size)
        self.assertEqual(new_st.st_mtime, old_mtime)
        self.backend.reply(
            200, _page(_row(self._ext("f1"), content_hash="sha256:" + "0" * 64))
        ).reply(*_updated("m1"))
        self._run()
        patches = [r for r in self.requests if r["method"] == "PATCH"]
        self.assertEqual(len(patches), 1, self.requests)
        self.assertIn("version-TWO", patches[0]["json"]["content"])


class TestK22ReconciliationRegistersMatchedSlugs(_WriteCase):
    """A fresh/lost-state project: reconciliation's own listing already
    shows the server has a row for a local file with no state entry yet.
    Registering it immediately (rather than leaving it to the cursor walk,
    which could be many rounds away) makes it IMMEDIATELY eligible for the
    dirty scan too -- in a project this small that happens the SAME round,
    which is exactly the point: a file deleted before its own first turn is
    no longer a window reconciliation (one-time per state lifetime) can
    never close."""

    def _digest_for(self, slug):
        path = os.path.join(self.memory_dir, f"{slug}.md")
        with open(path, encoding="utf-8") as fh:
            _, body = _MOD._split_memory_frontmatter(fh.read())
        content, _ = _MOD._cap_for_wire(body, 10000)
        redacted, _ = _redact.redact_text(content)
        return _ingest_client.content_hash(redacted)

    def test_a_file_confirmed_by_reconciliation_is_registered_and_resolves_unchanged(self):
        self._write("f1")
        digest = self._digest_for("f1")
        st = os.stat(os.path.join(self.memory_dir, "f1.md"))
        meta = {
            "layer": "fact", "external_id": self._ext("f1"), "container_id": CONTAINER,
            "content_hash": digest, "aria.memory_slug": "f1", "aria.project": "proj",
            "aria.memory_dir": self.key, "aria.truncated": False,
            "aria.description": "a description", "aria.memory_type": "feedback",
            "aria.modified": _MOD._mtime_iso(st),
        }
        self.backend.reply(200, _page(_row(self._ext("f1"), content_hash=digest, extra=meta)))
        self.backend.reply(200, _page())  # ends the reconciliation scan
        # The SAME round's dirty-scan picks up the just-registered
        # placeholder (it has no file_hash of its own yet) and re-verifies
        # it against the server -- a second GET, resolving to "unchanged".
        self.backend.reply(200, _page(_row(self._ext("f1"), content_hash=digest, extra=meta)))
        self._run()
        self.assertIn("f1", self._state().get("files", {}))
        self.assertEqual([r["method"] for r in self.requests if r["method"] != "GET"], [])
        self.assertEqual(self._last_entry()["reason"], "unchanged")
        # The recorded entry now carries a real file_hash: a LATER round
        # (nothing changed) is a pure local scan, zero calls.
        self.requests.clear()
        self._run()
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "none")


class TestK23DoubleQuoteEscaping(unittest.TestCase):
    def test_an_escaped_quote_decodes_like_pyyaml(self):
        text = '---\ndescription: "say \\"hi\\" to me"\n---\n\nbody'
        fm, _ = _MOD._split_memory_frontmatter(text)
        self.assertEqual(fm["description"], 'say "hi" to me')

    def test_a_malformed_double_quoted_value_falls_back_to_strip_only(self):
        text = '---\ndescription: "unterminated \\x"\n---\n\nbody'
        fm, _ = _MOD._split_memory_frontmatter(text)
        self.assertIn("description", fm)  # must not raise; some string comes back

    def test_doubled_single_quotes_become_one_literal_quote(self):
        text = "---\ndescription: 'it''s fine'\n---\n\nbody"
        fm, _ = _MOD._split_memory_frontmatter(text)
        self.assertEqual(fm["description"], "it's fine")


class TestK24AssemblyFingerprintScope(unittest.TestCase):
    def test_fingerprint_changes_when_the_assembly_version_changes(self):
        original = _MOD._current_fingerprint()
        with mock.patch.object(_MOD, "_ASSEMBLY_VERSION", _MOD._ASSEMBLY_VERSION + 1):
            bumped = _MOD._current_fingerprint()
        self.assertNotEqual(original, bumped)


class TestK24AssemblyChangeDirtiesEndToEnd(_WriteCase):
    def test_an_assembly_version_bump_alone_dirties_synced_files(self):
        self._write("f1")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        self._run()
        self.requests.clear()
        with mock.patch.object(_MOD, "_ASSEMBLY_VERSION", _MOD._ASSEMBLY_VERSION + 1):
            self.backend.reply(
                200, _page(_row(self._ext("f1"), content_hash="sha256:" + "0" * 64))
            ).reply(*_updated("m1"))
            self._run()
        self.assertEqual([r["method"] for r in self.requests], ["GET", "PATCH"])


class TestK25StateSubdirCollisionIsKnownAndReported(unittest.TestCase):
    """K25's own fix (renaming the dotless "memory-sync" state subdirectory
    to one no project slug can ever produce) is deliberately NOT applied --
    the owner ruling's own corollary (X1, item 2) and its unchanged-items
    list (item 7) both give this exact path literally, including the
    dotless directory name, and a binding ruling is not this round's to
    rewrite. This test documents the collision as a known, CURRENT fact
    (not a regression guard for a fix that does not exist) -- see
    not_fixed / owner_questions for the K25 write-up."""

    def test_a_project_named_memory_sync_collides_with_the_state_subdir(self):
        self.assertEqual(_identity.normalize_slug("memory-sync"), _MOD._STATE_SUBDIR)
        self.assertEqual(_MOD._STATE_SUBDIR, _MOD.HOOK)


class TestK26ListReconciliationDeadlineBound(_WriteCase):
    def test_the_listing_response_body_is_read_through_the_deadline_bound_reader(self):
        """"f1" is seeded as ALREADY synced so the round makes exactly ONE
        request (the reconciliation listing itself) -- otherwise the
        upsert path's OWN (pre-existing, unrelated) use of
        ``_ingest_client._read_body`` for its lookup/POST would also be
        captured by the spy, which a mutant that reverted JUST the
        reconciliation listing back to a plain ``resp.read(N)`` would not
        be caught by."""
        path = self._write("f1")
        st = os.stat(path)
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key),
            {"cursor": 0, "reconciled": False, "files": {"f1": {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }}},
        )
        calls = []
        real = _ingest_client._read_body

        def spy(resp, deadline, cap, timeout):
            calls.append(deadline)
            return real(resp, deadline, cap, timeout)

        self.backend.reply(*_empty_lookup())  # the ONLY request this round makes
        with mock.patch.object(_ingest_client, "_read_body", side_effect=spy):
            self._run()
        self.assertEqual(len(self.requests), 1, self.requests)
        self.assertEqual(self.requests[0]["method"], "GET")
        self.assertNotIn("external_id", self.requests[0]["query"])  # the LISTING query, not a per-document lookup
        self.assertEqual(len(calls), 1, calls)
        self.assertIsNotNone(calls[0])


class TestK27Patch404Retries(_WriteCase):
    def test_a_404_on_patch_clears_the_mapping_and_reposts(self):
        self._write("f1", body="new content")
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(
            200, _page(_row(self._ext("f1"), content_hash="sha256:" + "9" * 64))
        )  # lookup finds a row...
        self.backend.reply(404, {"detail": "gone"})  # ...but the PATCH 404s (deleted concurrently)
        self.backend.reply(*_empty_lookup()).reply(*_created("m2"))  # retry: re-lookup, then POST
        self._run()
        methods = [r["method"] for r in self.requests]
        self.assertEqual(methods, ["GET", "GET", "PATCH", "GET", "POST"], methods)
        self.assertEqual(self._last_entry()["reason"], "none")
        self.assertIn("f1", self._state().get("files", {}))

    def test_a_second_404_on_the_retry_is_reported_normally(self):
        self._write("f1", body="new content")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(200, _page(_row(self._ext("f1"), content_hash="sha256:" + "9" * 64)))
        self.backend.reply(404, {"detail": "gone"})
        self.backend.reply(200, _page(_row(self._ext("f1"), content_hash="sha256:" + "9" * 64)))
        self.backend.reply(404, {"detail": "gone again"})
        self._run()
        self.assertFalse(self._last_entry()["ok"])
        self.assertNotIn("f1", self._state().get("files", {}))


class TestR2C04PatchRetryTriggersOnlyOnTheWritesOwn404(unittest.TestCase):
    """R2-C04: the retry must key off the WRITE call's own HTTP status
    (``write_status``), never ``status`` (the last status ANY call on this
    outcome received, including an unrelated dedup DELETE) -- a timeout on
    the PATCH itself, arriving right after a dedup delete that happened to
    404, must not be retried; and a dedup_merged fact from the first
    attempt must survive into a successful retry's own outcome."""

    class _FakeClient:
        def __init__(self, outcomes, remaining=100.0):
            self._outcomes = list(outcomes)
            self.calls = []
            self._remaining = remaining

        def upsert(self, layer, external_id, content, metadata, *, local_updated_at, updated_key):
            self.calls.append(external_id)
            return self._outcomes.pop(0)

        def remaining(self):
            return self._remaining

    @staticmethod
    def _make_outcome(*, aborts_reason, write_status, status, memory_id=None, dedup_merged=0, calls=1, action=None):
        out = _ingest_client.Outcome()
        out.status = status
        out.write_status = write_status
        out.memory_id = memory_id
        out.dedup_merged = dedup_merged
        out.calls = calls
        out.action = action
        if aborts_reason is not None:
            out.reasons = [aborts_reason]
        return out

    def test_a_write_timeout_right_after_a_dedup_404_is_not_retried(self):
        first = self._make_outcome(
            aborts_reason="timeout", write_status=None, status=404, memory_id="m1", dedup_merged=1,
        )
        client = self._FakeClient([first])
        result = _MOD._upsert_retrying_404(
            client, "fact", "k/f1", "content", {}, local_updated_at=None, updated_key="aria.modified",
        )
        self.assertEqual(len(client.calls), 1, "a non-404 write status must never retry")
        self.assertIs(result, first)
        self.assertEqual(result.dedup_merged, 1)

    def test_a_real_404_on_the_write_itself_still_retries(self):
        first = self._make_outcome(aborts_reason="http_error", write_status=404, status=404, memory_id="m1")
        second = self._make_outcome(aborts_reason=None, write_status=200, status=200, action="updated")
        client = self._FakeClient([first, second])
        result = _MOD._upsert_retrying_404(
            client, "fact", "k/f1", "content", {}, local_updated_at=None, updated_key="aria.modified",
        )
        self.assertEqual(len(client.calls), 2)
        self.assertIs(result, second)

    def test_dedup_merged_from_the_first_attempt_survives_a_successful_retry(self):
        first = self._make_outcome(
            aborts_reason="http_error", write_status=404, status=404, memory_id="m1",
            dedup_merged=1, calls=3,
        )
        second = self._make_outcome(aborts_reason=None, write_status=201, status=201, calls=2, action="created")
        client = self._FakeClient([first, second])
        result = _MOD._upsert_retrying_404(
            client, "fact", "k/f1", "content", {}, local_updated_at=None, updated_key="aria.modified",
        )
        self.assertEqual(result.dedup_merged, 1)
        self.assertIn("dedup_merged", result.reasons)
        self.assertEqual(result.calls, 5)

    def test_insufficient_budget_before_the_retry_does_not_retry(self):
        first = self._make_outcome(aborts_reason="http_error", write_status=404, status=404, memory_id="m1")
        client = self._FakeClient([first], remaining=0.5)
        with mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 3.0):
            result = _MOD._upsert_retrying_404(
                client, "fact", "k/f1", "content", {}, local_updated_at=None, updated_key="aria.modified",
            )
        self.assertEqual(len(client.calls), 1)
        self.assertIs(result, first)


class TestR2C11PageFullUsesRawPageLength(_WriteCase):
    """R2-C11: ``page_full`` must be judged on the RAW page length the
    lookup actually received (``outcome.page_rows``), never ``outcome.
    found`` (the VERIFIED count after ``_is_ours`` filtering) -- a full
    page where one row fails verification still means more duplicates may
    exist beyond it, so the mapping must be retained even though every
    VERIFIED row on this page was deleted."""

    def _seed_gone(self):
        """``kept`` stays on disk, correctly synced, so "gone" vanishing is
        an ordinary single vanished file -- not the K01 "zero local files"
        guard, which would block the delete attempt entirely and make
        these assertions pass for the wrong reason."""
        kept_path = self._write("kept")
        path = self._write("gone")
        entries = {}
        for slug, p in (("kept", kept_path), ("gone", path)):
            st = os.stat(p)
            entries[slug] = {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(p), "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": _MOD._current_fingerprint(),
            }
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key), {"cursor": 0, "reconciled": True, "files": entries},
        )
        os.remove(path)

    def test_a_full_raw_page_with_one_unverified_row_still_blocks_clearing(self):
        self._seed_gone()
        ours = [
            _row(self._ext("gone"), row_id=f"{i:08d}-1111-4111-8111-111111111111",
                 created_at=f"2026-10-01T10:00:00.00000{i}Z")
            for i in range(4)
        ]
        unverified = _row("another-project/gone", row_id="99999999-1111-4111-8111-111111111111")
        self.backend.reply(200, _page(*(ours + [unverified])))  # 5 raw rows, 4 ours
        for _ in range(4):
            self.backend.reply(204, None)
        self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(len(deletes), 4, self.requests)
        self.assertIn("gone", self._state().get("files", {}), "a full raw page must not clear the mapping")

    def test_a_full_page_fully_deleted_still_retains_the_mapping(self):
        """The other edge: every row on a FULL page is ours and gets
        deleted (found == LOOKUP_LIMIT == deleted) -- still retained,
        because a 6th duplicate could exist beyond this one page."""
        self._seed_gone()
        rows = [
            _row(self._ext("gone"), row_id=f"{i:08d}-1111-4111-8111-111111111111",
                 created_at=f"2026-10-01T10:00:00.00000{i}Z")
            for i in range(5)
        ]
        self.backend.reply(200, _page(*rows))
        for _ in range(5):
            self.backend.reply(204, None)
        self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(len(deletes), 5, self.requests)
        self.assertIn("gone", self._state().get("files", {}))


# ════════════════════════════════════════════════════════════════════════
# post_implementation fix round R3
# ════════════════════════════════════════════════════════════════════════

class TestR3T01ZeroRegularFilesGuardIgnoresIndeterminate(_WriteCase):
    """R3-T01 (post_implementation R3): R2-C01 widened orphan
    reconciliation's CANDIDATE set to ``set(local_files) | indeterminate``
    so an indeterminate slug could never look like an orphan -- but the
    SAME union also reached the zero-REGULAR-files guard (``if not
    local_slugs``), which exists to catch "something about directory
    resolution is wrong" (K01). A directory holding nothing but a dangling
    symlink (or an entry ``lstat`` cannot resolve) has ZERO regular files
    -- exactly the signal the guard exists for -- yet the widened union
    made it look non-empty, silently disabling the guard and letting
    every one of this project's own rows look like an orphan."""

    def test_a_dangling_symlink_alone_does_not_disable_the_zero_files_guard(self):
        linked_target = os.path.join(self.tmp.name, "nonexistent-target.md")
        os.symlink(linked_target, os.path.join(self.memory_dir, "stray.md"))
        rows = [
            _row(self._ext(s), row_id=f"{i:08d}-1111-4111-8111-111111111111")
            for i, s in enumerate(("a", "b", "c"))
        ]
        self.backend.reply(200, _page(*rows))
        self.backend.reply(200, _page())
        # Scripted in full (not just the listing) so a guard that FAILS to
        # trip is caught by an actual DELETE landing, not by an incidental
        # unscripted-request abort further down client.delete()'s own path.
        for row in rows:
            self.backend.reply(200, _page(row)).reply(204, None)
        self._run()
        self.assertEqual([r["method"] for r in self.requests if r["method"] == "DELETE"], [], self.requests)
        self.assertEqual(self._last_entry()["reason"], "orphan_guard")

    def test_an_lstat_failure_mock_twin_also_does_not_disable_the_zero_files_guard(self):
        """Mock twin of the dangling-symlink test above (so the behaviour
        is pinned without depending on a real symlink's own OS-level
        resolution failure)."""
        stray_path = os.path.join(self.memory_dir, "stray.md")
        with open(stray_path, "w", encoding="utf-8") as fh:
            fh.write("placeholder")
        real_lstat = os.lstat

        def flaky_lstat(path, *a, **kw):
            if path == stray_path:
                raise PermissionError(13, "Permission denied")
            return real_lstat(path, *a, **kw)

        rows = [
            _row(self._ext(s), row_id=f"{i:08d}-1111-4111-8111-111111111111")
            for i, s in enumerate(("a", "b", "c"))
        ]
        self.backend.reply(200, _page(*rows))
        self.backend.reply(200, _page())
        for row in rows:
            self.backend.reply(200, _page(row)).reply(204, None)
        with mock.patch.object(os, "lstat", side_effect=flaky_lstat):
            self._run()
        self.assertEqual([r["method"] for r in self.requests if r["method"] == "DELETE"], [], self.requests)
        self.assertEqual(self._last_entry()["reason"], "orphan_guard")

    def test_tracked_rows_are_not_soft_deleted_when_only_an_indeterminate_file_remains(self):
        """The compounding scenario (R3-S01): state already tracks x/y as
        synced, both vanished from disk, and the only thing left in the
        memory directory is a dangling symlink. The pending-delete guard
        (K01) already refuses to touch x/y for exactly this reason (zero
        REGULAR local files); reconciliation must reach the SAME refusal
        in the SAME round, not quietly soft-delete the very rows the
        other guard just refused to touch."""
        for slug in ("x", "y"):
            path = self._write(slug)
            st = os.stat(path)
            _hook_state.update_state_at(
                _MOD._memory_state_path(self.key),
                lambda s, slug=slug, path=path, st=st: {
                    **s, "cursor": 0,
                    "files": {**(s.get("files") or {}), slug: {
                        "mtime": st.st_mtime, "size": st.st_size,
                        "ctime": getattr(st, "st_ctime_ns", None),
                        "file_hash": _MOD._whole_file_hash(path),
                        "synced_at": "2026-01-01T00:00:00Z",
                        "redaction_fingerprint": _MOD._current_fingerprint(),
                    }},
                },
            )
            os.remove(path)
        os.symlink(os.path.join(self.tmp.name, "nonexistent.md"), os.path.join(self.memory_dir, "stray.md"))
        self.backend.reply(200, _page(_row(self._ext("x")), _row(self._ext("y"))))
        self.backend.reply(200, _page())
        # Scripted in full: pre-fix, only 2 orphans (<= max(5, 20%)) so the
        # RATIO guard does not trip either -- the zero-REGULAR-files guard
        # is the only thing that can stop this, and it must stop it before
        # a single DELETE is attempted.
        self.backend.reply(200, _page(_row(self._ext("x")))).reply(204, None)
        self.backend.reply(200, _page(_row(self._ext("y")))).reply(204, None)
        self._run()
        deletes = [r for r in self.requests if r["method"] == "DELETE"]
        self.assertEqual(deletes, [], self.requests)
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "orphan_guard")
        self.assertNotIn("orphans_deleted", entry)
        state = self._state()
        self.assertIn("x", state.get("files", {}))
        self.assertIn("y", state.get("files", {}))


class TestR3T03CursorAdvanceAndPlaceholderPersistAreCovered(_WriteCase):
    """R3-T03 (post_implementation R3, test_gap): the binding TASK-006
    acceptance item "a deterministic local error skips that file and
    still advances the cursor" and O1's own "a genuine state-write
    failure is never buried" both rely on two code paths
    (``_advance_cursor_only``, and the ``elif to_register:`` placeholder
    persist in ``_collect``) that no existing test exercised strongly
    enough to fail if either were deleted outright (verified by mutation
    on a scratch copy: both guts can be removed and the pre-R3 suite
    still passes in full)."""

    def test_a_deterministic_422_skip_advances_the_on_disk_cursor(self):
        self._write("f0")
        self._write("f1")
        self._write("f2")
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))  # f0: created
        self.backend.reply(*_empty_lookup()).reply(422, {"detail": "nope"})  # f1: rejected, skip+advance
        self.backend.reply(*_empty_lookup()).reply(
            403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "off"}}
        )  # f2: round-abort
        self._run()
        self.assertEqual(self._last_entry()["reason"], "ingest_disabled")
        self.assertEqual(self._state().get("cursor"), 2, "the cursor must sit AT f2, not wherever f1 left it")
        self.requests.clear()
        self.backend.reply(*_empty_lookup()).reply(*_created("m2"))
        self._run()
        posted = [r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"]
        self.assertEqual(posted, [self._ext("f2")], "the next round must resume AT f2, not wrap to f0")

    def test_a_locally_unparsable_file_also_advances_the_on_disk_cursor(self):
        self._write("f0")
        bad_path = self._write("f1")
        with open(bad_path, "wb") as fh:
            fh.write(b"\xff\xfe\x00not valid utf8 \x80\x81")
        self._write("f2")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(*_created("m0"))  # f0
        # f1: file_unparsable -- a LOCAL skip, no network call for f1 itself
        self.backend.reply(*_empty_lookup()).reply(
            403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "off"}}
        )  # f2: round-abort
        self._run()
        self.assertEqual(self._last_entry()["reason"], "ingest_disabled")
        self.assertEqual(self._state().get("cursor"), 2)

    def test_a_failed_cursor_only_persist_is_folded_into_the_round(self):
        """Pins the write INSIDE ``_advance_cursor_only`` itself (used
        only when a deterministic local skip has no state entry of its
        own to merge the advance into) -- not ``_sync_file``'s per-file
        merge path, which ``TestPerFileStateWriteFailure`` already
        covers."""
        self._write("f0")
        self._write("f1")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(422, {"detail": "nope"})  # f0: rejected, skip+advance
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))  # f1: created normally

        real_update = _hook_state.update_state_at

        def side_effect(path, mutate):
            if mutate.__qualname__ == "_advance_cursor_only.<locals>.<lambda>":
                current, _ = _hook_state.read_state_at(path)
                return current, ["state_write_failed"]
            return real_update(path, mutate)

        with mock.patch.object(_hook_state, "update_state_at", side_effect=side_effect):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "state_write_failed")
        self.assertIn("rejected_422", entry.get("also_failed", []))

    def test_a_failed_placeholder_persist_after_an_unsettled_reconciliation_is_folded_in(self):
        """Pins the ``elif to_register:`` branch: reconciliation does NOT
        conclude this round (the guard ceiling trips), but a matched slug
        was still confirmed -- the K22 placeholder write for it must
        still happen, and a genuine failure to persist it must still
        reach the round's own reasons/also_failed, exactly like every
        other per-file persist in this module."""
        self._write("kept")
        rows = [_row(self._ext(f"gone{i}"), row_id=f"{i:08d}-1111-4111-8111-111111111111")
                for i in range(6)]  # 6 orphans > max(5, 20%) -- guard trips, reconciliation unsettled
        kept_row = _row(self._ext("kept"))
        self.backend.reply(200, _page(*rows, kept_row))
        self.backend.reply(200, _page())

        real_update = _hook_state.update_state_at

        def side_effect(path, mutate):
            if mutate.__qualname__ == "_collect.<locals>.<lambda>":
                current, _ = _hook_state.read_state_at(path)
                return current, ["state_write_failed"]
            return real_update(path, mutate)

        with mock.patch.object(_hook_state, "update_state_at", side_effect=side_effect):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "state_write_failed")
        self.assertIn("orphan_guard", entry.get("also_failed", []))
        # The write genuinely failed -- self-healing (reconciliation is
        # retried next round, since it never reached `done`) is fine;
        # vanishing from the ledger is the bug this test pins.
        self.assertNotIn("kept", self._state().get("files", {}))


class TestR3T04FactsSurviveAbandonmentBeforeMerge(_WriteCase):
    """R3-T04 (post_implementation R3): two facts this round already
    knows at the INSTANT a worker thread is abandoned are not yet in
    ``run["reasons"]`` -- ``indeterminate`` (only turned into the
    "unknown" scalar reason at the very TAIL of ``_collect``) and a
    per-row orphan REJECTION (``_reconcile_orphans`` only merges its own
    locally-accumulated ``reasons`` into ``run["reasons"]`` once it
    returns, unlike a SUCCESSFUL deletion, which R2-C07 already writes
    straight into ``run`` the instant it happens). ``main()``'s own
    also_failed computation must not lose either one just because the
    round never reached the point that would normally have recorded it."""

    def test_an_indeterminate_file_is_still_reported_after_a_later_hang(self):
        os.symlink(
            os.path.join(self.tmp.name, "nonexistent-target.md"),
            os.path.join(self.memory_dir, "linked.md"),
        )
        real_lock = _MOD._acquire_run_lock

        def slow_lock(path):
            time.sleep(2.0)
            return real_lock(path)

        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.3), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_acquire_run_lock", side_effect=slow_lock):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertIn("linked", entry.get("unresolved_files", []))
        self.assertIn("unknown", entry.get("also_failed", []))

    def test_an_orphan_rejection_is_still_reported_after_a_later_orphan_hangs(self):
        self._write("kept")
        row_a = _row(self._ext("a"), row_id="aaaaaaaa-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000002Z")
        row_b = _row(self._ext("b"), row_id="bbbbbbbb-1111-4111-8111-111111111111",
                     created_at="2026-10-01T10:00:00.000001Z")
        self.backend.reply(200, _page(row_a, row_b))
        self.backend.reply(200, _page())
        self.backend.reply(200, _page(row_a)).reply(422, {"detail": "nope"})  # a: rejected, non-abort

        real_delete = _ingest_client.IngestClient.delete

        def hanging_delete(self_client, layer, external_id):
            if external_id == self._ext("b"):
                time.sleep(2.0)  # exceeds the shortened work budget below
            return real_delete(self_client, layer, external_id)

        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.5), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 0.05), \
                mock.patch.object(_ingest_client.IngestClient, "delete", hanging_delete):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertIn("rejected_422", entry.get("also_failed", []))


class TestR3T06FailedEntryAttribution(unittest.TestCase):
    """R3-T06 (post_implementation R3): ``_tally_result`` picked the
    FIRST failure-class reason in list order, not the one ``worst_reason``
    (the same rule the scalar ledger reason uses) would pick -- and
    ``dedup_merged`` always runs BEFORE the write call inside
    ``upsert()``, so it is always first whenever both occur, silently
    replacing the real abort reason in ``run["extra"]["failed"]``. The
    status recorded must likewise be the FAILING call's own status
    (R4-C4: ``decided_status``, generalised from ``write_status`` so a
    LOOKUP-phase failure also gets to name its own status -- see
    ``TestR4C4DecidedStatusAttribution`` below), never ``outcome.status``
    (clobbered by an EARLIER call -- a dedup DELETE, or the lookup GET --
    whenever the call that actually decided the outcome got no response
    at all). These three tests build ``Outcome`` objects by hand rather
    than through a real round, so each one also sets ``decided_status``
    itself, exactly as ``_ingest_client.py``'s own write/delete call sites
    do right alongside ``write_status`` (``upsert``'s POST/PATCH,
    ``_delete_row``) -- a real lookup-phase failure sets ONLY
    ``decided_status`` (``write_status`` stays ``None``), which the second
    test below already exercises without needing to say so, since both
    fields are ``None`` there regardless."""

    @staticmethod
    def _run_dict():
        return {"extra": {}}

    def test_a_dedup_then_http_error_write_reports_the_write_not_the_dedup(self):
        outcome = _ingest_client.Outcome()
        outcome.dedup_merged = 1
        outcome.status = 204  # the dedup DELETE's own status
        outcome.fail("dedup_merged")
        outcome.status = 500  # the write call itself: PATCH -> 500
        outcome.write_status = 500
        outcome.decided_status = 500  # R4-C4: the write call decided this outcome
        outcome.fail("http_error")
        run = self._run_dict()
        _MOD._tally_result(run, "f1", list(outcome.reasons), outcome)
        failed = run["extra"]["failed"]
        self.assertEqual(len(failed), 1, failed)
        self.assertEqual(failed[0]["reason"], "http_error")
        self.assertEqual(failed[0]["status"], 500)

    def test_a_dedup_404_then_write_timeout_does_not_report_the_dedups_404(self):
        outcome = _ingest_client.Outcome()
        outcome.dedup_merged = 1
        outcome.status = 404  # the dedup DELETE's own 404 (treated as success)
        outcome.fail("dedup_merged")
        outcome.write_status = None  # the write call itself got no response at all
        # decided_status stays None too: nothing decided this outcome with
        # an actual status to report (the real _ingest_client.py code sets
        # it to `None` here for the exact same reason it leaves
        # write_status at `None`).
        outcome.fail("timeout")
        run = self._run_dict()
        _MOD._tally_result(run, "f1", list(outcome.reasons), outcome)
        failed = run["extra"]["failed"]
        self.assertEqual(len(failed), 1, failed)
        self.assertEqual(failed[0]["reason"], "timeout")
        self.assertNotIn("status", failed[0])

    def test_a_plain_422_on_a_new_file_is_unaffected(self):
        """No regression on the existing, simpler shape (no dedup
        involved at all): the write's own status must still surface."""
        outcome = _ingest_client.Outcome()
        outcome.write_status = 422
        outcome.decided_status = 422  # R4-C4: the write call decided this outcome
        outcome.status = 422
        outcome.fail("rejected_422")
        run = self._run_dict()
        _MOD._tally_result(run, "bad", list(outcome.reasons), outcome)
        failed = run["extra"]["failed"]
        self.assertEqual(failed[0]["reason"], "rejected_422")
        self.assertEqual(failed[0]["status"], 422)


class TestR3T02PersistentStateReadFailureSelfHealsEndToEnd(_WriteCase):
    """R3-T02 (post_implementation R3), integration-level confirmation of
    the ``_hook_state.py`` fix (unit-pinned directly in
    ``test_hook_state.TestExplicitPathState``): a persistently unreadable
    state file must self-heal within this hook's OWN first per-file
    write, not refuse forever -- otherwise the cursor never advances past
    0 and this project's first 5 files are the only ones ever attempted,
    however many sessions actually run."""

    def test_an_eacces_state_file_self_heals_and_the_round_progresses(self):
        for i in range(7):
            self._write(f"f{i}")
        path = _MOD._memory_state_path(self.key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("{}")
        real_open = open
        real_replace = os.replace
        blocked = {"on": True}

        def flaky_open(target, *a, **kw):
            if target == path and blocked["on"]:
                raise PermissionError(13, "Permission denied")
            return real_open(target, *a, **kw)

        def spying_replace(src, dst, *a, **kw):
            result = real_replace(src, dst, *a, **kw)
            if dst == path:
                blocked["on"] = False
            return result

        self.backend.reply(*_empty_lookup())  # reconciliation, round 1
        for i in range(5):
            self.backend.reply(*_empty_lookup()).reply(*_created(f"m{i}"))
        with mock.patch.object(_hook_state, "open", create=True, side_effect=flaky_open), \
                mock.patch.object(_hook_state.os, "replace", side_effect=spying_replace):
            self._run()  # round 1: self-heals via its own first successful per-file write
            self.requests.clear()
            self.backend.reply(*_empty_lookup()).reply(*_created("m5"))
            self.backend.reply(*_empty_lookup()).reply(*_created("m6"))
            self._run()  # round 2: healed -- the cursor must have actually advanced
        posted = [r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"]
        self.assertEqual(posted, [self._ext("f5"), self._ext("f6")])


class TestR4C1OneTimeFactsSurviveAnAbandonedPersist(_WriteCase):
    """R4-C1 (fix round 4): ``_tally_result`` (dedup_merged / the per-file
    failed[] tally) and the dirty-scan loop's own OSError handler used to
    write into ``run["extra"]`` only AFTER that same file's own blocking
    per-file persist (``_hook_state.update_state_at``) returned. A worker
    thread abandoned while stuck inside THAT call -- a slow disk, a
    contended lock -- lost the fact entirely: the round's own ledger row
    already says ``timeout``, but a ONE-TIME, DESTRUCTIVE fact
    (``dedup_merged``: two rows already merged into one, server-side) or
    an already-seen dirty-scan error vanished from the record for good,
    because the call that would have recorded it simply never got to run.
    These three tests reproduce that shape directly: a hang is injected
    into the SPECIFIC call known to block, the work budget is shortened so
    the thread is genuinely abandoned (not merely slow), and the ledger
    row is read immediately afterward -- the fact must already be there."""

    def test_a_dedup_merged_fact_survives_a_hung_persist_right_after_it(self):
        """E3: "dup" just deduped (two rows -> one DELETE, dedup_merged=1)
        and its own write (PATCH) completed -- but persisting ITS OWN
        state entry hangs past the work budget. Before this fix,
        dedup_merged never reached run["extra"] at all (the call that
        writes it ran AFTER the now-hung persist), and it never reached
        also_failed either (that needs run["reasons"], only extended once
        _sync_file itself returns -- which an abandoned thread never
        does)."""
        self._write("dup")
        dup_rows = _page(
            _row(self._ext("dup"), row_id="11111111-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000001Z"),
            _row(self._ext("dup"), row_id="22222222-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000002Z"),
        )
        self.backend.reply(*_empty_lookup())  # reconciliation: nothing to see
        self.backend.reply(200, dup_rows).reply(204, None).reply(*_updated("m1"))

        real_update = _hook_state.update_state_at

        def hanging_update(path, mutate):
            if "dup" in (mutate.__defaults__ or ()):
                time.sleep(2.0)  # exceeds the shortened work budget below
            return real_update(path, mutate)

        # _MIN_REMAINING_SECONDS (3.0s) must also shrink: otherwise the
        # per-file budget check trips before "dup" is even attempted,
        # reporting budget_exhausted (a normal, non-abandoned return)
        # instead of ever reaching the hang (TestR2C07's own precedent).
        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.3), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 0.05), \
                mock.patch.object(_hook_state, "update_state_at", side_effect=hanging_update):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertEqual(entry.get("dedup_merged"), 1, entry)
        self.assertIn("dedup_merged", entry.get("also_failed", []))

    def test_a_dirty_scan_error_on_one_file_survives_a_later_files_hang(self):
        """E5: the dirty-scan loop hits a deterministic I/O error on "a"
        (recorded as a dirty_scan_errors entry) and then HANGS while
        hashing "b" -- past the work budget, the thread is abandoned.
        Before this fix, dirty_scan_errors was a local list only written
        into run["extra"] (and folded into the "unknown" scalar reason)
        once the WHOLE scan loop finished -- so the fact already known
        about "a" never reached the ledger row at all."""
        a_path = self._write("a", body="original a")
        b_path = self._write("b", body="original b")
        fingerprint = _MOD._current_fingerprint()
        state = {"cursor": 0, "reconciled": True, "files": {}}
        for slug, path in (("a", a_path), ("b", b_path)):
            st = os.stat(path)
            state["files"][slug] = {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path),
                "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fingerprint,
            }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        # Touch both files so the mtime+size fast path misses and
        # _dirty_check recomputes the whole-file hash for each -- "a"'s
        # own recompute hits a transient I/O error, "b"'s hangs.
        with open(a_path, "a", encoding="utf-8") as fh:
            fh.write("\n\nmore")
        with open(b_path, "a", encoding="utf-8") as fh:
            fh.write("\n\nmore")
        real_open = open

        def flaky_open(target, *a, **kw):
            if target == a_path:
                raise OSError(5, "Input/output error")
            if target == b_path:
                time.sleep(2.0)  # exceeds the shortened work budget below
            return real_open(target, *a, **kw)

        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.3), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "open", create=True, side_effect=flaky_open):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertEqual(entry.get("dirty_scan_errors"), ["a"], entry)
        self.assertIn("unknown", entry.get("also_failed", []))

    def test_a_confirmed_deletion_survives_a_later_files_hung_state_drop(self):
        """Same shape, on the pending-delete side (R4-C1's own optional
        item 4): "gone-a" is confirmed deleted server-side AND its own
        state-drop persist completes normally; "gone-b" is ALSO confirmed
        deleted server-side, but ITS OWN state-drop persist hangs. Before
        this fix, `deleted_count` was a local counter in _collect, only
        copied into run["extra"]["deleted"] once the WHOLE vanished-files
        loop finished -- so "gone-a"'s already-confirmed deletion was lost
        too, not just "gone-b"'s."""
        kept_path = self._write("kept")
        a_path = self._write("gone-a")
        b_path = self._write("gone-b")
        fingerprint = _MOD._current_fingerprint()
        state = {"cursor": 0, "reconciled": True, "files": {}}
        for slug, path in (("kept", kept_path), ("gone-a", a_path), ("gone-b", b_path)):
            st = os.stat(path)
            state["files"][slug] = {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path),
                "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fingerprint,
            }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        os.remove(a_path)
        os.remove(b_path)
        self.backend.reply(200, _page(_row(self._ext("gone-a")))).reply(204, None)
        self.backend.reply(200, _page(_row(self._ext("gone-b")))).reply(204, None)

        real_update = _hook_state.update_state_at

        def hanging_update(path, mutate):
            if "gone-b" in (mutate.__defaults__ or ()):
                time.sleep(2.0)  # exceeds the shortened work budget below
            return real_update(path, mutate)

        with mock.patch.object(_MOD, "_WORK_BUDGET_SECONDS", 0.3), \
                mock.patch.object(_MOD, "_DEADLINE_SLACK_SECONDS", 0.0), \
                mock.patch.object(_MOD, "_MIN_REMAINING_SECONDS", 0.05), \
                mock.patch.object(_hook_state, "update_state_at", side_effect=hanging_update):
            self._run()
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "timeout")
        self.assertEqual(entry.get("deleted"), 1, entry)


class TestR4C1ProductionTailRealKill(unittest.TestCase):
    """R4-C1's own required subprocess coverage: an in-process test cannot
    show that an abandoned worker thread is actually KILLED -- the daemon
    thread in TestR4C1OneTimeFactsSurviveAnAbandonedPersist's own tests
    keeps running, unobserved, until the whole TEST PROCESS eventually
    exits, which overstates how long a late write could plausibly still be
    "in flight" in production. A real subprocess, ended by
    ``_hook_runner.finish``'s ``os._exit(0)``, shows the real bound: the
    process (hung thread included) is gone well before the 2-second hang
    this test injects could ever complete."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.path.join(self.tmp.name, "proj")
        os.makedirs(self.cwd)
        self.state_dir = os.path.join(self.tmp.name, "state")
        self.config_dir = os.path.join(self.tmp.name, "claude-config")
        self.key, _degraded = _identity.memory_dir_key(self.cwd)
        self.memory_dir = os.path.join(self.config_dir, "projects", self.key, "memory")
        os.makedirs(self.memory_dir)
        self.backend = _Backend()
        self.addCleanup(self.backend.close)

    def _build_hanging_copy(self):
        """A throwaway copy of every hook sibling, with ``memory_sync.py``'s
        own work budget shortened by TEXT substitution (there is no live
        process to ``mock.patch`` across) and a tiny, env-var-gated hang
        appended to the COPIED ``_hook_state.py`` -- never the real one --
        so ``update_state_at`` sleeps for the one call whose ``mutate``
        closes over the slug named by ``MEMORY_SYNC_TEST_HANG_SLUG``,
        exactly the same ``mutate.__defaults__`` technique the in-process
        tests above use via ``mock.patch``."""
        target = os.path.join(self.tmp.name, "slow-copy")
        os.makedirs(target)
        names = (
            "memory_sync.py", "_identity.py", "_hook_runner.py",
            "_ingest_client.py", "_hook_state.py", "_redact.py",
        )
        for name in names:
            shutil.copy(os.path.join(_HOOKS_DIR, name), os.path.join(target, name))

        script_path = os.path.join(target, "memory_sync.py")
        with open(script_path, encoding="utf-8") as fh:
            source = fh.read()
        new_source = (
            source
            .replace("_WORK_BUDGET_SECONDS = 20.0", "_WORK_BUDGET_SECONDS = 0.5")
            .replace("_DEADLINE_SLACK_SECONDS = 1.0", "_DEADLINE_SLACK_SECONDS = 0.0")
            # Otherwise the per-file budget check trips before "dup" is
            # even attempted (TestR2C07's own precedent, same reasoning as
            # the in-process hang tests above).
            .replace("_MIN_REMAINING_SECONDS = 3.0", "_MIN_REMAINING_SECONDS = 0.05")
        )
        assert new_source != source, "the budget constants were not found to shorten"
        with open(script_path, "w", encoding="utf-8") as fh:
            fh.write(new_source)

        hook_state_path = os.path.join(target, "_hook_state.py")
        with open(hook_state_path, "a", encoding="utf-8") as fh:
            fh.write(
                "\n\n"
                "# TEST-ONLY HANG INJECTION -- appended to a throwaway copy by\n"
                "# test_memory_sync.py's own TestR4C1ProductionTailRealKill; never\n"
                "# present in the real hooks/_hook_state.py. Activated only when\n"
                "# MEMORY_SYNC_TEST_HANG_SLUG is set, so this file otherwise behaves\n"
                "# exactly like the real one.\n"
                "import os as _test_hang_os\n"
                "\n"
                '_TEST_HANG_SLUG = _test_hang_os.environ.get("MEMORY_SYNC_TEST_HANG_SLUG")\n'
                "if _TEST_HANG_SLUG:\n"
                "    import time as _test_hang_time\n"
                "\n"
                "    _real_update_state_at_for_test = update_state_at\n"
                "\n"
                "    def update_state_at(path, mutate):  # noqa: F811 - test-only override\n"
                "        if _TEST_HANG_SLUG in (mutate.__defaults__ or ()):\n"
                "            _test_hang_time.sleep(2.0)\n"
                "        return _real_update_state_at_for_test(path, mutate)\n"
            )
        return script_path

    def test_a_dedup_merged_fact_survives_a_real_kill_of_the_hung_persist(self):
        _write_memory_file(self.memory_dir, "dup")
        dup_rows = _page(
            _row(f"{self.key}/dup", row_id="11111111-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000001Z"),
            _row(f"{self.key}/dup", row_id="22222222-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000002Z"),
        )
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(200, dup_rows).reply(204, None).reply(*_updated("m1"))

        script = self._build_hanging_copy()
        run_env = _scrub_subprocess_env({
            "NEXUS_API_URL": self.backend.url,
            "NEXUS_HOOK_STATE_DIR": self.state_dir,
            "NEXUS_DEFAULT_USER_ID": USER,
            "NEXUS_CONTAINER_ID": CONTAINER,  # else the real hostname -- mismatches the scripted rows' metadata
            "CLAUDE_CONFIG_DIR": self.config_dir,
            "MEMORY_SYNC_TEST_HANG_SLUG": "dup",
        })
        started = time.monotonic()
        result = subprocess.run(
            [sys.executable, script],
            input=json.dumps({"cwd": self.cwd}).encode(),
            capture_output=True,
            timeout=20,
            env=run_env,
        )
        elapsed = time.monotonic() - started
        self.assertEqual((result.stdout, result.returncode), (b"", 0))
        # The injected hang is 2.0s; a real kill via os._exit(0) must end
        # the process well before that -- an in-process test cannot show
        # this at all (the thread survives until the whole TEST process
        # exits, not this one hook run).
        self.assertLess(elapsed, 1.5, "the process waited for the hung thread instead of being killed")

        ledgers = glob.glob(os.path.join(self.state_dir, "*", "memory-sync.json"))
        self.assertEqual(len(ledgers), 1, ledgers)
        with open(ledgers[0], encoding="utf-8") as fh:
            entry = json.load(fh)[-1]
        self.assertEqual(entry["reason"], "timeout")
        self.assertEqual(entry.get("dedup_merged"), 1, entry)
        self.assertIn("dedup_merged", entry.get("also_failed", []))


class TestR4C3MidRoundStateRebuildIsVisible(_WriteCase):
    """R4-C3 (fix round 4): R3-T02 made a persistently-unreadable state
    file self-heal when the round's OWN opening read already finds
    EACCES/EPERM -- correct, and still covered by
    TestR3T02PersistentStateReadFailureSelfHealsEndToEnd above. But the
    SAME rebuild-from-``{}`` branch also fires when the file is perfectly
    healthy at the START of a round and only becomes unreadable to a
    LATER per-file write within the SAME round (a lock/ownership race) --
    there, R3-T02's own premise ("this uid will never read it again")
    does not hold, and the rebuild silently drops ``reconciled``, every
    OTHER file's own entry, and any pending-delete bookkeeping, while the
    round's own ledger row reads completely clean (correctly, per owner
    ruling item 4 -- a repaired file whose write still landed is not
    itself a failure; this fix is pure visibility, not a reversal of
    that)."""

    def test_a_mid_round_permission_flip_is_counted_not_silently_absorbed(self):
        f1_path = self._write("f1")
        f2_path = self._write("f2")
        fingerprint = _MOD._current_fingerprint()
        state = {"cursor": 0, "reconciled": True, "files": {}}
        for slug, path in (("f1", f1_path), ("f2", f2_path)):
            st = os.stat(path)
            state["files"][slug] = {
                "mtime": st.st_mtime, "size": st.st_size,
                "ctime": getattr(st, "st_ctime_ns", None),
                "file_hash": _MOD._whole_file_hash(path),
                "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fingerprint,
            }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        # f1 is edited so it is this round's one dirty candidate; f2 stays
        # untouched (clean, and never revisited this round).
        self._write("f1", body="edited body")
        self.backend.reply(
            200, _page(_row(self._ext("f1"), content_hash="sha256:" + "1" * 64)),
        ).reply(*_updated("m1"))

        state_path = _MOD._memory_state_path(self.key)
        real_open = open
        read_count = {"n": 0}

        def flaky_open(target, *a, **kw):
            if target == state_path:
                read_count["n"] += 1
                if read_count["n"] > 1:  # 1 = this round's own opening read (clean)
                    raise PermissionError(13, "Permission denied")
            return real_open(target, *a, **kw)

        with mock.patch.object(_hook_state, "open", create=True, side_effect=flaky_open):
            self._run()
        entry = self._last_entry()
        # The round's own ok/reason stay clean -- R3-T02's self-heal is
        # correct behaviour, not something this fix reverses.
        self.assertEqual(entry["reason"], "none")
        self.assertTrue(entry["ok"])
        self.assertEqual(entry.get("state_rebuilt"), 1, entry)
        # The rebuild genuinely happened (same as before this fix) -- f2's
        # own entry and `reconciled` are gone; this test only pins that
        # the fact is now VISIBLE, not that the rebuild itself is undone
        # (a separate, owner-flagged question outside this cluster).
        self.assertNotIn("f2", self._state().get("files", {}))

    def test_an_initially_corrupt_state_file_does_not_count_as_a_mid_round_rebuild(self):
        """The ALREADY-accepted R3-T02 shape (the round's own opening read
        is what finds the trouble) must NOT also increment
        ``state_rebuilt`` -- that counter is specifically for a flip
        AFTER a clean start, not for the self-heal continuing to resolve
        brokenness the round already knew about from its very first
        read."""
        self._write("f1")
        state_path = _MOD._memory_state_path(self.key)
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as fh:
            fh.write("{}")
        real_open = open

        def flaky_open(target, *a, **kw):
            if target == state_path:
                raise PermissionError(13, "Permission denied")
            return real_open(target, *a, **kw)

        self.backend.reply(*_empty_lookup()).reply(*_empty_lookup()).reply(*_created("m1"))
        with mock.patch.object(_hook_state, "open", create=True, side_effect=flaky_open):
            self._run()
        entry = self._last_entry()
        self.assertNotIn("state_rebuilt", entry)


class TestR4C4DecidedStatusAttribution(_WriteCase):
    """R4-C4 (fix round 4): a LOOKUP-phase failure (never reaches
    upsert's/delete's own write call at all -- ``_refused`` on the lookup
    itself, "2xx but not a memory list", ``filter_suspect``) left
    ``write_status`` at ``None`` forever, so ``_tally_result``'s old
    ``if outcome.write_status is not None`` guard never set ``status`` on
    that file's own ``failed[]`` entry -- a regression from 87a54df, which
    read the plain (and differently-scoped) ``status`` field and got this
    particular case right by accident. ``decided_status``
    (``_ingest_client.py``) fixes the attribution; these are its
    end-to-end confirmations, plus the companion fix (``dedup_merged``
    must not win a file's own ``reason`` slot over a genuine write
    rejection it has no status in common with)."""

    def test_a_new_files_lookup_failure_still_names_its_own_status(self):
        self._write("f1")
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(500, {"detail": "boom"})  # f1's own lookup itself fails
        self._run()
        entry = self._last_entry()
        failed = entry.get("failed") or []
        self.assertTrue(
            any(f.get("slug") == "f1" and f.get("status") == 500 for f in failed), entry
        )

    def test_a_pending_deletes_lookup_failure_also_names_its_own_status(self):
        kept_path = self._write("kept")  # avoids the K01 zero-local-files guard
        fingerprint = _MOD._current_fingerprint()
        kst = os.stat(kept_path)
        state = {
            "cursor": 0, "reconciled": True,
            "files": {
                "kept": {"mtime": kst.st_mtime, "size": kst.st_size,
                         "ctime": getattr(kst, "st_ctime_ns", None),
                         "file_hash": _MOD._whole_file_hash(kept_path),
                         "synced_at": "2026-01-01T00:00:00Z", "redaction_fingerprint": fingerprint},
                "gone": {"mtime": 1.0, "size": 1, "ctime": None,
                         "file_hash": "sha256:" + "0" * 64,
                         "synced_at": "2026-01-01T00:00:00Z", "redaction_fingerprint": fingerprint},
            },
        }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)
        self.backend.reply(401, {"detail": "nope"})  # "gone"'s own delete-lookup fails
        self._run()
        entry = self._last_entry()
        failed = entry.get("failed") or []
        self.assertTrue(
            any(f.get("slug") == "gone" and f.get("status") == 401 for f in failed), entry
        )

    def test_a_dedup_then_a_422_write_reports_the_rejection_not_the_dedup(self):
        self._write("dup")
        dup_rows = _page(
            _row(self._ext("dup"), row_id="11111111-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000001Z"),
            _row(self._ext("dup"), row_id="22222222-1111-4111-8111-111111111111",
                 created_at="2026-10-01T10:00:00.000002Z"),
        )
        self.backend.reply(*_empty_lookup())  # reconciliation
        self.backend.reply(200, dup_rows).reply(204, None).reply(422, {"detail": "nope"})
        self._run()
        entry = self._last_entry()
        self.assertEqual(entry.get("dedup_merged"), 1, entry)  # the fact is still counted
        failed = entry.get("failed") or []
        self.assertTrue(
            any(
                f.get("slug") == "dup" and f.get("reason") == "rejected_422" and f.get("status") == 422
                for f in failed
            ),
            entry,
        )


class TestModuleCleanupOrdering(unittest.TestCase):
    """Mirrors test_handoff_sync.py's / test_session_capture.py's own class
    of the same name: unittest.case.doModuleCleanups runs every registered
    module cleanup LIFO but re-raises only the FIRST exception it collects,
    silently dropping the rest -- so this file's own setUpModule must
    register _assert_stderr_not_left_wrapped BEFORE _assert_home_untouched."""

    def test_the_home_leak_check_is_registered_after_the_stderr_check(self):
        import inspect
        source = inspect.getsource(setUpModule)
        stderr_pos = source.index("_assert_stderr_not_left_wrapped")
        home_pos = source.index("_assert_home_untouched, home")
        self.assertLess(stderr_pos, home_pos)


if __name__ == "__main__":
    unittest.main(verbosity=2)
