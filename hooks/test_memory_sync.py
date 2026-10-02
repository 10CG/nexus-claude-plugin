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
        self._write("bad")
        self._write("zzz-good")
        self.backend.reply(*_empty_lookup())
        self.backend.reply(*_empty_lookup()).reply(422, {"detail": "nope"})
        self.backend.reply(*_empty_lookup()).reply(*_created())
        self._run()
        methods = [r["method"] for r in self.requests]
        # Both files are ATTEMPTED (a 422 does not abort the round); only
        # "zzz-good" actually lands.
        self.assertEqual(methods.count("POST"), 2, methods)
        self.assertEqual(self._last_entry()["reason"], "rejected_422")
        state = self._state()
        self.assertNotIn("bad", state.get("files", {}))
        self.assertIn("zzz-good", state.get("files", {}))

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

    def _fail_first_update(self):
        """Patches ``_hook_state.update_state_at`` so its FIRST call (the
        per-file persist under test) returns a genuine failure without
        writing anything, while every LATER call (the round-end
        cursor/reconciled persist) runs for real -- a transient failure on
        exactly one write, not a wholesale breakage of the primitive."""
        real = _hook_state.update_state_at
        calls = []

        def side_effect(path, mutate):
            calls.append(path)
            if len(calls) == 1:
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
        with self._fail_first_update():
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
        with self._fail_first_update():
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
    def test_a_round_end_persist_failure_is_a_followup_row_not_a_lost_main_row(self):
        """K02 (ruling item 4 / the handoff_sync-converged shape): the
        round's cursor/reconciled advance now happens in ``_record``,
        AFTER the main ledger row -- a genuine failure to persist it must
        not retroactively corrupt that already-written row; it is a
        SEPARATE follow-up row."""
        self._write("f1")
        self.backend.reply(*_empty_lookup())  # reconciliation: sets new_reconciled True
        self.backend.reply(*_empty_lookup()).reply(*_created("m1"))
        real = _hook_state.update_state_at

        # Fail only the ROUND-END persist, recognised by its own mutate
        # closure's parameter names (every PER-FILE persist's mutate closes
        # over "slug"/"entry"/"to_register" instead -- see memory_sync.py's
        # own _mutate in _record) -- not by call order, which this fix is
        # explicitly allowed to rearrange.
        def selective(path, mutate):
            try:
                is_round_end = "cursor" in mutate.__code__.co_varnames
            except Exception:
                is_round_end = False
            if is_round_end:
                current, _ = _hook_state.read_state_at(path)
                return current, ["state_write_failed"]
            return real(path, mutate)

        with mock.patch.object(_hook_state, "update_state_at", side_effect=selective):
            self._run()
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertGreaterEqual(len(entries), 2, entries)
        main_row, followup = entries[-2], entries[-1]
        self.assertEqual(main_row["reason"], "none")
        self.assertTrue(main_row["ok"])
        self.assertEqual(followup["reason"], "state_write_failed")
        self.assertFalse(followup["ok"])
        # The file itself DID sync (the per-file persist was not touched).
        self.assertIn("f1", self._state().get("files", {}))

    def test_also_failed_is_present_even_empty_on_a_timeout_row(self):
        """K02 ruling item 3: main()'s own abnormal-exit branch always
        sets also_failed (even to []), since _collect's own normal-path
        computation never ran. A REAL worker-thread abandonment -- every
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
    root CI runner (chmod 000 is a no-op for root) by mocking os.stat for
    the one candidate file."""

    def test_an_os_stat_failure_during_the_dirty_scan_is_skipped_not_fatal(self):
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
        real_stat = os.stat

        def flaky_stat(path, *a, **kw):
            if path == bad_path:
                raise PermissionError(13, "Permission denied")
            return real_stat(path, *a, **kw)

        self.backend.reply(200, _page(_row(self._ext("good"), content_hash="sha256:" + "1" * 64))).reply(*_updated("m1"))
        with mock.patch.object(os, "stat", side_effect=flaky_stat):
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
        entry = self._last_entry()
        self.assertTrue(entry["ok"])
        self.assertEqual(entry["reason"], "nothing_to_do")
        self.assertTrue(entry.get("peer_running"))

    def test_the_lock_is_released_so_the_next_round_proceeds_normally(self):
        lock_fd = _MOD._acquire_run_lock(_MOD._memory_run_lock_path(self.key))
        self._write("f1")
        self._run()
        _MOD._release_run_lock(lock_fd)
        self.requests.clear()
        self.backend.reply(*_empty_lookup()).reply(*_empty_lookup()).reply(*_created())
        self._run()
        self.assertTrue(any(r["method"] == "POST" for r in self.requests))


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
        self.assertEqual(self._last_entry()["reason"], "budget_exhausted")
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
        fp = _MOD._current_fingerprint()
        files = {}
        for i in range(5):
            slug = f"poison-{i}"
            self._write(slug)
            files[slug] = {
                "mtime": 1.0, "size": -1, "ctime": -1,  # always dirty (forces recompute every round)
                "file_hash": "sha256:" + "0" * 64, "synced_at": "2026-01-01T00:00:00Z",
                "redaction_fingerprint": fp,
            }
        _hook_state.write_state_at(
            _MOD._memory_state_path(self.key), {"cursor": 0, "reconciled": True, "files": files},
        )
        self._write("aa-new")
        for _ in range(4):  # 4 of the 5 poisoned dirty files get a slot (each REJECTED, still a POST attempt)...
            self.backend.reply(*_empty_lookup()).reply(422, {"detail": "poison"})
        self.backend.reply(*_empty_lookup()).reply(*_created())  # ...the reserved slot goes to the new file
        self._run()
        posts = [r["json"]["metadata"]["external_id"] for r in self.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 5, self.requests)  # 4 rejected attempts + aa-new's own
        self.assertIn(self._ext("aa-new"), posts, "the new file must get its slot THIS round, not starve")
        self.assertNotIn(self._ext("poison-4"), posts, "the 5th poison file is left for a later round")
        self.assertIn("aa-new", self._state().get("files", {}))
        self.assertEqual(self._last_entry()["reason"], "rejected_422")


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
