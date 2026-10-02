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

    def test_unterminated_block_is_empty(self):
        fm, body = _MOD._split_memory_frontmatter("---\ndescription: d\nno closing fence")
        self.assertEqual(fm, {})

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
        self.assertEqual(_MOD._list_memory_files(os.path.join(self.tmp.name, "nope")), {})

    def test_memory_index_is_excluded_case_insensitively(self):
        self._touch("MEMORY.md")
        self._touch("memory.md")  # a project that happens to use lowercase
        self._touch("feedback_x.md")
        out = _MOD._list_memory_files(self.memory_dir)
        self.assertEqual(out, {"feedback_x": os.path.join(self.memory_dir, "feedback_x.md")})

    def test_non_md_files_are_ignored(self):
        self._touch("notes.txt")
        self._touch("a.md")
        out = _MOD._list_memory_files(self.memory_dir)
        self.assertEqual(list(out), ["a"])

    def test_slug_is_the_filename_without_the_extension(self):
        self._touch("project_nexus_prod_deployed.md")
        out = _MOD._list_memory_files(self.memory_dir)
        self.assertEqual(list(out), ["project_nexus_prod_deployed"])

    def test_a_subdirectory_is_not_a_candidate(self):
        os.makedirs(os.path.join(self.memory_dir, "sub.md"))
        self._touch("real.md")
        out = _MOD._list_memory_files(self.memory_dir)
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
        st = os.stat(path)
        state = {
            "cursor": 0, "reconciled": True,
            "files": {slug: {"mtime": st.st_mtime, "size": st.st_size,
                              "file_hash": _MOD._whole_file_hash(path),
                              "synced_at": "2026-01-01T00:00:00Z",
                              "redaction_fingerprint": _MOD._current_fingerprint()}},
        }
        _hook_state.write_state_at(_MOD._memory_state_path(self.key), state)

    def test_a_vanished_file_is_deleted_and_the_mapping_clears(self):
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
        path = self._write("gone")
        st = os.stat(path)
        state = {
            "cursor": 0, "reconciled": True,
            "files": {"gone": {"mtime": st.st_mtime, "size": st.st_size,
                                "file_hash": _MOD._whole_file_hash(path),
                                "synced_at": "2026-01-01T00:00:00Z",
                                "redaction_fingerprint": _MOD._current_fingerprint()}},
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
