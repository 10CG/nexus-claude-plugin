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

import builtins
import glob
import http.server
import importlib.util
import inspect
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


def _assert_stderr_not_left_wrapped():
    """R5-c03 regression guard: `_run_main` patches `sys.stderr` back to
    whatever it was on entry (see its own docstring), so nothing calling
    `mod.main()` THROUGH it should be able to leave this module's global
    `sys.stderr` permanently replaced with a `_StderrGuard`. A future test
    that calls `mod.main()` directly, bypassing `_run_main`, would not be
    caught by that fix alone -- this is the module-wide backstop."""
    if isinstance(sys.stderr, _MOD._StderrGuard):
        raise AssertionError(
            "a test left sys.stderr wrapped in _StderrGuard -- every OTHER "
            "test_*.py module run in this same `unittest discover` process "
            "(this file sorts first, alphabetically) would inherit it"
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
    # R6-c03: registration order matters here, not just presence -- LIFO
    # means whichever addModuleCleanup call is LAST in this function's own
    # source order fires FIRST, and unittest.case.doModuleCleanups runs
    # every registered cleanup but re-raises only the FIRST exception it
    # collects, silently dropping the rest (confirmed empirically,
    # TestModuleCleanupOrdering below). `_assert_stderr_not_left_wrapped`
    # must therefore be registered BEFORE `_assert_home_untouched`, so the
    # HOME assertion -- a hermeticity violation, the more actionable of
    # the two -- is the one whose error survives if both ever fail on the
    # same run, rather than being swallowed by the other. An earlier
    # revision of this function registered them in the opposite order,
    # with a "LIFO: this runs first" comment on `_assert_home_untouched`
    # that the registration order right above it made false.
    unittest.addModuleCleanup(_assert_stderr_not_left_wrapped)
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

    Returns ``mod.main()``'s own return value (R3-c13): the value
    ``__main__`` uses to choose between ``sys.exit(0)`` and
    ``os._exit(0)`` (see ``_hook_runner.finish``) was previously discarded
    here, so nothing in this test module could tell a regression that
    dropped it (e.g. ``main`` returning only ``left_behind``, silently
    forgetting a ledger-write that was itself left behind) from correct
    behaviour.

    ``sys.stderr`` is patched to ITSELF (R5-c03), not left untouched: the
    real ``mod.main()`` permanently replaces the GLOBAL ``sys.stderr``
    with a ``_StderrGuard`` the first time it runs in this process
    (R4-c06) and never restores it -- with nothing here undoing that,
    every test in every OTHER ``test_*.py`` module, run in the SAME
    process via ``unittest discover``, used to execute with ``sys.stderr``
    silently wrapped from this file's very first ``_run_main`` call
    onward (``test_handoff_sync.py`` sorts FIRST, alphabetically, among
    this plugin's eleven test files, R6-c08: a count that has already
    gone stale once as files were added -- ``ls hooks/test_*.py`` is the
    source of truth, not this number). ``mock.patch.object`` restores whatever
    ``sys.stderr`` was at ENTRY, regardless of what ``main()`` did to it
    meanwhile -- the same isolation pattern ``os.environ`` gets from
    ``mock.patch.dict`` two lines below, just for an attribute instead of
    a mapping.
    """
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


class _BrokenStderr:
    """A stand-in for ``sys.stderr`` whose ``write`` always raises
    ``BrokenPipeError`` -- what a real closed pipe (the host process has
    already exited) looks like to a ``print(..., file=sys.stderr)`` call
    (R2-c05)."""

    def write(self, *args, **kwargs):
        raise BrokenPipeError("stderr closed")

    def flush(self):
        raise BrokenPipeError("stderr closed")


_PYTHON_PATH_VARS = ("PYTHONPATH", "PYTHONHOME")


def _scrub_subprocess_env(extra=None):
    """The environment a real hook subprocess runs under: every ``NEXUS_*``
    var dropped (callers set exactly the ones a fixture needs), plus
    ``PYTHONPATH`` / ``PYTHONHOME`` (R4-c15).

    The latter two matter specifically for ``TestImportGuards``' partial-
    install copies: those directories deliberately OMIT one sibling module
    to make ``import`` fail. If the developer machine (or a future CI
    image) happens to have ``PYTHONPATH`` set to this real ``hooks/``
    directory, the "missing" module imports anyway via that fallback --
    the guard never fires, `stderr` comes back empty, and an assertion
    checking for the guard's own text goes RED for a reason that has
    nothing to do with a regression in the guard itself (confirmed
    empirically: with ``PYTHONPATH`` pointed at the real ``hooks/``, all
    three ``TestImportGuards`` text assertions failed with an empty
    `stderr`, and the run ALSO tripped this module's own home-hygiene
    check, because the hook then actually ran for real instead of exiting
    immediately on the intended import failure). A test that depends on a
    module genuinely being absent must not inherit a search path that
    puts it back.
    """
    run_env = {
        k: v for k, v in os.environ.items()
        if not k.startswith("NEXUS_") and k not in _PYTHON_PATH_VARS
    }
    if extra:
        run_env.update(extra)
    return run_env


def _run_hook(stdin_text, env=None, want_stderr=False, script=None):
    """Drive the real script as a subprocess; return (stdout_bytes, exit_code).

    ``script`` (R1-c31) overrides which file is run -- a partial-install
    copy in a temp dir, for the import-guard tests, instead of the real
    ``_HOOK_SCRIPT``.
    """
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
    """``proc.communicate(payload, timeout=timeout)``, but a regression
    that makes the hook hang fails FAST and leaves no orphan (R4-c13).

    The three real-closed-pipe subprocess tests below cannot use
    ``subprocess.run``'s own ``timeout=`` (its cleanup already kills the
    child) -- they need the Popen object alive afterward to read
    ``returncode``. A bare ``try: communicate(timeout=) finally: proc.wait
    (timeout=)`` looks safe but is not: on a genuine timeout,
    ``communicate`` raises ``TimeoutExpired`` with the child STILL
    running, and the ``finally``'s own ``proc.wait(timeout=)`` then raises
    a SECOND ``TimeoutExpired`` (which, being raised inside ``finally``,
    REPLACES the first one -- confirmed empirically: this shape takes 2x
    the timeout to fail and leaves the child alive afterward). Killing the
    child explicitly on timeout, then re-raising, is what actually reports
    the hang quickly and does not leak a process.
    """
    try:
        return proc.communicate(payload, timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)
        raise
    finally:
        proc.wait(timeout=timeout)


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

    def test_a_tie_in_updated_at_is_broken_by_filename(self):
        """R3-c11: `_locate`'s fallback keeps the FIRST candidate that beats
        the running maximum (strict `>`), and `_candidates`'s trailing
        `sorted(out)` is what makes that first-seen order deterministic
        (the filesystem's own `os.listdir` order is not, and is NOT
        reliably reproducible from a test, which is exactly why
        `os.listdir` is mocked directly below rather than trusted to hand
        back names in creation order). `z-owned-by-other.md` sorts AFTER
        `a-owned-by-me.md`, so it must lose the tie -- even when the
        directory listing itself hands it back FIRST -- even though
        nothing else distinguishes them. An earlier version of this test
        wrote the two files in `z`-then-`a` order and trusted the real
        filesystem to preserve it; on this system (and most modern ones)
        `os.listdir` does not, so dropping `sorted()` still passed (this
        mock is what actually pins the OS-independent claim)."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "z-owned-by-other.md", updated_at="2026-09-20T00:00:00Z")
        _write_handoff(self.handoff_dir, "a-owned-by-me.md", updated_at="2026-09-20T00:00:00Z")
        with mock.patch.object(
            _MOD.os, "listdir", return_value=["z-owned-by-other.md", "a-owned-by-me.md"]
        ):
            self.assertEqual(_MOD._locate(self.handoff_dir), ("a-owned-by-me.md", None))

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
        shaped or stat-shaped, as long as SOME per-entry OSError surfaces.

        R4-c01: `os.lstat` is mocked with the SAME error here, not left
        real -- the original fixture only mocked `os.stat`, so the
        fallback `os.lstat` call underneath ran unmocked against a
        perfectly ordinary, readable file and always succeeded, landing
        the entry in `undecidable` regardless of whether the code's OWN
        `except` on the lstat call was narrow (`FileNotFoundError`) or
        broad (any `OSError`) -- the two only disagree when lstat ITSELF
        also fails, which this fixture never exercised. Parametrized over
        three non-FileNotFoundError errnos (EACCES / EIO / ESTALE): the
        365fc39 regression made the broad `except OSError: continue`
        swallow all three as a silent "gone" race, when something is in
        fact still there and unresolved."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md")
        for exc in (
            PermissionError(13, "Permission denied"),
            OSError(5, "Input/output error"),
            OSError(116, "Stale file handle"),
        ):
            with self.subTest(exc=exc):
                stderr = io.StringIO()
                with mock.patch.object(_MOD.os, "stat", side_effect=exc), \
                        mock.patch.object(_MOD.os, "lstat", side_effect=exc), \
                        mock.patch.object(sys, "stderr", stderr):
                    result = _MOD._locate(self.handoff_dir)
                self.assertEqual(result, (None, "pointer_unresolved"))
                self.assertIn("docs/handoff", stderr.getvalue())

    def test_an_unstattable_candidate_entry_real_chmod_is_pointer_unresolved(self):
        """R4-c01, real-filesystem sibling of the mocked test above:
        `docs/handoff` itself loses its execute (search) bit while
        remaining readable, so `os.listdir` still succeeds (it only needs
        read) but `os.stat` AND `os.lstat` on any entry inside it both
        fail with EACCES (both need to traverse INTO the directory to
        reach the entry). This is the actual regression shape (a bare
        `chmod -R 644 docs/` instead breaks the PARENT's own execute bit
        first, which fails `os.listdir` one level up and never reaches
        this per-entry branch at all -- confirmed empirically during
        verification of this finding)."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md")
        os.chmod(self.handoff_dir, 0o644)  # rw-r--r--: read but no execute/search
        self.addCleanup(os.chmod, self.handoff_dir, 0o755)  # so TemporaryDirectory cleanup can remove it
        try:
            os.stat(os.path.join(self.handoff_dir, "a.md"))
        except PermissionError:
            pass
        else:
            self.skipTest("running as a user unaffected by chmod (e.g. root)")
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("docs/handoff", stderr.getvalue())

    def test_an_unstattable_candidates_own_lstat_failure_does_not_sabotage_a_healthy_sibling(self):
        """R5-c06: mutating the lstat-OSError `pass` two tests above (R4-
        c01, `_candidates`'s own per-entry `except OSError:` re-raising
        instead of falling through to `undecidable.append`) passed the
        WHOLE suite before this test existed (confirmed against a temp
        copy) -- every existing fixture for that branch uses a SINGLE
        candidate, so "that one entry becomes undecidable, landing on
        pointer_unresolved" and "the whole listing re-raises, caught by
        `_locate`'s OUTER except, ALSO landing on pointer_unresolved" are
        indistinguishable: both produce the identical `(None,
        "pointer_unresolved")` with "docs/handoff" in the stderr detail
        either way. A second, HEALTHY candidate is what tells them apart
        -- mirroring `test_a_single_eloop_entry_does_not_sabotage_other_
        candidates` above, which already pins this for the SIBLING branch
        (a dangling/looping symlink, where `os.lstat` itself SUCCEEDS and
        only `os.stat` fails) -- this covers the one where `os.lstat`
        ITSELF also raises a non-`FileNotFoundError` `OSError`."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        broken = os.path.abspath(os.path.join(self.handoff_dir, "a.md"))
        real_stat, real_lstat = os.stat, os.lstat

        def failing_stat(path, *a, **kw):
            if os.path.abspath(path) == broken:
                raise OSError(5, "Input/output error")
            return real_stat(path, *a, **kw)

        def failing_lstat(path, *a, **kw):
            if os.path.abspath(path) == broken:
                raise OSError(5, "Input/output error")
            return real_lstat(path, *a, **kw)

        with mock.patch.object(_MOD.os, "stat", side_effect=failing_stat), \
                mock.patch.object(_MOD.os, "lstat", side_effect=failing_lstat):
            result = _MOD._locate(self.handoff_dir)
        # b.md must still resolve normally -- a.md's own unresolvable
        # lstat must not take the whole directory listing down with it.
        self.assertEqual(result, ("b.md", None))

    def test_a_candidate_that_vanishes_at_both_stat_and_lstat_is_dropped_quietly(self):
        """R4-c01 pinning test, the OTHER side of the same branch: when
        `os.lstat` ALSO raises `FileNotFoundError` (not merely some other
        OSError), the entry really is gone -- a listdir-then-stat race,
        R2-c03's own original case -- and must stay a silent drop, not
        `undecidable`. Directly exercises `_candidates` (not `_locate`)
        since this shape, alone in an otherwise-empty directory, is
        indistinguishable from "no handoff kept here" one level up."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md")
        with mock.patch.object(_MOD.os, "stat", side_effect=FileNotFoundError(2, "gone")), \
                mock.patch.object(_MOD.os, "lstat", side_effect=FileNotFoundError(2, "gone")):
            candidates, undecidable = _MOD._candidates(self.handoff_dir)
        self.assertEqual((candidates, undecidable), ([], []))

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

    def test_a_dangling_md_symlink_candidate_is_pointer_unresolved_not_quiet(self):
        """R3-c05, first leg: a `*.md` entry that is ITSELF a dangling
        symlink (its target is gone, not the directory entry) makes
        `os.stat` raise `FileNotFoundError` exactly like a genuine
        list-then-stat race -- but `os.lstat` on the SAME path still finds
        the symlink entry itself, so this is "something is here and
        cannot be resolved", not "gone by the time we looked". The old
        code read both alike and silently dropped the candidate, landing
        on the quiet `no_handoff` a project that keeps NO handoffs would
        also produce -- indistinguishable from the outside."""
        self._mkdir()
        os.symlink(
            os.path.join(self.tmp.name, "does-not-exist-target.md"),
            os.path.join(self.handoff_dir, "dangling.md"),
        )
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("dangling.md", stderr.getvalue())

    def test_a_dangling_docs_ancestor_is_pointer_unresolved_not_quiet(self):
        """R3-c05, second leg: `docs/` itself (the PARENT of
        `docs/handoff`) being a dangling symlink makes `os.listdir
        (handoff_dir)` raise `FileNotFoundError` the exact same way as an
        ordinary missing directory -- `os.path.islink(handoff_dir)` alone
        (the check the dangling-handoff-dir-itself leg above relies on)
        never catches it, because the dangling link sits one level up."""
        docs = os.path.dirname(self.handoff_dir)
        os.symlink(os.path.join(self.tmp.name, "nonexistent-docs-target"), docs)
        result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))

    def test_a_valid_docs_symlink_without_a_handoff_dir_stays_quiet(self):
        """R4-c02: the ancestor check above must fire only for a DANGLING
        `docs` symlink, not for a valid one. A monorepo that links
        `docs -> website/docs` (or any project whose `docs` happens to be
        a symlink to a real, existing directory that simply keeps no
        `handoff/` subdirectory) makes `os.listdir(handoff_dir)` raise
        `FileNotFoundError` the ordinary way -- nothing is missing except
        the handoff directory itself, the common `no_handoff` case every
        other project without handoffs also hits. The 365fc39 ancestor
        check tested `os.path.islink(docs)` alone, true for ANY symlink
        whether or not its target exists, so this healthy shape was
        wrongly re-raised as `pointer_unresolved` -- a project that will
        never write a handoff got a failure-class report every single
        session, with a `detail` naming a directory that does not exist."""
        docs = os.path.dirname(self.handoff_dir)
        real_target = os.path.join(self.tmp.name, "real-docs-target")
        os.makedirs(real_target)
        os.symlink(real_target, docs)
        self.assertEqual(_MOD._locate(self.handoff_dir), (None, "no_handoff"))

    def test_a_single_eloop_entry_does_not_sabotage_other_candidates(self):
        """R3-c05, third leg: a self-referential symlink (`ELOOP` on
        `os.stat`) is neither `FileNotFoundError` (so the old per-entry
        `except FileNotFoundError` never caught it at all) nor a
        directory-level failure -- it used to escape `_candidates`
        entirely, and `_locate`'s outer `except OSError` then read the
        WHOLE directory as unresolved even though a perfectly good sibling
        candidate sat right next to it."""
        self._mkdir()
        loop_path = os.path.join(self.handoff_dir, "loop.md")
        os.symlink(loop_path, loop_path)  # self-referential -> ELOOP on stat()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-20T10:00:00Z")
        self.assertEqual(_MOD._locate(self.handoff_dir), ("a.md", None))

    def test_an_unreadable_newer_candidate_is_pointer_unresolved_not_a_silent_older_pick(self):
        """R3-c06: with no pointer, the fallback scan compares every
        candidate's frontmatter `updated-at` -- but a candidate that
        cannot even be OPENED (permission denied, a stale NFS handle) used
        to be read exactly like one with no parseable `updated-at` at all
        (`_probe_frontmatter`'s blanket `except OSError: return {}`), so
        it silently lost the comparison to an older, merely-readable
        sibling. Whether the unreadable one was really the newest is
        unknowable from here, so this must fail loud rather than guess."""
        self._mkdir()
        older = _write_handoff(self.handoff_dir, "2026-09-10-old.md", updated_at="2026-09-10T00:00:00Z")
        newer = _write_handoff(self.handoff_dir, "2026-09-20-new.md", updated_at="2026-09-20T00:00:00Z")
        os.chmod(newer, 0o000)
        self.addCleanup(os.chmod, newer, 0o644)
        try:
            with open(newer):
                pass
        except PermissionError:
            pass
        else:
            self.skipTest("running as a user unaffected by chmod 000 (e.g. root)")
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("2026-09-20-new.md", stderr.getvalue())
        self.assertNotEqual(result, (os.path.basename(older), None))  # never silently the older one

    def test_an_unreadable_newer_candidate_is_pointer_unresolved_even_as_root(self):
        """R4-c11: the chmod-000 test above SKIPS when the suite runs as
        root (root ignores the permission bits, so `open()` succeeds
        anyway) -- and this plugin's CI image (`node:20-bookworm`, no
        `user:` declared) runs as root, so that test never actually RUNS
        there. This is the exact same gap R2-c09 already found and fixed
        for the OTHER chmod-000 test in this class (the unreadable
        DIRECTORY, not a candidate FILE); this one never got a mock
        twin. Injecting the error via mock exercises the identical
        `_probe_frontmatter`/`_locate` branch regardless of who runs the
        suite."""
        self._mkdir()
        older = _write_handoff(self.handoff_dir, "2026-09-10-old.md", updated_at="2026-09-10T00:00:00Z")
        newer = _write_handoff(self.handoff_dir, "2026-09-20-new.md", updated_at="2026-09-20T00:00:00Z")

        def failing_open(target, *a, **kw):
            if os.path.abspath(target) == os.path.abspath(newer):
                raise PermissionError(13, "denied")
            return builtins.open(target, *a, **kw)

        stderr = io.StringIO()
        with mock.patch.object(_MOD, "open", create=True, side_effect=failing_open), \
                mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("2026-09-20-new.md", stderr.getvalue())
        self.assertNotEqual(result, (os.path.basename(older), None))  # never silently the older one

    def test_a_pointer_to_a_dangling_target_does_not_silently_fall_back_to_an_older_candidate(self):
        """R4-c03: `latest.md` names an entry that IS listed but lands in
        `undecidable` (here, a dangling symlink; ELOOP is the same shape)
        alongside an older, perfectly healthy sibling. Before this fix,
        `target in candidates` was simply False (the dangling entry is not
        in `candidates`, only in `undecidable`), so the fallback newest-
        updated-at scan ran over `candidates` alone and silently returned
        the OLDER document -- even though `latest.md` explicitly named a
        DIFFERENT, presumably newer one. Ruling 13: a dangling POINTER
        TARGET must never silently change which document is ingested --
        either the pointer's own document is ingested, or the run reports
        `pointer_unresolved` naming what could not be read. This is
        distinct from Amendment A9 row c18 (the FALLBACK scan, with no
        explicit pointer, quietly leaving an undecidable sibling out of
        the comparison) -- that shape is untouched by this fix, only an
        explicit pointer resolving to an undecidable entry is."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "2026-09-10-old.md", updated_at="2026-09-10T00:00:00Z")
        dangling = os.path.join(self.handoff_dir, "2026-09-29-new.md")
        os.symlink(os.path.join(self.tmp.name, "does-not-exist-target.md"), dangling)
        with open(os.path.join(self.handoff_dir, "latest.md"), "w", encoding="utf-8") as fh:
            fh.write("# Latest\n\n**Latest**: [2026-09-29-new.md](./2026-09-29-new.md)\n")
        stderr = io.StringIO()
        extra = {}
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("2026-09-29-new.md", extra["detail"])
        self.assertIn("2026-09-29-new.md", stderr.getvalue())

    def test_an_unreadable_latest_md_with_multiple_candidates_is_pointer_unresolved(self):
        """R5-c01: latest.md points at the OLDER candidate, but a
        chmod(000) latest.md can no longer be read to confirm that at all.
        The old `os.path.isfile` guard folded "cannot even tell what
        latest.md says" into the exact same `None` as "there never was a
        latest.md at all" -- so the newest-`updated-at` fallback below ran
        anyway and silently picked `b.md`, even though the (now unreadable)
        pointer names `a.md` specifically. With two candidates to choose
        between, that is exactly the silent which-document swap ruling 13
        forbids -- one level up from R4-c03's own (an EXPLICIT pointer
        landing in `undecidable`); this is the pointer FILE itself being
        unresolvable, not its target."""
        self._mkdir()
        older = _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        _write_latest_pointer(self.handoff_dir, "a.md")
        latest_path = os.path.join(self.handoff_dir, "latest.md")
        os.chmod(latest_path, 0o000)
        self.addCleanup(os.chmod, latest_path, 0o644)
        try:
            with open(latest_path):
                pass
        except PermissionError:
            pass
        else:
            self.skipTest("running as a user unaffected by chmod 000 (e.g. root)")
        stderr = io.StringIO()
        extra = {}
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("latest.md", extra["detail"])
        self.assertIn("latest.md", stderr.getvalue())
        self.assertNotEqual(result, (os.path.basename(older), None))

    def test_a_mocked_read_failure_on_latest_md_is_pointer_unresolved_even_as_root(self):
        """R5-c01, sibling of the chmod test above: injected via a mocked
        `open` (EIO / ESTALE shapes chmod cannot produce, and that run
        identically whether or not the suite happens to run as root --
        R2-c09's own reasoning for pairing a real-chmod test with a mocked
        one). R6-c07: also asserts on the diagnostic TEXT, not just that
        `latest.md` is named -- the exception's own `repr()` (its class
        name and errno) is what actually tells a reader an I/O error from
        a renamed heading or a bad timestamp."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        _write_latest_pointer(self.handoff_dir, "a.md")
        latest_path = os.path.join(self.handoff_dir, "latest.md")

        def failing_open(target, *a, **kw):
            if os.path.abspath(target) == os.path.abspath(latest_path):
                raise OSError(5, "Input/output error")
            return builtins.open(target, *a, **kw)

        extra = {}
        stderr = io.StringIO()
        with mock.patch.object(_MOD, "open", create=True, side_effect=failing_open), \
                mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("latest.md", stderr.getvalue())
        self.assertIn("latest.md", extra["detail"])
        self.assertIn("OSError(", extra["detail"])

    def test_a_dangling_latest_md_symlink_with_multiple_candidates_is_pointer_unresolved(self):
        """R5-c01: latest.md ITSELF (not its target line's basename, R4-c03's
        shape) is a dangling symlink -- `os.path.isfile` used to read that
        as plain `False`, indistinguishable from no pointer file at all."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        os.symlink(
            os.path.join(self.tmp.name, "does-not-exist-target.md"),
            os.path.join(self.handoff_dir, "latest.md"),
        )
        result = _MOD._locate(self.handoff_dir)
        self.assertEqual(result, (None, "pointer_unresolved"))

    def test_a_self_referential_latest_md_symlink_with_multiple_candidates_is_pointer_unresolved(self):
        """R5-c01: latest.md -> latest.md (ELOOP). `os.lstat` alone cannot
        catch this (it does not follow the final component, so it succeeds
        on the symlink entry itself) -- resolving what is really there
        needs a FOLLOWING stat, which is where ELOOP actually surfaces.
        R6-c07: also pins the diagnostic TEXT -- the `repr()` of the
        `OSError` `os.stat` raises for ELOOP, not just that SOME failure
        was reported."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        loop_path = os.path.join(self.handoff_dir, "latest.md")
        os.symlink(loop_path, loop_path)
        extra = {}
        result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("OSError(", extra["detail"])

    def test_a_directory_named_latest_md_with_multiple_candidates_is_pointer_unresolved(self):
        """R5-c01: a directory named latest.md is confirmed by `os.lstat`
        (something IS there) but fails the regular-file check that follows
        -- it must never be opened (a directory raises `IsADirectoryError`
        on `open`, but this must not even try, matching the FIFO/device
        guard R1-c13 already established for the "no pointer" leg). R6-c07:
        also pins the diagnostic TEXT -- the literal "not a regular file",
        the one leg here with no exception object to format instead."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        os.mkdir(os.path.join(self.handoff_dir, "latest.md"))
        extra = {}
        result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("not a regular file", extra["detail"])

    def test_an_unresolved_latest_md_with_a_single_candidate_falls_back_quietly(self):
        """The R2-c13 carve-out (R5-c01, tightened by R6-c01 to count
        `undecidable` too, see the group right below): with only ONE
        `*.md` entry in the directory TOTAL -- one resolvable candidate,
        ZERO undecidable siblings -- an unresolved latest.md cannot change
        WHICH document gets ingested -- there is nothing else it could
        have named -- so this stays the quiet, successful fallback rather
        than a failure, exactly like R2-c13's own FIFO fixture
        (`TestPointerTargetReadSafety`). A directory-shaped latest.md
        exercises the same carve-out through the `os.stat`-follows-
        symlinks leg instead of R2-c13's FIFO leg."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        os.mkdir(os.path.join(self.handoff_dir, "latest.md"))
        self.assertEqual(_MOD._locate(self.handoff_dir), ("a.md", None))

    def test_an_unresolved_latest_md_with_a_single_candidate_and_an_undecidable_sibling_is_pointer_unresolved(self):
        """R6-c01: the exemption above counts ONLY `candidates`, the
        RESOLVABLE `*.md` entries -- not `undecidable` (a dangling or
        self-referential `*.md` symlink: listed by `_candidates`, but its
        own stat cannot be resolved). A directory holding exactly one
        resolvable candidate ALONGSIDE an undecidable sibling used to
        satisfy `len(candidates) == 1` and fall through to the quiet
        fallback anyway -- even though the (also unresolved) `latest.md`
        could perfectly well have named the undecidable sibling instead of
        the lone candidate. That is the SAME silent which-document swap
        ruling 13 forbids; the exemption just failed to count both places
        a second document could be hiding. Directory form here (matching
        the control test right above); the two tests below repeat this
        with a dangling `latest.md` symlink and a mocked read failure,
        mirroring the three forms already pinned for the multi-candidate
        case earlier in this class."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        os.symlink(
            os.path.join(self.tmp.name, "does-not-exist-undecidable-target.md"),
            os.path.join(self.handoff_dir, "b.md"),
        )
        os.mkdir(os.path.join(self.handoff_dir, "latest.md"))
        extra = {}
        result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("latest.md", extra["detail"])

    def test_a_dangling_latest_md_symlink_with_a_single_candidate_and_an_undecidable_sibling_is_pointer_unresolved(self):
        """R6-c01, second form: `latest.md` ITSELF a dangling symlink (as
        opposed to the undecidable `*.md` SIBLING entry) -- a different
        path through the same exemption as the directory form above."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        os.symlink(
            os.path.join(self.tmp.name, "does-not-exist-undecidable-target.md"),
            os.path.join(self.handoff_dir, "b.md"),
        )
        os.symlink(
            os.path.join(self.tmp.name, "does-not-exist-latest-target.md"),
            os.path.join(self.handoff_dir, "latest.md"),
        )
        extra = {}
        result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("latest.md", extra["detail"])

    def test_a_mocked_read_failure_on_latest_md_with_a_single_candidate_and_an_undecidable_sibling_is_pointer_unresolved(self):
        """R6-c01, third form: injected via a mocked `open` (root-safe,
        matching R5-c01's own mocked sibling below) rather than a real
        dangling symlink or directory."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        os.symlink(
            os.path.join(self.tmp.name, "does-not-exist-undecidable-target.md"),
            os.path.join(self.handoff_dir, "b.md"),
        )
        latest_path = os.path.join(self.handoff_dir, "latest.md")
        _write_latest_pointer(self.handoff_dir, "a.md")

        def failing_open(target, *a, **kw):
            if os.path.abspath(target) == os.path.abspath(latest_path):
                raise OSError(5, "Input/output error")
            return builtins.open(target, *a, **kw)

        extra = {}
        with mock.patch.object(_MOD, "open", create=True, side_effect=failing_open):
            result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("latest.md", extra["detail"])

    def test_an_unresolved_latest_mds_single_candidate_with_no_parseable_updated_at_still_names_latest_md_in_detail(self):
        """R6-c02: the single-entry exemption falls through with
        `target=None` (never a real filename) to the newest-`updated-at`
        scan -- so when the lone candidate ALSO has no parseable
        `updated-at`, this run reaches `_locate`'s OWN final "none with a
        parseable updated-at" exit. That exit used to build its `detail`
        from scratch, discarding the `pointer_unresolved` reason
        `_pointer_target` already computed -- so a session where BOTH
        `latest.md` (here, a directory) AND the single candidate's
        frontmatter are broken reported a ledger `detail` that only
        mentions frontmatter, sending whoever reads it to fix the wrong
        file, and no stderr line naming `latest.md` at all."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", frontmatter_lines=["---", "track-id: x", "---"])
        os.mkdir(os.path.join(self.handoff_dir, "latest.md"))
        extra = {}
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            result = _MOD._locate(self.handoff_dir, extra)
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("latest.md", extra["detail"])
        self.assertIn("not a regular file", extra["detail"])
        self.assertIn("latest.md", stderr.getvalue())

    def test_an_unstattable_latest_md_is_pointer_unresolved_not_a_silent_older_pick(self):
        """R6-c04: `_pointer_target`'s FIRST existence check (`os.lstat`)
        has its own `except OSError as exc: return None, f"{exc!r}"` leg,
        distinct from the SECOND check's (`os.stat`, which the ELOOP /
        directory / dangling-symlink tests elsewhere in this class already
        pin) -- but nothing exercised it directly: the only EIO-shaped
        fixture in this class targeting `latest.md` (the mocked-`open`
        test above) fails at the THIRD step (reading the file), past both
        stat calls, so this FIRST leg stayed provably dead even though
        R5-c01 claims to cover 'EIO / ESTALE'. Root-safe (mocked, not
        chmod, and run for both errno shapes via subTest): EACCES / EIO /
        ESTALE on `os.lstat` itself is the real-world trigger (a directory
        that lost its search bit, an NFS mount that dropped mid-call), and
        must not silently fall through to the newest-`updated-at` scan and
        pick the OLDER candidate over the one `latest.md` actually names."""
        self._mkdir()
        older = _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        _write_latest_pointer(self.handoff_dir, "a.md")
        latest_path = os.path.abspath(os.path.join(self.handoff_dir, "latest.md"))
        real_lstat = os.lstat
        for exc in (OSError(5, "Input/output error"), OSError(116, "Stale file handle")):
            with self.subTest(exc=exc):

                def failing_lstat(path, *a, _exc=exc, **kw):
                    if os.path.abspath(path) == latest_path:
                        raise _exc
                    return real_lstat(path, *a, **kw)

                extra = {}
                stderr = io.StringIO()
                with mock.patch.object(_MOD.os, "lstat", side_effect=failing_lstat), \
                        mock.patch.object(sys, "stderr", stderr):
                    result = _MOD._locate(self.handoff_dir, extra)
                self.assertEqual(result, (None, "pointer_unresolved"))
                self.assertNotEqual(result, (os.path.basename(older), None))
                self.assertIn("latest.md", extra["detail"])
                self.assertIn("OSError(", extra["detail"])
                self.assertIn("latest.md", stderr.getvalue())

    def test_a_healthy_symlinked_latest_md_resolves_its_target(self):
        """R6-c06: `_pointer_target` deliberately uses `os.stat` (which
        FOLLOWS symlinks), not `os.lstat`, to resolve what a symlinked
        `latest.md` actually points at. (Not Aether's shape: Aether's
        `docs/handoff/latest.md` is a symlink straight to a handoff
        DOCUMENT, which has no `**Latest**:` line and therefore reads as
        "no pointer" -> the quiet updated-at fallback; a symlinked POINTER
        file, as here, is the case this test pins.) Nothing pinned the
        HEALTHY case before this test; only dangling and self-referential
        symlinks were covered, and both stay `unresolved` no matter which
        stat call is used to tell them apart, so neither would catch a
        regression to `os.lstat` here -- which would read a healthy
        symlinked pointer as `not a regular file` (`lstat` reports the
        SYMLINK's own mode, never `S_ISREG`) and report `pointer_
        unresolved` on a project with two or more candidates every single
        session, even though nothing is actually broken."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        real_pointer = os.path.join(self.tmp.name, "real-latest.md")
        with open(real_pointer, "w", encoding="utf-8") as fh:
            fh.write("# Latest\n\n**Latest**: [a.md](./a.md)\n")
        os.symlink(real_pointer, os.path.join(self.handoff_dir, "latest.md"))
        self.assertEqual(_MOD._locate(self.handoff_dir), ("a.md", None))

    def test_a_missing_latest_md_with_multiple_candidates_still_falls_back_quietly(self):
        """Control for the whole group above: latest.md genuinely ABSENT
        (the ordinary case, no pointer configured at all) must stay the
        quiet fallback even with multiple candidates -- only "exists but
        unresolved" is loud, never "does not exist"."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        _write_handoff(self.handoff_dir, "b.md", updated_at="2026-09-20T00:00:00Z")
        self.assertEqual(_MOD._locate(self.handoff_dir), ("b.md", None))

    def test_a_closed_stderr_does_not_turn_an_unlistable_directory_into_http_error(self):
        """R3-c04: the old code printed `[{HOOK}] {detail}` with a bare
        `print(..., file=sys.stderr)` BEFORE setting `extra["detail"]`. A
        closed stderr pipe makes that raise `BrokenPipeError` -- a
        `ConnectionError` subclass, which used to escape `_locate`
        uncaught and which `_hook_state.reason_for_exception` reads as
        `http_error` one layer up in `_collect`/`main` -- misreporting a
        purely LOCAL "cannot list docs/handoff" condition as a network
        failure, with the `detail` that would have said what really
        happened never even set."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md")
        extra = {}
        with mock.patch.object(_MOD, "_candidates", side_effect=OSError("denied")), \
                mock.patch.object(sys, "stderr", _BrokenStderr()):
            result = _MOD._locate(self.handoff_dir, extra)  # must not raise BrokenPipeError
        self.assertEqual(result, (None, "pointer_unresolved"))
        self.assertIn("could not be listed", extra["detail"])

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

    def test_cap_does_not_truncate_when_length_exactly_equals_the_limit(self):
        """R3-c09: `_cap`'s own `<=` boundary (`if len(text) <= limit: return
        text`) had no test pinning the `==` case specifically -- every
        existing `_cap` fixture used a `limit` strictly SHORTER than the
        text. Changing `<=` to `<` still passed the whole suite until this
        test was added (confirmed by mutation): `_cap('X' * 100, 100)` must
        come back verbatim, not truncated, and a `limit` one longer must
        too."""
        text = "X" * 100
        self.assertEqual(_MOD._cap(text, 100), text)
        self.assertEqual(_MOD._cap(text, 101), text)
        self.assertNotEqual(_MOD._cap(text, 99), text)  # the actually-shorter case still truncates

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

    def test_a_bare_divider_line_under_a_heading_is_still_empty(self):
        """R3-c07: the REAL Aria template (`aria/templates/session-handoff.md`)
        puts a `---` divider immediately before EVERY `## §N` heading,
        itself included -- so a handoff whose author deleted §6/§2's body
        but left the heading and that trailing divider in place (exactly
        ruling 3 / R1-c01's "template skeleton not filled in") reads as
        heading, blank line, `---`. That `---` line is technically
        non-whitespace, so the plain `line.strip()` truthiness check used
        to read it as real content and would have shipped
        "## §2 ...\\n\\n---" as an episode. Shaped after the real template,
        not a synthetic one: the fixture below mirrors
        `templates/session-handoff.md`'s own layout verbatim."""
        body = (
            "# T\n\n"
            "## §6 Next session 入口 + 优先级建议\n\n---\n\n"
            "## §2 未完成 / Carry-forward 清单\n\n---\n"
        )
        self.assertEqual(_MOD._build_content(body), (None, "empty_sections"))

    def test_a_bare_divider_line_of_any_recognised_character_is_still_empty(self):
        """R4-c12: the two ORIGINAL R3-c07 fixtures (this class) only ever
        used `---`, even though `_is_divider_line` also recognises `***`
        and `___` (A9-8 already names all three as the intended set) --
        a mutant narrowing it to just `-` passed the whole suite before
        this test existed (confirmed against a temp copy). Parametrized
        over all three characters, plus a longer run of each (still a
        thematic break per the same rule)."""
        for marker in ("---", "***", "___", "-----", "*****", "_____"):
            with self.subTest(marker=marker):
                body = (
                    "# T\n\n"
                    f"## §6 Next session 入口 + 优先级建议\n\n{marker}\n\n"
                    f"## §2 未完成 / Carry-forward 清单\n\n{marker}\n"
                )
                self.assertEqual(_MOD._build_content(body), (None, "empty_sections"))

    def test_a_divider_line_does_not_hide_real_content_before_it(self):
        """The other direction of R3-c07's fix: a section with GENUINE
        content followed by the SAME trailing `---` divider (the common,
        filled-in shape) must still read as present -- the divider must be
        ignored only when it is the section's ONLY non-blank line, not
        stripped from real content."""
        body = "# T\n\n## §6 Next session\n\nreal six body\n\n---\n\n## §2 Carry\n\ntwo body\n\n---\n"
        content, reason = _MOD._build_content(body)
        self.assertIsNone(reason)
        self.assertIn("real six body", content)
        self.assertIn("two body", content)

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


class TestParseInstantIsShared(unittest.TestCase):
    """R4-c14: handoff_sync.py used to carry a second, byte-identical copy
    of `_ingest_client._parse_instant` -- locating (this file, picking the
    candidate with the newest `updated-at`) and stale-checking
    (`_ingest_client`, comparing `local_updated_at` against the server's
    own value) parsed the same frontmatter value through what were two
    independently-editable implementations. Asserting the SAME object
    (not merely equal behaviour on today's fixtures) is what actually
    rules out a future edit to one copy silently not reaching the other;
    a temp copy with only handoff_sync.py's OWN (then-separate) copy
    changed passed the whole suite before this consolidation."""

    def test_handoff_sync_reuses_the_ingest_client_object(self):
        self.assertIs(_MOD._parse_instant, _ingest_client._parse_instant)


class TestPointerTargetReadSafety(_HandoffDirCase):
    """R1-c13: latest.md is confirmed a regular file (``os.lstat`` then
    ``os.stat`` + ``stat.S_ISREG``, R5-c01 -- originally ``os.path.
    isfile``, R6-c08) before it is opened, and its read is capped -- a
    FIFO or a huge file must not block the hook or exhaust memory just to
    find one pointer line."""

    def test_a_non_regular_latest_md_reads_as_unresolved(self):
        """R2-c13: ``_pointer_target`` runs on a watchdog thread of THIS
        test's own, with its own 1 s bound (the ``test_hook_runner.py``
        pattern, ``test_a_budget_that_stopped_working_fails_fast_not_
        slow``) rather than being called directly on the main test
        thread. ``unittest`` has NO per-test default timeout -- an
        earlier revision of this docstring claimed there was one -- so if
        the ``stat.S_ISREG`` guard this test means to pin (R6-c08: this
        docstring, the method name, and the comment below previously
        described the R1-c13-era ``os.path.isfile`` check R5-c01
        replaced) ever regressed, ``open()`` on a FIFO with no writer
        blocks forever, and calling it directly here would hang this
        test, and the whole suite behind it, rather than failing fast."""
        self._mkdir()
        _write_handoff(self.handoff_dir, "a.md", updated_at="2026-09-10T00:00:00Z")
        fifo_path = os.path.join(self.handoff_dir, "latest.md")
        os.mkfifo(fifo_path)  # a directory would also fail S_ISREG; a FIFO is the risk this guards
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
        # R5-c01: `_pointer_target` now returns `(target, unresolved)` -- a
        # FIFO is "exists but not a readable regular file", so `unresolved`
        # is set (not silently "no pointer" the way `os.path.isfile` alone
        # used to read it). `_locate` still falls back QUIETLY here because
        # there is only ONE candidate to choose between (no document could
        # be silently swapped for another) -- not because of a blocking
        # open().
        target, unresolved = result.get("pointer", (None, None))
        self.assertIsNone(target)
        self.assertIsNotNone(unresolved)
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
        return _run_main(_MOD, {"cwd": self.cwd, "session_id": session_id}, env)

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


class TestNotConfiguredShortCircuits(_WriteCase):
    """R5-c07: digest item 2 -- ``NEXUS_API_URL`` empty must return
    ``not_configured`` BEFORE any git call or filesystem scan ("Do nothing
    else (no git, no file reads)"). The code already does this (``_collect``
    returns right after the ``base_url`` check, textually before
    ``_identity.project_root`` / ``_locate`` are even referenced), but no
    existing test pinned it: ``test_not_configured_makes_no_request_when_
    no_api_url`` (``TestRuns``) only asserts zero REQUESTS and the final
    reason, both of which stay correct even if the check were moved PAST
    ``project_root``/``_locate`` -- confirmed against a temp copy: with a
    POPULATED, resolvable handoff directory (so ``_locate`` would succeed
    silently rather than changing the reason to ``no_handoff``, which is
    what an EMPTY handoff dir does under the same mutant and is why that
    shape alone would not have caught this), the observable reason stays
    ``not_configured`` either way -- only the wasted git subprocess and
    directory scan differ, invisible to a black-box request/reason check
    alone."""

    def test_project_root_and_locate_are_never_called(self):
        self._write("2026-09-20-1000-x.md")
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")
        with mock.patch.object(
            _identity, "project_root", side_effect=AssertionError("must not be called")
        ), mock.patch.object(_MOD, "_locate", side_effect=AssertionError("must not be called")):
            self._run(NEXUS_API_URL="")
        self.assertEqual(self.requests, [])
        self.assertEqual(self._last_entry()["reason"], "not_configured")


class TestChosenDocumentReadCap(_WriteCase):
    """R2-c18: R1-c13 added ``_MAX_DOCUMENT_CHARS`` for TWO reads -- the
    ``latest.md`` pointer probe (``_pointer_target``, fixture in
    ``TestPointerTargetReadSafety``) and the CHOSEN document's own read
    inside ``_collect`` (``fh.read(_MAX_DOCUMENT_CHARS)``) -- but only the
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
        with mock.patch.object(_MOD, "_MAX_DOCUMENT_CHARS", 200):
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


class TestRunMainRestoresStderr(_WriteCase):
    """R5-c03: `main()` permanently replaces the GLOBAL `sys.stderr` with a
    `_StderrGuard` (R4-c06) the FIRST time it runs in a process, and
    nothing undoes that -- `_run_main` (every `_WriteCase` test's own
    in-process harness) patches `os.environ` and `sys.stdin` around the
    call but never touched `sys.stderr`. `unittest discover` sorts
    `test_handoff_sync.py` FIRST among this plugin's eleven `test_*.py`
    files (alphabetical, R6-c08: this count has already gone stale once),
    so every test in the other ten, run in the SAME process via
    `python3 -m unittest discover`, used to run with
    `sys.stderr` silently wrapped in this guard from this file's very
    first `_run_main` call onward -- confirmed empirically (a probe that
    runs one `TestWritePath` test via `unittest`, then inspects
    `type(sys.stderr)` afterward, in a fresh interpreter) before this fix.
    Production is unaffected (one hook, one process, one `main()` call);
    this is a test-hygiene-only fix, scoped to this file's own harness."""

    def test_a_run_does_not_leave_sys_stderr_permanently_wrapped(self):
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        before = sys.stderr
        self.assertNotIsInstance(before, _MOD._StderrGuard)  # the real fixture precondition
        self._run()
        self.assertIs(sys.stderr, before)


class TestMainDoesNotDoubleWrapStderr(unittest.TestCase):
    """R5-c08: `main()`'s own guard (`if not isinstance(sys.stderr,
    _StderrGuard):`) promises, in its own comment, that repeated in-process
    `main()` calls do not repeatedly wrap `sys.stderr` -- untested:
    mutating it to `if True:` (always re-wrap) passed the whole suite
    before this test existed (confirmed against a temp copy). `_run_main`
    cannot exercise this directly any more (R5-c03's own fix): it now
    restores `sys.stderr` to whatever it was on ENTRY after every call, so
    two `_run_main` calls in a row each start from a FRESH, unwrapped
    value -- the second call's guard check would see `False` regardless
    of whether the guard itself still exists. This calls `mod.main()`
    directly, twice, under ONE unrestored `sys.stderr` patch, which is
    what actually lets the SECOND call observe the FIRST call's guard
    installed."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_dir = os.path.join(self.tmp.name, "state")

    def test_two_direct_main_calls_leave_sys_stderr_wrapped_exactly_once(self):
        sentinel = _BrokenStderr()
        clean = {k: os.environ[k] for k in _BASE_ENV_KEYS if k in os.environ}
        clean["NEXUS_HOOK_STATE_DIR"] = self.state_dir  # NEXUS_API_URL absent -> not_configured, no network
        old_stdin = sys.stdin
        try:
            with mock.patch.dict(os.environ, clean, clear=True), \
                    mock.patch.object(sys, "stderr", sentinel):
                sys.stdin = io.StringIO("{}")
                _MOD.main()
                sys.stdin = io.StringIO("{}")
                _MOD.main()
                # Still exactly ONE layer: `sys.stderr` is a `_StderrGuard`
                # wrapping the ORIGINAL sentinel directly, not a
                # `_StderrGuard` wrapping a `_StderrGuard` wrapping it.
                self.assertIsInstance(sys.stderr, _MOD._StderrGuard)
                self.assertIs(sys.stderr._real, sentinel)
        finally:
            sys.stdin = old_stdin


class TestModuleCleanupOrdering(unittest.TestCase):
    """R6-c03: `setUpModule`'s two hermeticity backstops (`_assert_home_
    untouched`, `_assert_stderr_not_left_wrapped`) are registered via
    `unittest.addModuleCleanup`, which runs LIFO -- whichever call is LAST
    in `setUpModule`'s own source order fires FIRST. `unittest.case.
    doModuleCleanups` (confirmed below, against a throwaway registration
    list, not the real module-wide one) runs every registered cleanup
    regardless of earlier failures, but re-raises only the FIRST exception
    it collects -- so if BOTH ever fail on the same run, only the one
    registered LAST is ever reported; the other's `AssertionError` is
    silently dropped, not merely deprioritised."""

    def test_the_home_leak_check_is_registered_after_the_stderr_check(self):
        """Reads `setUpModule`'s own source (rather than re-registering
        the two functions here in a hand-picked order) to pin the actual
        PRODUCTION registration order -- a reimplementation could quietly
        drift away from what `setUpModule` really does, the way an
        earlier revision of that function did (it registered
        `_assert_home_untouched` FIRST, right next to a "LIFO: this runs
        first" comment that the registration order made false: `_assert_
        stderr_not_left_wrapped`, registered second, actually ran first
        and swallowed the home-leak report whenever both failed
        together)."""
        # The full `unittest.addModuleCleanup(...)` CALL, not the bare
        # function name: `setUpModule`'s own explanatory comment above
        # these two calls (necessarily) mentions both names too, and a
        # bare-name search would find whichever one that PROSE happens to
        # say first, regardless of the actual call order below it --
        # silently vacuous, not merely a weaker check.
        source = inspect.getsource(setUpModule)
        home_pos = source.index("unittest.addModuleCleanup(_assert_home_untouched")
        stderr_pos = source.index("unittest.addModuleCleanup(_assert_stderr_not_left_wrapped")
        self.assertGreater(
            home_pos, stderr_pos,
            "_assert_home_untouched must be the LAST addModuleCleanup call in "
            "setUpModule so it is the FIRST to run (LIFO) and its AssertionError "
            "is not the one doModuleCleanups silently drops",
        )

    def test_doModuleCleanups_only_reports_the_first_of_several_failing_cleanups(self):
        """Confirms the stdlib mechanism the test above relies on, against
        a throwaway registration list (saved and restored below) rather
        than the real module-wide one `setUpModule` already populated --
        clearing that for real mid-suite would drop this module's own
        HOME/stderr backstops for every test that runs after this one.
        Both cleanups RUN (side effect observed via `called`, confirming
        `doModuleCleanups` does not stop at the first failure), but only
        the LAST-REGISTERED one's exception survives to be reported --
        this is what makes the source-order assertion above load-bearing
        rather than cosmetic."""
        saved = list(unittest.case._module_cleanups)
        unittest.case._module_cleanups.clear()
        self.addCleanup(unittest.case._module_cleanups.extend, saved)
        called = []

        def first():
            called.append("first")
            raise AssertionError("first failed")

        def second():
            called.append("second")
            raise AssertionError("second failed")

        unittest.addModuleCleanup(first)
        unittest.addModuleCleanup(second)  # registered last -> runs first -> wins
        with self.assertRaises(AssertionError) as ctx:
            unittest.case.doModuleCleanups()
        self.assertEqual(called, ["second", "first"])  # both ran, in LIFO order
        self.assertEqual(str(ctx.exception), "second failed")  # only the last-registered survives


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

    def test_ledger_carries_http_status_and_never_the_body(self):
        """R3-c14: digest item 11 requires the ledger `extra` to carry the
        HTTP status and never the request/response body or a secret --
        but no test read `entry['status']` back, and none asserted the
        ledger's serialised form excludes the BODY. Dropping `status=` from
        the write path, or (separately) adding the outbound `content` to
        `extra`, both still passed the whole suite (confirmed by
        mutation): the body's own marker string below is unique enough
        that its presence in the ledger row would mean real content
        leaked into local, less-protected storage."""
        marker = "UNIQUE-CONTENT-MARKER-DO-NOT-LEAK-INTO-THE-LEDGER"
        self._write("2026-09-20-1000-x.md", section6=f"## §6 Next session 入口\n\n{marker}")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        create_entry = self._last_entry()
        self.assertEqual(create_entry["status"], 201)
        self.assertNotIn(marker, json.dumps(create_entry, ensure_ascii=False))

        self._write("2026-09-20-1000-x.md", section6=f"## §6 Next session 入口\n\nCHANGED {marker}")
        self.backend.reply(200, _page(_row(
            "docs/handoff/2026-09-20-1000-x.md", content_hash="something-else",
        ))).reply(200, {"memory_id": "t1::proj::11111111-1111-4111-8111-111111111111"})
        self._run(session_id="sess-1")
        patch_entry = self._last_entry()
        self.assertEqual(patch_entry["status"], 200)
        self.assertNotIn(marker, json.dumps(patch_entry, ensure_ascii=False))

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

    def test_a_closed_stderr_during_a_full_lookup_page_does_not_turn_into_http_error(self):
        """R4-c06: `_ingest_client._lookup`'s OWN "dedup page full"
        diagnostic (`LOOKUP_LIMIT=5`) is a bare, unguarded
        `print(..., file=sys.stderr)` -- unlike every stderr write THIS
        file makes, which all go through `_warn` (R3-c03). Ruling 15
        forbids fixing this inside `_ingest_client.py` itself, so `main()`
        must protect it from here instead: without that, a closed stderr
        pipe turns this bare print's own `BrokenPipeError` into an
        uncaught exception that escapes `_lookup`/`upsert` entirely,
        misreported one layer up as `http_error` (`reason_for_exception`
        reads `BrokenPipeError`, a `ConnectionError` subclass, that way)
        even though the GET this run made got a perfectly good response,
        and the dedup/write that response called for never happens."""
        path = self._write("2026-09-20-1000-x.md", updated_at="2026-09-20T10:00:00Z")
        with open(path, encoding="utf-8") as fh:
            _fm, body = _MOD._split_frontmatter(fh.read())
        content, _reason = _MOD._build_content(body)
        digest = _ingest_client.content_hash(content)
        stored = {k: v for k, v in self._expected_metadata(path, "sess-1").items() if k.startswith("aria.")}
        rows = [
            _row(
                "docs/handoff/2026-09-20-1000-x.md", content_hash=digest, extra=stored,
                created_at=f"2026-09-19T0{i}:00:00.000001Z",
                row_id=f"{i:08x}-1111-4111-8111-111111111111",
            )
            for i in range(5)  # == LOOKUP_LIMIT: a full page
        ]
        self.backend.reply(200, _page(*rows))
        for _ in range(4):  # the 4 non-canonical rows, each dedup-deleted
            self.backend.reply(204, None)
        with mock.patch.object(sys, "stderr", _BrokenStderr()):
            self._run(session_id="sess-1")
        self.assertEqual(
            [r["method"] for r in self.requests], ["GET", "DELETE", "DELETE", "DELETE", "DELETE"]
        )
        entry = self._last_entry()
        self.assertNotEqual(entry["reason"], "http_error")
        self.assertEqual(entry["reason"], "dedup_merged")
        self.assertEqual(entry["dedup_merged"], 4)

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

    def test_cap_for_wire_recuts_content_just_under_the_cap_too(self):
        """R3-c02: the previous `_cap_for_wire` derived its per-iteration
        `limit` from `_CONTENT_CAP` itself (`limit -= overflow`, starting
        from `_CONTENT_CAP`), not from `len(content)` -- so content whose
        length sat in the band `(_CONTENT_CAP - G, _CONTENT_CAP - G/2]`
        (`G` = the net character growth one redaction hit adds; 19 for the
        shortest URL-userinfo match, a 4-char password against the
        23-char marker) satisfied the old `len(content) <= limit` escape
        hatch and came back UNCHANGED even though its REDACTED form
        exceeded the cap. The R2-c08 test above only ever exercised content
        built at EXACTLY `_CONTENT_CAP`, which sits OUTSIDE that band (the
        fixed content length below, `_CONTENT_CAP - 10` = 3990, sits
        squarely inside it: only 9 characters of headroom before
        redaction, nowhere near enough to also cover the marker's own
        growth)."""
        secret_line = "postgresql://nexus:abcd@db-host:5432/nexus"
        self.assertEqual(len(_redact.find(secret_line)), 1)  # exactly one hit, net growth +19
        target_len = _MOD._CONTENT_CAP - 10
        filler = "A" * (target_len - len(secret_line) - 1)
        content = filler + "\n" + secret_line
        self.assertEqual(len(content), target_len)  # squarely inside the (3981, 3990] danger band
        result = _MOD._cap_for_wire(content)
        redacted, _hits = _redact.redact_text(result)
        # The invariant that actually matters (and that the old code broke
        # for this exact length): whatever `_cap_for_wire` decided to do
        # with the secret -- keep it whole or drop it whole, `_cap` itself
        # never leaves a partial fragment -- the WIRE (redacted) content
        # must never exceed the cap.
        self.assertLessEqual(len(redacted), _MOD._CONTENT_CAP)
        # And, whichever way it went, no PARTIAL fragment of the secret
        # (the OLD hard mid-line cut's own failure mode) is on the wire.
        if "abcd@db-host" not in result:
            self.assertNotIn("nexus:abcd", result)

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

    def test_a_skip_class_reason_does_not_persist_a_drifted_container_id(self):
        """R5-c07: `run["persist_container_id"]` (initialised `False` in
        `main()`) is only ever reassigned on the ONE line right after
        `client.upsert(...)` returns (ruling 2) -- an early return before
        the write path never reaches it, so THIS run's own drifted
        container_id must not land in state just because `_collect`
        happened to already know it by the time it returned early. No
        existing test combines "this container's id has already drifted"
        with a SKIP-class early return that never reaches `client.upsert`
        at all: every existing identity-drift test (`test_identity_drift_
        with_a_500_does_not_persist_the_new_id` and its 403/timeout
        siblings) still reaches `client.upsert` -- a round-abort outcome
        is decided THERE, on the write path, not before it -- so none of
        them would catch a future regression that moved the persist flag
        earlier, ahead of the owner/opt-out checks."""
        self._write("2026-09-20-1000-x.md", updated_at="2026-09-20T10:00:00Z")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        with mock.patch.object(_identity, "container_id", return_value="dev-box-a"):
            self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "none")
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-a")

        # A second, NEWER document owned by someone else -- `_locate`'s
        # no-pointer fallback picks it over the first (ruling 1: newest
        # updated-at wins across ALL candidates, ownership is checked only
        # after). This container's own id has drifted to dev-box-b in the
        # meantime, but `not_owner` is decided well before the write path.
        self._write(
            "2026-09-20-1100-y.md", owner=f"owner/{OTHER_UUID}", updated_at="2026-09-20T11:00:00Z",
        )
        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self._run(session_id="sess-2")
        self.assertEqual(self._last_entry()["reason"], "not_owner")
        # Only the FIRST run's GET+POST -- the second made zero requests.
        self.assertEqual([r["method"] for r in self.requests], ["GET", "POST"])

        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-a")  # unchanged by the skip-class run

    def test_a_content_failure_does_not_persist_a_drifted_container_id_empty_sections(self):
        """R6-c05: the test right above only proves the persist flag was
        not moved ahead of the OWNER check -- a regression that moved it
        to right AFTER the owner check but still BEFORE `_build_content`
        (still well ahead of the one real assignment site, right after
        `client.upsert` returns) would pass every existing test INCLUDING
        that one, because `not_owner` returns even earlier and never
        reaches such a mutant at all (confirmed: applying exactly that
        mutation to a temp copy left the whole suite green). `empty_
        sections` is decided strictly AFTER the owner check (ruling 3 /
        R1-c01) -- the same shape ruling 2 protects against, just for a
        different early-return reason, and one this class's own R2-c10
        sibling (`TestContentReasonsEndToEnd`) never combined with a
        drifted id."""
        self._write("2026-09-20-1000-x.md", updated_at="2026-09-20T10:00:00Z")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        with mock.patch.object(_identity, "container_id", return_value="dev-box-a"):
            self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "none")
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-a")

        # A second, NEWER document -- same owner, but an empty body -- with
        # this container's own id already drifted to dev-box-b.
        self._write(
            "2026-09-20-1100-y.md", body="   \n\n  ", updated_at="2026-09-20T11:00:00Z",
        )
        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self._run(session_id="sess-2")
        self.assertEqual(self._last_entry()["reason"], "empty_sections")
        # Only the FIRST run's GET+POST -- the second made zero requests.
        self.assertEqual([r["method"] for r in self.requests], ["GET", "POST"])

        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-a")  # unchanged by the skip-class run

    def test_a_content_failure_does_not_persist_a_drifted_container_id_sections_unparsed(self):
        """R6-c05, second form: `sections_unparsed` -- a NONTRIVIAL body
        (>= `_MIN_NONTRIVIAL_BODY`) with neither section parseable -- is
        the FAILURE-class sibling of the quiet `empty_sections` case right
        above; both are decided inside `_build_content`, strictly after
        the owner check, so both close the same gap in `test_a_skip_
        class_reason_does_not_persist_a_drifted_container_id`."""
        self._write("2026-09-20-1000-x.md", updated_at="2026-09-20T10:00:00Z")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        with mock.patch.object(_identity, "container_id", return_value="dev-box-a"):
            self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "none")
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-a")

        body = "# Title\n\n" + ("prose with no known section headings at all. " * 10)
        self.assertGreaterEqual(len(body.strip()), _MOD._MIN_NONTRIVIAL_BODY)
        self._write(
            "2026-09-20-1100-y.md", body=body, updated_at="2026-09-20T11:00:00Z",
        )
        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self._run(session_id="sess-2")
        self.assertEqual(self._last_entry()["reason"], "sections_unparsed")
        self.assertEqual([r["method"] for r in self.requests], ["GET", "POST"])

        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-a")  # unchanged by the failure-class run

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

    def test_main_return_value_reflects_a_ledger_write_left_behind(self):
        """R3-c13: `main()`'s return value is what `__main__` uses to
        choose between `os._exit(0)` and `sys.exit(0)` (via
        `_hook_runner.finish`) -- dropping `record_left_behind` from
        `main`'s own `return record_left_behind or left_behind` would
        leave a ledger-write thread abandoned mid-write with no
        protection at all, and until `_run_main` was fixed (this same
        finding) to return `main()`'s own value, nothing here could have
        caught it: every existing test that stalls the ledger write only
        ever inspects the LEDGER, never what `main()` reported back."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")

        release = threading.Event()
        self.addCleanup(release.set)

        def stalled_update_state(name, cwd, mutate):
            release.wait(30)
            return dict(mutate({})), []

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"), \
                mock.patch.object(_hook_state, "update_state", side_effect=stalled_update_state), \
                mock.patch.object(_MOD, "_LEDGER_BUDGET_SECONDS", 0.2):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            with mock.patch("sys.stderr"):
                returned = self._run(session_id="sess-1")
        self.assertTrue(returned)

    def test_main_return_value_is_false_on_a_clean_run(self):
        """The other half of R3-c13: a clean run (nothing left behind)
        must return a FALSY value, not merely "truthy sometimes" -- the
        pair together pin the actual boolean, not just its presence."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        returned = self._run(session_id="sess-1")
        self.assertFalse(returned)

    def test_record_does_not_persist_when_the_work_thread_was_abandoned(self):
        """R3-c12: `_record`'s persist gate is `run["persist_container_id"]
        and not work_left_behind` -- but no fixture called `_record`
        directly with a combination isolating the SECOND half.
        `_collect` sets `persist_container_id = True` a few lines before
        it returns, so a work thread abandoned in that exact window
        would leave the flag True while the run's true outcome is
        unknown; dropping "and not work_left_behind" from the gate still
        passed the whole suite (confirmed by mutation), because reaching
        this combination in a real run needs an actual thread race, which
        no existing test drives at this level -- `_record` is a plain
        function and needs none of that to exercise directly."""
        _hook_state.update_state(_MOD.HOOK, self.cwd, lambda s: {**s, "container_id": CONTAINER})
        run = {
            "cwd": self.cwd, "calls": 0, "extra": {}, "persist_container_id": True,
            "deadline": _MOD.time.monotonic() + 10,
        }
        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            _MOD._record("none", _MOD.time.monotonic(), run, work_left_behind=True)
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), CONTAINER)  # NOT overwritten
        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertIsNone(entries[-1]["calls"])  # R1-c09's own rule still holds here too

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

    def test_a_transient_container_id_failure_during_the_persist_check_does_not_swallow_state_write_failed(self):
        """R4-c05: a full write-path run with drift calls `_identity.
        container_id()` up to THREE times, across TWO different functions
        (R6-c08: an earlier revision of this docstring attributed all
        three to `write()` alone, though its own very next clause already
        named two of them as being inside `_collect` instead) --
        `identity_drift`'s own `current=` and the `IngestClient`
        constructor, BOTH inside `_collect`, and -- protected by its OWN
        try/except, the actual fix -- the precompute right before
        `update_state`, inside `_record`'s own nested `write()` closure.
        This pins that the fix HOLDS for the precompute's own call
        specifically raising: `update_state` is never even reached (the
        `except` branch sets `state_write_failed` directly, see below),
        but the supplementary row it guarantees must still land.

        R5-c05: an EARLIER revision of this docstring (and the comment
        below) described the THIRD call as "the unprotected comparison"
        -- true of the CODE this fix REPLACED, no longer true of the code
        that replaced it: post-468fcc2 the comparison itself makes no
        call at all (`_new_state.get("container_id") == new_container_id`,
        a plain variable read) -- see the test right below this one for a
        mutant that reintroduces a FRESH, unprotected call at that exact
        spot, which nothing here catches."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")

        def failing_update_state(name, cwd, mutate):
            return {}, ["state_write_failed"]

        with mock.patch.object(
            _identity, "container_id",
            # The two real calls _collect makes (identity_drift's
            # `current=`, then the IngestClient constructor) return the
            # new id normally; the THIRD is the protected precompute in
            # `write()` itself, which is what this raises on.
            side_effect=["dev-box-b", "dev-box-b", RuntimeError("transient container_id failure")],
        ) as mock_container_id, mock.patch.object(
            _hook_state, "update_state", side_effect=failing_update_state
        ) as mock_update_state:
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            with mock.patch("sys.stderr"):
                self._run(session_id="sess-1")

        # R5-c05: the precompute's OWN exception is caught in the `except`
        # branch, which sets `state_write_failed` directly WITHOUT ever
        # reaching the `else` branch's `update_state` call -- so the mock
        # above is never invoked. Asserted explicitly (code-reviewer #5's
        # own point): the mock existing at all, unreached, is not itself a
        # sign that anything is wrong, but a future edit accidentally
        # making the precompute's exception NOT short-circuit past
        # `update_state` should be visible here, not just coincidentally
        # still pass because both paths happen to set the same reason.
        mock_update_state.assert_not_called()
        self.assertEqual(mock_container_id.call_count, 3)

        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        reasons = [e["reason"] for e in entries]
        # The run's own row must exist and say identity_changed regardless
        # (record_run happens before any of this: R2-c01's ordering) --
        # this much already holds even on the unfixed code.
        self.assertIn("identity_changed", reasons)
        # The point of this test: the supplementary state_write_failed row
        # must ALSO exist -- a genuine persist failure this run must not be
        # swallowed just because the precompute's own container_id() call
        # happened to raise.
        self.assertIn("state_write_failed", reasons)
        persist_row = entries[-1]
        self.assertEqual(persist_row["reason"], "state_write_failed")
        self.assertFalse(persist_row["ok"])

    def test_an_unprotected_call_reintroduced_at_the_comparison_does_not_swallow_state_write_failed(self):
        """R5-c05: 468fcc2 (R4-c05) fixed the ORIGINAL bug shape by making
        the comparison read a PRECOMPUTED variable (`new_container_id`)
        instead of calling `_identity.container_id()` a second,
        unprotected time -- but nothing pinned that the comparison stays
        a plain variable read: a mutant that changes it back to
        `_new_state.get("container_id") == _identity.container_id()`
        (textually identical to the pre-468fcc2 shape) passed the WHOLE
        suite, this test included, before this test existed (confirmed
        against a temp copy). `update_state` here returns a GENUINE
        persist failure (`state_write_failed`, an empty new state) -- the
        correct, fixed behaviour is to append the supplementary failure
        row using ONLY the three protected calls below; a FOURTH call
        reintroduced at the comparison consumes this fixture's trailing
        `RuntimeError` and escapes `write()` entirely uncaught (nothing
        wraps that `if` statement), silently losing the very row this
        fixture means to guarantee -- the identical "quieter" shape of
        R4-c05's own original bug, one `except` clause further out."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")

        def failing_update_state(name, cwd, mutate):
            return {}, ["state_write_failed"]

        with mock.patch.object(
            _identity, "container_id",
            # Exactly the three calls the FIXED code makes (drift check,
            # IngestClient constructor, protected precompute) succeed; a
            # mutant reintroducing a FOURTH, unprotected call at the
            # comparison hits this trailing RuntimeError instead.
            side_effect=["dev-box-b", "dev-box-b", "dev-box-b", RuntimeError("must not be called")],
        ), mock.patch.object(_hook_state, "update_state", side_effect=failing_update_state):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            with mock.patch("sys.stderr"):
                self._run(session_id="sess-1")

        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        reasons = [e["reason"] for e in entries]
        self.assertIn("identity_changed", reasons)
        self.assertIn("state_write_failed", reasons)
        persist_row = entries[-1]
        self.assertEqual(persist_row["reason"], "state_write_failed")
        self.assertFalse(persist_row["ok"])

    def test_a_transient_persist_failure_in_steady_state_does_not_append_a_spurious_failure_row(self):
        """R4-c09, mutant M01: deleting the `new_container_id is not None
        and _new_state.get("container_id") == new_container_id` check
        entirely (relying solely on the `"state_write_failed" not in
        persist_reasons` membership check below it) passes every OTHER
        existing test, because in every other fixture the two checks
        return early for the same reason. The one shape that tells them
        apart: STEADY STATE (this container's id is already the value on
        disk -- no drift at all this run) plus update_state's OWN write
        genuinely failing THIS run -- it returns the state as it stood
        BEFORE the mutate, which, in steady state, already equals the
        current id. Without the disk-trust check, this reads as a genuine
        persist failure and appends a spurious row even though nothing on
        disk is actually wrong."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")  # persists CONTAINER; steady state from here on

        def failing_update_state(name, cwd, mutate):
            # The write itself raised; update_state returns the PRE-mutate
            # state (see its own docstring) -- which, in steady state,
            # already carries today's container_id.
            return {"container_id": CONTAINER}, ["state_write_failed"]

        with mock.patch.object(_hook_state, "update_state", side_effect=failing_update_state):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            with mock.patch("sys.stderr"):
                self._run(session_id="sess-1")

        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertNotIn("state_write_failed", [e["reason"] for e in entries])
        self.assertEqual(entries[-1]["reason"], "none")

    def test_an_update_state_mutate_crash_is_not_reported_as_a_persist_failure(self):
        """R4-c09, mutant M02: reverting `"state_write_failed" not in
        persist_reasons` back to the literal `not persist_reasons` passes
        every OTHER existing test too, because the one shape where this
        matters -- ``update_state``'s own mutate callback raising
        (``reasons=["unknown"]``, state left exactly as it was before the
        mutate, per its own docstring) -- is not otherwise exercised.
        Pins the current, intended behaviour: an unrelated mutate crash is
        not reported as a persist failure."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")

        def crashing_update_state(name, cwd, mutate):
            current, _reasons = _hook_state.read_state(name, cwd)
            return current, ["unknown"]

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"), \
                mock.patch.object(_hook_state, "update_state", side_effect=crashing_update_state):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            with mock.patch("sys.stderr"):
                self._run(session_id="sess-1")

        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertNotIn("state_write_failed", [e["reason"] for e in entries])
        self.assertEqual(entries[-1]["reason"], "identity_changed")

    def test_identity_drift_persists_despite_a_degraded_lock_without_a_spurious_failure_row(self):
        """R3-c01 (a regression in the R2-c11 fix above): `_hook_state.
        update_state` can return a NON-EMPTY `reasons` list even when the
        write itself SUCCEEDED -- `_locked` degrades to writing UNLOCKED
        (appending `lock_unavailable`) rather than dropping the write when
        `flock` itself is refused (e.g. an NFS home without lock support).
        The code this fixes treated ANY non-empty `persist_reasons` as
        "persist failed" and appended a bogus `state_write_failed` row on
        top of the correctly-recorded `identity_changed` one -- and because
        `session_inject._failure_report` only reads the ledger's LAST row,
        that bogus row buried the real one."""
        self._write("2026-09-20-1000-x.md")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        self._run(session_id="sess-1")
        self.assertEqual(self._last_entry()["reason"], "none")

        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"), \
                mock.patch.object(_hook_state.fcntl, "flock", side_effect=OSError("no locks")):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m2"})
            self._run(session_id="sess-1")

        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertEqual(entries[-1]["reason"], "identity_changed")
        self.assertTrue(entries[-1]["identity_changed"])
        self.assertNotIn("state_write_failed", [e["reason"] for e in entries])
        state, _ = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(state.get("container_id"), "dev-box-b")  # persisted despite the degraded lock

        # And the drift is not re-reported next run: the persist really landed.
        with mock.patch.object(_identity, "container_id", return_value="dev-box-b"):
            self.backend.reply(200, _page()).reply(201, {"memory_id": "m3"})
            self._run(session_id="sess-1")
        self.assertNotEqual(self._last_entry()["reason"], "identity_changed")

    def test_corrupt_state_file_is_repaired_without_a_spurious_failure_row(self):
        """R3-c01, second scenario: a state file that fails to PARSE (not
        merely missing a key, contrast `test_identity_drift_unknown_does_
        not_set_identity_changed` below) makes `_hook_state.read_state`
        return `({}, ["unknown"])`; `identity_drift` then reports
        `unknown`, not `identity_changed` (R2-c06), since the previous
        identity is unknowable. The SAME corrupted read also flows through
        the persist step's own `update_state` call, which rebuilds from
        `{}` and WRITES A VALID FILE -- so persisting this run's
        container_id actually SUCCEEDS and repairs the state, even though
        `update_state` still returns `reasons=["unknown"]` (not `[]`) for
        that call. The old code read any non-empty `persist_reasons` as
        failure and appended a bogus `state_write_failed` row on top of
        the correctly-recorded `unknown`."""
        self._write("2026-09-20-1000-x.md")
        state_path = _hook_state.state_path(_MOD.HOOK, self.cwd)
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as fh:
            fh.write("{not valid json")
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m1"})
        with mock.patch("sys.stderr"):
            self._run(session_id="sess-1")

        entries, _ = _hook_state.read_ledger(_MOD.HOOK, self.cwd)
        self.assertEqual(entries[-1]["reason"], "unknown")
        self.assertNotIn("state_write_failed", [e["reason"] for e in entries])
        state, reasons = _hook_state.read_state(_MOD.HOOK, self.cwd)
        self.assertEqual(reasons, [])  # the corrupt file was repaired, not left broken
        self.assertEqual(state.get("container_id"), CONTAINER)

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

    def test_a_document_that_vanishes_between_locate_and_open_is_unknown_not_quiet(self):
        """R4-c10: after `_locate` has already picked a document, the
        `open()` inside `_collect` can still fail (a race, a permissions
        change) -- `except OSError` there returns the failure-class
        `unknown`, reported at the next SessionStart (ruling 13: never
        silently change WHICH document was ingested, and that includes
        never silently treating a disappearance as "nothing to ingest").
        Mutating this branch's `return "unknown"` to `return "no_handoff"`
        (a permanent, silent skip) passed the whole suite before this test
        existed (confirmed against a temp copy)."""
        path = self._write("2026-09-20-1000-x.md")
        # A pointer naming it directly (rather than the no-pointer
        # fallback scan) so `_locate` never itself calls `open()` on this
        # path to probe frontmatter -- isolating the ONE open() this test
        # means to fail to `_collect`'s own read of the chosen document.
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")

        def failing_open(target, *a, **kw):
            if os.path.abspath(target) == os.path.abspath(path):
                raise FileNotFoundError(2, "gone")
            return builtins.open(target, *a, **kw)

        with mock.patch.object(_MOD, "open", create=True, side_effect=failing_open):
            self._run(session_id="sess-1")
        self.assertEqual(self.requests, [])
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "unknown")
        self.assertIn("cannot read", entry["detail"])
        findings, _marks = _INJECT_MOD._failure_report(self.cwd)
        self.assertTrue(any("handoff-sync" in f for f in findings), findings)

    def test_a_pointer_target_that_loses_its_read_permission_is_unknown_not_quiet(self):
        """R4-c10, real-filesystem sibling of the mocked test above: a
        pointer resolving DIRECTLY to a candidate (``target in
        candidates``) never opens it to compare timestamps -- `_locate`
        only ``os.stat``s it (permission bits on the FILE itself do not
        block that, only the ENCLOSING directory's execute bit would) --
        so a chmod(000) document that `latest.md` names is still the
        chosen document, and only `_collect`'s own `open()` discovers it
        cannot actually be read."""
        path = self._write("2026-09-20-1000-x.md")
        _write_latest_pointer(self.handoff_dir, "2026-09-20-1000-x.md")
        os.chmod(path, 0o000)
        self.addCleanup(os.chmod, path, 0o644)
        try:
            with open(path):
                pass
        except PermissionError:
            pass
        else:
            self.skipTest("running as a user unaffected by chmod 000 (e.g. root)")
        self._run(session_id="sess-1")
        self.assertEqual(self.requests, [])
        entry = self._last_entry()
        self.assertEqual(entry["reason"], "unknown")
        self.assertIn("cannot read", entry["detail"])

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
        return _run_main(_MOD, {"cwd": cwd, "session_id": session_id}, env)

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


class TestCurrentBranchForwarding(unittest.TestCase):
    """R3-c10: `_current_branch`'s only line -- forwarding to
    `_identity.current_branch` -- is unconditionally patched away in EVERY
    other test class that drives `main()` (needed so those get a fixed,
    known branch name), so that forwarding line itself never actually ran
    anywhere in the existing suite: replacing its body with `return None`
    still passed the whole suite (confirmed by mutation). Calling
    `_MOD._current_branch` directly, in a test class that patches nothing
    on `_MOD` at all, is what actually exercises the real, unmocked
    function body -- an earlier version of this test instead wrapped a
    call to `main()` in `mock.patch.object(_MOD, "_current_branch",
    side_effect=_identity.current_branch)`, which replaces the ATTRIBUTE
    itself with a new Mock and so never runs the original function's body
    (mutated or not) either; it passed even against the `return None`
    mutant for exactly that reason."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = os.path.join(self.tmp.name, "repo")
        os.makedirs(self.repo)
        subprocess.run(["git", "init", "-q", "-b", "a-real-branch", self.repo], check=True, capture_output=True)
        with open(os.path.join(self.repo, "README.md"), "w", encoding="utf-8") as fh:
            fh.write("x\n")
        subprocess.run(["git", "-C", self.repo, "add", "README.md"], check=True, capture_output=True)
        subprocess.run(
            ["git", "-C", self.repo, "-c", "user.email=t@example.com", "-c", "user.name=t",
             "commit", "-q", "-m", "seed"],
            check=True, capture_output=True,
        )

    def test_current_branch_forwards_the_real_value(self):
        self.assertEqual(_MOD._current_branch(self.repo), "a-real-branch")


class TestSilenceStderrFdHygiene(unittest.TestCase):
    """R4-c08: `_silence_stderr` opens a devnull fd BEFORE calling
    `sys.stderr.fileno()` -- if THAT raises (a test double, or any future
    stderr replacement without a real `fileno`), the blanket `except
    Exception: pass` swallows it, but the devnull fd already opened on the
    line before is never closed."""

    def test_does_not_leak_a_devnull_fd_when_fileno_is_unavailable(self):
        opened = []
        real_open = os.open

        def tracking_open(path, flags):
            fd = real_open(path, flags)
            opened.append(fd)
            return fd

        with mock.patch.object(_MOD.os, "open", side_effect=tracking_open), \
                mock.patch.object(sys, "stderr", _BrokenStderr()):  # no .fileno() at all
            _MOD._silence_stderr()  # must not raise
        # Resolving fileno() first (the fix) means a target-less devnull is
        # never opened in the first place; opening it before checking
        # fileno() (the bug) opened exactly one and left it dangling.
        for fd in opened:
            with self.assertRaises(OSError):
                os.fstat(fd)  # fstat on a closed fd raises EBADF; a leaked one would not


class TestStderrGuardProtectsWriteWithBudget(unittest.TestCase):
    """R4-c04, the OTHER half (the import guards are covered end to end by
    ``TestImportGuards``' closed-pipe tests): ``_hook_runner.write_with_
    budget``'s own fallback ``print(..., file=sys.stderr)`` is a bare,
    unguarded write too (a "net under a net" for a ``write()`` that does
    not already protect itself, per that module's own docstring). Once
    ``_StderrGuard`` is installed (``main()``, before the work thread
    starts), it protects THIS print as well, since ``write_with_budget``
    looks up ``sys.stderr`` fresh at call time same as everything else --
    the fix in ``main()`` is not scoped to only this file's own ``_warn``
    call sites. R5-c04: proving that specifically needs a DIRECT test of
    ``_StderrGuard`` itself (below) -- the end-to-end test through
    ``write_with_budget`` alone cannot tell "``_StderrGuard`` protected
    this" apart from "``run_with_deadline``'s own separate, OUTER
    ``except Exception`` did", because the latter would swallow whatever
    escaped the former just the same."""

    def test_a_write_that_raises_does_not_escape_through_a_broken_real_stream(self):
        """R5-c04: this pins the LAYERED integration -- `write_with_budget`
        completes (not left behind) even when its target raises AND the
        fallback diagnostic print that follows also fails -- but it does
        NOT, on its own, pin `_StderrGuard`'s OWN `except OSError`
        specifically: `run_with_deadline`'s own OUTER `except Exception`
        (`_hook_runner.py`'s `work()`) would swallow whatever escaped
        `_StderrGuard` just the same, so `left_behind` reads `False`
        whether or not `_StderrGuard` protects anything at all (confirmed:
        removing its `except OSError` entirely still leaves this exact
        assertion green). `test_stderr_guard_wrapping_a_broken_real_
        stream_does_not_raise` below tests the class directly, which is
        what actually pins ITS contract; this one stays as the end-to-end
        "nothing hangs or crashes the caller" claim its own name makes."""
        guarded_stderr = _MOD._StderrGuard(_BrokenStderr())  # no real fd behind it either
        with mock.patch.object(sys, "stderr", guarded_stderr):
            def failing_write():
                raise RuntimeError("boom, escapes write()'s own protections")

            left_behind = _MOD._hook_runner.write_with_budget(failing_write, 2.0, "probe")
        self.assertFalse(left_behind)  # the thread completed; nothing hung or crashed the test

    def test_stderr_guard_wrapping_a_broken_real_stream_does_not_raise(self):
        """R5-c04: the direct pin the test above cannot provide -- calls
        `_StderrGuard`'s own `write`/`flush` straight, with nothing
        upstream (`run_with_deadline`'s outer catch) able to paper over a
        regression here. `sys.stderr` is patched to this SAME guard object
        first: `_silence_stderr()` (triggered internally by the OSError
        below) reads the GLOBAL `sys.stderr`, not `self`, and this keeps
        that call safe -- it delegates through `_BrokenStderr`'s missing
        `fileno()` (an `AttributeError`, swallowed by `_silence_stderr`'s
        own blanket except) instead of redirecting the REAL test
        process's fd 2 to `/dev/null` for the rest of the suite."""
        guard = _MOD._StderrGuard(_BrokenStderr())
        with mock.patch.object(sys, "stderr", guard):
            self.assertEqual(guard.write("x"), 1)  # swallowed, not raised
            guard.flush()  # must not raise either

    def test_stderr_guard_passes_through_unknown_attributes(self):
        """R5-c09: `__getattr__` is purely defensive -- nothing in this
        plugin currently reads anything off `sys.stderr` beyond `write` /
        `flush` / `fileno` (all three explicitly defined) -- but deleting
        it entirely still passed the WHOLE suite before this test existed
        (confirmed against a temp copy). A future consumer (stdlib code, a
        sibling module, `_ingest_client`) reading e.g. `sys.stderr.
        encoding` off an already-installed guard would otherwise hit a
        bare `AttributeError` with nothing here to catch that regression."""
        class _Extra:
            encoding = "utf-8"

            def isatty(self):
                return False

        real = _Extra()
        guard = _MOD._StderrGuard(real)
        self.assertEqual(guard.encoding, "utf-8")
        self.assertFalse(guard.isatty())  # a bound method, delegated and callable
        with self.assertRaises(AttributeError):
            guard.does_not_exist_anywhere

    def test_stderr_guard_wrapping_none_does_not_raise(self):
        """R5-c02: `sys.stderr is None` is not only the interpreter-startup
        shape `_warn` itself guards against (R4-c07) -- `main()` wraps
        WHATEVER `sys.stderr` currently is, `None` included, in a
        `_StderrGuard` (R4-c06), and any LATER write through that guard
        (from `_ingest_client`'s own unguarded prints, ruling 15) must not
        raise `AttributeError` calling `.write`/`.flush` on a `None`
        `_real`. No existing test constructed `_StderrGuard(None)` --
        `test_a_write_that_raises_does_not_escape_through_a_broken_real_
        stream` above only ever wraps a `_BrokenStderr()`; mutating either
        method's `self._real is None` guard away passed the whole suite
        before this test existed (confirmed against a temp copy)."""
        guard = _MOD._StderrGuard(None)
        self.assertEqual(guard.write("x"), 1)
        guard.flush()  # must not raise


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

    def _run_with_closed_stderr(self, script):
        """Like TestSubprocess's own closed-pipe helper, reused here: a
        closed read end BEFORE the child ever writes means every write to
        the write end is EPIPE -- the only way to observe the interpreter's
        own unconditional reflush-at-shutdown failing a SECOND time with no
        Python-level except anywhere near it (R3-c03's own docstring)."""
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
        """R4-c04: unlike every OTHER stderr write in this file (which all
        go through `_warn`, R3-c03), the three import guards used a bare
        `print(..., file=sys.stderr)`. On a closed pipe that raises
        BrokenPipeError with no `_silence_stderr`-style protection, and
        CPython's own unconditional reflush at shutdown (behind the
        guard's `sys.exit(0)`) then retries the same failed write with no
        Python-level except anywhere near it -- exit code 120, not 0.
        Confirmed empirically (a real closed pipe, not a mock) against the
        unfixed guard before this fix."""
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
        """R5-c02: `_warn`'s own `sys.stderr is None` branch (R4-c07) is
        only EVER reachable from inside these three import guards --
        `main()` installs `_StderrGuard` before any other `_warn` call in
        this file runs, so `TestSubprocess.test_fd_2_closed_before_the_
        interpreter_starts_still_exits_zero_with_no_stdout` (a FULLY
        installed copy, garbage stdin) never actually exercises it: by the
        time THAT test's `_warn(diagnostic)` runs, `sys.stderr` is already
        a `_StderrGuard`, never literally `None` again. Without this
        branch, `print(msg, file=None)` silently FALLS BACK to `sys.
        stdout` -- putting the diagnostic on the one channel a SessionEnd
        hook's contract requires to stay empty, while still exiting 0 (the
        return code alone cannot tell the two apart, which is why this
        asserts `stdout`, not just `code`). Mutating away `_warn`'s `if
        sys.stderr is None: return` passed the whole suite before this
        test existed (confirmed against a temp copy)."""
        for missing in ("_identity.py", "_hook_runner.py", "_ingest_client.py"):
            with self.subTest(missing=missing):
                # `_copy_hook_without` always targets the SAME fixed
                # "partial-install" subdirectory of `self.tmp` -- fine for
                # every OTHER test here (one call each), but this loop
                # calls it three times in the same test, so each iteration
                # gets its own throwaway temp dir instead.
                iter_tmp = tempfile.TemporaryDirectory()
                self.addCleanup(iter_tmp.cleanup)
                target = os.path.join(iter_tmp.name, "partial-install")
                os.makedirs(target)
                for name in (
                    "handoff_sync.py", "_identity.py", "_hook_runner.py",
                    "_ingest_client.py", "_hook_state.py", "_redact.py",
                ):
                    if name != missing:
                        shutil.copy(os.path.join(_HOOKS_DIR, name), target)
                script = os.path.join(target, "handoff_sync.py")
                run_env = _scrub_subprocess_env()
                proc = subprocess.Popen(
                    [sys.executable, script],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None,
                    env=run_env, preexec_fn=lambda: os.close(2),
                )
                stdout, _stderr = _communicate_kill_on_timeout(proc, b"{}", 20)
                self.assertEqual((stdout, proc.returncode), (b"", 0))


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

    def test_garbage_stdin_with_a_closed_stderr_pipe_still_exits_zero(self):
        """R3-c03: garbage stdin drives `_collect` to raise, which `main()`
        reports via `_warn(diagnostic)` -- a `print(..., file=sys.stderr)`.
        Swallowing the `OSError` from THAT ONE call is not enough on its
        own: CPython's own interpreter shutdown (`flush_std_files`, behind
        every plain `sys.exit()`) unconditionally flushes stdout AND
        stderr again once this process is on its way out, and a buffered
        writer whose `write()` raised does not discard what it failed to
        write -- the retry targets the SAME closed pipe and fails again,
        this time with no Python-level `except` anywhere near it, and
        CPython's own hardcoded response to THAT is exit code 120. A REAL
        closed pipe is the only way to observe this (confirmed empirically
        against the unfixed hook before this fix): `subprocess.run(...,
        capture_output=True)` keeps its OWN read end of the stderr pipe
        open for the whole run, which can never reproduce it -- the
        write always succeeds from the child's point of view."""
        r, w = os.pipe()
        os.close(r)  # closed BEFORE the child ever writes: every write to `w` is EPIPE
        run_env = _scrub_subprocess_env()
        run_env["NEXUS_HOOK_STATE_DIR"] = self.state_dir
        proc = subprocess.Popen(
            [sys.executable, _HOOK_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=w, env=run_env,
        )
        os.close(w)  # only the child holds the write end now
        stdout, _stderr = _communicate_kill_on_timeout(proc, b"{not json", 20)
        self.assertEqual((stdout, proc.returncode), (b"", 0))

    def test_fd_2_closed_before_the_interpreter_starts_still_exits_zero_with_no_stdout(self):
        """R4-c07: when fd 2 is closed BEFORE the interpreter even starts
        (as opposed to a pipe that closes mid-run, the shape every other
        closed-pipe test above covers), CPython sets `sys.stderr` to
        `None` rather than a stream object -- confirmed empirically
        (`python3 -c "import sys; print(repr(sys.stderr))"` under the same
        `preexec_fn` reports `None`). `print(msg, file=None)` FALLS BACK to
        `sys.stdout` (also confirmed empirically), which would put
        `_warn`'s diagnostic on the one channel a SessionEnd hook's
        contract requires to stay empty. Garbage stdin drives `_collect` to
        raise, so `main()` calls `_warn(diagnostic)` on the main thread.

        R5-c02: this does NOT, on its own, exercise `_warn`'s own `sys.
        stderr is None` branch (an earlier revision of this docstring
        claimed it did) -- `main()` installs `_StderrGuard(sys.stderr)`
        (wrapping the `None` this test starts with) BEFORE the work thread
        even runs, so by the time THIS `_warn(diagnostic)` call happens,
        `sys.stderr` is already a `_StderrGuard` instance, never literally
        `None` again. What this test actually pins is only the GUARD's
        FLUSH-side `self._real is None` handling, not its write-side one
        (R6-c08): this run's exit code is what CPython's shutdown-time
        reflush produces, and that reflush only ever calls `.flush()` --
        removing `write`'s own `None` guard instead still exits 0 through
        this exact path (the `AttributeError` it would raise escapes
        `_warn`, is caught by `__main__`'s own blanket `except Exception`,
        and `flush`'s still-intact guard no-ops at shutdown same as ever;
        confirmed empirically against a temp copy -- see the `_StderrGuard`
        class docstring's own two-guards paragraph). `write`'s side is
        pinned directly instead, by `test_stderr_guard_wrapping_none_
        does_not_raise` above (see `TestImportGuards`' sibling test above
        for the one shape that DOES exercise `_warn`'s OWN `sys.stderr is
        None` branch, a different guard again: a missing sibling module,
        whose bare `print` runs before any `_StderrGuard` exists to wrap
        anything)."""
        run_env = _scrub_subprocess_env()
        run_env["NEXUS_HOOK_STATE_DIR"] = self.state_dir
        proc = subprocess.Popen(
            [sys.executable, _HOOK_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None, env=run_env,
            preexec_fn=lambda: os.close(2),
        )
        stdout, _stderr = _communicate_kill_on_timeout(proc, b"{not json", 20)
        self.assertEqual((stdout, proc.returncode), (b"", 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
