#!/usr/bin/env python3
"""SessionEnd memory-file-sync hook for the nexus-memory plugin (workflow C,
change 2 TASK-006): ingests this project's Claude Code auto-memory files
(``<CLAUDE_CONFIG_DIR or ~/.claude>/projects/<memory dir key>/memory/*.md``,
excluding ``MEMORY.md`` -- the index, never synced) as ``layer=fact`` rows
(``docs/architecture/memory-layers.md`` §2 / §3.3), one fact per file.

Design contract (proposal ``memory-layer-contract-and-aria-structured-
ingestion`` workflow C; Amendment A8 / A8-2; X1, owner 2026-10-01):

  - Stdlib-only Python 3, zero third-party deps. FAIL-OPEN ALWAYS: exit 0,
    no stdout, ever -- a SessionEnd hook must never block session teardown.
  - **Batched, not all-at-once**: at most ``_BATCH_SIZE`` files touched per
    run (dirty files that changed since their last sync, THEN not-yet-synced
    files picked up by a persisted cursor), so a project's first full sync
    spreads across many SessionEnd runs instead of trying ~100 files' worth
    of embeddings in one 20-second budget.
  - **Cursor rule, written down once and not re-derived per run**: a file's
    ``synced_at`` / whole-file hash only advance in LOCAL state after that
    file gets a non-aborting answer from the server (2xx POST/PATCH, or an
    honest "nothing changed") -- never before. Any round-abort condition
    (403 ``STRUCTURED_INGEST_DISABLED``, 429, a network/timeout failure, the
    run's own time budget) stops the batch AT the failing file; the NEXT
    run's dirty-check / cursor walk naturally retries it first, because its
    state was never advanced.
  - **X1 (owner 2026-10-01, prefix isolation)**: this machine's
    ``NEXUS_DEFAULT_USER_ID`` can be (and, on this machine, is) shared by
    every project, so ``external_id`` alone is not enough to keep two memory
    directories' rows apart -- every ``external_id`` this hook writes is
    prefixed ``<memory dir key>/<slug>``, and its own state file is keyed by
    that SAME string, not by the project basename ``_hook_state.project_dir``
    would use (two worktrees sharing a basename must not share one state
    file -- see ``_identity.memory_dir_key``'s own docstring). Orphan
    reconciliation only ever considers rows whose ``external_id`` starts
    with ``"<memory dir key>/"`` -- WITH the trailing slash, so one memory
    dir key that happens to be a literal string prefix of another's (this
    machine has several) can never claim the other's rows.
  - **A9-11 (owner 2026-10-01): this hook carries NO drift-detection code.**
    An earlier draft of workflow D had every writer check and persist
    ``container_id`` for the injection recipe's benefit; the owner moved
    that entirely into session_inject (TASK-007), which checks and reports
    it once, at SessionStart, instead of up to three times from three
    writers each keeping their own copy. ``container_id`` is still read here
    (it is the provenance key every row carries and the X1 prefix's sibling
    identity), just never compared against a remembered previous value.
  - **A9-13 (owner 2026-10-01): required modules are ``{_identity,
    _hook_runner}``, same as every other hook -- no import degradation.**
    ``_ingest_client`` is ALSO required here (not peripheral bookkeeping,
    unlike ``_hook_state`` in session_capture.py / session_inject.py): this
    hook's entire job is a write through it, and it unconditionally imports
    ``_hook_state`` (which needs ``fcntl``) itself, so there is nothing
    useful left to do without it either.
  - **A9-21 (owner 2026-10-01): the shared stderr guard.** The worker thread
    below calls into ``_ingest_client``, whose own several stderr writes are
    not this file's to own; ``_hook_runner.guard_stderr()``, installed
    before the thread starts (TASK-012), protects every later write to
    ``sys.stderr`` in the process, not just this file's own. The import
    guards immediately below run BEFORE ``_hook_runner`` is even imported
    (one of them exists to report exactly that failing), so they cannot
    depend on it -- hence the local ``_import_warn`` / ``_silence_stderr``
    pair, written like session_capture.py's own (not handoff_sync.py's: that
    hook predates the shared guard and kept its own module-wide ``_warn``
    used everywhere; this one, like session_capture.py, uses a bare
    ``print(..., file=sys.stderr)`` everywhere ELSE, relying on
    ``guard_stderr()`` once it is installed).
  - **A9-7 (owner 2026-10-01): persistence happens entirely INSIDE
    ``_collect``, strictly before ``_record`` ever runs -- there is
    deliberately no handoff_sync-style "persist something once, after the
    ledger row, and append a follow-up row if that persist fails" step
    here.** Every per-file state update (a file's own ``synced_at`` / hash /
    fingerprint, a vanished file's entry being dropped once its DELETE
    confirms) is written via its own ``_hook_state.update_state_at`` call as
    soon as that file's outcome is known, and the round's cursor /
    reconciled flag are written by ONE more such call at the very end of
    ``_collect`` -- all of it before ``_collect`` returns its reason string,
    hence all of it before ``main()`` ever calls ``_record``. A genuine
    failure from any of those writes (``state_write_failed``) is therefore
    always still available to be folded into THIS round's accumulated
    reasons and reported via the ONE ledger row ``_record`` writes, with
    ``also_failed`` (A9-20) carrying anything ``worst_reason`` did not pick
    as the scalar -- there is no later point in the run where a persist
    could fail AFTER the row was already on disk, so the two-row shape
    handoff_sync needed (for its ``container_id`` persist, which runs
    inside ``_record``, after ``record_run``) does not apply here.
  - **A9-20 (owner 2026-10-01): every ledger row carries
    ``extra["also_failed"]``** -- the other failure-class reasons this round
    produced besides the one ``worst_reason`` chose as the scalar
    (``_hook_state.also_failed``, new in this task; memory_sync's own
    ``orphans_deleted`` is its first caller). Needed because
    ``orphans_deleted`` -- like ``dedup_merged`` -- is a ONE-TIME,
    destructive fact about this exact run: the rows it just soft-deleted
    will not be there to delete again, so if a same-run higher-priority
    failure (an ``http_error`` on a later file, say) wins the scalar slot,
    ``orphans_deleted`` would otherwise vanish from the record for good.
  - **Frontmatter: two structures, both accepted** (flat top-level keys, or
    one level of indentation under a top-level ``metadata:`` block -- real
    Claude Code memory files on this machine split roughly 42:60 between
    the two). Only ``description`` (always top-level in every real sample),
    ``type``, ``modified`` and ``originSessionId`` are read; ``modified``
    falls back to the file's own mtime (ISO-8601 UTC) when absent, which
    this machine's corpus shows is the common case (~75%).
  - **Content cap**: each file's body (frontmatter stripped) is capped at
    ``_CONTENT_CAP`` characters, cut at the last PARAGRAPH boundary at or
    before the cut point (falling back to a line boundary, then a hard cut
    only when not even one line fits) -- never mid-line, for the same
    reason ``handoff_sync._cap`` retreats to a line boundary: a value-level
    redaction rule needs to see a secret's whole shape, and a cut landing
    inside one ships half of it before ``_ingest_client``'s redaction pass
    ever sees the other half. ``aria.truncated`` is ALWAYS sent as an
    explicit boolean (Amendment A8): PATCH is a shallow merge, so a file
    that shrinks back under the cap must explicitly send ``false`` or the
    stored ``true`` never clears.
  - **Local dirty-check hash is of the WHOLE FILE, frontmatter included**
    (Amendment A8, follow-up): the server's own ``content_hash`` is of the
    redacted BODY only, so hashing only the body locally would never notice
    an edit confined to the frontmatter (``description`` -- the one field
    the injection recipe renders) and that file would never be re-sent.
  - **A8-2 (redaction-rule fingerprint)**: each file's own state entry also
    records a fingerprint of ``_redact.py``'s source; a mismatch marks that
    file dirty regardless of its content hash, so a redaction-rule change
    gets amortised back over every previously-synced file instead of never
    reaching rows this hook has already stopped re-sending (memory_sync's
    steady state is zero calls).
  - **Orphan reconciliation runs once per state lifetime**, not every run:
    it is guarded by a ``reconciled`` flag in state (set once reconciliation
    reaches a SAFE conclusion -- nothing to delete, or everything found WAS
    deleted) rather than by "state looks empty", because a project that
    genuinely keeps zero local memory files would otherwise look "unreconciled"
    forever and report the safety guard (``orphan_guard``, a failure-class
    reason) on every single session. A guard trip or a mid-cleanup abort
    does NOT set the flag, so it is retried on a later run rather than
    silently accepted as settled.
"""

import hashlib
import json
import os
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# Siblings are imported by name, which only works while this file's directory
# is on sys.path. PYTHONSAFEPATH=1 / `python -P` (3.11+) takes it off, and
# this hook is one self-contained file like its two SessionEnd siblings -- so
# put it back rather than let an interpreter setting switch the plugin off.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)


def _import_warn(message):
    """Print one diagnostic line to stderr, never raising.

    Used ONLY by the three import guards immediately below: at this point in
    the file ``_hook_runner`` may itself be the missing piece (one of these
    guards exists to report exactly that), so nothing here may depend on
    it -- in particular not ``_hook_runner.guard_stderr()``, which is what
    protects every OTHER stderr write in this file (installed at the top of
    ``main()``, before the worker thread starts). Named and shaped exactly
    like session_capture.py's own pair of the same name (Amendment A9-21):
    everywhere else in this file uses a bare ``print(..., file=sys.stderr)``
    instead, relying on that installed guard.

    ``sys.stderr is None`` (fd 2 closed before the interpreter even started)
    is checked first: a bare ``print(message, file=None)`` does not raise --
    it silently FALLS BACK to ``sys.stdout``, which would put this
    diagnostic on the one channel a SessionEnd hook's contract requires to
    stay empty. There is no real file descriptor to redirect in that shape,
    so this simply skips the write.

    A closed stderr PIPE is a different shape (a live stream whose own
    ``write`` raises an ``OSError``, e.g. ``BrokenPipeError``): swallowing
    that from this one call is not enough on its own, because CPython's own
    interpreter shutdown unconditionally reflushes stdout AND stderr again
    once this process is on its way out, retrying the SAME write against the
    SAME closed pipe with no Python-level ``except`` left near it at that
    point -- exit code 120, not 0. ``_silence_stderr`` below reroutes the
    underlying descriptor to ``os.devnull`` so that retry lands somewhere
    that accepts anything.
    """
    if sys.stderr is None:
        return
    try:
        print(message, file=sys.stderr)
    except OSError:
        _silence_stderr()


def _silence_stderr():
    """After a failed write through ``_import_warn`` above, stop the
    interpreter retrying it on the way out.

    ``sys.stderr.fileno()`` is resolved BEFORE ``os.open``: the reverse
    order opens the devnull fd first, and a failing ``fileno()`` (a test
    double, or any future stderr replacement with no real descriptor) would
    then leave it dangling, swallowed by the blanket ``except Exception:
    pass`` below.
    """
    try:
        target_fd = sys.stderr.fileno()
        devnull = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(devnull, target_fd)
        finally:
            os.close(devnull)
    except Exception:
        pass  # not a real file descriptor (tests), or nothing left to protect


try:
    import _identity
except Exception as exc:  # a broken or partial install
    # Imported by a test this must stay loud. Run as a hook it must not be:
    # a traceback is exit 1, reported as a hook error on every session end.
    if __name__ != "__main__":
        raise
    _import_warn(f"[memory-sync] cannot import _identity: {exc!r}")
    sys.exit(0)

try:
    import _hook_runner
except Exception as exc:  # a broken or partial install
    if __name__ != "__main__":
        raise
    _import_warn(f"[memory-sync] cannot import _hook_runner: {exc!r}")
    sys.exit(0)

try:
    import _ingest_client
except Exception as exc:
    # NOT optional bookkeeping (contrast session_capture.py / session_inject.py's
    # `_hook_state`): this hook's whole job is a write through this client,
    # and it unconditionally imports `_hook_state` (which needs `fcntl`)
    # itself -- so a platform without it can do nothing useful here either
    # (A9-13 / Amendment A5-3).
    if __name__ != "__main__":
        raise
    _import_warn(f"[memory-sync] cannot import _ingest_client: {exc!r}")
    sys.exit(0)

import _hook_state  # noqa: E402 - guaranteed importable: _ingest_client already imports it
import _redact  # noqa: E402 - guaranteed importable: _ingest_client already imports it

HOOK = "memory-sync"  # names the ledger file; must stay in session_inject._EXPECTED_LEDGERS

# The name half of X-Nexus-Source. The backend attributes a request by this
# exact string against an allowlist (nexus `mcp_attribution._KNOWN_CLIENTS`);
# renaming it here sends every write to source="unknown".
SOURCE_NAME = "memory-sync-hook"

_HTTP_TIMEOUT_SECONDS = 8
# How long the ledger write may hold up the exit. Normally it takes
# milliseconds; this is the cap for when it does not.
_LEDGER_BUDGET_SECONDS = 2.0
# The hook's own deadline for everything before the ledger (see main()).
_WORK_BUDGET_SECONDS = 20.0
# IngestClient's own `deadline` sits this far inside the work budget, so the
# client can refuse a request it would not finish in time rather than the
# work-budget thread being abandoned mid-call with nothing recorded at all.
_DEADLINE_SLACK_SECONDS = 1.0

# C row: at most this many files (deletes + dirty + new, combined) touched
# per run -- a first full sync spreads over many SessionEnd runs instead of
# trying to embed ~100 files in one 20-second budget.
_BATCH_SIZE = 5
# Checked before every single file this run is about to touch (delete,
# dirty resync, or new); below this many seconds left on the work deadline,
# the round stops here rather than starting a call it could not finish.
_MIN_REMAINING_SECONDS = 3.0

# Content assembly: body only (frontmatter stripped), capped.
_CONTENT_CAP = 10000
_TRUNCATION_MARKER = "\n\n…[truncated]"
# A regular file this small is already generous for any real memory file
# (every file on this machine runs a few KB); the cap exists so a FIFO or a
# device node masquerading as a *.md file cannot block a read indefinitely
# or exhaust memory, not because any real document is expected to hit it
# (mirrors handoff_sync.py's own `_MAX_DOCUMENT_CHARS`).
_MAX_DOCUMENT_CHARS = 1_048_576

# The index file Claude Code itself writes and reads; never a fact. Compared
# case-insensitively, mirroring handoff_sync.py's own exclusion-name
# convention (a project that happens to keep "memory.md" must stay as quiet
# as one with "MEMORY.md").
_MEMORY_INDEX_NAME = "MEMORY.MD"

# Frontmatter: the only keys this hook reads, from EITHER a flat top-level
# `key: value` line or one level of indentation under a top-level
# `metadata:` block (real Claude Code memory files use both shapes).
# `description` is always top-level in every real sample on this machine;
# `name` is read by nothing here (the slug comes from the FILENAME, per the
# C row, not the frontmatter).
_TOP_LEVEL_KEYS = frozenset({"description", "type", "modified", "originSessionId"})
_NESTED_KEYS = frozenset({"type", "modified", "originSessionId"})
_BOM = "﻿"

# Orphan reconciliation: list every layer=fact row for this container, one
# page at a time (contract §6.2 cursor paging -- before_created_at +
# before_id, never offset).
_RECONCILE_PAGE_LIMIT = 100
# A ceiling against a pathological server (an infinite page sequence that
# never returns empty), not a real-world limit: no real project has this
# many memory files.
_RECONCILE_MAX_PAGES = 1000
_RECONCILE_MAX_BODY_BYTES = 2 * 1024 * 1024


# ── on-disk memory directory ─────────────────────────────────────────────

def _memory_dir(key):
    """``<CLAUDE_CONFIG_DIR or ~/.claude>/projects/<key>/memory`` -- where
    Claude Code itself keeps this project's auto-memory files, under the
    SAME key (X1) this hook's external_id prefix and state file use."""
    config_dir = os.environ.get("CLAUDE_CONFIG_DIR") or os.path.expanduser("~/.claude")
    return os.path.join(config_dir, "projects", key, "memory")


def _list_memory_files(memory_dir):
    """``{slug: path}`` for every resolvable ``*.md`` file directly in
    ``memory_dir`` (non-recursive), excluding the index (``MEMORY.md``,
    case-insensitive) and anything that is not a regular file.

    Comes back ``{}`` when the directory does not exist -- the common case
    for any project with no Claude Code memory yet, or none for THIS
    container's config dir -- and that is not an error. Any entry whose own
    stat fails (gone since ``listdir``, an ordinary race; a permission
    problem) is simply left out of this round's candidates, same as a
    vanished handoff candidate in handoff_sync.py's own ``_candidates``: a
    file that disappeared a moment ago is not this hook's problem to
    diagnose, and one that cannot be stat'd cannot be read either.
    """
    try:
        names = os.listdir(memory_dir)
    except OSError:
        return {}
    out = {}
    for name in names:
        if not name.lower().endswith(".md") or name.upper() == _MEMORY_INDEX_NAME:
            continue
        path = os.path.join(memory_dir, name)
        try:
            is_file = stat.S_ISREG(os.stat(path).st_mode)
        except OSError:
            continue
        if is_file:
            out[name[:-3]] = path  # slug = filename without ".md" (C row)
    return out


# ── frontmatter (flat or nested `metadata:`, two structures) ────────────

def _unquote(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def _split_memory_frontmatter(text):
    """``(frontmatter, body)``. ``frontmatter`` holds only the keys this
    hook reads (see ``_TOP_LEVEL_KEYS`` / ``_NESTED_KEYS`` above), from
    EITHER structure real Claude Code memory files use. A document with no
    well-formed ``---``-delimited block at its very first line (tolerating a
    UTF-8 BOM) has no frontmatter at all: ``{}`` and the whole text as
    ``body``.

    Deliberately flat -- one level of quote-stripping, no YAML block
    scalars (``|`` / ``>``), no multi-document nesting beyond the single
    ``metadata:`` level real files use -- the same scope handoff_sync.py's
    own frontmatter parser accepts, for the same reason: nothing in the
    real corpus needs more, and a hand-rolled general YAML parser is a much
    larger surface to get subtly wrong than this file's actual job calls for.

    A line is only ever read as a NESTED key while directly inside a
    ``metadata:`` block (indented, non-blank, immediately following that
    key or another nested line) -- an indented line reached any other way
    is skipped outright rather than treated as a top-level key stripped of
    its leading whitespace, which would let an indented ``modified:`` that
    is not actually inside a recognised block masquerade as a top-level one.
    """
    if text.startswith(_BOM):
        text = text[len(_BOM):]
    lines = text.split("\n")
    if not lines or lines[0].rstrip("\r") != "---":
        return {}, text
    end = None
    for i in range(1, len(lines)):
        if lines[i].rstrip("\r") == "---":
            end = i
            break
    if end is None:  # unterminated block: not a parseable frontmatter
        return {}, text
    frontmatter = {}
    i = 1
    while i < end:
        line = lines[i]
        if not line.strip() or line[:1] in (" ", "\t"):
            i += 1  # blank, or indented outside a recognised nested block
            continue
        if ":" not in line:
            i += 1
            continue
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        if key == "metadata" and not value:
            i += 1
            while i < end and lines[i][:1] in (" ", "\t") and lines[i].strip():
                sub_key, sep, sub_value = lines[i].strip().partition(":")
                if sep and sub_key.strip() in _NESTED_KEYS:
                    frontmatter[sub_key.strip()] = _unquote(sub_value)
                i += 1
            continue
        if key in _TOP_LEVEL_KEYS:
            frontmatter[key] = _unquote(value)
        i += 1
    # The closing fence is conventionally followed by a blank line before the
    # real content (every sample on this machine does this); stripped here
    # rather than left for the cap/truncation logic to trip over, so a fresh
    # file's content never opens or closes on a stray empty line.
    body = "\n".join(lines[end + 1:]).strip("\n")
    return frontmatter, body


# ── content: body only, capped at a paragraph boundary ───────────────────

def _cap_body(body, limit):
    """``(content, truncated)``: ``body`` cut to at most ``limit``
    characters at the last PARAGRAPH boundary (a blank line) at or before
    the cut point, falling back to a line boundary, then -- only when not
    even one line fits -- a hard cut. Mirrors ``handoff_sync._cap``'s own
    layered fallback and the reason for it: a value-level redaction rule
    needs to see a secret's whole shape, and a cut landing mid-secret ships
    half of it before ``_ingest_client``'s own redaction pass ever sees the
    other half.
    """
    if len(body) <= limit:
        return body, False
    if limit < len(_TRUNCATION_MARKER):
        return "", True  # nothing useful fits; not reachable at _CONTENT_CAP's real size
    budget = limit - len(_TRUNCATION_MARKER)
    cut = body[:budget]
    boundary = cut.rfind("\n\n")
    if boundary == -1:
        boundary = cut.rfind("\n")
    if boundary != -1:
        cut = cut[:boundary]
    return cut.rstrip() + _TRUNCATION_MARKER, True


# ── metadata / identity ───────────────────────────────────────────────────

def _mtime_iso(stat_result):
    """``modified`` fallback (~75% of real files on this machine have no
    frontmatter ``modified`` at all): the file's own mtime, ISO-8601 UTC
    with millisecond precision, matching the shape Claude Code's own
    ``modified`` values use."""
    dt = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _whole_file_hash(path):
    """sha256 of the file's raw bytes, frontmatter included -- the LOCAL
    dirty-check hash (Amendment A8 follow-up). Deliberately a different
    value, and a different state key, from the server's own
    ``content_hash`` (``_ingest_client.content_hash``, of the redacted BODY
    only): a file whose only edit is its frontmatter ``description`` has an
    unchanged body hash but a changed whole-file hash, and it is exactly
    that edit this hash exists to catch."""
    with open(path, "rb") as fh:
        return "sha256:" + hashlib.sha256(fh.read(_MAX_DOCUMENT_CHARS + 1)).hexdigest()


def _current_fingerprint():
    """sha256 of ``_redact.py``'s own source (Amendment A8-2). A mismatch
    against a file's stored fingerprint marks it dirty regardless of its
    content hash -- the bytes on disk have not changed, the rule that will
    be applied to them has, and memory_sync's steady state (zero calls)
    would otherwise never re-send a file whose stored row needs rewriting
    under the new rule."""
    try:
        with open(_redact.__file__, "rb") as fh:
            return "sha256:" + hashlib.sha256(fh.read()).hexdigest()
    except OSError:
        return "unknown"


def _build_memory_metadata(key, project_name, slug, frontmatter, modified):
    """§3.3 + X1's two added keys for this row. A source key that is absent
    from ``frontmatter`` is simply omitted (PATCH is a shallow merge, §4):
    sending nothing for a key leaves the stored value alone."""
    metadata = {
        "aria.memory_slug": slug,
        "aria.modified": modified,
        "aria.memory_dir": key,  # X1
        "aria.project": project_name,  # X1
    }
    if frontmatter.get("type"):
        metadata["aria.memory_type"] = frontmatter["type"]
    if frontmatter.get("description"):
        metadata["aria.description"] = frontmatter["description"]
    if frontmatter.get("originSessionId"):
        metadata["aria.origin_session"] = frontmatter["originSessionId"]
    return metadata


# ── memory-sync's own state file (X1 corollary: keyed by memory dir key) ─

def _memory_state_path(key):
    """``${NEXUS_HOOK_STATE_DIR}/memory-sync/<memory dir key>.json`` -- NOT
    ``_hook_state.state_path(HOOK, cwd)`` (keyed by ``project_dir(cwd)``'s
    basename-derived slug): the X1 corollary (owner 2026-10-01) requires
    this file be keyed by the SAME string as the external_id prefix and the
    on-disk memory directory, so two working directories that happen to
    share a basename cannot read and write the same state file -- see
    ``_identity.memory_dir_key``'s own docstring for the mass-deletion
    shape that keying-by-basename would otherwise reproduce."""
    return _hook_state.state_path_at(os.path.join(HOOK, f"{key}.json"))


def _merge_file_entry(state, slug, entry):
    new = dict(state)
    files = dict(new.get("files") or {})
    files[slug] = entry
    new["files"] = files
    return new


def _drop_file_entry(state, slug):
    new = dict(state)
    files = dict(new.get("files") or {})
    files.pop(slug, None)
    new["files"] = files
    return new


# ── per-file dirty check (mtime+size fast path, A8-2 fingerprint) ───────

def _dirty_check(path, stored, fingerprint):
    """``(dirty, file_hash)`` for an already-synced file (``stored`` is its
    existing state entry, never ``None``).

    mtime+size fast path (C row): the whole-file hash is only recomputed
    when either changed since the file's last successful sync -- an
    untouched file costs one ``stat()``, nothing else. A content rewrite
    that happens to preserve mtime (e.g. a script using ``os.utime`` to roll
    it back) is still caught, because such a rewrite essentially always
    changes the file's SIZE too, which alone is enough to fail the fast
    path and force a hash recompute. A fingerprint mismatch (A8-2) dirties
    the file regardless of mtime/size -- the bytes on disk have not
    changed, the redaction rule that will be applied to them has.
    """
    st = os.stat(path)
    if st.st_mtime == stored.get("mtime") and st.st_size == stored.get("size"):
        file_hash = stored.get("file_hash")
        content_changed = False
    else:
        file_hash = _whole_file_hash(path)
        content_changed = file_hash != stored.get("file_hash")
    fingerprint_changed = stored.get("redaction_fingerprint") != fingerprint
    return (content_changed or fingerprint_changed), file_hash


# ── one file: read, build, upsert, persist on success ────────────────────

def _sync_file(client, key, project_name, slug, path, fingerprint):
    """Attempt to sync one memory file as a ``layer=fact`` row. Returns
    ``(reasons, aborts, calls)``.

    On a non-aborting, COMPLETED write (created / updated / unchanged /
    stale_local) this ALSO persists the file's own state entry immediately
    (C row: "先处理后推进" -- process, then advance; never the other way
    round) -- the caller must not do it again. ``stale_local`` counts as
    completed (Amendment A8-2): the server's copy is newer, so there is
    nothing more for THIS run to do with this file, and leaving it dirty
    forever would retry it every round for no reason.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read(_MAX_DOCUMENT_CHARS)
    except UnicodeDecodeError:
        return ["file_unparsable"], False, 0
    except OSError:
        # Listed (or dirty-checked) a moment ago, gone or unreadable now: an
        # ordinary race, not one of the C row's own named reasons.
        return ["unknown"], False, 0
    try:
        st = os.stat(path)
        file_hash = _whole_file_hash(path)
    except OSError:
        return ["unknown"], False, 0

    frontmatter, body = _split_memory_frontmatter(text)
    content, truncated = _cap_body(body, _CONTENT_CAP)
    modified = frontmatter.get("modified") or _mtime_iso(st)
    metadata = _build_memory_metadata(key, project_name, slug, frontmatter, modified)
    metadata["aria.truncated"] = truncated  # Amendment A8: always explicit

    outcome = client.upsert(
        "fact", f"{key}/{slug}", content, metadata,
        local_updated_at=modified, updated_key="aria.modified",
    )
    if outcome.aborts_round:
        return list(outcome.reasons), True, outcome.calls
    if outcome.action in ("created", "updated", "unchanged") or "stale_local" in outcome.reasons:
        entry = {
            "mtime": st.st_mtime,
            "size": st.st_size,
            "file_hash": file_hash,
            "synced_at": _now_iso(),
            "redaction_fingerprint": fingerprint,
        }
        _hook_state.update_state_at(
            _memory_state_path(key),
            lambda s, slug=slug, entry=entry: _merge_file_entry(s, slug, entry),
        )
    return list(outcome.reasons), False, outcome.calls


def _delete_file(client, key, slug):
    """Attempt to delete the server-side row(s) for a locally-vanished
    file. Returns ``(reasons, aborts, calls)``.

    On confirmed deletion (or an honest "nothing_to_do" -- the row was
    already gone) this ALSO clears the file's local state entry; on any
    OTHER outcome it does not, so a failed delete is retried on the next
    round, before anything else in the batch, exactly as the C row
    requires.
    """
    outcome = client.delete("fact", f"{key}/{slug}")
    if outcome.aborts_round:
        return list(outcome.reasons), True, outcome.calls
    if outcome.deleted > 0 or "nothing_to_do" in outcome.reasons:
        _hook_state.update_state_at(
            _memory_state_path(key), lambda s, slug=slug: _drop_file_entry(s, slug)
        )
    return [r for r in outcome.reasons if r != "nothing_to_do"], False, outcome.calls


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ── orphan reconciliation (once per state lifetime; contract §6.2) ──────

def _list_fact_page(base_url, token, user_id, container_id, deadline, before_created_at, before_id):
    """One page of this container's ``layer=fact`` rows. Returns
    ``(rows, reason, calls)``; exactly one of ``rows`` / ``reason`` is
    non-``None``.

    Deliberately NOT part of ``_ingest_client.IngestClient`` (TASK-010's own
    notes: "本客户端不做对账" -- that module's contract is the per-document
    upsert/delete idempotency protocol, verified row by row against one
    ``external_id``; this is a single, cursor-paged listing of EVERY row
    this container has in this layer, with none of that per-row
    verification -- there is no ``external_id`` to verify against, deciding
    which ``external_id``\\ s should no longer exist locally is the whole
    point). Read-only, so it does not need that module's write-path
    hardening (redaction, the unsendable-payload guards, the
    ``X-Bulk-Import`` header): the heaviest thing this does is GET a page of
    up to 100 rows.
    """
    remaining = (deadline - time.monotonic()) if deadline is not None else None
    if remaining is not None and remaining <= 0:
        return None, "timeout", 0
    query = {
        "user_id": user_id, "layer": "fact", "container_id": container_id,
        "limit": _RECONCILE_PAGE_LIMIT,
    }
    if before_created_at is not None and before_id is not None:
        query["before_created_at"] = before_created_at
        query["before_id"] = before_id
    url = f"{base_url}/memories?" + urllib.parse.urlencode(query)
    headers = {
        "Accept": "application/json",
        "X-Nexus-Source": _identity.source_header(SOURCE_NAME),
        "User-Agent": f"nexus-{SOURCE_NAME}/{_identity.plugin_version()}",
    }
    if token:
        headers["X-API-Key"] = token
    req = urllib.request.Request(url, headers=headers)
    timeout = min(_HTTP_TIMEOUT_SECONDS, remaining) if remaining is not None else _HTTP_TIMEOUT_SECONDS
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(_RECONCILE_MAX_BODY_BYTES + 1)
            status = resp.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        try:
            raw = exc.read(_RECONCILE_MAX_BODY_BYTES + 1)
        except Exception:  # noqa: BLE001 - the error body is optional
            raw = b""
    except Exception as exc:  # noqa: BLE001 - every transport failure has a reason
        return None, _hook_state.reason_for_exception(exc), 1
    if len(raw) > _RECONCILE_MAX_BODY_BYTES:
        return None, "http_error", 1
    if not (200 <= status < 300):
        return None, ("rate_limited" if status == 429 else "http_error"), 1
    try:
        body = json.loads(raw)
    except ValueError:
        return None, "http_error", 1
    rows = body.get("memories") if isinstance(body, dict) else None
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        return None, "http_error", 1
    return rows, None, 1


def _collect_fact_rows(base_url, token, user_id, container_id, deadline):
    """Every ``layer=fact`` row this container has, collected IN FULL
    before the caller decides anything (contract §6.2 + the C row's own
    "先收齐全部页、过护栏、再开始删" -- reconciling against a result set
    that is still shrinking while it is being read would make the
    reconciliation delete rows from its own as-yet-unseen tail). Returns
    ``(rows, reason, calls)``.
    """
    rows = []
    calls = 0
    before_created_at = before_id = None
    for _ in range(_RECONCILE_MAX_PAGES):
        page, reason, page_calls = _list_fact_page(
            base_url, token, user_id, container_id, deadline, before_created_at, before_id,
        )
        calls += page_calls
        if reason is not None:
            return None, reason, calls
        if not page:
            return rows, None, calls
        rows.extend(page)
        last = page[-1]
        before_created_at, before_id = last.get("created_at"), last.get("id")
        if not before_created_at or not before_id:
            # A row this contract requires to carry both; cannot safely page
            # further without them.
            return None, "http_error", calls
    return None, "http_error", calls  # pathological: more pages than any real project has


def _reconcile_orphans(client, key, local_slugs, base_url, token, deadline):
    """One-time (per state lifetime) cleanup: soft-delete this container's
    ``layer=fact`` rows under this project's X1 prefix that no longer have
    a local file. Returns ``(reason, done, deleted, calls)``.

    ``done`` is True only when reconciliation reached a SAFE conclusion
    (nothing to delete, or everything found WAS deleted) -- the caller must
    not mark state "reconciled" on anything else, so a guard trip or a
    mid-cleanup abort is retried on a LATER run rather than silently
    accepted as settled.

    The guard (C row): local file count 0, or more orphans than
    ``max(5, 20% of this project's synced row count)`` -- delete nothing,
    report ``orphan_guard``. ``local_slugs`` empty is checked FIRST, ahead of
    and independent of the ratio check: zero local files makes EVERY one of
    this project's own rows look like an orphan, which is the single
    strongest signal that something about directory resolution (an
    unexpected ``cwd``, a ``CLAUDE_CONFIG_DIR`` mismatch) is wrong, not that
    every file was genuinely deleted at once -- a project with zero local
    files AND zero server rows still reaches a clean ``(None, True)``, since
    there is nothing to guard against in the first place.
    """
    rows, reason, calls = _collect_fact_rows(
        base_url, token, client.user_id, client.container_id, deadline
    )
    if reason is not None:
        return reason, False, 0, calls
    prefix = key + "/"
    synced_count = 0
    orphans = []
    for row in rows:
        meta = row.get("metadata")
        external_id = meta.get("external_id") if isinstance(meta, dict) else None
        if not isinstance(external_id, str) or not external_id.startswith(prefix):
            continue  # X1: never another project's rows, whatever they are
        synced_count += 1
        if external_id[len(prefix):] not in local_slugs:
            orphans.append(external_id)
    if not local_slugs:
        return ("orphan_guard" if orphans else None), (not orphans), 0, calls
    if not orphans:
        return None, True, 0, calls
    if len(orphans) > max(5, synced_count * 0.2):
        return "orphan_guard", False, 0, calls
    deleted = 0
    for external_id in orphans:
        remaining = (deadline - time.monotonic()) if deadline is not None else None
        if remaining is not None and remaining < _MIN_REMAINING_SECONDS:
            return "budget_exhausted", False, deleted, calls
        outcome = client.delete("fact", external_id)
        calls += outcome.calls
        if outcome.aborts_round:
            return outcome.reason, False, deleted, calls
        deleted += outcome.deleted
    return None, True, deleted, calls


# ── the work ─────────────────────────────────────────────────────────────

def _remaining(run):
    return run["deadline"] - time.monotonic()


def _collect(run):
    """Do the work. Returns the reason string; raises only for a stdin
    payload that is not a JSON object (the runner maps it via
    ``_hook_state.reason_for_exception``, which resolves unrecognised
    exceptions to ``unknown``)."""
    raw = sys.stdin.read()
    event = json.loads(raw) if raw.strip() else {}
    if not isinstance(event, dict):
        raise ValueError("SessionEnd payload is not a JSON object")
    if isinstance(event.get("cwd"), str) and event["cwd"]:
        run["cwd"] = event["cwd"]
    cwd = run["cwd"] or os.getcwd()

    base_url = os.environ.get("NEXUS_API_URL", "").rstrip("/")
    if not base_url:
        return "not_configured"  # the default for a fresh install; do nothing else

    key, degraded = _identity.memory_dir_key(cwd)
    run["extra"]["memory_dir"] = key
    toplevel, _ = _identity.project_root(cwd)
    project_name = os.path.basename(toplevel) if toplevel else os.path.basename(cwd.rstrip("/"))

    local_files = _list_memory_files(_memory_dir(key))
    run["extra"]["local_files"] = len(local_files)

    state_path = _memory_state_path(key)
    state, reasons = _hook_state.read_state_at(state_path)
    files_state = dict(state.get("files") or {})
    reconciled = bool(state.get("reconciled"))
    sorted_slugs = sorted(local_files)
    n = len(sorted_slugs)
    cursor = state.get("cursor")
    if not isinstance(cursor, int) or cursor < 0 or cursor >= n:
        cursor = 0

    token = os.environ.get("NEXUS_API_TOKEN", "")
    client = _ingest_client.IngestClient(
        base_url, token, _identity.user_id(cwd), _identity.container_id(), SOURCE_NAME,
        timeout=_HTTP_TIMEOUT_SECONDS, bulk=True, deadline=run["deadline"],
        identity_degraded=degraded,
    )

    fingerprint = _current_fingerprint()
    budget = _BATCH_SIZE
    aborted = False
    calls = 0
    orphans_deleted = 0

    # -- pending deletes (vanished local files), retried first every round --
    vanished = sorted(slug for slug in files_state if slug not in local_files)
    for slug in vanished:
        if aborted or budget <= 0:
            break
        if _remaining(run) < _MIN_REMAINING_SECONDS:
            reasons.append("budget_exhausted")
            aborted = True
            break
        r, ab, c = _delete_file(client, key, slug)
        reasons.extend(r)
        calls += c
        budget -= 1
        if ab:
            aborted = True

    # -- orphan reconciliation: independent of the batch above/below (a
    # failure here does not stop the sync batch, and vice versa), but still
    # gated by the SAME budget check as every other network-making phase,
    # and refused outright under a guessed identity (mirrors IngestClient's
    # own identity_degraded guard on upsert/delete -- this listing is not a
    # method of that class (see _list_fact_page's own docstring) so it does
    # not inherit that guard for free, and a bulk-delete decision is exactly
    # the kind of call a guessed user_id/key must never be allowed to drive) --
    if not reconciled:
        if degraded:
            reasons.append("identity_unresolved")
        elif _remaining(run) < _MIN_REMAINING_SECONDS:
            reasons.append("budget_exhausted")
            aborted = True
        else:
            recon_reason, recon_done, recon_deleted, recon_calls = _reconcile_orphans(
                client, key, set(local_files), base_url, token, run["deadline"],
            )
            calls += recon_calls
            if recon_deleted:
                orphans_deleted = recon_deleted
                reasons.append("orphans_deleted")
            elif recon_reason:
                reasons.append(recon_reason)
            if recon_done:
                reconciled = True

    # -- dirty set: already-synced files whose content or redaction rule
    # changed since their last sync (checked first, ahead of the cursor) --
    dirty = []
    if not aborted:
        for slug in sorted_slugs:
            stored = files_state.get(slug)
            if stored is None:
                continue  # "new" -- handled by the cursor walk below
            is_dirty, _ = _dirty_check(local_files[slug], stored, fingerprint)
            if is_dirty:
                dirty.append(slug)

    for slug in dirty:
        if aborted or budget <= 0:
            break
        if _remaining(run) < _MIN_REMAINING_SECONDS:
            reasons.append("budget_exhausted")
            aborted = True
            break
        r, ab, c = _sync_file(client, key, project_name, slug, local_files[slug], fingerprint)
        reasons.extend(r)
        calls += c
        budget -= 1
        if ab:
            aborted = True

    # -- cursor walk: not-yet-synced files, resuming where the last round
    # stopped (an abort or a budget exhaustion), wrapping at the list's end --
    if not aborted and n and budget > 0:
        i = cursor
        examined = 0
        while examined < n and budget > 0:
            slug = sorted_slugs[i]
            i = (i + 1) % n
            examined += 1
            if slug in files_state:
                continue  # already synced & clean (or just handled above) -- free skip
            if _remaining(run) < _MIN_REMAINING_SECONDS:
                reasons.append("budget_exhausted")
                cursor = (i - 1) % n  # resume AT this same file next time
                aborted = True
                break
            r, ab, c = _sync_file(client, key, project_name, slug, local_files[slug], fingerprint)
            reasons.extend(r)
            calls += c
            budget -= 1
            if ab:
                cursor = (i - 1) % n  # resume AT the failing file next time
                aborted = True
                break
        else:
            cursor = i  # ran out of budget, or completed a full lap with none new

    _, persist_reasons = _hook_state.update_state_at(
        state_path,
        lambda s, cursor=cursor, reconciled=reconciled: {**s, "cursor": cursor, "reconciled": reconciled},
    )
    reasons.extend(persist_reasons)

    run["calls"] = calls
    if orphans_deleted:
        run["extra"]["orphans_deleted"] = orphans_deleted
    final_reason = _hook_state.worst_reason(reasons)
    also = _hook_state.also_failed(reasons, final_reason)
    if also:
        run["extra"]["also_failed"] = also
    return final_reason


def _record(reason, started, run, work_left_behind):
    """Append this run to the ledger, within a budget. Never raises.

    Nothing is persisted here beyond the ledger row itself -- see the
    module docstring's A9-7 paragraph for why that is a deliberate
    departure from handoff_sync.py's own ``_record`` (which persists
    ``container_id`` AFTER ``record_run``, inside this same budgeted write,
    and appends a follow-up row on a genuine failure to do so): every state
    update this hook makes already happened inside ``_collect``, strictly
    before this function is ever called.
    """
    elapsed_ms = int((time.monotonic() - started) * 1000)
    # A work thread abandoned mid-call never reaches the line that assigns
    # `run["calls"]`, so it is still sitting at its initial 0 -- which reads
    # as "definitely made no requests" when the truth is "we do not know".
    calls = None if work_left_behind else run["calls"]
    cwd = run["cwd"]
    extra = dict(run["extra"]) or None

    def write():
        try:
            _hook_state.record_run(
                HOOK, ok=not _hook_state.is_failure_reason(reason), reason=reason,
                elapsed_ms=elapsed_ms, calls=calls, cwd=cwd, extra=extra,
            )
        except Exception as exc:  # record_run does not raise by contract; the net under it
            print(f"[{HOOK}] could not record this run ({reason}): {exc!r}", file=sys.stderr)

    left_behind = _hook_runner.write_with_budget(write, _LEDGER_BUDGET_SECONDS, f"{HOOK}-ledger")
    if left_behind:
        print(
            f"[{HOOK}] ledger write still running after {_LEDGER_BUDGET_SECONDS}s; "
            f"leaving it behind, this run ({reason}) may go unrecorded",
            file=sys.stderr,
        )
    return left_behind


def main():
    """Run the hook. Returns True when a worker thread had to be left behind."""
    started = time.monotonic()
    # Installed before the work thread starts (Amendment A9-21): _collect,
    # on that thread, calls into _ingest_client, whose own stderr writes
    # this file does not own but must still not let crash the run.
    # guard_stderr() is idempotent, so repeated in-process main() calls
    # within the same test process do not double-wrap.
    _hook_runner.guard_stderr()
    run = {
        "cwd": None,
        "calls": 0,
        "extra": {},
        # An absolute time.monotonic() value, about _DEADLINE_SLACK_SECONDS
        # before the work budget itself expires: IngestClient refuses a
        # request it could not finish in time rather than this whole worker
        # thread being abandoned mid-call with nothing recorded.
        "deadline": started + _WORK_BUDGET_SECONDS - _DEADLINE_SLACK_SECONDS,
    }

    outcome, left_behind = _hook_runner.run_with_deadline(
        lambda: _collect(run), _WORK_BUDGET_SECONDS, f"{HOOK}-work"
    )

    reason = "unknown"
    diagnostic = None
    if left_behind:
        reason = "timeout"
        diagnostic = f"[{HOOK}] {reason}: no result after {_WORK_BUDGET_SECONDS}s; leaving the work behind"
    elif "result" not in outcome:
        exc = outcome.get("error", RuntimeError("the worker ended without a result"))
        reason = _hook_state.reason_for_exception(exc)
        diagnostic = f"[{HOOK}] {reason}: {exc!r}"
    else:
        reason = outcome["result"]

    # _record (the ledger row for THIS run) runs BEFORE the diagnostic
    # print, not after (mirrors handoff_sync.py's R2-c05 / TASK-012's
    # verification item 2 for session_capture / session_inject): a stderr
    # write that fails -- a closed pipe, the host already exiting -- must
    # not be able to lose this run's row by raising before _record ever
    # runs. guard_stderr() (above) is a second, independent net: even the
    # reordering does not help if a LATER stderr write in _record's own
    # ledger-write path were to fail the same way.
    record_left_behind = _record(reason, started, run, left_behind)
    if diagnostic is not None:
        print(diagnostic, file=sys.stderr)
    return record_left_behind or left_behind


if __name__ == "__main__":
    left_behind = False
    try:
        left_behind = bool(main())
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session teardown over memory-file sync.
        pass
    _hook_runner.finish(left_behind)
