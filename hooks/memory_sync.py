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
  - **A9-7 (owner 2026-10-01, REVISED post_implementation R2 -- supersedes
    the R1 revision of this paragraph): EVERYTHING this round persists is
    written INSIDE ``_collect``, the instant it is known -- there is no
    round-end persist left at all, handoff_sync-shaped or otherwise.**
    Every per-file state update (a file's own ``synced_at`` / hash /
    fingerprint, a vanished file's entry being dropped once its DELETE
    confirms) is written via its own ``_hook_state.update_state_at`` call
    as soon as that file's outcome is known, strictly before ``_record``
    ever runs. The round's cursor advances THE SAME WAY, per file --
    merged into that file's own state-entry write when there is one for a
    cursor-walk file, or on its own for a deterministic local skip that
    writes no entry (``_advance_cursor_only``) -- and ``reconciled`` is
    persisted the instant reconciliation itself concludes. The R1 revision
    of this paragraph moved the cursor/reconciled persist into ``_record``,
    AFTER ``record_run``, reasoning that the handoff_sync-style two-row
    shape was required by this ruling -- it was not: the ruling's own text
    requires PER-FILE persistence, explicitly contrasted with handoff_
    sync's own round-end shape, and the R1 revision reproduced the exact
    failure mode A9-7 forbids (a round-end write that could stall AFTER a
    file's own successful sync, parking its cursor advance behind an
    unrelated slow lock). Removing that write removes R1's own follow-up
    ``state_write_failed`` row with it: a genuine persist failure anywhere
    in this chain is simply one more reason THIS round produced (``_fold_
    persist_reasons``), folded in before ``_collect`` ever returns -- there
    is no longer a later write for it to arrive too late for. Every FACT
    this round produces (reasons, ``calls``, ``orphans_deleted``) is still
    written straight into ``run`` as it happens, not accumulated in a local
    variable only transferred at the very end -- so an exception partway
    through a round does not lose what already happened before it
    (post_implementation R1 finding K02, unchanged by R2).
  - **Deviation registration (A9-7, orchestrator ruling O1, 2026-10-02):**
    the paragraph above is a DELIBERATE departure from the literal
    TASK-006 notes wording for A9-7 ("只在真失败时补一行 state_write_failed,
    补写行带上主行 reason 与主行全部 also_failed") -- that sentence describes
    TWO ledger rows per run: this round's own row, plus a SEPARATE
    follow-up row appended only on a genuine persist failure, carrying the
    main row's reason/also_failed forward. R2-C03/C05 above removed that
    follow-up row entirely, along with the round-end persist it existed to
    cover, and R2's own commit message did not list the removal in its
    deviation registry even though it is one (gate finding, 2026-10-02,
    post_implementation R2 self-audit: the engineering argument was sound
    but the departure from a binding ruling's literal text was never
    flagged for owner sign-off the way the team's own process requires).
    The orchestrator ruling accepts R2's single-row shape for THIS hook
    specifically: A9-7's actual purpose -- the round's own row is never
    lost, a reason is never buried, a genuine persist failure is never
    reported as a clean run -- is met MORE strongly by folding than by a
    follow-up row, because nothing in this file is EVER persisted after
    the row in the first place (everything moves inside ``_collect``,
    strictly before ``_record`` runs -- see above). Do NOT restore a
    two-row shape and do NOT move any persistence after the row without a
    NEW owner ruling superseding O1; this paragraph is the pending
    write-back to the parent repo's proposal.md as Amendment A10.**
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

import errno
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
import fcntl  # noqa: E402 - guaranteed importable: _hook_state already imports it (K09 run lock)

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

# K25 (post_implementation R1, NOT applied -- see not_fixed): a project
# literally named "memory-sync" normalizes (_identity.normalize_slug) to a
# project-slug directory that COLLIDES with this constant's own value, so
# session_inject's ledger scan can read this hook's own per-memory-dir-key
# state files as that project's unreadable ledgers (reproduced; see the R1
# report). The cluster's own fix -- a leading "." no project slug can ever
# produce -- is NOT applied here: the owner ruling's own corollary (X1,
# item 2) and the unchanged-items list (item 7) both give this exact path
# literally, `${NEXUS_HOOK_STATE_DIR}/memory-sync/<memory dir key>.json`,
# and "do not change behaviour a binding ruling covers, except as the
# ruling says" leaves no room to rename it unilaterally. Left as `HOOK`
# (below) on purpose, matching the ruling's literal text; the collision
# risk is real but reported to the owner (see owner_questions), not solved
# here. K09's new run-lock file lives in the SAME directory for the same
# reason -- no new namespace to litigate.
_STATE_SUBDIR = HOOK

# K24: bumped whenever _split_memory_frontmatter / _cap_body / _cap_for_wire
# / _build_memory_metadata's own content/metadata ASSEMBLY rules change in a
# way that would change what an already-synced file's row looks like on the
# wire -- same reasoning as _redact.py's own fingerprint (A8-2), folded into
# the SAME fingerprint so a rule change amortises back over every
# already-synced file exactly once, not only when _redact.py itself changes.
_ASSEMBLY_VERSION = 2


# ── on-disk memory directory ─────────────────────────────────────────────

def _memory_dir(key):
    """``<CLAUDE_CONFIG_DIR or ~/.claude>/projects/<key>/memory`` -- where
    Claude Code itself keeps this project's auto-memory files, under the
    SAME key (X1) this hook's external_id prefix and state file use."""
    config_dir = os.environ.get("CLAUDE_CONFIG_DIR") or os.path.expanduser("~/.claude")
    return os.path.join(config_dir, "projects", key, "memory")


def _list_memory_files(memory_dir):
    """``(files, indeterminate, reason)``.

    ``files`` is ``{slug: path}`` for every resolvable ``*.md`` file
    directly in ``memory_dir`` (non-recursive), excluding the index
    (``MEMORY.md``, case-insensitive) and anything that is not a regular
    file (following one level of symlink). ``indeterminate`` is the set of
    slugs whose own listing entry could NOT be conclusively resolved --
    something is there, it just was not safely stat-able -- and must be
    treated as "still present" everywhere a vanished-file or orphan
    decision is made, never as deleted. ``reason`` is ``None`` unless the
    directory itself could not be listed for anything other than "it does
    not exist" (``ENOENT`` -- the common, unremarkable case for a project
    with no Claude Code memory yet).

    K01 (post_implementation R1): the previous version of this function
    collapsed EVERY ``OSError`` from ``os.listdir`` -- ``EACCES``, ``EIO``,
    ``ESTALE``, ``ENOTDIR`` included -- to the exact same ``{}`` as "this
    directory genuinely does not exist", and a per-entry ``stat`` failure
    was simply dropped from the result with no trace. The caller (
    ``_collect``) computed its pending-DELETE set as "a slug state
    remembers that is not a key of this return value" -- so "I could not
    tell whether this project's memory directory is even the right one"
    became indistinguishable from "every file in it was deleted", and a
    transient directory-resolution failure (a misconfigured
    ``CLAUDE_CONFIG_DIR``, an ``NFS`` hiccup, a permissions change) drove a
    real, server-side soft-delete of every row this project had ever
    synced -- repeating, and completing, on the very next round. ``reason``
    and ``indeterminate`` exist so the caller can refuse to treat either
    shape as "confirmed gone" (see the pending-delete computation in
    ``_collect``).

    A dangling or otherwise unresolvable symlink is ``indeterminate``, not
    skipped outright the way a genuinely vanished (``ENOENT`` on
    ``lstat``) entry is: the NAME is still there (``os.listdir`` saw it),
    only its TARGET could not be confirmed -- the same "judged, not
    knowable" split the A9-11/A9-19 handoff-sync lineage already applies to
    its own candidate scan.
    """
    try:
        names = os.listdir(memory_dir)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return {}, set(), None
        return {}, set(), "unknown"
    out = {}
    indeterminate = set()
    for name in names:
        if not name.lower().endswith(".md") or name.upper() == _MEMORY_INDEX_NAME:
            continue
        slug = name[:-3]  # slug = filename without ".md" (C row)
        path = os.path.join(memory_dir, name)
        try:
            entry_stat = os.lstat(path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                continue  # the NAME itself is gone: an ordinary listdir race
            indeterminate.add(slug)
            continue
        if stat.S_ISLNK(entry_stat.st_mode):
            try:
                entry_stat = os.stat(path)  # follow the link once
            except OSError:
                # Dangling target, a loop (ELOOP), or unreadable: something
                # is still THERE (the link itself exists) -- not knowable as
                # "resolves to a regular file", but just as surely not
                # knowable as "deleted" either.
                indeterminate.add(slug)
                continue
        if stat.S_ISREG(entry_stat.st_mode):
            out[slug] = path
    return out, indeterminate, None


# ── frontmatter (flat or nested `metadata:`, two structures) ────────────

def _unquote(value):
    """Strip one layer of quoting from a frontmatter scalar value.

    K23: a double-quoted value is first tried as a JSON string literal
    (``json.loads``), which -- unlike the previous plain ``value[1:-1]`` --
    actually decodes YAML's double-quote escapes (``\\"``, ``\\\\``, ...;
    YAML's double-quoted scalar escaping is a superset of JSON's own, and
    every escape this corpus's real files use is the JSON subset). Falls
    back to the old strip-only behaviour for anything that is not valid
    JSON once quoted (YAML escapes JSON does not have, or simply malformed
    input) rather than raising. A single-quoted value uses YAML's own
    escape instead (a doubled quote is a literal one), which JSON has no
    equivalent for.
    """
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        try:
            decoded = json.loads(value)
        except ValueError:
            decoded = None
        if isinstance(decoded, str):
            return decoded
        return value[1:-1]
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def _split_memory_frontmatter(text):
    """``(frontmatter, body)``. ``frontmatter`` holds only the keys this
    hook reads (see ``_TOP_LEVEL_KEYS`` / ``_NESTED_KEYS`` above), from
    EITHER structure real Claude Code memory files use. A document with no
    well-formed ``---``-delimited block at its very first line (tolerating a
    UTF-8 BOM) has no frontmatter at all: ``{}`` and the whole text as
    ``body``.

    A document that DOES open with a ``---`` line but never closes it is a
    DIFFERENT case (K07): ``frontmatter`` comes back ``None`` (never
    ``{}``, which means "no block at all, the whole text is the body") so
    the caller can tell "this file genuinely has no frontmatter" apart from
    "this file's frontmatter is broken" -- the latter is a local
    deterministic error (``file_unparsable``, C row) the caller must skip
    and report, never silently upload with the open fence's own
    ``name:``/``description:`` lines shipped as plain content and no
    ``aria.description`` at all.

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
    if end is None:  # unterminated block: not a parseable frontmatter (K07)
        return None, text
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


def _cap_for_wire(body, limit):
    """``(content, truncated)``: ``body`` capped at ``limit`` characters,
    re-cutting as many times as needed so the REDACTED text -- what
    ``_ingest_client`` actually puts on the wire -- also fits ``limit``
    (K06, mirrors ``handoff_sync._cap_for_wire``).

    ``_cap_body`` alone is redaction-OBLIVIOUS (deliberately -- see its own
    docstring): it cuts the RAW body to ``limit`` characters, but a
    redaction MARKER can be LONGER than the secret it replaces
    (``[redacted:url-userinfo]`` is 23 characters against a 4-character
    minimum password), so content that lands at EXACTLY ``_CONTENT_CAP`` --
    honouring ``_cap_body``'s own invariant -- can still exceed the backend's
    own ``content`` ``max_length=10000`` by the time redaction has run,
    which the backend answers with a permanent per-file 422 (``rejected_422``)
    that a whole-file-hash dirty check can never clear on its own, because
    the bytes on disk never change again.

    Re-running the SAME redaction pass here (rather than predicting its
    growth analytically) is the simplest thing that stays correct;
    ``_ingest_client`` redacts this same text again on the way out, which is
    idempotent (a redaction marker itself matches no rule). Bounded: each
    iteration's cut is by at least the previous iteration's overflow, so
    this converges in one or two passes for any realistic document; capped
    at ``limit`` iterations as a hard ceiling against a pathological future
    redaction rule that never converges. ``limit`` for each re-cut is
    derived from ``len(content)`` itself, not from the original ``limit``
    (the same R3-c02 lesson ``handoff_sync._cap_for_wire`` already carries):
    computing it from the original would sometimes yield a limit LARGER
    than the current content, reading as "already short enough" while the
    REDACTED form still overflows.
    """
    content, truncated = _cap_body(body, limit)
    for _ in range(limit):
        redacted, _hits = _redact.redact_text(content)
        overflow = len(redacted) - limit
        if overflow <= 0:
            return content, truncated
        new_limit = len(content) - overflow
        if new_limit <= 0:
            return content, truncated  # nothing left to safely cut
        content, cut_again = _cap_body(content, new_limit)
        truncated = truncated or cut_again
    return content, truncated


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
    """sha256 of ``_redact.py``'s own source, folded with ``_ASSEMBLY_
    VERSION`` (Amendment A8-2, widened by K24). A mismatch against a file's
    stored fingerprint marks it dirty regardless of its content hash -- the
    bytes on disk have not changed, the RULE that will be applied to them
    has, and memory_sync's steady state (zero calls) would otherwise never
    re-send a file whose stored row needs rewriting under the new rule.

    K24: the same amortise-over-every-synced-file reasoning applies equally
    to a change in how THIS file assembles a row (``_split_memory_
    frontmatter``, ``_cap_body``/``_cap_for_wire``, ``_build_memory_
    metadata``) -- a file's own bytes never change just because this
    module's parsing/assembly rules did, so a fingerprint scoped to
    ``_redact.py`` alone would never dirty it either. One fingerprint, not
    two independent ones: the two numbers do not need to vary separately,
    and a single stored value keeps the existing state schema and every
    comparison site (``_dirty_check``) unchanged.
    """
    try:
        with open(_redact.__file__, "rb") as fh:
            digest = hashlib.sha256(fh.read())
    except OSError:
        return "unknown"
    digest.update(f"assembly:{_ASSEMBLY_VERSION}".encode("ascii"))
    return "sha256:" + digest.hexdigest()


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
    shape that keying-by-basename would otherwise reproduce. The directory
    is ``_STATE_SUBDIR`` (``== HOOK``, i.e. the literal ``"memory-sync"``
    the owner ruling's own corollary gives this path as) -- see that
    constant's own docstring (K25) for a real, REPORTED-not-fixed
    collision this literal name has with the ledger directory namespace
    for a project whose own slug happens to be "memory-sync"."""
    return _hook_state.state_path_at(os.path.join(_STATE_SUBDIR, f"{key}.json"))


def _memory_run_lock_path(key):
    """``${NEXUS_HOOK_STATE_DIR}/memory-sync/<memory dir key>.run.lock`` --
    a per-memory-dir-key, whole-ROUND mutual-exclusion lock (K09): two
    SessionEnd runs for the SAME memory directory (two sessions in the same
    project ending within the same short window) each start from the same
    unlocked state snapshot and would otherwise both select, and both POST,
    the same brand-new file -- this client's own idempotency protocol only
    de-duplicates on the FOLLOWING run's lookup, so the steady state
    (zero-call once everything is synced, orphan reconciliation retired
    after one state lifetime) never naturally re-visits the pair to merge
    it. Deliberately a SEPARATE file from ``_memory_state_path``'s own
    per-write ``.lock`` (``_hook_state._locked`` already takes that one for
    each individual read-modify-write): this one is held for the WHOLE
    network-making portion of one round, which ``_locked`` is not shaped
    for and must not be repurposed to do."""
    return _hook_state.state_path_at(os.path.join(_STATE_SUBDIR, f"{key}.run.lock"))


_NO_RUN_LOCK = -1  # sentinel: the lock could not even be ATTEMPTED; proceed unlocked


def _acquire_run_lock(path):
    """Non-blocking per-round lock (K09). Returns an fd to release later,
    ``None`` when a PEER run already holds it (the caller must do no
    network work this round), or ``_NO_RUN_LOCK`` when the lock could not
    even be attempted (the state directory is not writable, the filesystem
    does not support ``flock``) -- degrading to "proceed WITHOUT a lock"
    rather than refusing to ever work again, because that failure mode is
    not the one this lock exists to guard against and treating it as
    "contended" would silently stop every future round.

    R2-C02 (post_implementation R2): only ``BlockingIOError`` (``EAGAIN`` /
    ``EWOULDBLOCK`` -- what ``flock(..., LOCK_NB)`` actually raises for a
    lock a peer genuinely holds) means contention. Any OTHER ``OSError``
    from the ``flock`` call itself (``ENOLCK``, ``EOPNOTSUPP``, ``ENOSYS``,
    ``EINVAL``, an ``EACCES`` from a test double, ...) means the lock could
    not be taken AT ALL -- the previous code folded every ``OSError`` here
    into "a peer holds it", which on a filesystem that cannot ``flock``
    (NFS without lockd, say) meant this hook did ZERO work, forever, while
    every OTHER hook on the same filesystem degrades and keeps going
    (``_hook_state._locked``'s own ``lock_unavailable`` precedent)."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    except OSError:
        return _NO_RUN_LOCK
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    except OSError:
        os.close(fd)
        return _NO_RUN_LOCK
    return fd


def _release_run_lock(fd):
    if fd is None or fd == _NO_RUN_LOCK:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        os.close(fd)
    except OSError:
        pass


def _coerce_files_map(value):
    """K05: ``state["files"]`` as a dict, whatever it actually holds on
    disk. A hand-edited or pre-X1-migration state file can carry a list, a
    string, or entries that are themselves not dicts; ``dict(value or {})``
    (the previous shape here) either raises (``dict(["a-string"])``) or
    silently invents nonsense keys (``dict(["ab"])`` -> ``{"a": "b"}``) --
    either way corrupting the NEXT merge/drop instead of self-healing it.
    Anything not already a dict is treated as absent, which lets the
    upcoming write replace it with a well-formed one."""
    return dict(value) if isinstance(value, dict) else {}


def _merge_file_entry(state, slug, entry):
    new = dict(state)
    new["files"] = {**_coerce_files_map(new.get("files")), slug: entry}
    return new


def _drop_file_entry(state, slug):
    new = dict(state)
    files = _coerce_files_map(new.get("files"))
    files.pop(slug, None)
    new["files"] = files
    return new


def _register_placeholder_entries(state, to_register):
    """K22: used only by orphan reconciliation, for a slug this round's
    listing just confirmed exists BOTH locally and on the server but which
    has no state entry yet (the common case right after a lost/rebuilt
    state file, before every local file has been individually re-synced).
    Registering it now -- with no ``file_hash`` of its own -- moves it out
    of "not in state, handled by the cursor walk eventually" and into the
    dirty set starting the VERY NEXT round (the fast path in
    ``_dirty_check`` cannot match ``None`` against a real mtime/size, so it
    recomputes and finds nothing actually changed -> ``unchanged``). Without
    this, a file deleted in the window between a state loss and this same
    file's own turn on the cursor would never be seen as "vanished" (it was
    never IN ``state["files"]`` to begin with) and its server row would be
    orphaned forever -- reconciliation itself only runs once per state
    lifetime.
    """
    new = dict(state)
    files = _coerce_files_map(new.get("files"))
    for slug, entry in to_register.items():
        files.setdefault(slug, entry)
    new["files"] = files
    return new


# ── per-file dirty check (mtime+size fast path, A8-2 fingerprint) ───────

def _dirty_check(path, stored, fingerprint):
    """``(dirty, file_hash)`` for an already-synced file (``stored`` is its
    existing state entry, never ``None``).

    mtime+size+ctime fast path (C row, widened by K21): the whole-file hash
    is only recomputed when one of the three changed since the file's last
    successful sync -- an untouched file costs one ``stat()``, nothing
    else. mtime+size ALONE missed a same-length content rewrite whose
    script also rolls mtime back with ``os.utime`` (the earlier docstring
    here assumed that was "essentially always" paired with a size change,
    which is false for e.g. a single fixed-width character edited in
    place) -- ``st_ctime`` is bumped by the kernel on ANY inode metadata or
    content change and cannot itself be set back by ``os.utime`` (POSIX has
    no syscall for that), so it closes exactly that gap. A fingerprint
    mismatch (A8-2 / K24) dirties the file regardless of mtime/size/ctime --
    the bytes on disk have not changed, the rule that will be applied to
    them has. A false-positive "dirty" from the ctime check alone (e.g. a
    chmod with no content change) costs one extra hash recompute, which
    then compares equal and is simply ``unchanged`` -- never an extra wire
    call.
    """
    st = os.stat(path)
    same_stat = (
        st.st_mtime == stored.get("mtime")
        and st.st_size == stored.get("size")
        and getattr(st, "st_ctime_ns", None) == stored.get("ctime")
    )
    if same_stat:
        file_hash = stored.get("file_hash")
        content_changed = False
    else:
        file_hash = _whole_file_hash(path)
        content_changed = file_hash != stored.get("file_hash")
    fingerprint_changed = stored.get("redaction_fingerprint") != fingerprint
    return (content_changed or fingerprint_changed), file_hash


# ── one file: read, build, upsert, persist on success ────────────────────

def _fold_persist_reasons(reasons, persist_reasons):
    """Append only a GENUINE write failure from ``update_state_at``'s own
    returned reasons into ``reasons`` (mutated in place). Per the owner
    ruling on A9-7 (post_implementation R1, item 4): ``lock_unavailable`` (a
    degraded-but-unlocked write that still completed) and ``unknown`` (a
    corrupt state file ``update_state_at``'s own read side just repaired,
    whose write then still landed) are NOT failures of this call -- only
    ``state_write_failed`` means the write itself did not happen. The
    previous code folded ALL of ``persist_reasons`` in unconditionally,
    which reported a purely-local, successfully-self-healed lock hiccup as
    a round failure the ledger's ``ok`` field and ``worst_reason`` both then
    acted on.

    R2-C06: ``update_state_at``'s own ``state_write_failed`` now ALSO
    covers a transient read-side failure it refused to build a write on
    top of (``_hook_state._read_state_file_detailed``'s ``safe_to_
    rebuild=False``) -- the write genuinely did not happen in that case
    either, so it belongs in this same bucket; only a repaired CORRUPT
    file (``unknown``, a true self-heal whose write still landed) stays
    excluded."""
    if "state_write_failed" in persist_reasons:
        reasons.append("state_write_failed")


def _upsert_retrying_404(client, layer, external_id, content, metadata, *, local_updated_at, updated_key):
    """``client.upsert`` with ONE retry when the PATCH target vanished
    between this call's own lookup and its write (K27): the row a
    ``upsert`` just found by lookup can be deleted server-side (another
    concurrent run's dedup/orphan cleanup, a console/MCP delete) in the
    narrow window before the PATCH reaches it, which the backend answers
    404 -- classified ``http_error`` (a round-abort reason) by
    ``_ingest_client``, same as any other non-2xx it does not special-case.
    Per the owner ruling (post_implementation R1, item covering K27): a 404
    on a PATCH this client itself just looked up (``outcome.memory_id`` is
    set -- never a 404 from the LOOKUP call itself, which would leave
    ``memory_id`` unset and is a configuration problem, not a race) clears
    the stale mapping and re-queries ONCE rather than aborting the whole
    round and parking every file behind this one for a full round. A
    second 404 (or any other failure) on the retry is reported normally --
    this is a single race-closing retry, not a loop.

    R2-C04 (post_implementation R2): the judgment is ``outcome.write_
    status`` -- the status the upsert's OWN create/update call received --
    never ``outcome.status`` (the last HTTP status ANY call on this
    outcome saw). A dedup delete runs, when it runs, BEFORE the PATCH/POST
    this function is really asking about; ``_dedup`` treats its own 404 as
    "already gone" and keeps going, but ``_call`` still overwrites
    ``status`` with it -- so if the SUBSEQUENT write then fails at the
    transport level (a timeout, a connection reset: no response, hence no
    NEW status), the stale 404 from the unrelated delete used to read as
    "the write 404'd", triggering a retry for a network error the C row
    requires this round to simply STOP on, and silently erasing that
    delete's own ``dedup_merged`` fact in the process (a one-time,
    destructive, must-not-vanish fact per A9-20). The budget is checked
    again before spending a second round-trip on this one file; carrying
    ``outcome``'s own ``dedup_merged`` into the retry (and the reason, if
    it is not already there) keeps that fact alive whichever outcome is
    finally returned -- ``redacted`` is NOT carried forward, because both
    attempts redact the exact same text and summing would double-count it.
    """
    outcome = client.upsert(
        layer, external_id, content, metadata,
        local_updated_at=local_updated_at, updated_key=updated_key,
    )
    if outcome.aborts_round and outcome.write_status == 404 and outcome.memory_id:
        remaining = client.remaining()
        if remaining is not None and remaining < _MIN_REMAINING_SECONDS:
            return outcome
        retry = client.upsert(
            layer, external_id, content, metadata,
            local_updated_at=local_updated_at, updated_key=updated_key,
        )
        retry.calls += outcome.calls
        retry.dedup_merged += outcome.dedup_merged
        if outcome.dedup_merged and "dedup_merged" not in retry.reasons:
            retry.reasons.insert(0, "dedup_merged")
        return retry
    return outcome


def _tally_result(run, slug, reasons, outcome):
    """K08: fold one file's result into this round's ledger-visible
    bookkeeping -- ``run["extra"]["redacted"]`` / ``["dedup_merged"]``
    (summed across every file this round touched, not just the one that
    happens to win the scalar ``reason``) and a bounded ``["failed"]`` list
    naming which file hit a failure-class reason, so a round that fails on
    file 47 of 100 does not leave a reader to guess which one. ``outcome``
    is ``None`` for the local, pre-``upsert``/``delete`` failures (a read
    error, an unparsable frontmatter) that never got as far as making a
    client call."""
    if outcome is not None:
        if outcome.redacted:
            run["extra"]["redacted"] = run["extra"].get("redacted", 0) + outcome.redacted
        if outcome.dedup_merged:
            run["extra"]["dedup_merged"] = run["extra"].get("dedup_merged", 0) + outcome.dedup_merged
    failure = next(
        (r for r in reasons if r != "state_write_failed" and _hook_state.is_failure_reason(r)), None,
    )
    if failure is None:
        return
    failed = run["extra"].setdefault("failed", [])
    if len(failed) >= 5:  # bounded: a reader needs examples, not every row of a bad batch
        return
    entry = {"slug": slug, "reason": failure}
    if outcome is not None:
        if outcome.status is not None:
            entry["status"] = outcome.status
        if outcome.detail:
            entry["detail"] = str(outcome.detail)[:200]
    failed.append(entry)


def _advance_cursor_only(state_path, next_cursor):
    """Persist ONLY the round's cursor advance (R2-C03): for a cursor-WALK
    file whose own outcome produced no per-file state entry to merge it
    into -- a deterministic local skip (``file_unparsable``,
    ``rejected_422``, ``unknown``) or an ordinary ``FileNotFoundError``
    race. ``next_cursor`` is ``None`` for every call from the DIRTY set
    (cursor only ever moves while walking NEW files, never while
    re-syncing an already-known one), in which case this is a no-op.
    Never called when the round is ABORTING on this file: the C row
    requires the NEXT round resume AT the failing file, which is already
    where the on-disk cursor sits from the previous successful advance --
    see ``_sync_file``'s own docstring. Returns ``persist_reasons`` so the
    caller folds a genuine failure in exactly like every other per-file
    persist in this module."""
    if next_cursor is None:
        return []
    _, persist_reasons = _hook_state.update_state_at(
        state_path, lambda s, next_cursor=next_cursor: {**s, "cursor": next_cursor}
    )
    return persist_reasons


def _sync_file(client, key, project_name, slug, path, fingerprint, run, *, next_cursor=None):
    """Attempt to sync one memory file as a ``layer=fact`` row. Returns
    ``(reasons, aborts, calls)``. Tallies into ``run["extra"]`` as it goes
    (K08) -- see ``_tally_result``.

    On a non-aborting, COMPLETED write (created / updated / unchanged /
    stale_local) this ALSO persists the file's own state entry immediately
    (C row: "先处理后推进" -- process, then advance; never the other way
    round) -- the caller must not do it again. ``stale_local`` counts as
    completed (Amendment A8-2): the server's copy is newer, so there is
    nothing more for THIS run to do with this file, and leaving it dirty
    forever would retry it every round for no reason.

    A genuine failure to persist that entry (``state_write_failed``) is
    folded into the REASONS this function returns -- not discarded. This
    call always runs strictly before ``_record`` (A9-7), so there is no
    already-written ledger row for the failure to arrive too late for; it
    is simply one more reason this round produced.

    ``next_cursor`` (R2-C03, post_implementation R2): the CURSOR WALK's
    caller (``_collect``) passes the on-disk cursor position to persist
    once this file's own outcome is known to be non-aborting -- ``None``
    for a DIRTY-set call, which never advances the cursor. The binding
    ruling (A9-7) requires the cursor to advance PER FILE, in state,
    exactly like a file's own ``synced_at``/hash -- never deferred to a
    round-end write that runs after the ledger row (the earlier
    post_implementation R1 revision of this comment argued that shape was
    required here; it was not, and moving it into ``_record`` reproduced
    the very failure mode A9-7 exists to prevent -- see this module's own
    changelog / git history for ``_record``). It is persisted MERGED into
    this file's own state-entry write when there is one (one
    ``update_state_at`` call, not two), or on its own
    (``_advance_cursor_only``) for a deterministic local skip that writes
    no entry of its own -- never when ``outcome.aborts_round`` is true,
    where the cursor must stay exactly where it already is.

    K19: the file is read exactly ONCE (one ``open`` + one ``fstat`` off
    the SAME descriptor), and the whole-file hash is computed from those
    SAME bytes -- the previous version opened the file a second time purely
    to stat it and hash it again, which left a window for a concurrent
    rewrite (another session, or Claude Code's own background memory
    writer) to land BETWEEN the two opens: ``state`` would then remember
    the SECOND version's mtime/size/hash while the row actually sent to the
    server was built from the FIRST version's bytes, and the discrepancy is
    permanent (the fast path in ``_dirty_check`` and the local hash both
    agree with the recorded, wrong, state from then on). ``synced_at`` is
    likewise stamped at READ time, not after the round-trip to the server,
    for the same "what state records must describe the bytes actually
    sent" reason.
    """
    state_path = _memory_state_path(key)
    try:
        with open(path, "rb") as fh:
            st = os.fstat(fh.fileno())
            raw = fh.read(_MAX_DOCUMENT_CHARS + 1)
    except FileNotFoundError:
        # Listed (or dirty-checked) a moment ago, gone now: an ordinary
        # race, not one of the C row's own named reasons -- nothing to
        # report, the next round's listing simply will not see it either.
        # Still a non-aborting outcome, so the cursor walk still advances
        # past it (R2-C03).
        reasons = []
        _fold_persist_reasons(reasons, _advance_cursor_only(state_path, next_cursor))
        return reasons, False, 0
    except OSError:
        _tally_result(run, slug, ["unknown"], None)
        reasons = ["unknown"]
        _fold_persist_reasons(reasons, _advance_cursor_only(state_path, next_cursor))
        return reasons, False, 0
    if not stat.S_ISREG(st.st_mode):
        _tally_result(run, slug, ["unknown"], None)
        reasons = ["unknown"]
        _fold_persist_reasons(reasons, _advance_cursor_only(state_path, next_cursor))
        return reasons, False, 0
    synced_at = _now_iso()  # K19: the instant the bytes below were read
    file_hash = "sha256:" + hashlib.sha256(raw[: _MAX_DOCUMENT_CHARS + 1]).hexdigest()
    try:
        text = raw[:_MAX_DOCUMENT_CHARS].decode("utf-8")
    except UnicodeDecodeError:
        _tally_result(run, slug, ["file_unparsable"], None)
        reasons = ["file_unparsable"]
        _fold_persist_reasons(reasons, _advance_cursor_only(state_path, next_cursor))
        return reasons, False, 0

    frontmatter, body = _split_memory_frontmatter(text)
    if frontmatter is None:  # K07: an opened but never-closed frontmatter block
        _tally_result(run, slug, ["file_unparsable"], None)
        reasons = ["file_unparsable"]
        _fold_persist_reasons(reasons, _advance_cursor_only(state_path, next_cursor))
        return reasons, False, 0
    content, truncated = _cap_for_wire(body, _CONTENT_CAP)  # K06: capped post-redaction
    modified = frontmatter.get("modified") or _mtime_iso(st)
    metadata = _build_memory_metadata(key, project_name, slug, frontmatter, modified)
    metadata["aria.truncated"] = truncated  # Amendment A8: always explicit

    outcome = _upsert_retrying_404(
        client, "fact", f"{key}/{slug}", content, metadata,
        local_updated_at=modified, updated_key="aria.modified",
    )
    if outcome.aborts_round:
        _tally_result(run, slug, outcome.reasons, outcome)
        return list(outcome.reasons), True, outcome.calls
    reasons = list(outcome.reasons)
    if outcome.action in ("created", "updated", "unchanged") or "stale_local" in outcome.reasons:
        entry = {
            "mtime": st.st_mtime,
            "size": st.st_size,
            "ctime": getattr(st, "st_ctime_ns", None),  # K21
            "file_hash": file_hash,
            "synced_at": synced_at,
            "redaction_fingerprint": fingerprint,
        }

        def _mutate(s, slug=slug, entry=entry, next_cursor=next_cursor):
            new = _merge_file_entry(s, slug, entry)
            if next_cursor is not None:  # R2-C03: one write, not two
                new["cursor"] = next_cursor
            return new

        _, persist_reasons = _hook_state.update_state_at(state_path, _mutate)
        _fold_persist_reasons(reasons, persist_reasons)
    else:
        # A deterministic, non-aborting outcome with no entry of its own
        # to merge the advance into (e.g. rejected_422) -- the cursor
        # still advances past it (C row: skip and advance), on its own.
        _fold_persist_reasons(reasons, _advance_cursor_only(state_path, next_cursor))
    _tally_result(run, slug, reasons, outcome)
    return reasons, False, outcome.calls


def _delete_file(client, key, slug, run):
    """Attempt to delete the server-side row(s) for a locally-vanished
    file. Returns ``(reasons, aborts, calls, cleared)``. Tallies into
    ``run["extra"]`` as it goes (K08) -- see ``_tally_result``.

    On confirmed deletion (or an honest "nothing_to_do" -- the row was
    already gone) this ALSO clears the file's local state entry; on any
    OTHER outcome it does not, so a failed delete is retried on the next
    round, before anything else in the batch, exactly as the C row
    requires. ``cleared`` tells the caller whether that happened, so a
    round's ledger can report how many pending deletes actually finished
    (K01, point 4) without re-deriving it from ``reasons``.

    K20: the mapping is cleared ONLY when every row the lookup found this
    call was confirmed deleted AND that lookup page was not itself full
    -- not merely ``outcome.deleted > 0``, which the previous version
    accepted. ``IngestClient.delete`` only ever looks at ONE page of up to
    ``LOOKUP_LIMIT`` rows and stops at the FIRST row it fails to delete (a
    non-abort rejection such as ``422`` or ``filter_suspect`` is not a
    round-abort reason, so the loop does not raise or set ``aborts_round``
    -- it simply returns with ``deleted`` shy of ``found``): a file with
    more duplicate rows than fit on one page, or one whose later row was
    rejected after earlier ones already succeeded, both left
    ``outcome.deleted > 0`` while rows for this ``external_id`` still
    existed server-side -- and clearing the mapping right there means this
    hook never looks at that slug again (orphan reconciliation only runs
    once per state lifetime, and a cleared slug is not even a candidate
    for it).

    R2-C11 (post_implementation R2): "full" is judged on ``outcome.
    page_rows`` -- the RAW row count the lookup page actually carried --
    never ``outcome.found`` (the VERIFIED count, after ``_is_ours``
    filtering). A page of exactly ``LOOKUP_LIMIT`` raw rows where one of
    them fails verification (another project's row that happened to share
    a page, say) leaves ``found < LOOKUP_LIMIT`` even though a FULL page
    really was returned -- using ``found`` for the fullness check let that
    case clear the mapping after deleting every row THIS client could
    verify, even though a duplicate outside this one page could still
    exist server-side.

    A genuine failure to persist that clear (``state_write_failed``) is
    folded into the returned reasons, same as ``_sync_file`` above and for
    the same reason: this call, too, always runs strictly before
    ``_record``.
    """
    outcome = client.delete("fact", f"{key}/{slug}")
    if outcome.aborts_round:
        _tally_result(run, slug, outcome.reasons, outcome)
        return list(outcome.reasons), True, outcome.calls, False
    reasons = [r for r in outcome.reasons if r != "nothing_to_do"]
    page_full = outcome.page_rows >= _ingest_client.LOOKUP_LIMIT
    cleared = "nothing_to_do" in outcome.reasons or (
        outcome.found > 0 and outcome.deleted == outcome.found and not page_full
    )
    if cleared:
        _, persist_reasons = _hook_state.update_state_at(
            _memory_state_path(key), lambda s, slug=slug: _drop_file_entry(s, slug)
        )
        _fold_persist_reasons(reasons, persist_reasons)
    _tally_result(run, slug, reasons, outcome)
    return reasons, False, outcome.calls, cleared


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
    # K26: read the body in deadline-bound chunks (``_ingest_client.
    # _read_body``), not a single ``resp.read(N)``/``exc.read(N)`` -- urllib's
    # own ``timeout`` bounds a single socket OPERATION, not the whole read,
    # so a server that drips a few bytes per tick could keep this call (and
    # the worker thread running it) alive well past ``deadline``, past the
    # hook's own work budget, and into the thread being abandoned entirely
    # (``timeout`` reported with NOTHING this round recorded, including
    # whatever the earlier pages / pending-delete phase already did -- the
    # exact failure mode the per-request ``_call`` deadline in
    # ``_ingest_client`` exists to prevent for every OTHER request this hook
    # makes).
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = _ingest_client._read_body(resp, deadline, _RECONCILE_MAX_BODY_BYTES, timeout)
            status = resp.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        try:
            raw = _ingest_client._read_body(exc, deadline, _RECONCILE_MAX_BODY_BYTES, timeout)
        except Exception:  # noqa: BLE001 - the error body is optional
            raw = b""
    except _ingest_client._Oversize:
        return None, "http_error", 1
    except Exception as exc:  # noqa: BLE001 - every transport failure has a reason
        return None, _hook_state.reason_for_exception(exc), 1
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

    R2-C10 (post_implementation R2, forward-progress check): a backend
    that silently ignores the ``before_created_at``/``before_id`` query
    keys (contract §6.2's "a caller must verify every filter key it
    sent") would otherwise re-serve the IDENTICAL first page forever --
    unbounded except by ``_RECONCILE_MAX_PAGES``/the deadline, and this
    hook would never notice it was making no progress at all. The first
    row's id reappearing on a LATER page is the same "did my filter
    actually take" signal ``_ingest_client``'s own per-document lookup
    already treats as ``filter_suspect`` one level down (a single row);
    here it is a whole-LISTING-level version of the same check.
    """
    rows = []
    calls = 0
    before_created_at = before_id = None
    seen_ids = set()
    for _ in range(_RECONCILE_MAX_PAGES):
        page, reason, page_calls = _list_fact_page(
            base_url, token, user_id, container_id, deadline, before_created_at, before_id,
        )
        calls += page_calls
        if reason is not None:
            return None, reason, calls
        if not page:
            return rows, None, calls
        first_id = page[0].get("id")
        if first_id is not None and first_id in seen_ids:
            return None, "filter_suspect", calls
        for row in page:
            row_id = row.get("id")
            if row_id is not None:
                seen_ids.add(row_id)
        rows.extend(page)
        last = page[-1]
        before_created_at, before_id = last.get("created_at"), last.get("id")
        if not before_created_at or not before_id:
            # A row this contract requires to carry both; cannot safely page
            # further without them.
            return None, "http_error", calls
    return None, "http_error", calls  # pathological: more pages than any real project has


def _reconcile_orphans(client, key, local_slugs, base_url, token, deadline, run):
    """One-time (per state lifetime) cleanup: soft-delete this container's
    ``layer=fact`` rows under this project's X1 prefix that no longer have
    a local file. Returns ``(reasons, done, calls, matched_slugs)``.

    ``local_slugs`` (R2-C01, post_implementation R2): the caller passes
    ``set(local_files) | indeterminate`` -- every slug ``_list_memory_
    files`` could not conclusively confirm as gone must be treated as
    "still present" here exactly like everywhere else a vanished-file or
    orphan decision is made (the pending-delete walk already did this;
    this call did not, which let an indeterminate file's server row look
    exactly like a genuine orphan and get soft-deleted the moment it fell
    inside the guard's ceiling). The caller is responsible for separately
    reporting an ``indeterminate`` slug (it is never silently absorbed as
    "resolved" just because it happens to be a candidate here).

    ``run`` (R2-C07, post_implementation R2): every successful deletion's
    count and reason are written straight into ``run["extra"]
    ["orphans_deleted"]`` / ``run["reasons"]`` (the SAME list ``_collect``
    already tracks, via ``run["reasons"] = reasons`` there) THE INSTANT it
    happens -- mirroring ``_ingest_client._dedup``'s own A8-6 pattern --
    rather than accumulated in a local variable and reported only once
    this function returns. A worker thread abandoned mid-batch (a local
    stall unrelated to any one network call) must not lose a deletion that
    already happened before the stall; the previous version's local
    ``deleted`` counter was invisible to the ledger row ``main()`` builds
    from whatever ``run`` holds when a thread is abandoned.

    ``done`` is True only when reconciliation reached a SAFE conclusion
    (nothing to delete, or everything found WAS deleted) -- the caller must
    not mark state "reconciled" on anything else, so a guard trip or a
    mid-cleanup abort is retried on a LATER run rather than silently
    accepted as settled. ``matched_slugs`` (K22) is every local slug a
    VERIFIED row confirms the server already has -- the caller registers
    these into state right away (see ``_register_placeholder_entries``) so
    a file deleted in the window before its own turn on the cursor is not
    permanently orphaned (reconciliation itself never runs a second time).

    The guard (C row): local file count 0, or more orphans than
    ``max(5, 20% of this project's synced row count)`` -- delete nothing,
    report ``orphan_guard``. ``local_slugs`` empty is checked FIRST, ahead of
    and independent of the ratio check: zero local files makes EVERY one of
    this project's own rows look like an orphan, which is the single
    strongest signal that something about directory resolution (an
    unexpected ``cwd``, a ``CLAUDE_CONFIG_DIR`` mismatch) is wrong, not that
    every file was genuinely deleted at once -- a project with zero local
    files AND zero server rows still reaches a clean ``([], True)``, since
    there is nothing to guard against in the first place.

    K04 (post_implementation R1): a row is only ever treated as "ours" once
    its ``external_id`` prefix, ``metadata.layer == "fact"`` AND
    ``metadata.container_id`` (both filter keys this call's own listing
    query sends) have ALL been individually verified -- contract §6.2's
    "a caller must verify every filter key it sent, and treat a non-empty
    page where none of them verified as the filter having silently failed"
    applies here exactly as it already does to ``_ingest_client``'s own
    per-document lookup. The previous version verified the ``external_id``
    prefix alone and nothing else, so a page returned under a filter the
    backend silently ignored (a renamed query key) could seat another
    container's or another layer's rows as this project's own candidates --
    protected from an actual wrong DELETE by ``_ingest_client``'s own
    ``_is_ours`` on the write path, but the resulting ``filter_suspect``
    signal (this function had none) never reached the ledger, so the whole
    page was silently skipped as "zero orphans" instead of reported.

    K03 (post_implementation R1): every non-``nothing_to_do`` reason this
    function's own delete loop produces for an orphan it could NOT delete
    -- a round-abort (stops the loop outright) or a per-row rejection
    (``rejected_422``, ``filter_suspect``, ``unknown`` -- none of them
    round-abort reasons, so OTHER orphans in the same batch still get their
    own turn) -- is returned in ``reasons`` rather than silently dropped by
    an ``elif`` that only ever kept ONE of "some rows got deleted" or "one
    reason why a row did not". The caller folds these into the round's own
    accumulated reasons and decides whether to stop the REST of the round
    (``_ingest_client.ROUND_ABORT_REASONS`` plus ``budget_exhausted``). Each
    such per-row rejection (not a round abort) is ALSO tallied via
    ``_tally_result`` (R2-C10), so it is named in ``run["extra"]["failed"]``
    like every other per-file failure this module reports.
    """
    rows, list_reason, calls = _collect_fact_rows(
        base_url, token, client.user_id, client.container_id, deadline
    )
    if list_reason is not None:
        # R2-C10: attribute a listing failure to the RECONCILE stage
        # specifically -- without this, a 429/500/oversize page/missing
        # cursor field here looks identical in the ledger to a per-file
        # write failure, and a reader cannot tell this was not even an
        # attempt to sync a file.
        run["extra"]["reconcile"] = {"reason": list_reason, "pages": calls}
        return [list_reason], False, calls, set()
    prefix = key + "/"
    synced_count = 0
    orphans = []
    matched_slugs = set()
    prefixed_seen = False
    verified_seen = False
    for row in rows:
        meta = row.get("metadata")
        external_id = meta.get("external_id") if isinstance(meta, dict) else None
        if not isinstance(external_id, str) or not external_id.startswith(prefix):
            continue  # X1: never another project's rows, whatever they are
        prefixed_seen = True
        if not (
            isinstance(meta.get("layer"), str) and meta["layer"] == "fact"
            and isinstance(meta.get("container_id"), str) and meta["container_id"] == client.container_id
        ):
            continue  # §6.2: a filter key we sent did not verify on this row
        verified_seen = True
        synced_count += 1
        slug = external_id[len(prefix):]
        if slug in local_slugs:
            matched_slugs.add(slug)
        else:
            orphans.append(external_id)
    if prefixed_seen and not verified_seen:
        # Every row sharing our prefix failed EITHER verification -- the
        # same "non-empty page, nothing verified" shape _ingest_client's own
        # _lookup treats as filter_suspect, not as "we simply have none".
        return ["filter_suspect"], False, calls, matched_slugs
    if not local_slugs:
        return (["orphan_guard"] if orphans else []), (not orphans), calls, matched_slugs
    if not orphans:
        return [], True, calls, matched_slugs
    if len(orphans) > max(5, synced_count * 0.2):
        return ["orphan_guard"], False, calls, matched_slugs
    deleted = 0
    reasons = []
    for external_id in orphans:
        remaining = (deadline - time.monotonic()) if deadline is not None else None
        if remaining is not None and remaining < _MIN_REMAINING_SECONDS:
            reasons.append("budget_exhausted")
            break
        outcome = client.delete("fact", external_id)
        calls += outcome.calls
        slug = external_id[len(prefix):]
        if outcome.aborts_round:
            reasons.append(outcome.reason)
            _tally_result(run, slug, outcome.reasons, outcome)
            break  # a round-abort condition: do not attempt more orphans this round
        if outcome.deleted > 0 or "nothing_to_do" in outcome.reasons:
            deleted += 1
            # R2-C07: written into `run` the INSTANT this deletion is
            # known, not accumulated in `deleted` alone and reported only
            # once this function returns -- see this function's own
            # docstring.
            run["extra"]["orphans_deleted"] = run["extra"].get("orphans_deleted", 0) + 1
            if run["extra"]["orphans_deleted"] == 1:
                run["reasons"].append("orphans_deleted")
        else:
            # A per-row rejection that is NOT a round-abort reason (422,
            # filter_suspect, an unsendable id): record it and keep trying
            # the REST of this batch's orphans -- one bad row must not block
            # every other one (K04). Also tallied (R2-C10), so the failed
            # slug is named like any other per-file failure.
            non_abort = [r for r in outcome.reasons if r and r != "nothing_to_do"]
            non_abort = non_abort or ["unknown"]
            reasons.extend(non_abort)
            _tally_result(run, slug, non_abort, outcome)
    done = not reasons and deleted == len(orphans)
    return reasons, done, calls, matched_slugs


# ── the work ─────────────────────────────────────────────────────────────

def _remaining(run):
    return run["deadline"] - time.monotonic()


def _collect(run):
    """Do the work. Returns the reason string, or ``None`` when a PEER run
    already holds this memory directory's round lock (R2-C13: the caller,
    ``main()``, must not write any ledger row at all in that case -- see
    there). Raises only for a stdin payload that is not a JSON object (the
    runner maps it via ``_hook_state.reason_for_exception``, which resolves
    unrecognised exceptions to ``unknown``).

    K02 (post_implementation R1): every fact this round produces is written
    straight into ``run`` AS IT HAPPENS -- ``run["reasons"]`` (this
    function's own working list; the SAME object, not a copy), ``run
    ["calls"]``, ``run["extra"]`` -- rather than accumulated in local
    variables and only transferred to ``run`` in one block at the very end.
    An exception this function does not itself catch still reaches
    ``run_with_deadline``'s own blanket handler and ends the round WITHOUT
    ever reaching that transfer; writing straight into ``run`` means
    whatever already happened (an orphan actually deleted server-side, a
    file actually PATCHed) survives into the ledger row even when
    something LATER in the same round goes wrong.

    R2-C03 (post_implementation R2, supersedes the R1 revision of this
    paragraph): the round's cursor/reconciled advance is NOT an exception
    to the above any more. An earlier revision persisted it in ``_record``,
    AFTER the ledger row, on the premise that the binding ruling (A9-7)
    required the two-row shape ``handoff_sync``'s own end-of-run persist
    uses -- it did not: the ruling's own text requires the cursor to
    advance PER FILE, in state, same as a file's own ``synced_at``/hash,
    and explicitly contrasts that with ``handoff_sync``'s round-end shape.
    The cursor is now persisted inside ``_sync_file`` the instant a
    cursor-walk file's own outcome is known (merged into that file's state
    entry, or on its own for a deterministic skip -- see
    ``_advance_cursor_only``); ``reconciled`` is persisted at the instant
    reconciliation itself concludes, below. ``_record`` persists nothing.
    """
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
    run["key"] = key
    run["extra"]["memory_dir"] = key
    toplevel, _ = _identity.project_root(cwd)
    project_name = os.path.basename(toplevel) if toplevel else os.path.basename(cwd.rstrip("/"))

    local_files, indeterminate, list_reason = _list_memory_files(_memory_dir(key))
    run["extra"]["local_files"] = len(local_files)

    state_path = _memory_state_path(key)
    state, reasons = _hook_state.read_state_at(state_path)
    run["reasons"] = reasons  # K02: the SAME list, mutated as this round goes

    files_state_raw = state.get("files")
    if isinstance(files_state_raw, dict):
        # R2-C08: a non-dict ENTRY is normalized to the K22 empty
        # placeholder, never dropped -- a slug the state file still names
        # must stay a tracked key (present locally -> re-hashed by the
        # dirty check's "no file_hash to match" fallback; vanished locally
        # -> still a candidate for the pending-delete walk below). The
        # previous code dropped such an entry outright, which made a
        # "ghost" slug with no local file permanently invisible to the
        # vanished-file computation (it was never a key of files_state to
        # begin with): its server row was never queued for DELETE, and the
        # shape could never actually self-heal -- every round re-reported
        # `unknown` for good.
        files_state = {s: (e if isinstance(e, dict) else {}) for s, e in files_state_raw.items()}
        shape_bad = any(not isinstance(e, dict) for e in files_state_raw.values())
    else:
        files_state = {}
        shape_bad = files_state_raw is not None
    reconciled_raw = state.get("reconciled")
    reconciled = reconciled_raw is True
    shape_bad = shape_bad or (reconciled_raw is not None and not isinstance(reconciled_raw, bool))
    if shape_bad:
        # K05: a well-formed JSON object whose own VALUES are the wrong
        # shape (``files`` not a dict, an entry not a dict, ``reconciled``
        # not a bool -- a hand edit, a pre-X1 migration, a partial write a
        # crash interrupted) is reported once, like any other corrupt
        # state, and simply treated as if those parts were absent: the next
        # per-file write heals the shape for good (``_coerce_files_map``).
        reasons.append("unknown")

    sorted_slugs = sorted(local_files)
    n = len(sorted_slugs)
    cursor = state.get("cursor")
    if not isinstance(cursor, int) or cursor < 0 or cursor >= n:
        cursor = 0

    if list_reason is not None:
        # K01: the directory listing itself failed for a reason OTHER than
        # "it does not exist" -- EACCES / EIO / ESTALE / ENOTDIR all read,
        # to a caller that only checks "is this slug a key of the result",
        # exactly like "every file in it was deleted". Treating that as
        # license to soft-delete every row this project has ever synced is
        # the single most destructive mistake this hook can make; refuse
        # every destructive phase this round and report it instead.
        reasons.append(list_reason)
        run["calls"] = 0
        return _hook_state.worst_reason(reasons)

    lock_fd = _acquire_run_lock(_memory_run_lock_path(key))
    if lock_fd is None:
        # K09: a concurrent SessionEnd run for this SAME memory directory
        # already holds the round lock -- both runs would otherwise start
        # from the same unlocked snapshot and race to POST the same new
        # file twice (this client's own idempotency protocol only
        # de-duplicates on a LATER run's lookup, and the steady state here
        # is zero calls, so the pair is never naturally revisited to merge
        # it). Doing no network work this round is always safe: the peer
        # run is doing it instead.
        #
        # R2-C13 (post_implementation R2): this round writes NO ledger row
        # of its own any more (signalled to main() by returning None) --
        # the winner's own row (or whatever it last wrote, including a
        # purely-LOCAL failure with no network call at all) already covers
        # this round, and the two used to race to append a row each,
        # landing in either order; when the winner's row was itself a
        # local-only failure, the loser's harmless nothing_to_do row could
        # land AFTER it and bury it from session_inject's "read the last
        # row" reporter. There is no "stuck peer" to report instead: flock
        # releases the instant that process exits.
        return None
    if lock_fd == _NO_RUN_LOCK:
        # R2-C02: a degraded (unavailable, not contended) run lock is not a
        # failure of this round -- same contract as _hook_state's own
        # lock_unavailable -- but it is worth a look if it persists, so it
        # is recorded in extra, never in reasons.
        run["extra"]["run_lock"] = "unavailable"
    try:
        token = os.environ.get("NEXUS_API_TOKEN", "")
        client = _ingest_client.IngestClient(
            base_url, token, _identity.user_id(cwd), _identity.container_id(), SOURCE_NAME,
            timeout=_HTTP_TIMEOUT_SECONDS, bulk=True, deadline=run["deadline"],
            identity_degraded=degraded,
        )

        fingerprint = _current_fingerprint()
        budget = _BATCH_SIZE
        aborted = False
        deleted_count = 0

        # -- pending deletes (vanished local files), retried first every round.
        # K01: a slug this round's listing could not conclusively resolve
        # (``indeterminate``) is never treated as "vanished" -- something is
        # still there, it just was not safely stat-able. Zero local files
        # while state still remembers synced entries is the SAME
        # unexpected-directory-resolution signal _reconcile_orphans already
        # guards against; it must stop THIS phase too, not only orphan
        # reconciliation, which is independent and may not even run this
        # round (already reconciled). --
        vanished = sorted(slug for slug in files_state if slug not in local_files and slug not in indeterminate)
        if not local_files and vanished:
            reasons.append("orphan_guard")
            run["extra"]["pending_delete_guard"] = len(vanished)  # R2-C10
            vanished = []
        for slug in vanished:
            if aborted or budget <= 0:
                break
            if _remaining(run) < _MIN_REMAINING_SECONDS:
                reasons.append("budget_exhausted")
                aborted = True
                break
            r, ab, c, cleared = _delete_file(client, key, slug, run)
            reasons.extend(r)
            run["calls"] += c
            budget -= 1
            if cleared:
                deleted_count += 1
            if ab:
                aborted = True
        if deleted_count:
            run["extra"]["deleted"] = deleted_count

        # -- orphan reconciliation: independent of the batch above/below (a
        # failure here does not stop the sync batch, and vice versa) UNLESS
        # a prior phase has already aborted the round (K03: reconciliation
        # must not start fresh network work once this round is already
        # stopping, and a reconciliation abort likewise stops the rest of
        # THIS round -- it is not "independent" of what comes after it,
        # only of what came before), still gated by the SAME budget check
        # as every other network-making phase, and refused outright under a
        # guessed identity (mirrors IngestClient's own identity_degraded
        # guard on upsert/delete -- this listing is not a method of that
        # class (see _list_fact_page's own docstring) so it does not
        # inherit that guard for free, and a bulk-delete decision is
        # exactly the kind of call a guessed user_id/key must never be
        # allowed to drive) --
        if not reconciled and not aborted:
            if degraded:
                reasons.append("identity_unresolved")
            elif _remaining(run) < _MIN_REMAINING_SECONDS:
                reasons.append("budget_exhausted")
                aborted = True
            else:
                # R2-C01: the candidate set is local files UNION
                # indeterminate -- a slug _list_memory_files could not
                # conclusively resolve must never be treated as an orphan
                # (see _reconcile_orphans's own docstring).
                recon_candidates = set(local_files) | indeterminate
                recon_reasons, recon_done, recon_calls, matched_slugs = _reconcile_orphans(
                    client, key, recon_candidates, base_url, token, run["deadline"], run,
                )
                run["calls"] += recon_calls
                for r in recon_reasons:
                    reasons.append(r)
                    if r in _ingest_client.ROUND_ABORT_REASONS or r == "budget_exhausted":
                        aborted = True  # K03: a reconciliation abort stops the REST of this round too
                # K22: a row reconciliation just confirmed exists BOTH
                # locally and on the server is registered right away,
                # without a file_hash of its own -- see
                # _register_placeholder_entries's docstring.
                to_register = {s: {} for s in matched_slugs if s not in files_state} if matched_slugs else {}
                if recon_done:
                    # R2-C03/C05: persisted the INSTANT reconciliation
                    # concludes -- combined with the K22 placeholder
                    # registration into ONE update_state_at call when
                    # there is one, or on its own when there is not. A
                    # genuine failure to persist it is simply one more
                    # reason THIS round produced (_fold_persist_reasons),
                    # folded in before _collect ever returns -- never a
                    # separate follow-up row (there is no round-end
                    # write left for it to arrive too late for).
                    reconciled = True

                    def _mark_reconciled(s, to_register=to_register):
                        new = _register_placeholder_entries(s, to_register) if to_register else dict(s)
                        new["reconciled"] = True
                        return new

                    _, persist_reasons = _hook_state.update_state_at(state_path, _mark_reconciled)
                    _fold_persist_reasons(reasons, persist_reasons)
                    if to_register:
                        files_state.update(to_register)
                elif to_register:
                    _, persist_reasons = _hook_state.update_state_at(
                        state_path,
                        lambda s, to_register=to_register: _register_placeholder_entries(s, to_register),
                    )
                    _fold_persist_reasons(reasons, persist_reasons)
                    files_state.update(to_register)

        # -- dirty set: already-synced files whose content or redaction rule
        # changed since their last sync (checked first, ahead of the
        # cursor). K05: a per-file stat/hash failure here must not abort the
        # whole round -- FileNotFoundError is an ordinary race (the next
        # round's listing settles it), anything else is reported once and
        # that one file is simply left out of this round. K18: a file whose
        # CONTENT actually changed is tried before one that is dirty only
        # because the redaction-rule fingerprint changed (A8-2) -- a real
        # edit must not queue behind a backlog of fingerprint-only churn. --
        dirty = []
        content_changed_of = {}
        dirty_scan_errors = []
        if not aborted:
            for slug in sorted_slugs:
                stored = files_state.get(slug)
                if stored is None:
                    continue  # "new" -- handled by the cursor walk below
                try:
                    is_dirty, file_hash = _dirty_check(local_files[slug], stored, fingerprint)
                except FileNotFoundError:
                    continue  # vanished mid-scan: an ordinary race
                except OSError:
                    dirty_scan_errors.append(slug)
                    continue
                if is_dirty:
                    dirty.append(slug)
                    content_changed_of[slug] = file_hash != stored.get("file_hash")
            dirty.sort(key=lambda s: (not content_changed_of.get(s, True), s))
        if dirty_scan_errors:
            reasons.append("unknown")
            run["extra"]["dirty_scan_errors"] = sorted(dirty_scan_errors)[:5]

        # R2-C09 (post_implementation R2): the one-slot reservation for a
        # waiting new file that used to live here is REMOVED -- it violated
        # the binding TASK-006 acceptance list ("dirty set first", the C
        # row) and A8-2's "N slots per round, spread across rounds": a round
        # with >= N genuinely dirty files spends its whole batch on them,
        # same as any other round that happens to have >= N work items.
        # (The sort directly above -- content-changed before
        # fingerprint-only churn -- is the OTHER half of K18 and stays: it
        # decides ORDER within the dirty set, which stays compatible with
        # "dirty set first".) A persistently-failing backlog of exactly N
        # files starving a new file forever is a known, accepted limit of
        # this shape (reported via a failure-class reason every round, so
        # it is visible) -- not something this task resolves.
        dirty_cap = budget
        dirty_attempts = 0
        for slug in dirty:
            if aborted or budget <= 0 or dirty_attempts >= dirty_cap:
                break
            if _remaining(run) < _MIN_REMAINING_SECONDS:
                reasons.append("budget_exhausted")
                aborted = True
                break
            r, ab, c = _sync_file(client, key, project_name, slug, local_files[slug], fingerprint, run)
            reasons.extend(r)
            run["calls"] += c
            budget -= 1
            dirty_attempts += 1
            if ab:
                aborted = True

        # -- cursor walk: not-yet-synced files, resuming where the last round
        # stopped (an abort or a budget exhaustion), wrapping at the list's
        # end. R2-C03: `next_cursor=i` (the position AFTER this slug) is
        # passed into `_sync_file` so a non-aborting outcome persists the
        # cursor advance on disk the instant it is known; on an abort, the
        # on-disk cursor is simply left where the PREVIOUS successful
        # advance put it (already this failing slug's own position), so
        # nothing further needs writing here. --
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
                    aborted = True
                    break
                r, ab, c = _sync_file(
                    client, key, project_name, slug, local_files[slug], fingerprint, run,
                    next_cursor=i,
                )
                reasons.extend(r)
                run["calls"] += c
                budget -= 1
                if ab:
                    aborted = True
                    break
    finally:
        _release_run_lock(lock_fd)

    if indeterminate:
        # R2-C01: reported every round a slug stays unresolved -- in
        # steady state (nothing else to do) this is the ONLY thing this
        # round has to say, and it must not be silently absorbed into a
        # clean "none" just because no network call happened to be made
        # for it. Bounded like dirty_scan_errors (K05): a reader needs
        # examples, not every unresolved slug in a bad batch.
        reasons.append("unknown")
        run["extra"]["unresolved_files"] = sorted(indeterminate)[:5]

    return _hook_state.worst_reason(reasons)


def _record(reason, started, run, work_left_behind):
    """Append this run to the ledger, within a budget. Never raises.

    R2-C03/C05 (post_implementation R2, supersedes the R1 shape this
    function used to have): this function persists NOTHING of its own any
    more. Every fact this round produces -- a file's own state entry, the
    cursor's per-file advance, the reconciled flag -- is written inside
    ``_collect``, the INSTANT it is known (see ``_sync_file`` /
    ``_advance_cursor_only`` / the orphan-reconciliation block there). A
    genuine persist failure anywhere in that chain is folded into THIS
    round's own ``reasons`` (``_fold_persist_reasons``) before ``_collect``
    ever returns, so it is already part of ``reason``/``also_failed`` by
    the time this function runs. There is therefore no longer a separate
    end-of-round write that could fail AFTER the ledger row, and so no more
    follow-up ``state_write_failed`` row either -- the earlier R1 shape
    existed only because that round-end write existed; removing the write
    removes the need for the follow-up row with it.
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
        if reason is None:
            # R2-C13: a peer run already holds this memory directory's
            # round lock -- see _collect's own docstring / the K09 branch
            # there. This round writes NO ledger row of its own.
            return False

    # R2-C12 (post_implementation R2): computed exactly ONCE, here, for
    # EVERY path that reaches this point -- a clean round, not_configured,
    # the K01 directory-listing guard, an abnormal exit, all of them --
    # not just the abnormal-exit branch the previous version limited this
    # to. The previous version's own also_failed, computed only at the
    # tail of _collect's NORMAL return, meant a round that exited through
    # any EARLIER return wrote a row with no also_failed key at all. A
    # timeout/exception abandons the worker before _collect's own
    # computation could run (when there was one); run["reasons"] may
    # already hold real failures from whatever the round DID finish before
    # that (K02: those are written into `run` as they happen). Always set,
    # even to `[]` -- presence says "this was computed", not "nothing else
    # failed".
    run["extra"]["also_failed"] = _hook_state.also_failed(list(run.get("reasons") or []) + [reason], reason)

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
