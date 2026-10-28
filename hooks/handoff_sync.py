#!/usr/bin/env python3
"""SessionEnd handoff-ingestion hook for the nexus-memory plugin (workflow B,
change 2 TASK-005): ingests the project's own, most recent Aria session
handoff -- most recent AMONG THE ONES THIS CONTAINER OWNS (Amendment A9-2,
owner ruling 2026-10-01: candidates are grouped by ``owner-container`` into
own / others / uncertain before any selection; the pre-A9 R1-c07 behaviour
picked the newest project-wide first and only then checked ownership, so a
shared ``latest.md`` pointer at the OTHER container's document silenced this
container's own ingestion under a quiet ``not_owner`` for weeks) -- as a
``layer=session_summary`` episode
(``docs/architecture/memory-layers.md`` §3.2), the same layer the session
aggregator writes for an auto-summarised session -- a human-authored handoff
is just another episode with ``aria.source=handoff`` provenance instead of an
aggregator hash.

One SessionEnd run ingests at most ONE handoff document: the ``latest.md``
pointer (text or symlink, A9-23) when it resolves to a document of this
container's own group, or -- otherwise -- whichever OWN-group candidate's
frontmatter ``updated-at`` is newest (Amendment A4-5, owner ruling
2026-09-21; A9-2).

Design contract (proposal ``memory-layer-contract-and-aria-structured-
ingestion`` workflow B; handoff format ``standards/conventions/session-
handoff.md`` §2.2 / §2.3):

  - Stdlib-only Python 3, zero third-party deps (mirrors session_capture.py /
    session_inject.py). FAIL-OPEN ALWAYS: exit 0, no stdout, ever -- a
    SessionEnd hook must never block session teardown.
  - SessionEnd payload (stdin JSON): {session_id, transcript_path, cwd,
    hook_event_name, reason}. Only ``cwd`` and ``session_id`` are read; a
    non-object payload raises (the shared runner below maps it to a reason).
  - Owner-only (D row's identity rule, shared with memory_sync.py /
    session_inject.py): a handoff is ingested only when its frontmatter
    ``owner-container`` uuid equals THIS container's aria uuid
    (``~/.aria/container-id``). Two containers pulling the same handoff file
    must not both PATCH the same external_id.
  - Content = the H1 title line, then §6 ("Next session 入口"), then §2
    ("未完成 / Carry-forward 清单") -- §6 first because it is read most, so a
    4000-char cap trims §2's tail before touching §6's.
  - Idempotency, redaction and the PATCH-vs-POST protocol all live in the
    shared ``_ingest_client`` (TASK-010); this file only decides WHAT to
    send: which document, whose it is, and what its content and metadata
    are.
  - RUN LEDGER + hook-run skeleton: same shape as the other two SessionEnd
    hooks (shared via ``_hook_runner``), work against a deadline of its own
    (``_WORK_BUDGET_SECONDS``), ledger write capped (``_LEDGER_BUDGET_SECONDS``).
    hooks.json gives this hook ``timeout: 60`` (SessionEnd hooks share a
    1.5 s budget unless one declares longer).
  - Import guards mirror session_capture.py's, with one difference: here
    ``_ingest_client`` is REQUIRED, not optional bookkeeping. The other two
    hooks treat their ledger (``_hook_state``, which hard-imports ``fcntl``)
    as peripheral -- losing it must not stop the capture / injection. This
    hook's entire job is a write through ``_ingest_client``, which itself
    unconditionally imports ``_hook_state``; there is nothing useful left to
    do without it, so its import failure is treated the same as
    ``_identity`` / ``_hook_runner``'s (Amendment A5-3).
"""

import json
import os
import re
import socket
import stat
import sys
import time
from datetime import timedelta

# Siblings are imported by name, which only works while this file's directory
# is on sys.path. PYTHONSAFEPATH=1 / `python -P` (3.11+) takes it off, and
# this hook is one self-contained file like its two SessionEnd siblings -- so
# put it back rather than let an interpreter setting switch the plugin off.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)


def _warn(message):
    """Print one diagnostic line to stderr, never raising (R2-c05).

    A closed stderr pipe (the host is already exiting) makes ``print``
    raise ``BrokenPipeError``; every stderr write this file makes -- the
    three import guards immediately below (R4-c04), the main thread, AND
    inside ``_record``'s own ledger-write closure, all of which used to
    print directly -- goes through this instead of a bare ``print`` -- see
    ``main()`` below for why that ordering, not just this swallow, is what
    actually protects the ledger row.

    Defined here, before the import guards, and NOT in ``_hook_runner``
    (R4-c04): this function must keep working when ``_identity`` or
    ``_hook_runner`` themselves are the missing piece, which is exactly
    the condition the two guards below exist for -- so it cannot depend on
    either, or on anything else this file imports from a sibling module.

    Swallowing the ``OSError`` from THIS call alone is not sufficient on
    its own (R3-c03): CPython's own interpreter shutdown
    (``flush_std_files``, behind every plain ``sys.exit()``, not just the
    ``os._exit`` path ``_hook_runner.finish`` takes when a thread was left
    behind) unconditionally flushes stdout AND stderr again once this
    process is on its way out, regardless of what any Python-level
    ``except`` already caught -- and a buffered writer whose own
    ``write()`` raised does not discard the bytes it failed to write, so
    that flush retries the SAME bytes against the SAME closed pipe, with
    no ``except`` anywhere near it this time (confirmed empirically: a
    real closed-pipe subprocess with every ``print(..., file=sys.stderr)``
    already wrapped in a swallowing ``except OSError`` still exits 120).
    Rerouting the FILE DESCRIPTOR itself to ``os.devnull``, the same way
    ``session_inject._silence_stdout`` already does for stdout, is what
    stops the retry from failing too -- it protects every later write to
    fd 2 from this point on, not just this one call's own.

    ``sys.stderr is None`` (R4-c07) is a DIFFERENT shape from a closed
    PIPE, and is checked first: CPython sets ``sys.stderr`` to ``None``
    (never a stream object) when fd 2 is already closed BEFORE the
    interpreter even starts, rather than a pipe that closes mid-run --
    confirmed empirically. ``print(message, file=None)`` does not raise;
    it silently FALLS BACK to ``sys.stdout`` (also confirmed empirically),
    which would put this diagnostic on the one channel a SessionEnd hook's
    contract requires to stay empty. There is no real file descriptor to
    redirect in this shape (fd 2 was never opened at all), so this simply
    skips the write rather than risking stdout.
    """
    if sys.stderr is None:
        return
    try:
        print(message, file=sys.stderr)
    except OSError:
        _silence_stderr()


def _silence_stderr():
    """After a failed stderr write, stop the interpreter retrying it on the
    way out (R3-c03). See ``_warn`` above for why swallowing the write
    itself is not enough.

    ``sys.stderr.fileno()`` is resolved BEFORE ``os.open`` (R4-c08): the
    reverse order opened the devnull fd first, and if ``fileno()`` then
    raised (a test double, or any future stderr replacement without a
    real one) the blanket ``except Exception: pass`` below swallowed that
    too, but the devnull fd already opened on the line before was never
    closed -- a leak on every such call.
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
    _warn(f"[handoff-sync] cannot import _identity: {exc!r}")
    sys.exit(0)

try:
    import _hook_runner
except Exception as exc:  # a broken or partial install
    if __name__ != "__main__":
        raise
    _warn(f"[handoff-sync] cannot import _hook_runner: {exc!r}")
    sys.exit(0)

try:
    import _ingest_client
except Exception as exc:
    # NOT optional bookkeeping (contrast session_capture.py / session_inject.py's
    # `_hook_state`): this hook's whole job is a write through this client,
    # and it unconditionally imports `_hook_state` (which needs `fcntl`)
    # itself -- so a platform without it can do nothing useful here either.
    if __name__ != "__main__":
        raise
    _warn(f"[handoff-sync] cannot import _ingest_client: {exc!r}")
    sys.exit(0)

import _hook_state  # noqa: E402 - guaranteed importable: _ingest_client already imports it
import _redact  # noqa: E402 - guaranteed importable: _ingest_client already imports it (R2-c08)

HOOK = "handoff-sync"  # names the ledger file; must stay in session_inject._EXPECTED_LEDGERS

# The name half of X-Nexus-Source. The backend attributes a request by this
# exact string against an allowlist (nexus `mcp_attribution._KNOWN_CLIENTS`);
# renaming it here sends every write to source="unknown".
SOURCE_NAME = "handoff-sync-hook"

_HTTP_TIMEOUT_SECONDS = 8
# How long the ledger write may hold up the exit. Normally it takes
# milliseconds; this is the cap for when it does not (see _record).
_LEDGER_BUDGET_SECONDS = 2.0
# The hook's own deadline for everything before the ledger (see main).
# NOT a sum that covers the nominal worst case (R1-c35: an earlier version
# of this comment tried to state that sum and got the total wrong; R2-c21
# found the SAME comment's restated total also wrong, and its HTTP call
# count too low -- a dedup page can delete several rows, not just one --
# so this revision deliberately does not restate a call count or a total
# at all, to stop that number drifting out of sync a third time): two git
# calls (project_root's memoised one, and _current_branch's own, separate,
# un-memoised one) plus a handful of HTTP calls (a lookup, zero or more
# dedup deletes, and a final create/update) could, if every one of them
# ran its full nominal timeout back to back, add up to well more than this
# budget. What actually keeps a slow run inside it is IngestClient's own
# ``deadline`` (derived from this same budget, see main): it refuses a
# call it could not finish rather than let the wall clock run out from
# under it. This number only has to clear ordinary latency plus that
# refusal path, and, with the ledger budget, stay under the host's
# timeout.
_WORK_BUDGET_SECONDS = 20.0

# A9-1 (owner ruling 2026-10-01): minutes-or-more attribution tolerance. The
# corpus motivation: 52 of 112 handoffs with an updated-at carry a :00 second
# -- humans rounding when writing frontmatter by hand -- so the window must
# absorb minute-level rounding, comfortably. 120s.
_ATTRIBUTION_TOLERANCE_SECONDS = 120
# IngestClient's own `deadline` sits this far inside the work budget, so the
# client can refuse a request it would not finish in time rather than the
# work-budget thread being abandoned mid-call with nothing recorded at all.
_DEADLINE_SLACK_SECONDS = 1.0

# Content assembly (proposal workflow B): H1, then §6, then §2, capped.
_CONTENT_CAP = 4000
# A body at or above this length with neither section parseable is a broken
# template, not an empty handoff -- see _build_content.
_MIN_NONTRIVIAL_BODY = 200
_TRUNCATION_MARKER = "\n\n…[truncated]"

# Locate (Amendment A4-5, owner ruling 2026-09-21): candidates are every
# regular *.md file directly in docs/handoff/, excluding the pointer file
# itself and a README a project may keep there. The exclusion is
# case-insensitive (ruling 6, TASK-005 R1 fix round, R1-c12): a project that
# only keeps a lowercase readme.md must stay as quiet as one with README.md,
# not report pointer_unresolved on every single session.
_HANDOFF_EXCLUDED_NAMES = frozenset({"latest.md", "README.md"})
_HANDOFF_EXCLUDED_NAMES_UPPER = frozenset(n.upper() for n in _HANDOFF_EXCLUDED_NAMES)
_LATEST_MD = "latest.md"
# Frontmatter is always a handful of short lines (session-handoff.md §2.3.1);
# the fallback-locate scan reads every candidate's, so a bounded probe beats
# loading each whole document just to compare one timestamp.
#
# Named _CHARS, not _BYTES (R4-c17): every read these two feed is opened
# text-mode (``encoding="utf-8", errors="replace"``), and ``fh.read(n)`` on
# a text-mode file reads ``n`` CHARACTERS, not ``n`` bytes -- confirmed
# empirically (``fh.read(5)`` against 10 repeated 3-byte CJK characters
# returns exactly 5 of them, not 5 bytes' worth). A CJK-heavy handoff can
# therefore run to roughly 3x this many bytes on disk (up to 4x for a
# character requiring 4 UTF-8 bytes) before the cap engages -- the cap
# still does its job (bounding a FIFO/pathological read), the numbers
# below just were not what their old _BYTES name implied.
_FRONTMATTER_PROBE_CHARS = 4096
# Read caps (R1-c13): a regular file this small is already generous for any
# real handoff (docs/handoff/*.md run a few KB); the cap exists so a FIFO or
# a device node masquerading as a *.md file cannot block a read indefinitely
# or exhaust memory, not because any real document is expected to hit it.
_MAX_DOCUMENT_CHARS = 1_048_576

# The Aria collector's own pointer pattern (standards/conventions/session-
# handoff.md §3.2 "H5 fix"; also state-scanner's `_LATEST_POINTER_RE`). The
# deprecated arrow style (`→ [file]`) and a multi-track deprecation banner
# (no `**Latest**:` line at all) both simply fail to match -- which is
# exactly "no pointer", handled by the newest-`updated-at` fallback below.
_LATEST_POINTER_RE = re.compile(r"^\*\*Latest\*\*:\s*\[[^\]]+\]\(\.?/?([^)]+?)\)", re.MULTILINE)

# Frontmatter: only these six keys are known (session-handoff.md §2.3.1 plus
# this plugin's own `nexus-ingest` opt-out). Unknown keys are ignored.
_FRONTMATTER_KEYS = frozenset(
    {"track-id", "owner-container", "phase", "status", "updated-at", "nexus-ingest"}
)
# A literal U+FEFF here (rather than this escape) is invisible in a diff and
# has twice been silently stripped by an editor or formatting tool in this
# codebase's history (R1-c32) -- which would make BOM tolerance AND its own
# test fixture fail open together, with nothing to show for it.
_BOM = "\ufeff"

# Section headings: `## §6 ...` / `## §2 ...` (session-handoff.md §2 skeleton).
# `(?!\d)` excludes `## §60`; a `### §6.1` line does not match `^## ` at all
# (three hashes, not two), so it is never mistaken for a section start and is
# swept up as one of that section's own subsections instead.
_SECTION_6_RE = re.compile(r"^## §6(?!\d)")
_SECTION_2_RE = re.compile(r"^## §2(?!\d)")
_H2_RE = re.compile(r"^## ")


# ── locate ───────────────────────────────────────────────────────────────

def _candidates(handoff_dir):
    """``(names, undecidable)``: ``names`` -- the resolvable regular ``*.md``
    files directly in ``handoff_dir`` (non-recursive, sorted), excluding
    ``latest.md`` and ``README.md`` (case-insensitively -- ruling 6). Both
    come back ``[]`` when the directory itself does not exist -- the common
    case, most projects keep no handoffs, and must stay quiet (``no_handoff``).

    Any OTHER ``OSError`` from the top-level ``listdir`` (permission denied,
    a stale NFS handle) is RE-RAISED rather than folded into that same
    ``[]``: ruling 5 (R1-c06) -- "cannot tell" is not "no handoff", and
    reading the two alike is how a broken/unreadable directory goes quiet
    forever. ``_locate`` below turns the re-raised error into
    ``pointer_unresolved`` with a stderr line naming it, instead of the
    silent skip this used to be.

    A ``FileNotFoundError`` from ``listdir`` covers two different shapes
    (R2-c03): the ordinary "does not exist at all", and ``handoff_dir``
    itself being a DANGLING symlink (a broken mount, an unlinked shared
    volume) -- something WAS configured here, so that one is re-raised too,
    same as any other "cannot tell". R3-c05 adds a third: the PARENT
    (``docs/``) being the dangling symlink -- ``os.listdir(handoff_dir)``
    fails the exact same way (``handoff_dir`` cannot be reached at all), but
    ``os.path.islink(handoff_dir)`` alone never catches it, since the
    dangling link sits one level up.

    ``undecidable`` (R3-c05) names entries that were listed but whose OWN
    stat could not be resolved -- a dangling ``*.md`` symlink, ``ELOOP`` (a
    self-referential symlink), a per-entry permission problem -- as opposed
    to one that is simply GONE by the time this loop reaches it (listed a
    moment ago, unlinked since: an ordinary race, R2-c03's own reading,
    kept for that case alone). Since A9-19 (owner ruling 2026-10-01) each
    entry carries its OWN bound exception -- ``(name, OSError)`` pairs, the
    error type no longer discarded at the ``except`` site -- because the
    fallback-scan report writes "which entry, what error" into the detail.
    The two used to be indistinguishable by
    exception type: ``os.stat`` follows symlinks, so both a vanished entry
    and a dangling one raise the identical ``FileNotFoundError``. Filtering
    with ``os.path.isfile`` directly (rather than ``os.stat`` + `S_ISREG``
    explicitly) would make the same mistake a second way -- that function
    catches ``OSError`` internally and returns ``False``, so ANY per-entry
    stat failure reads as "not a regular file" and silently drops the
    candidate, landing on the quiet ``no_handoff``/a wrong pick instead of
    the loud "cannot tell" ruling 5 requires. ``os.lstat`` (which does NOT
    follow the link) is the tell: if IT still finds the entry, something is
    really there and unresolved (``undecidable``); if it ALSO raises
    ``FileNotFoundError``, the entry is genuinely gone (a race, dropped
    quietly, matching R2-c03).
    """
    try:
        names = os.listdir(handoff_dir)
    except FileNotFoundError:
        docs = os.path.dirname(handoff_dir)
        # R4-c02: the ancestor leg must fire only for a DANGLING `docs`
        # symlink -- `os.path.islink(docs)` alone is true for ANY symlink,
        # whether or not its target exists, so a project whose `docs` is a
        # perfectly valid symlink (a monorepo's `docs -> website/docs`)
        # and simply keeps no `handoff/` subdirectory used to be re-raised
        # here too, turning the ordinary, quiet `no_handoff` case into a
        # `pointer_unresolved` failure reported every single session.
        if os.path.islink(handoff_dir) or (os.path.islink(docs) and not os.path.exists(docs)):
            raise
        return [], []
    except NotADirectoryError:
        return [], []
    out = []
    undecidable = []  # (name, exception) pairs -- A9-19 binds the error
    for name in names:
        if name.upper() in _HANDOFF_EXCLUDED_NAMES_UPPER or not name.endswith(".md"):
            continue
        path = os.path.join(handoff_dir, name)
        try:
            is_file = stat.S_ISREG(os.stat(path).st_mode)
        except OSError as stat_exc:
            try:
                os.lstat(path)
            except FileNotFoundError:
                continue  # gone even at the symlink-entry level: a real race
            except OSError as lstat_exc:
                # R4-c01: EACCES / EIO / ESTALE etc. on the lstat call
                # itself -- as opposed to FileNotFoundError -- is NOT "gone
                # by the time we looked": something is still there and this
                # run simply cannot resolve it (a directory that lost its
                # execute/search bit is the common real-world shape --
                # R5-c10: this used to say "see the test with the same
                # name below", which cannot be right in a SOURCE file with
                # no tests in it at all; the tests actually live in
                # test_handoff_sync.py's TestLocate:
                # test_an_unstattable_candidate_entry_is_pointer_
                # unresolved_not_quiet (mocked) and its real_chmod sibling
                # pin THIS branch alone). The old code read ANY OSError
                # here the same as FileNotFoundError, quietly dropping the
                # candidate and, if it was the only one, landing on
                # `no_handoff` -- exactly the silent stop ruling 5 /
                # ruling 13 rule out. The lstat exception is the one bound
                # on this leg: it is the error that proves "still there
                # but unresolvable".
                undecidable.append((name, lstat_exc))
                continue
            # A dangling / ELOOP symlink (lstat SUCCEEDS, only the
            # following stat fails): the entry itself is really there. The
            # stat exception is the binding one on this leg -- the errors
            # differ per leg (dangling is FileNotFoundError, ELOOP is
            # OSError), and A9-19's whole-round report writes that type
            # into the detail so a reader can tell a deleted-neighbour
            # case from a permission case without reproducing the run.
            undecidable.append((name, stat_exc))
            continue
        if is_file:
            out.append(name)
    return sorted(out), undecidable


def _pointer_target(handoff_dir):
    """``(target, unresolved)``: ``target`` is the basename ``latest.md``
    points to, or ``None`` when there is no pointer to follow -- a missing
    ``latest.md``, a multi-track deprecation banner (no ``**Latest**:``
    line), or the deprecated arrow-style pointer: none of those match the
    collector pattern, and none of them is "wrong", so the caller falls
    back to the newest ``updated-at`` among the candidates QUIETLY. Exactly
    one of ``target`` / ``unresolved`` can be meaningful at a time, but both
    can be ``None`` together (no pointer line found in an otherwise
    perfectly readable ``latest.md``).

    ``unresolved``, when not ``None``, is a short description of why
    ``latest.md`` itself could not be resolved at all -- a directory, a
    permission error, a stale handle (R5-c01). This is a DIFFERENT shape
    from "no pointer": a project that genuinely keeps no ``latest.md`` must
    stay quiet, but one whose ``latest.md`` exists in SOME form and cannot
    be resolved must not have that read failure silently reinterpreted as
    "no pointer" -- the old ``os.path.isfile`` check folded both into the
    exact same ``False``, so the newest-``updated-at`` fallback below ran
    anyway and could silently pick a DIFFERENT document than the one the
    (unreadable) pointer might actually have named -- exactly the silent
    which-document swap ruling 13 forbids, and the same shape R4-c03
    already closes one level down (an EXPLICIT pointer TARGET landing in
    ``undecidable``) -- this is the pointer FILE itself being unresolvable.
    The caller (``_locate``) decides what an ``unresolved`` result means:
    with two or more ``*.md`` entries it could have named -- resolvable
    candidates and undecidable ones counted together (R6-c01) -- it reports
    ``pointer_unresolved`` (guessing could ingest the wrong one); only with
    exactly ONE such entry in total is there nothing else ``latest.md``
    could have named, so that leg falls through unchanged and quiet
    (R2-c13's FIFO fixture stays green).

    A9-23 (owner ruling 2026-10-01, option A 当指针) adds the SYMLINK
    shape, Aether's real layout: a symlink ``latest.md`` is the pointer
    ITSELF whenever its fully-resolved path (both sides realpath'd) lands
    in ``docs/handoff``'s top level -- its basename is returned as
    ``target`` and judged by the caller exactly like a text pointer's. A
    dangling or looping symlink is decided by its LINK TEXT
    (``os.readlink``, which never follows): a text resolving into the
    directory top level and naming an ``undecidable`` entry reports through
    the caller's R4-c03 branch; anything else -- outside the directory, a
    subdirectory, an excluded name -- is the quiet "points at a
    non-candidate" fallback, NOT an ``unresolved`` error (the pre-A9 code
    reported the ELOOP/dangling ``repr`` here every single session;
    R6-c09: that report never prevented a wrong pick anyway). A healthy
    symlink pointing OUTSIDE the top level (at a pointer file elsewhere, or
    into a subdirectory) is NOT the pointer: the target's CONTENT is read
    and the ``**Latest**:`` line searched there, exactly the pre-A9
    behaviour for those shapes.

    ``os.lstat`` -- not ``os.path.isfile``, which this replaces -- is
    checked FIRST specifically so a genuinely ABSENT ``latest.md``
    (``FileNotFoundError``, the overwhelmingly common case: every project
    without a hand-maintained pointer) is told apart from one that exists
    in SOME form but cannot be resolved. ``os.path.isfile`` silently folded
    a dangling symlink, ``ELOOP``, and a directory into the exact same
    ``False`` a missing file produces; R1-c13 added it only to keep a FIFO
    or device node from being opened, a guard that still holds below.

    Once something is confirmed to exist, ``os.stat`` (which DOES follow
    symlinks, unlike the ``lstat`` just used to confirm presence) resolves
    what is actually at the far end of the path -- WITHOUT opening it, so a
    FIFO or a device node cannot block this call the way ``open()`` would
    (R1-c13: ``stat()`` reads metadata only, it does not wait for a writer
    the way opening a FIFO does -- confirmed empirically against R2-c13's
    own FIFO fixture). Anything other than a regular file at that point (a
    directory, FIFO, or device node; dangling/looping symlinks no longer
    reach this leg -- A9-23 decides them by link text above) is
    ``unresolved`` and is never opened.

    The read itself is capped (``_MAX_DOCUMENT_CHARS``) for the same reason
    a regular file that is merely huge must not be read in full just to
    find one pointer line.
    """
    path = os.path.join(handoff_dir, _LATEST_MD)
    try:
        link_stat = os.lstat(path)
    except FileNotFoundError:
        return None, None  # the ordinary case: no latest.md at all
    except OSError as exc:
        # Something (usually the ENCLOSING directory) makes even the
        # existence check itself fail -- same "cannot tell" shape as the
        # two legs below, just caught one step earlier.
        return None, f"{exc!r}"
    if stat.S_ISLNK(link_stat.st_mode):
        # A9-23 (owner ruling 2026-10-01, option A 当指针): a SYMLINK
        # latest.md is Aether's real layout (a git-tracked mode-120000
        # link, repointed by hand each session close) and is treated as
        # the pointer itself whenever it resolves into docs/handoff's TOP
        # LEVEL -- both sides realpath'd, so the judgement is by FULL
        # PATH, never bare basename (a basename-only check would mis-match
        # a top-level document sharing the name of a subdirectory target,
        # and would break the healthy-symlink-to-a-pointer-FILE case the
        # content read below still handles). The returned basename is then
        # judged by _locate exactly like a text pointer's: a candidate ->
        # use it; an undecidable entry -> pointer_unresolved (R4-c03); in
        # no list -> the same quiet fallback as an ordinary pointer naming
        # a nonexistent file.
        try:
            os.stat(path)  # does the whole chain resolve? (result unused: only failure matters)
        except OSError:
            # Dangling or looping chain -- including the case where the
            # link names an entry that exists but is itself unresolvable
            # (its own stat failure propagates up the chain). A9-23:
            # decide by the LINK TEXT (os.readlink never follows), NOT by
            # the error -- the pre-A9 code returned the ELOOP/FileNotFoundError
            # repr as `unresolved` here and this fired EVERY session
            # (R6-c09: the report never prevented a wrong pick anyway).
            try:
                text = os.readlink(path)
            except OSError as exc:
                return None, f"{exc!r}"
            lexically = os.path.normpath(
                text if os.path.isabs(text) else os.path.join(handoff_dir, text)
            )
            # R1 review (post_implementation): the DIRECTORY half of the
            # joined path is resolved (realpath), the BASENAME half stays
            # lexical -- never realpath the whole thing: realpath resolves
            # THROUGH a final dangling symlink and hands back the TARGET's
            # path, which would quietly re-route a link naming a top-level
            # undecidable entry away from the R4-c03 report; and never
            # compare a lexical dirname against a realpath'd directory (a
            # handoff_dir behind a symlink component never compares equal).
            try:
                resolved_parent = os.path.realpath(os.path.dirname(lexically))
                real_dir = os.path.realpath(handoff_dir)
            except OSError:
                resolved_parent, real_dir = os.path.dirname(lexically), handoff_dir
            if resolved_parent == real_dir:
                return os.path.basename(lexically), None
            # Names something outside the directory / in a subdirectory /
            # an excluded name: in NO list, the quiet "points at a
            # non-candidate" fallback.
            return None, None
        # Healthy chain: resolve fully and compare directories.
        try:
            resolved_path = os.path.realpath(path)
            real_dir = os.path.realpath(handoff_dir)
        except OSError as exc:
            return None, f"{exc!r}"
        if os.path.dirname(resolved_path) == real_dir:
            return os.path.basename(resolved_path), None
        # A healthy symlink pointing OUTSIDE the directory (at a pointer
        # file elsewhere) or INTO a subdirectory (a document there): not
        # the pointer -- fall through and read the link's TARGET CONTENT,
        # exactly the pre-A9 behaviour for these shapes.
    try:
        resolved = os.stat(path)
    except OSError as exc:  # unreachable for a symlink chain that resolved above
        return None, f"{exc!r}"
    if not stat.S_ISREG(resolved.st_mode):
        # A directory, FIFO, device node, socket: something IS configured
        # at this path, but it is not a file this hook may open (R1-c13).
        return None, "not a regular file"
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(_MAX_DOCUMENT_CHARS)
    except OSError as exc:
        return None, f"{exc!r}"
    match = _LATEST_POINTER_RE.search(text)
    if not match:
        return None, None  # no pointer line: multi-track banner / arrow style / mismatch
    target = match.group(1).strip()
    return (os.path.basename(target) if target else None), None


def _probe_frontmatter(path):
    """Just enough of a candidate's head to read its frontmatter, for the
    fallback scan below (which reads every candidate).

    Only ``FileNotFoundError`` (the file listed a moment ago is gone by the
    time this reads it -- an ordinary race) is swallowed to ``{}``. Any
    OTHER ``OSError`` -- permission denied, a stale NFS handle -- is
    RE-RAISED (R3-c06): the caller (``_locate``'s fallback scan) must not
    read "could not open this candidate" the same as "this candidate has
    no parseable updated-at". The two used to look identical to the
    fallback scan -- both simply fail to contribute a timestamp -- which
    let a newer document nobody can currently read lose, silently, to an
    older one that just happens to still be readable.
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(_FRONTMATTER_PROBE_CHARS)
    except FileNotFoundError:
        return {}
    frontmatter, _body = _split_frontmatter(text)
    return frontmatter


_GROUP_OWN = "own"
_GROUP_OTHERS = "others"
_GROUP_UNCERTAIN = "uncertain"


def _owner_group(frontmatter, identity):
    """``"own"`` / ``"others"`` / ``"uncertain"`` -- A9-2's three-way
    grouping of one candidate by its ``owner-container``, judged against
    ``identity`` (``{"uuid", "hostname", "label"}``, captured by the
    caller and handed in as data -- this function never touches the
    environment).

    ``own``: the second segment is uuid-shaped and equals the local aria
    uuid -- this container wrote it. ``others``: a uuid that differs, or a
    non-uuid second segment that equals NEITHER the hostname NOR the
    non-empty label (A9-3: clearly somebody else's machine -- a quiet
    ``not_owner``, not the every-session ``identity_unresolved`` the
    pre-A9 code recorded). ``uncertain``: everything that might still be
    THIS machine in a historical state -- a missing ``owner-container`` (or
    no parseable frontmatter at all), or a non-uuid second segment equal to
    the hostname or the non-empty label (Aria's ``get_container_id`` used
    to write exactly that before uuid pinning). The distinction is the
    whole point: "uncertain" candidates must never be silently passed over
    as if they were somebody else's (TASK-001 C1), so the selection rules
    in ``_locate`` treat a newer uncertain candidate, or an empty own group
    with uncertain candidates present, as ``identity_unresolved``.
    """
    raw = frontmatter.get("owner-container")
    if not isinstance(raw, str) or not raw.strip():
        return _GROUP_UNCERTAIN
    doc_uuid = _identity.owner_container_uuid(raw)
    if doc_uuid is not None:
        return _GROUP_OWN if doc_uuid == identity["uuid"] else _GROUP_OTHERS
    # A9-3: non-uuid second segment -- one cheap local comparison splits
    # "clearly someone else's machine" (others, quiet) from "possibly this
    # machine before uuid pinning" (uncertain, reportable). Case folded on
    # both sides (aria_label lowercases its half for the same reason).
    second = raw.rsplit("/", 1)[-1].strip().lower()
    if not second:
        return _GROUP_UNCERTAIN  # an owner half with no uuid half at all: "missing"
    hostname = (identity["hostname"] or "").lower()
    label = (identity["label"] or "").lower()
    if second == hostname or (label and second == label):
        return _GROUP_UNCERTAIN
    return _GROUP_OTHERS


def _locate(handoff_dir, extra=None, *, identity):
    """``(name, reason)``: the candidate filename (inside ``handoff_dir``) to
    ingest, or a reason when none could be located. Exactly one of the two
    is non-``None``.

    ``identity`` (A9-2, required keyword) is ``{"uuid": str-or-None,
    "hostname": str, "label": str-or-None}`` -- the local container's aria
    uuid, hostname and aria label, captured by the CALLER and handed in as
    data (never read here: this function runs on the round worker thread,
    and environment/filesystem identity reads belong to the calling thread
    -- the same env-once discipline the other SessionEnd hooks follow).
    It drives the A9-2 owner grouping: candidates are grouped into
    own/others/uncertain BEFORE any selection, a pointer is only followed
    to an own-group document (a pointer at somebody else's document
    selects this container's own newest instead -- the fix for shared
    latest.md pointers, which used to silence this container's ingestion
    under a quiet ``not_owner``), an uncertain-group document blocks with
    ``identity_unresolved`` rather than being quietly passed over
    (TASK-001 C1), and the no-pointer fallback compares the OWN group's
    newest ``updated-at`` while a NEWER uncertain candidate blocks the
    pick. The caller additionally returns ``not_owner`` (quiet) when every
    candidate is provably somebody else's and ``identity_unresolved`` when
    none are provably ours but some might be.

    Amendment A4-5 (owner ruling 2026-09-21): a project with no handoff
    directory, or one with nothing but a pointer and/or a README in it, is
    the common case and must stay quiet (``no_handoff``, a skip). A project
    that DOES keep handoffs, where neither the pointer nor the fallback
    resolves to one, means a renamed template or a broken frontmatter --
    ``pointer_unresolved``, a failure, reported at the next SessionStart.

    A directory that exists but could not even be LISTED (ruling 5, R1-c06)
    is the same failure, not the quiet ``no_handoff`` leg: ``_candidates``
    re-raises anything other than "the directory does not exist" for this
    to catch, name on stderr, and fold into the existing failure-class
    reason rather than inventing a new one. R3-c05 adds a second failure
    shape at the SAME level: every listed ``*.md`` entry existing only in
    ``_candidates``'s ``undecidable`` list (a dangling symlink, ``ELOOP``)
    is "something was configured here, none of it could be resolved" --
    the same ``pointer_unresolved`` leg, not the quiet one, even though
    ``listdir`` itself succeeded. A9-19 (owner ruling 2026-10-01, closing
    the old A9 row c18) adds the third shape: a directory with SOME
    resolvable candidates alongside an undecidable one DOES hold back the
    FALLBACK scan below -- the unstattable entry may be exactly the newest
    document, so the whole round reports ``pointer_unresolved`` naming the
    entry and its error type (the pre-A9 code silently left it out of the
    newest-``updated-at`` comparison). A WORKING pointer that resolves to
    a resolvable candidate bypasses that scan entirely, so an undecidable
    SIBLING does not sink a round whose choice was never in doubt. A
    pointer that names an undecidable entry EXPLICITLY is a different
    case, handled before the fallback scan even starts (R4-c03): silently
    substituting an
    older, merely-readable sibling for the specific document latest.md
    points at is exactly the silent which-document swap ruling 13 forbids,
    so that path reports ``pointer_unresolved`` instead.

    ``extra``, given (R2-c04), is a ledger ``extra`` dict updated in place
    with a ``detail`` key on every ``pointer_unresolved`` exit: the
    several origins -- unlistable directory, every candidate undecidable,
    latest.md itself unresolved with more than one entry it could have
    named, undecidable ones included (R5-c01 / R6-c01), a pointer naming
    an undecidable entry, a candidate
    that could not even be READ to compare, candidates that exist but none
    resolved a timestamp -- read identically on the ledger otherwise, and
    stderr from a SessionEnd hook is not a channel anyone reads. ``None``
    (the default) skips this -- callers that only care about the reason,
    like most of this file's own tests, need not provide one. Every such
    exit calls ``_warn`` (R3-c03/c04) -- INCLUDING, since R6-c02, the LAST
    `return` in this function's own source order ("no candidate resolved
    a parseable updated-at"), which used to be the one exception (R4-c16:
    an earlier revision of THIS paragraph claimed ALL exits called
    `_warn`, which stopped being true the moment a second exit existed;
    R5-c10 then found the revision meant to correct that instead pointed
    at the wrong item in THIS paragraph's own listing -- "the last"
    refers to the last `return` in the function body, not whichever
    origin happens to be listed last in this prose; R6-c02 closed the gap
    itself -- giving that exit the SAME `_warn` call every other one
    already had, once a directory where BOTH latest.md and the lone
    candidate's frontmatter were broken was found reporting a `detail`
    that named only the frontmatter, with no stderr line at all -- rather
    than just correcting the claim about it a third time). A bare
    ``print`` here, on a closed stderr pipe, would raise BrokenPipeError
    OUT of this function (a ``ConnectionError`` subclass, which
    ``_hook_state.reason_for_exception`` reads as ``http_error`` one layer
    up) before ``extra["detail"]`` was ever set -- turning "docs/handoff
    could not be listed" into a misleading network-failure report with no
    detail at all; irrelevant to the exit R6-c02 just changed specifically
    (it goes through ``_warn``, which never raises this way, R3-c03), but
    still why ``extra["detail"]`` is set BEFORE the diagnostic write is
    even attempted on every exit, not only this one. A future exit added
    to this function without a matching `_warn` call would make this
    paragraph wrong a third time -- update it rather than letting the
    claim silently drift again.
    """
    try:
        candidates, undecidable = _candidates(handoff_dir)
    except OSError as exc:
        detail = f"docs/handoff exists but could not be listed: {exc!r}"
        if extra is not None:
            extra["detail"] = _short(detail)
        # This order (detail set, THEN _warn) is no longer load-bearing on
        # its own (R4-c16): the ORIGINAL reason for it, back when `_warn`
        # was a bare `print`, was that a BrokenPipeError from that print
        # would escape this function before `extra["detail"]` was ever
        # reached. R3-c03 made `_warn` itself never raise, for exactly
        # this reason among others -- so swapping this order today changes
        # nothing a test can observe (confirmed: an equivalent-mutant
        # check, not a gap). Kept in THIS order anyway, matching every
        # other exit below, because "the ledger detail is always set
        # before any stderr write is even attempted" is simpler to reason
        # about than "it happens to not matter here".
        _warn(f"[{HOOK}] {detail}")
        return None, "pointer_unresolved"
    if not candidates and not undecidable:
        return None, "no_handoff"
    if not candidates:
        # Every listed *.md entry was undecidable (R3-c05): something WAS
        # kept here, so this is the loud "cannot tell" leg, not "nothing
        # here" -- the same distinction ruling 5 draws for an unlistable
        # directory, one level down. A9-19: the per-entry error types ride
        # along (a dangling neighbour and a permission loss read the same
        # without them).
        detail = (
            f"{len(undecidable)} candidate(s) in docs/handoff could not be resolved: "
            + ", ".join(f"{name} ({exc!r})" for name, exc in undecidable)
        )
        if extra is not None:
            extra["detail"] = _short(detail)
        _warn(f"[{HOOK}] {detail}")
        return None, "pointer_unresolved"
    undecidable_by_name = dict(undecidable)
    target, pointer_unresolved = _pointer_target(handoff_dir)
    if pointer_unresolved is not None and len(candidates) + len(undecidable) > 1:
        # R5-c01 / ruling 13: latest.md exists in SOME form but could not
        # be resolved (a dangling or self-referential symlink, a
        # directory, a permission error, a stale handle) -- a DIFFERENT
        # shape from simply being absent. With two or more `*.md` entries
        # to choose between -- RESOLVABLE candidates and undecidable ones
        # alike (R6-c01: originally just `len(candidates) > 1`, which
        # missed the case below) -- silently falling through to the
        # newest-`updated-at` scan below risks ingesting a DIFFERENT
        # document than whatever the unreadable pointer might actually
        # have named -- the same silent which-document swap R4-c03 already
        # closes one level down (an EXPLICIT pointer TARGET landing in
        # `undecidable`), just caught here for the pointer FILE itself.
        # With only ONE `*.md` entry in the directory TOTAL there is
        # nothing else it could have named, so that leg falls through
        # unchanged below (`target` is `None` here, which never matches a
        # real filename, so the fallback scan runs exactly as it would for
        # a genuinely absent pointer) -- R2-c13's FIFO fixture stays quiet,
        # not a failure. R6-c01: an UNDECIDABLE sibling counts toward that
        # total exactly like a resolvable one -- latest.md could just as
        # well have named the undecidable entry as the lone candidate, so
        # one healthy candidate PLUS one undecidable sibling is still two
        # things it could have meant, not one. The original `len(
        # candidates) > 1` check missed this: a directory holding exactly
        # one resolvable candidate alongside an undecidable entry (a
        # dangling/looping `*.md` symlink, or one an EIO/EACCES lstat could
        # not resolve at all) satisfied `len(candidates) == 1` and fell
        # through to the quiet fallback -- silently picking the lone
        # candidate even though the unreadable pointer might have named
        # the undecidable sibling instead.
        detail = f"latest.md: {pointer_unresolved}"
        if extra is not None:
            extra["detail"] = _short(detail)
        _warn(f"[{HOOK}] {detail}")
        return None, "pointer_unresolved"
    if target in candidates:  # None never matches a real filename
        # A9-2 (owner ruling 2026-10-01): a pointer is only ever followed
        # to a document THIS container wrote -- the pre-A9 code followed it
        # unconditionally and then failed the owner check, which is exactly
        # how a shared pointer (nexus's latest.md pointing at the OTHER
        # container's handoff) silenced this container's own ingestion for
        # weeks: not_owner, a skip, zero requests, never reported beyond
        # the ledger.
        if identity["uuid"] is None:
            # Same "cannot tell" as the old owner check: the LOCAL side is
            # unreadable, so even a pointer-resolved candidate cannot be
            # judged. Reportable, never a silent skip.
            detail = "local aria uuid could not be determined"
            if extra is not None:
                extra["detail"] = _short(detail)
            _warn(f"[{HOOK}] {detail}")
            return None, "identity_unresolved"
        try:
            target_frontmatter = _probe_frontmatter(os.path.join(handoff_dir, target))
        except OSError as exc:
            # R3-c06 for the pointed document specifically.
            detail = f"{target}: pointer target could not be read: {exc!r}"
            if extra is not None:
                extra["detail"] = _short(detail)
            _warn(f"[{HOOK}] {detail}")
            return None, "pointer_unresolved"
        target_group = _owner_group(target_frontmatter, identity)
        if target_group == _GROUP_OWN:
            return target, None
        if target_group == _GROUP_UNCERTAIN:
            # A9-2: the pointer names a document whose ownership cannot be
            # decided -- silently passing over it for this container's
            # (older) own document would be exactly the C1 quiet-stop, so
            # this reports instead.
            detail = (
                f"{target}: owner-container unresolved "
                f"({target_frontmatter.get('owner-container')!r})"
            )
            if extra is not None:
                extra["detail"] = _short(detail)
            _warn(f"[{HOOK}] {detail}")
            return None, "identity_unresolved"
        # _GROUP_OTHERS: fall through to the own-group newest scan below.
    if target is not None and target in undecidable_by_name:
        # R4-c03 / ruling 13: latest.md explicitly names this entry, and it
        # IS listed -- just unresolved (a dangling symlink, ELOOP, a
        # per-entry permission problem). Falling through to the
        # newest-updated-at scan below would silently ingest a DIFFERENT,
        # older document instead of the one the pointer actually names,
        # with a clean-looking ledger row: exactly the silent
        # which-document swap ruling 13 forbids. A9-23 reaches this same
        # branch with a symlink latest.md whose LINK TEXT names the
        # undecidable entry (the dangling chain propagates the target's
        # own stat failure, so the readlink leg returns its basename).
        # A9-19: the bound error type rides along in the detail.
        detail = (
            f"{target}: pointer target could not be resolved "
            f"({undecidable_by_name[target]!r})"
        )
        if extra is not None:
            extra["detail"] = _short(detail)
        _warn(f"[{HOOK}] {detail}")
        return None, "pointer_unresolved"
    if undecidable:
        # A9-19 (owner ruling 2026-10-01, option A 全报): this point is
        # only reached when NO usable pointer resolved (none at all, or
        # one naming a non-candidate or another container's document), so
        # the choice below is about to be made by the newest-`updated-at`
        # fallback scan -- and an entry that cannot even be stat-ed may be
        # exactly the NEWEST document (own or not -- its owner cannot be
        # read either), silently excluded from that comparison. The other
        # three "cannot tell" shapes (unlistable directory,
        # all-undecidable, latest.md itself unresolved) already report;
        # this used to be the one quiet hold-out, and "bad entry count 1
        # vs 2" decided whether the same damage reported or not. The whole
        # round reports, the detail naming each entry and its error type.
        # A WORKING pointer to an OWN document (the branch above) bypasses
        # this scan entirely, which is the one shape A9-19 spares: a bad
        # sibling must not sink a round whose choice was never in doubt.
        detail = (
            f"{len(undecidable)} candidate(s) in docs/handoff could not be resolved: "
            + ", ".join(f"{name} ({exc!r})" for name, exc in undecidable)
        )
        if extra is not None:
            extra["detail"] = _short(detail)
        _warn(f"[{HOOK}] {detail}")
        return None, "pointer_unresolved"
    if identity["uuid"] is None:
        # Same check as the pointer branch, for the scan path: nothing can
        # be grouped without the local uuid.
        detail = "local aria uuid could not be determined"
        if extra is not None:
            extra["detail"] = _short(detail)
        _warn(f"[{HOOK}] {detail}")
        return None, "identity_unresolved"
    # A9-2's grouped scan: the fallback comparison runs over the OWN group
    # only -- every candidate is still probed (a newer UNCERTAIN candidate
    # blocks the pick below), while OTHERS are simply not in the running,
    # which is what fixes the shared-pointer shape end to end.
    own, uncertain = [], []  # own: (name, ts); uncertain: (name, ts, raw owner)
    for name in candidates:
        try:
            frontmatter = _probe_frontmatter(os.path.join(handoff_dir, name))
        except OSError as exc:
            # R3-c06: a candidate that IS resolvable as a file but cannot
            # be READ must not silently lose the newest-updated-at
            # comparison to an older, readable sibling -- that would ingest
            # the wrong document and report nothing wrong. Whichever
            # document turns out to be truly newest is unknowable here, so
            # this fails loud rather than guessing from what happens to be
            # readable.
            detail = f"{name}: frontmatter could not be read to compare updated-at: {exc!r}"
            if extra is not None:
                extra["detail"] = _short(detail)
            _warn(f"[{HOOK}] {detail}")
            return None, "pointer_unresolved"
        group = _owner_group(frontmatter, identity)
        ts = _parse_instant(frontmatter.get("updated-at"))
        if group == _GROUP_OWN:
            own.append((name, ts))
        elif group == _GROUP_UNCERTAIN:
            # The RAW owner value rides along (R1-c10): "which document,
            # which value was the problem", without reproducing the run.
            uncertain.append((name, ts, frontmatter.get("owner-container")))
    if not own:
        if uncertain:
            # A9-2: nothing here is provably this container's, and
            # something MIGHT be (uncertain) -- that is TASK-001 C1's
            # reportable case, not a quiet not_owner. R6-c02's courtesy
            # survives the A9-2 rewrite: when latest.md was ALSO broken
            # (single-entry exemption let this scan run anyway), naming
            # both files sends whoever reads the detail to fix the right
            # one(s), not just the frontmatter.
            parts = []
            if pointer_unresolved is not None:
                parts.append(f"latest.md: {pointer_unresolved}")
            # R1 review: the [:5] slice is bounded, but the BOUND must be
            # visible -- 40 uncertain candidates reporting 5 silently is
            # exactly the ledger-quietly-lying shape this file keeps closing.
            # R2 review: the count rides in its OWN extra key, never inside
            # the detail string -- detail passes through _short's 200-char
            # cap, and an inline marker appended before the cap was measured
            # to arrive HALF-CUT ("(+35 ") with real-length names, which is
            # worse than no marker. A separate key is never truncated.
            if extra is not None:
                extra["uncertain_candidates"] = len(uncertain)
            parts.append(
                "no own-container candidate; unresolved owner-container: "
                + ", ".join(f"{name}({raw!r})" for name, _ts, raw in uncertain[:5])
            )
            detail = "; ".join(parts)
            if extra is not None:
                extra["detail"] = _short(detail)
            _warn(f"[{HOOK}] {detail}")
            return None, "identity_unresolved"
        return None, "not_owner"  # every candidate is provably somebody else's: quiet
    newest_name, newest_ts = None, None
    for name, ts in own:
        if ts is not None and (newest_ts is None or ts > newest_ts):
            newest_name, newest_ts = name, ts
    if newest_name:
        for _name, ts, _raw in uncertain:
            # A9-2: an uncertain candidate that is NEWER than the own-group
            # pick blocks it -- it may be this container's own newest
            # document with an unparseable/legacy owner field. Uncertain
            # candidates with no comparable timestamp do NOT block (the
            # corpus: nexus's 20 uncertain candidates are all old documents
            # that must not hold the round hostage).
            if ts is not None and ts > newest_ts:
                detail = (
                    f"newer unresolved owner-container candidate than the own-group pick: "
                    f"{_name}"
                )
                if extra is not None:
                    extra["detail"] = _short(detail)
                _warn(f"[{HOOK}] {detail}")
                return None, "identity_unresolved"
        return newest_name, None
    # R6-c02: `pointer_unresolved`, when set, was computed by `_pointer_
    # target` above and then DELIBERATELY not acted on yet -- the
    # single-total-entry exemption a few lines up lets this fallback scan
    # run anyway (`target` stays `None`, which never matches a real
    # filename). Discarding it here, once the scan ALSO fails to find a
    # parseable `updated-at`, used to build this exit's `detail` from
    # scratch -- so a directory where BOTH latest.md (unreadable) AND the
    # lone candidate's frontmatter (unparseable) are broken reported a
    # ledger `detail` that only names the frontmatter problem, sending
    # whoever reads it to fix the wrong file. Naming `latest.md`
    # explicitly here is the same courtesy every OTHER `pointer_unresolved`
    # exit in this function already extends.
    if pointer_unresolved is not None:
        detail = f"latest.md: {pointer_unresolved}"
    else:
        detail = (
            f"{len(own)} own-container candidate(s) in docs/handoff, none with a "
            f"parseable updated-at and no pointer resolved to one"
        )
    if extra is not None:
        extra["detail"] = _short(detail)
    _warn(f"[{HOOK}] {detail}")
    return None, "pointer_unresolved"


# ── frontmatter ──────────────────────────────────────────────────────────

def _split_frontmatter(text):
    """``(frontmatter, body)``. ``frontmatter`` holds only the six known
    keys (a value is split on the FIRST ``:`` only, so an ISO timestamp
    value survives), stripped and with one level of matching quotes removed;
    unknown keys are ignored. A document with no well-formed ``---``-
    delimited block at its very first line (tolerating a UTF-8 BOM) has no
    frontmatter at all: ``{}`` and the whole text as ``body`` -- a handoff
    predating the frontmatter convention still has H1/§6/§2 to offer.
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
    for line in lines[1:end]:
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip()
        if key not in _FRONTMATTER_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        frontmatter[key] = value
    body = "\n".join(lines[end + 1:])
    return frontmatter, body


# R4-c14: this used to be a second, byte-identical copy of
# _ingest_client._parse_instant (only the docstring wording differed).
# Locating (this file, picking the newest-`updated-at` candidate) and
# stale-checking (_ingest_client, comparing local_updated_at against the
# server's own stored value) parse the SAME frontmatter value with what
# were two independent implementations that happened to agree today --
# changing either one's parsing rule alone (accepting/rejecting some
# input shape) would silently split what "the newest document" means from
# what "is this local copy stale" means, with no test able to catch the
# divergence (confirmed: a temp copy with only THIS file's copy changed
# passed the whole suite). Reusing the object directly, rather than
# keeping a second copy in sync by hand, is what actually rules that out
# -- and changes only this file (ruling 15: _ingest_client.py itself is
# untouched).
_parse_instant = _ingest_client._parse_instant


def _opted_out(frontmatter):
    """``"skip"`` / ``"allow"`` / ``"unparsable"`` -- the A9-5 three-state
    reading of ``nexus-ingest`` (owner ruling 2026-10-01, 发版前修改).

    ``skip`` / ``false`` / ``no`` / ``off`` / ``0`` are the opt-out (the
    author's clear intent in every spelling -- the pre-A9 code ingested the
    last four of these and the ledger row looked clean); ``true`` / ``yes``
    / ``on`` / ``1`` upload; EVERY OTHER value -- including the empty
    string once normalised -- is ``"unparsable"``: this key exists only to
    let the author control the upload, so an unrecognised value must not
    read as permission. The caller records the existing failure reason
    ``file_unparsable`` (no new reason) with the ORIGINAL value in the
    detail, reported every session until the value is fixed.

    Normalisation is unchanged from R1-c05 / R2-c02: strip a trailing
    inline ``# reason`` comment, then -- same as ``_split_frontmatter`` --
    one level of matching quotes, before normalising case. This frontmatter
    dialect is flat ``key: string`` only (session-handoff.md §2.3.8.3;
    Aria's own simple parser keeps an inline comment verbatim on the value
    exactly like ``_split_frontmatter`` above does), so ``Skip`` / ``SKIP``
    / ``skip  # has a secret in §6`` were all being read as "not skip" and
    ingested instead of honouring the recovery path (``docs/architecture/
    memory-layers.md`` §3.2) it exists for.

    The quote-stripping repeats ``_split_frontmatter``'s own step rather
    than relying on it: that function only strips a pair of quotes sitting
    at the very START and END of the WHOLE value, and a trailing comment
    means the closing quote is no longer the last character -- so a
    document written as ``nexus-ingest: "skip"  # reason`` (the quoted form
    a human editing frontmatter by hand is likely to use) reaches here
    still wearing its quotes, comment removed but quotes intact, and used
    to compare unequal to the bare word ``skip``.
    """
    value = frontmatter.get("nexus-ingest")
    if not isinstance(value, str):
        return "allow"  # key absent: nothing was opted, upload is the default
    value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    value = value.strip().lower()
    if value in ("skip", "false", "no", "off", "0"):
        return "skip"
    if value in ("true", "yes", "on", "1"):
        return "allow"
    return "unparsable"  # the caller puts the ORIGINAL frontmatter value in the detail


# ── content: H1 -> §6 -> §2, capped ─────────────────────────────────────

def _extract_h1(lines):
    for line in lines:
        if line.startswith("# "):
            return line.strip()
    return ""


def _is_divider_line(line):
    """True for a bare Markdown thematic-break line (``---`` / ``***`` /
    ``___``, three or more, optional surrounding whitespace) -- R3-c07.

    The real Aria template (``aria/templates/session-handoff.md``) puts one
    of these immediately before EVERY ``## §N`` heading, itself included:
    a handoff whose author deleted §6/§2's body but left the heading and
    that trailing divider in place -- exactly ruling 3 / R1-c01's "template
    skeleton not filled in" -- reads as heading, blank line, ``---``. The
    ``---`` line IS technically non-whitespace, so the plain
    ``line.strip()`` truthiness check ``_extract_section`` used to rely on
    read it as content and shipped "## §2 ...\\n\\n---" as a real episode.
    """
    stripped = line.strip()
    return len(stripped) >= 3 and stripped in (
        "-" * len(stripped), "*" * len(stripped), "_" * len(stripped)
    )


def _extract_section(lines, start_re):
    """The heading line matching ``start_re`` through the line before the
    next ``## `` heading (subsections included), or ``""`` when the section
    is not present at all -- OR present only as a bare heading with nothing
    but blank lines (or a bare divider line, R3-c07) under it (ruling 3,
    TASK-005 R1 fix round, R1-c01): a freshly created handoff whose §6/§2
    still just carry the template heading -- and, in the real template,
    the ``---`` divider that trails every section -- is not "content", and
    must not be read as one. Subsection headings and any other non-blank,
    non-divider line under the heading still count.
    """
    start = None
    for i, line in enumerate(lines):
        if start_re.match(line):
            start = i
            break
    if start is None:
        return ""
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if _H2_RE.match(lines[j]):
            end = j
            break
    if not any(
        line.strip() and not _is_divider_line(line) for line in lines[start + 1 : end]
    ):
        return ""
    return "\n".join(lines[start:end]).strip()


def _join(parts):
    return "\n\n".join(p for p in parts if p)


def _cap(text, limit):
    """Cut ``text``'s tail to fit within ``limit`` characters, appending a
    marker that itself counts toward the cap.

    Retreats to the last LINE boundary at or before the cut point rather
    than slicing mid-line (ruling 9 / R1-c02, R1-c11): a value-level
    redaction rule (``_redact``) has to see a secret's whole shape to catch
    it, and a cut that lands inside one -- `postgresql://user:` then the cut,
    password on the next chunk -- ships the fragment before it ever reaches
    ``_ingest_client``'s redaction pass. Dropping the whole line that does
    not fit is the price: only when NOT EVEN ONE line fits inside ``limit``
    (no newline at all within the budget) does this fall back to the old
    mid-line hard cut, because there is nothing else left to do with it.

    A ``limit`` too small for even the COMPLETE marker (R2-c07) comes back
    ``""``, not a slice of the marker itself: the old ``_TRUNCATION_MARKER[
    :limit]`` produced a partial marker like ``"\\n\\n…[t"`` for a budget of
    5 -- unreadable, and indistinguishable from real content that happened
    to get cut mid-marker, rather than a deliberate truncation notice.
    Nothing useful fits in that band; the caller (``_assemble_content``)
    already treats an empty result as "drop this section entirely" for
    exactly this reason. ``limit`` exactly equal to the marker's own length
    is the one point in this band worth keeping: the complete, un-truncated
    marker, with zero characters of real content ahead of it.
    """
    if len(text) <= limit:
        return text
    if limit < len(_TRUNCATION_MARKER):
        return ""
    budget = limit - len(_TRUNCATION_MARKER)
    cut = text[:budget]
    newline = cut.rfind("\n")
    if newline != -1:
        cut = cut[:newline]
    return cut.rstrip() + _TRUNCATION_MARKER


def _assemble_content(h1, section6, section2):
    """H1, then §6, then §2, joined and capped at ``_CONTENT_CAP``. §2's
    tail is cut first; only when H1 + §6 alone already reach the cap does
    §2 get dropped entirely and §6's tail get cut instead.

    H1 itself is also subject to the cap (ruling 9, R1-c11): a pathological
    title long enough to reach ``_CONTENT_CAP`` on its own used to come back
    verbatim, uncapped, because there was nothing left in the §6 budget to
    even fit the truncation marker -- the ``_cap(h1, ...)`` fallback below is
    what still holds the invariant "the result never exceeds
    ``_CONTENT_CAP``" in that corner.
    """
    full = _join([h1, section6, section2])
    if len(full) <= _CONTENT_CAP:
        return full
    head = _join([h1, section6])
    if len(head) < _CONTENT_CAP:
        sep = len("\n\n") if head and section2 else 0
        remaining = _CONTENT_CAP - len(head) - sep
        section2_cut = _cap(section2, remaining) if remaining > 0 else ""
        return _join([h1, section6, section2_cut]) if section2_cut else head
    sep = len("\n\n") if h1 and section6 else 0
    remaining = _CONTENT_CAP - len(h1) - sep
    section6_cut = _cap(section6, remaining) if remaining > 0 else ""
    if section6_cut:
        return _join([h1, section6_cut])
    # Even H1 alone meets or exceeds the cap: there is nothing left to trim
    # but H1 itself.
    return _cap(h1, _CONTENT_CAP)


def _build_content(body):
    """``(content, reason)``: exactly one is non-``None``.

    An empty/whitespace-only body is ``empty_sections`` (a skip: most
    handoffs are not empty, but nothing here is broken). A nontrivial body
    (>= ``_MIN_NONTRIVIAL_BODY`` chars once stripped) whose §6 and §2 both
    parse empty is ``sections_unparsed`` (a failure: a renamed heading, not
    an empty handoff) -- the same split as ``no_handoff`` / ``pointer_
    unresolved`` one level up, for the same reason.
    """
    stripped = body.strip()
    if not stripped:
        return None, "empty_sections"
    lines = body.splitlines()
    h1 = _extract_h1(lines)
    section6 = _extract_section(lines, _SECTION_6_RE)
    section2 = _extract_section(lines, _SECTION_2_RE)
    if not section6 and not section2:
        if len(stripped) >= _MIN_NONTRIVIAL_BODY:
            return None, "sections_unparsed"
        return None, "empty_sections"
    return _assemble_content(h1, section6, section2), None


def _cap_for_wire(content):
    """Re-cut ``content`` (already at most ``_CONTENT_CAP`` chars, per
    ``_build_content`` above) so the REDACTED text -- what ``_ingest_client``
    actually puts on the wire -- also fits the cap (R2-c08).

    ``_build_content`` / ``_assemble_content`` / ``_cap`` are deliberately
    redaction-OBLIVIOUS: they only know about characters, and a value-level
    redaction rule needs to see a secret's whole shape (see ``_cap``'s own
    docstring), which is exactly why the cut there retreats to a line
    boundary rather than the redactor's own match boundaries. But a
    redaction MARKER is longer than a short secret it replaces
    (``[redacted:url-userinfo]`` is 23 characters; a URL password can be as
    short as 4), so content this hook built at EXACTLY the cap -- honouring
    its own invariant -- can still leave the process longer than the cap by
    the time ``_ingest_client`` redacts it.

    This file owns the 4000-character business rule (``_CONTENT_CAP``);
    ``_ingest_client`` is generic and shared with memory_sync, so guessing a
    per-caller limit there is the wrong layer. Re-running the SAME redaction
    pass here (rather than trying to predict its growth analytically) is
    the simplest thing that is still correct; ``_ingest_client`` redacts
    this same text again on the way out, which is idempotent (the marker
    text itself matches no rule).

    Bounded: each iteration's cut is by at least the previous iteration's
    overflow (>= 1 whenever the loop runs again), so this converges in one
    or two passes for any realistic document; the loop is additionally
    capped at ``_CONTENT_CAP`` iterations as a hard ceiling against a
    pathological future redaction rule that never converges.

    ``limit`` is derived from ``content``'s OWN current length, not from
    ``_CONTENT_CAP`` (R3-c02: the previous ``limit -= overflow``, starting
    from ``_CONTENT_CAP`` itself, computed a limit LARGER than
    ``len(content)`` whenever ``content`` sat in the band
    ``(_CONTENT_CAP - G, _CONTENT_CAP - G/2]`` -- ``G`` the net character
    growth one redaction hit adds (23 for the shortest URL-userinfo match
    against its 4-character minimum: net +19). The old
    ``len(content) <= limit`` escape hatch then read as "already short
    enough", returning ``content`` UNCHANGED even though its REDACTED
    form -- what ``_ingest_client`` actually puts on the wire -- exceeded
    the cap by as much as ``G/2``. Computing ``limit`` from ``len(content)``
    itself means that check can only ever be true when there is genuinely
    nothing left to cut.
    """
    for _ in range(_CONTENT_CAP):
        redacted, _hits = _redact.redact_text(content)
        overflow = len(redacted) - _CONTENT_CAP
        if overflow <= 0:
            return content
        limit = len(content) - overflow
        if limit <= 0:
            # Nothing left to safely cut (e.g. the whole thing is one
            # matched secret) -- leave it to _ingest_client / the backend
            # rather than mangle it further.
            return content
        content = _cap(content, limit)
    return content


# ── metadata / external_id ──────────────────────────────────────────────

def _current_branch(cwd):
    """Delegates to _identity.current_branch. Module-level so tests can
    patch it directly (same pattern as the other two SessionEnd hooks)."""
    return _identity.current_branch(cwd)


def _external_id(root, doc_path):
    """The handoff path relative to ``root``, POSIX separators -- identical
    whether the hook runs from the root or a subdirectory (both resolve to
    the same ``root`` via ``_identity.project_root``)."""
    rel = os.path.relpath(doc_path, root)
    return rel.replace(os.sep, "/")


def _build_metadata(session_id, branch, frontmatter, owner_container_uuid, project=None):
    """§3.2 + Amendment A8-1's ``aria.*`` set for this row. ``branch`` and
    each ``aria.*`` key are omitted when the source value is absent (a PATCH
    is a shallow merge; sending nothing for a key leaves the stored value
    alone, which is the point -- see IDENTITY_KEYS in _ingest_client.py).

    ``aria.owner_container`` is the document's NORMALISED aria uuid (== the
    local uuid, since this is only reached after the owner check passed),
    not the raw two-segment frontmatter string -- proposal workflow D
    defines ``aria.owner_container`` as the aria identity, which this
    resolves in the caller's favour over the metadata list read literally.
    """
    metadata = {"session_id": session_id, "aria.source": "handoff"}
    if branch:
        metadata["branch"] = branch
    # X1-policy (TASK-007): the project scope key. Without it a handoff
    # episode fails the read side's row-by-row `metadata.project` check (the
    # D filters by project) and would be dropped as filter_suspect even in
    # its own project. Same derivation as session_capture/memory_sync:
    # _identity.project_key, CLAUDE_PROJECT_DIR first, git-toplevel fallback
    # reported by the caller.
    if project:
        metadata["project"] = project
    if frontmatter.get("track-id"):
        metadata["aria.track_id"] = frontmatter["track-id"]
    if owner_container_uuid:
        metadata["aria.owner_container"] = owner_container_uuid
    if frontmatter.get("phase"):
        metadata["aria.phase"] = frontmatter["phase"]
    if frontmatter.get("status"):
        metadata["aria.status"] = frontmatter["status"]
    if frontmatter.get("updated-at"):
        metadata["aria.updated_at"] = frontmatter["updated-at"]
    return metadata


def _short(text, cap=200):
    """A ledger-safe excerpt: bounded, never the content, never a secret --
    ``outcome.detail`` only ever carries HTTP/protocol context (§4 module
    docstring of _ingest_client.py)."""
    if not text:
        return None
    return str(text)[:cap]


def _session_start(transcript_path):
    """The EARLIEST timestamp in this session's transcript, or ``None``.

    A9-1's session-start oracle: the earliest TIMESTAMPED record of the
    transcript SessionEnd hands us. The first few records (mode /
    permission-mode / file-history-snapshot) carry no timestamp at all, and
    the first two user records of one real session were observed OUT OF
    ORDER -- so this is a minimum over every parsed timestamp, not "the
    first one seen". One linear pass, no JSON parse of lines that cannot
    hold the key.

    ``None`` means "cannot prove" -- unreadable path, missing file, no
    timestamped record at all -- and the caller's A9-1 rule sends that to
    the synthetic-id attribution, never a guess: once a row is created its
    session_id can never be corrected (an IDENTITY_KEY, §3.2), and a
    wrongly-attributed real session_id would additionally suppress that
    session's own aggregated summary for good (the aggregator's
    ``owned_episode`` skip).
    """
    if not isinstance(transcript_path, str) or not transcript_path:
        return None
    earliest = None
    try:
        with open(transcript_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if '"timestamp"' not in line:
                    continue  # cannot hold the key: skip the json.loads
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(entry, dict):
                    continue
                ts = _parse_instant(entry.get("timestamp"))
                if ts is not None and (earliest is None or ts < earliest):
                    earliest = ts
    except OSError:
        return None
    return earliest


# ── the work ─────────────────────────────────────────────────────────────

def _collect(run):
    """Do the work. Returns the reason string; raises only for a stdin
    payload that is not a JSON object (the runner maps it via
    ``_hook_state.reason_for_exception``, which resolves unrecognised
    exceptions to ``unknown``).

    ``run`` is filled in as facts become known, so that whatever happens
    next -- a return or an exception -- the ledger record has the right
    project. NOT the call count, if this
    thread is abandoned mid-``upsert`` (R1-c09): ``run["calls"]`` is only
    ever assigned AFTER ``upsert`` returns, so a thread abandoned while
    still inside it leaves the count at its initial 0 even though a real
    request may already be in flight or answered -- ``main`` reports that
    as ``calls=None`` (unknown), not a trustworthy 0, for exactly this
    reason.
    """
    raw = sys.stdin.read()
    event = json.loads(raw) if raw.strip() else {}
    if not isinstance(event, dict):
        raise ValueError("SessionEnd payload is not a JSON object")
    if isinstance(event.get("cwd"), str) and event["cwd"]:
        run["cwd"] = event["cwd"]
    cwd = run["cwd"] or os.getcwd()

    base_url = run["base_url"]
    if not base_url:
        return "not_configured"  # the default for a fresh install; do nothing else

    payload_session_id = event.get("session_id")

    # A9-2/A9-3: the identity bundle `_locate` groups candidates by. P1
    # (TASK-013, the same env-once discipline the other two SessionEnd
    # hooks follow): every input here was read ONCE by main(), on the
    # calling thread, before this worker started -- aria_uuid/aria_label
    # expanduser HOME, gethostname can raise, and this thread is one
    # run_with_deadline can only ABANDON, never kill.
    identity = {
        "uuid": run["aria_uuid"],
        "hostname": run["hostname"],
        "label": run["aria_label"],
    }

    toplevel, degraded = _identity.project_root(cwd)
    root = toplevel or cwd
    handoff_dir = os.path.join(root, "docs", "handoff")

    name, locate_reason = _locate(handoff_dir, run["extra"], identity=identity)
    if locate_reason:
        return locate_reason
    doc_path = os.path.join(handoff_dir, name)
    # Recorded as soon as a document is chosen -- before it is even opened
    # (R1-c10, ruling 9): every reason from here on, write path or not, now
    # says WHICH handoff it was about, instead of only the ones that reached
    # a successful upsert.
    # X1 (owner 2026-10-01; A10-12 (2): reuse _identity.memory_dir_key):
    # every external_id this hook writes carries the Claude Code project
    # directory key as a prefix -- with user_id now pinning the PERSON
    # (X1-policy, 2026-10-14), all of one user's projects share a pool, and
    # two projects' same-named handoffs would otherwise PATCH each other's
    # rows. The prefix derivation is the SAME single definition memory_sync
    # keys its external_ids, memory directory and state file by; a degraded
    # key never reaches the wire (identity_degraded refuses the write).
    key, _key_degraded = _identity.memory_dir_key(cwd)
    external_id = f"{key}/{_external_id(root, doc_path)}"
    run["extra"]["external_id"] = external_id
    try:
        with open(doc_path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(_MAX_DOCUMENT_CHARS)
    except OSError as exc:
        # Listed a moment ago by _locate, gone (or unreadable) now: a race
        # or a permissions change, not one of the digest's named reasons.
        run["extra"]["detail"] = _short(f"cannot read {name}: {exc!r}")
        return "unknown"

    frontmatter, body = _split_frontmatter(text)

    opt_out = _opted_out(frontmatter)
    if opt_out == "skip":
        return "opted_out"  # §3.2 recovery path: the owner check already passed in _locate
    if opt_out == "unparsable":
        # A9-5 (owner 2026-10-01): an unrecognised value is a refusal, not
        # permission. file_unparsable is already in the failure table, so
        # no new reason and no new table entry -- and the ORIGINAL value
        # goes in the detail, so whoever reads the ledger can fix the
        # document without reproducing the run.
        raw_value = frontmatter.get("nexus-ingest")
        run["extra"]["detail"] = _short(f"nexus-ingest={raw_value!r} is not a recognised value")
        _warn(f"[{HOOK}] {run['extra']['detail']}")
        return "file_unparsable"

    # A9-2: the owner check has ALREADY passed -- `_locate` only ever
    # hands back a document of this container's OWN group (a local uuid
    # that could not be determined, or a not-provably-somebody-else's
    # document, exits there with identity_unresolved / not_owner). The
    # document's own uuid half is still needed for aria.owner_container.
    doc_uuid = _identity.owner_container_uuid(frontmatter.get("owner-container"))

    content, content_reason = _build_content(body)
    if content_reason:
        return content_reason
    content = _cap_for_wire(content)  # R2-c08: the cap must hold post-redaction too

    # ---- write path: owner + content both passed ----
    # A9-1 (owner ruling 2026-10-01, 套餐 A 时间窗门控 + 合成 id 兜底): the
    # row's session_id is THIS session's id only when the document provably
    # belongs to it -- frontmatter updated-at not earlier than the session's
    # transcript start minus the minutes-level tolerance. Anything else --
    # an older document (a write that failed earlier, a first-round backlog,
    # a document this session never touched), an unreadable or absent
    # transcript, a missing payload session_id, an unparseable updated-at --
    # is "cannot prove", and the row goes in under the synthetic id
    # ``handoff:<external_id>``, hanging off no real session: that session's
    # own aggregated summary is unaffected (two rows for one session, the
    # accepted cost of the late sync), and the synthetic row can never steal
    # a session's summary slot. branch stays the run's branch in BOTH cases
    # (the document itself carries none, and an absent branch makes the row
    # near-unfindable in the branch-scoped injection query). Recorded in
    # extra as ``attribution`` (session|synthetic); no new reason.
    transcript_start = _session_start(event.get("transcript_path"))
    updated_at = _parse_instant(frontmatter.get("updated-at"))
    if (
        isinstance(payload_session_id, str) and payload_session_id
        and transcript_start is not None
        and updated_at is not None
        and updated_at >= transcript_start - timedelta(seconds=_ATTRIBUTION_TOLERANCE_SECONDS)
    ):
        session_id = payload_session_id
        run["extra"]["attribution"] = "session"
    else:
        session_id = f"handoff:{external_id}"
        run["extra"]["attribution"] = "synthetic"

    # X1-policy (TASK-007): project scope for the episode's metadata. The
    # git-source fallback is ledger-reported for the same reason as in
    # session_capture: a silent fallback would look "env-resolved" forever.
    project, project_source = _identity.project_key(cwd, run["project_env"])
    if project_source == _identity.PROJECT_SOURCE_GIT:
        run["extra"]["project_source"] = "git_fallback"
    metadata = _build_metadata(session_id, _current_branch(cwd), frontmatter, doc_uuid, project)

    # A9-11 (owner 2026-10-01): no drift check here, and no container_id
    # persistence. The compare-and-report moved to session_inject (every
    # startup, in systemMessage + the brief's first line, persisted only
    # after a successful stdout write); the two write hooks neither check
    # nor persist container_id anymore -- rows they wrote under an old
    # identity simply read as a peer container's on the injection side.

    # P1: token / container_id pre-read by main(); user_id INLINES
    # _identity.user_id's own two-line body with the pre-captured override
    # (calling the real one would read the environment on this thread when
    # the override is None -- the same reason memory_sync inlines it).
    user_id = run["default_user_id"] or _identity.project_slug(cwd)
    client = _ingest_client.IngestClient(
        base_url,
        run["token"],
        user_id,
        run["container_id"],
        SOURCE_NAME,
        timeout=_HTTP_TIMEOUT_SECONDS,
        deadline=run["deadline"],
        identity_degraded=degraded,
    )
    outcome = client.upsert(
        "session_summary",
        external_id,
        content,
        metadata,
        local_updated_at=frontmatter.get("updated-at"),
    )
    run["calls"] = outcome.calls
    run["extra"].update(
        # external_id is already in `extra` (set as soon as it was chosen,
        # above) -- not repeated here.
        action=outcome.action,
        redacted=outcome.redacted,
        dedup_merged=outcome.dedup_merged,
        # A10 D5 (2026-10-03): decided_status, never status -- status is the
        # state of whichever call happened to run LAST on this Outcome (a
        # dedup DELETE's 204 outlives a PATCH that never answered, and the
        # ledger would say the write succeeded when it did not). Rounds that
        # only ever LOOKED something up (unchanged / stale_local) decide
        # nothing: their status here is None, the honest reading.
        status=outcome.decided_status,
        detail=_short(outcome.detail),
    )
    return _hook_state.worst_reason(outcome.reasons)


def _record(reason, started, run, work_left_behind):
    """Append this run to the ledger, within a budget. Never raises.
    Returns True when the write had to be left behind.

    Mirrors the other two SessionEnd hooks' ``_record``: the write happens
    on a daemon thread abandoned after ``_LEDGER_BUDGET_SECONDS`` (the
    ledger takes a blocking lock, and a stall must not keep this process
    alive past the host's own timeout).

    ``work_left_behind`` is ``main()``'s own ``left_behind`` for the WORK
    thread (not this function's ledger-write one).

    A9-11 (owner 2026-10-01): the container_id persist that used to ride
    this write is GONE -- the drift check moved to session_inject, and this
    hook no longer touches its own state file at all.
    """
    elapsed_ms = int((time.monotonic() - started) * 1000)
    # R1-c09: a work thread abandoned mid-`upsert` never reaches the line
    # that assigns `run["calls"]`, so it is still sitting at its initial 0 --
    # which reads as "definitely made no requests" when the truth is "we do
    # not know". `None` says that honestly instead of a false 0.
    calls = None if work_left_behind else run["calls"]
    cwd = run["cwd"]
    extra = dict(run["extra"])

    def write():
        try:
            _hook_state.record_run(
                HOOK,
                ok=not _hook_state.is_failure_reason(reason),
                reason=reason,
                elapsed_ms=elapsed_ms,
                calls=calls,
                cwd=cwd,
                extra=extra or None,
            )
        except Exception as exc:  # record_run does not raise by contract; the net under it
            _warn(f"[{HOOK}] could not record this run ({reason}): {exc!r}")

    left_behind = _hook_runner.write_with_budget(write, _LEDGER_BUDGET_SECONDS, f"{HOOK}-ledger")
    if left_behind:
        _warn(
            f"[{HOOK}] ledger write still running after {_LEDGER_BUDGET_SECONDS}s; "
            f"leaving it behind, this run ({reason}) may go unrecorded"
        )
    return left_behind


def main():
    """Run the hook. Returns True when a worker thread had to be left behind."""
    started = time.monotonic()
    # Installed before the work thread starts (R4-c06; the class itself
    # moved into _hook_runner under Amendment A9-21 -- see that module's own
    # docstring): _collect, on that thread, calls into _ingest_client, whose
    # own stderr writes this file does not own (ruling 15) but must still
    # not let crash the run. guard_stderr() is idempotent, so repeated
    # in-process main() calls within the same test process do not
    # double-wrap (production runs this once per process, so it never
    # matters there).
    _hook_runner.guard_stderr()
    run = {
        "cwd": None,
        "calls": 0,
        "extra": {},
        # An absolute time.monotonic() value, about _DEADLINE_SLACK_SECONDS
        # before the work budget itself expires: IngestClient refuses a
        # request it could not finish in time rather than this whole
        # worker thread being abandoned mid-call with nothing recorded.
        "deadline": started + _WORK_BUDGET_SECONDS - _DEADLINE_SLACK_SECONDS,
    }

    # P1 (TASK-013; the discipline memory_sync established with its own
    # env-once round, review finding 6 of b3bb60d): every environment input
    # the work below needs is read HERE, on THIS (the calling) thread,
    # strictly before run_with_deadline starts the worker -- never inside
    # _collect, which runs on a daemon thread run_with_deadline can only
    # ABANDON, never kill. The worker reads every one of these back out of
    # `run`; the static guard in test_handoff_sync walks the same call
    # graph test_memory_sync's guard walks and fails on any environment
    # read reachable from _collect. The list, in read order: the backend
    # two, the state root (NEXUS_HOOK_STATE_DIR / expanduser), the user
    # override, the container identity (NEXUS_CONTAINER_ID or the
    # hostname), the A9-2/A9-3 bundle (aria uuid + label -- both
    # expanduser HOME -- and the hostname again for the comparison), and
    # CLAUDE_PROJECT_DIR for the X1-policy project key.
    try:
        run.update(
            base_url=os.environ.get("NEXUS_API_URL", "").rstrip("/"),
            token=os.environ.get("NEXUS_API_TOKEN", ""),
            default_user_id=os.environ.get(_identity.DEFAULT_USER_ID_ENV),
            container_id=_identity.container_id(),
            aria_uuid=_identity.aria_uuid(),
            aria_label=_identity.aria_label(),
            hostname=socket.gethostname(),
            project_env=os.environ.get(_identity.PROJECT_DIR_ENV),
        )
    except Exception as exc:  # noqa: BLE001 - recorded below, exactly like a worker exception
        # F3 (memory_sync's env-once review): a failure in these reads must
        # not become an unrecorded, silent exit through __main__'s blanket
        # handler -- it takes the SAME path a worker exception takes: an
        # `unknown` row plus a stderr line.
        outcome, left_behind = {"error": exc}, False
    else:
        outcome, left_behind = _hook_runner.run_with_deadline(
            lambda: _collect(run), _WORK_BUDGET_SECONDS, f"{HOOK}-work"
        )

    # _collect itself never calls print() DIRECTLY: the WORK stays off
    # stdio so an abandoned thread cannot interleave with anything main()
    # writes after giving up on it. Not quite absolute in practice, though
    # (R1-c35 said so of _ingest_client alone, and named two of its paths;
    # R2-c21 found that undercount, and R4-c16 found the FIX for that had
    # quietly drifted back into naming one -- "_locate ... prints its own
    # diagnostic line (an unlistable docs/handoff)" -- even though _locate
    # gained THREE MORE _warn call sites since: this revision goes back to
    # naming none, on purpose, rather than restate a list that can go
    # stale the same way a third time): both _locate, which _collect calls
    # directly, and _ingest_client write to stderr from SEVERAL places
    # each -- the same "spooky abandoned thread" risk _hook_runner's own
    # docstring already names, not something this hook adds beyond it.
    # (The run_with_deadline call itself now lives in the `else:` of the
    # P1 pre-read above, mirroring memory_sync's F3 shape.)

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
    # print, not after (R2-c05): the old order printed first, and a stderr
    # write that fails -- a closed pipe, the host already exiting -- used
    # to raise straight out of main() before _record ever ran, silently
    # losing the row for a run that had a genuine, useful reason to report
    # (timeout / a real exception). _warn's own swallow is a second,
    # independent net: even the reordering does not help if a LATER stderr
    # write in _record's own ledger-write path were to fail the same way.
    record_left_behind = _record(reason, started, run, left_behind)
    if diagnostic is not None:
        _warn(diagnostic)
    return record_left_behind or left_behind


if __name__ == "__main__":
    left_behind = False
    try:
        left_behind = bool(main())
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session teardown over handoff ingestion.
        pass
    _hook_runner.finish(left_behind)
