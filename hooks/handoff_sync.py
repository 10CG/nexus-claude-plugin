#!/usr/bin/env python3
"""SessionEnd handoff-ingestion hook for the nexus-memory plugin (workflow B,
change 2 TASK-005): ingests the project's own, most recent Aria session
handoff -- most recent project-wide, NOT most recent among the ones this
container owns (R1-c07: locate never filters by owner; ownership is checked
only AFTER the single newest candidate has already been picked, and a miss
there ends the run in ``not_owner``, not a search for an older document this
container does own -- Amendment A9) -- as a ``layer=session_summary`` episode
(``docs/architecture/memory-layers.md`` §3.2), the same layer the session
aggregator writes for an auto-summarised session -- a human-authored handoff
is just another episode with ``aria.source=handoff`` provenance instead of an
aggregator hash.

One SessionEnd run ingests at most ONE handoff document: the project's own
``docs/handoff/latest.md`` pointer, or -- when that pointer is absent,
mismatched or stale -- whichever candidate's frontmatter ``updated-at`` is
newest (Amendment A4-5, owner ruling 2026-09-21).

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
import stat
import sys
import time
from datetime import datetime, timezone

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
    kept for that case alone). The two used to be indistinguishable by
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
    undecidable = []
    for name in names:
        if name.upper() in _HANDOFF_EXCLUDED_NAMES_UPPER or not name.endswith(".md"):
            continue
        path = os.path.join(handoff_dir, name)
        try:
            is_file = stat.S_ISREG(os.stat(path).st_mode)
        except OSError:
            try:
                os.lstat(path)
            except FileNotFoundError:
                continue  # gone even at the symlink-entry level: a real race
            except OSError:
                # R4-c01: EACCES / EIO / ESTALE etc. on the lstat call
                # itself -- as opposed to FileNotFoundError -- is NOT "gone
                # by the time we looked": something is still there and this
                # run simply cannot resolve it (a directory that lost its
                # execute/search bit is the common real-world shape, see
                # the test with the same name below). The old code read
                # ANY OSError here the same as FileNotFoundError, quietly
                # dropping the candidate and, if it was the only one,
                # landing on `no_handoff` -- exactly the silent stop ruling
                # 5 / ruling 13 rule out. Falls through to the same
                # `undecidable.append` below as a successful lstat does.
                pass
            undecidable.append(name)
            continue
        if is_file:
            out.append(name)
    return sorted(out), undecidable


def _pointer_target(handoff_dir):
    """The basename ``latest.md`` points to, or ``None`` -- covers a missing
    ``latest.md``, a multi-track deprecation banner (no ``**Latest**:``
    line), and the deprecated arrow-style pointer alike: none of those match
    the collector pattern, so the caller falls back to the newest
    ``updated-at`` among the candidates.

    ``os.path.isfile`` is checked before ``open`` (R1-c13): without it, a
    FIFO or a device node named ``latest.md`` can block the read for as long
    as the hook's own work budget allows, rather than reading as "no
    pointer" the way any other unreadable file here does. The read itself is
    capped (``_MAX_DOCUMENT_CHARS``) for the same reason a regular file that
    is merely huge must not be read in full just to find one pointer line.
    """
    path = os.path.join(handoff_dir, _LATEST_MD)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(_MAX_DOCUMENT_CHARS)
    except OSError:
        return None
    match = _LATEST_POINTER_RE.search(text)
    if not match:
        return None
    target = match.group(1).strip()
    return os.path.basename(target) if target else None


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


def _locate(handoff_dir, extra=None):
    """``(name, reason)``: the candidate filename (inside ``handoff_dir``) to
    ingest, or a reason when none could be located. Exactly one of the two
    is non-``None``.

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
    ``listdir`` itself succeeded. When there is no pointer naming one of
    them specifically, a directory with SOME resolvable candidates
    alongside an undecidable one is not held back by the latter for the
    FALLBACK scan below: it is simply left out of the newest-``updated-at``
    comparison, the same as any other document this run cannot see (owner
    question A9 row c18 -- R4 post_implementation audit -- covers whether
    that silent exclusion is the right call; unchanged here). A pointer
    that names one of them EXPLICITLY is a different case, handled before
    the fallback scan even starts (R4-c03): silently substituting an
    older, merely-readable sibling for the specific document latest.md
    points at is exactly the silent which-document swap ruling 13 forbids,
    so that path reports ``pointer_unresolved`` instead.

    ``extra``, given (R2-c04), is a ledger ``extra`` dict updated in place
    with a ``detail`` key on every ``pointer_unresolved`` exit: the
    several origins -- unlistable directory, every candidate undecidable,
    a pointer naming an undecidable entry, candidates that exist but none
    resolved a timestamp, a candidate that could not even be READ to
    compare -- read identically on the ledger otherwise, and stderr from a
    SessionEnd hook is not a channel anyone reads. ``None`` (the default)
    skips this -- callers that only care about the reason, like most of
    this file's own tests, need not provide one. Every such exit EXCEPT
    the last (R4-c16: this used to claim ALL of them, which stopped being
    true the moment a second exit existed) also calls ``_warn``
    (R3-c03/c04): a bare ``print`` here, on a closed stderr pipe, would
    raise BrokenPipeError OUT of this function (a ``ConnectionError``
    subclass, which ``_hook_state.reason_for_exception`` reads as
    ``http_error`` one layer up) before ``extra["detail"]`` was ever set
    -- turning "docs/handoff could not be listed" into a misleading
    network-failure report with no detail at all. The "no candidate
    resolved a parseable updated-at" exit only sets ``detail``, with no
    matching ``_warn`` call -- an existing asymmetry, not something this
    revision changes; a future author adding one should keep it or, if
    intentionally leaving it out, drop this parenthetical instead of
    re-widening the claim back to "every exit".
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
        # directory, one level down.
        detail = (
            f"{len(undecidable)} candidate(s) in docs/handoff could not be resolved: "
            f"{', '.join(undecidable)}"
        )
        if extra is not None:
            extra["detail"] = _short(detail)
        _warn(f"[{HOOK}] {detail}")
        return None, "pointer_unresolved"
    target = _pointer_target(handoff_dir)
    if target in candidates:  # None never matches a real filename
        return target, None
    if target is not None and target in undecidable:
        # R4-c03 / ruling 13: latest.md explicitly names this entry, and it
        # IS listed -- just unresolved (a dangling symlink, ELOOP, a
        # per-entry permission problem). Falling through to the
        # newest-updated-at scan below would silently ingest a DIFFERENT,
        # older document instead of the one the pointer actually names,
        # with a clean-looking ledger row: exactly the silent
        # which-document swap ruling 13 forbids. This is deliberately
        # narrower than the fallback scan a few lines down (owner question
        # A9 row c18): only an EXPLICIT pointer resolving to an
        # undecidable entry fails loud here; the no-pointer fallback still
        # simply leaves an undecidable sibling out of the comparison.
        detail = f"{target}: pointer target could not be resolved"
        if extra is not None:
            extra["detail"] = _short(detail)
        _warn(f"[{HOOK}] {detail}")
        return None, "pointer_unresolved"
    newest_name, newest_ts = None, None
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
        ts = _parse_instant(frontmatter.get("updated-at"))
        if ts is not None and (newest_ts is None or ts > newest_ts):
            newest_name, newest_ts = name, ts
    if newest_name:
        return newest_name, None
    if extra is not None:
        extra["detail"] = _short(
            f"{len(candidates)} candidate(s) in docs/handoff, none with a parseable "
            f"updated-at and no pointer resolved to one"
        )
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
    """True when ``nexus-ingest`` selects the opt-out (R1-c05, TASK-005 R1
    fix round; quoted values R2-c02). Strips a trailing inline ``# reason``
    comment, then -- same as ``_split_frontmatter`` -- one level of matching
    quotes, before normalising case and comparing -- this frontmatter
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

    Only the literal ``skip`` is recognised once normalised: ``false`` /
    ``no`` / ``off`` are left ingesting, an open question for Amendment A9,
    not a guess made here.
    """
    value = frontmatter.get("nexus-ingest")
    if not isinstance(value, str):
        return False
    value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return value.strip().lower() == "skip"


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


def _build_metadata(session_id, branch, frontmatter, owner_container_uuid):
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


# ── the work ─────────────────────────────────────────────────────────────

def _collect(run):
    """Do the work. Returns the reason string; raises only for a stdin
    payload that is not a JSON object (the runner maps it via
    ``_hook_state.reason_for_exception``, which resolves unrecognised
    exceptions to ``unknown``).

    ``run`` is filled in as facts become known, so that whatever happens
    next -- a return or an exception -- the ledger record has the right
    project and (once the write path is reached) whether the container_id
    drift check must persist a new value. NOT the call count, if this
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

    base_url = os.environ.get("NEXUS_API_URL", "").rstrip("/")
    if not base_url:
        return "not_configured"  # the default for a fresh install; do nothing else

    session_id = event.get("session_id")

    toplevel, degraded = _identity.project_root(cwd)
    root = toplevel or cwd
    handoff_dir = os.path.join(root, "docs", "handoff")

    name, locate_reason = _locate(handoff_dir, run["extra"])
    if locate_reason:
        return locate_reason
    doc_path = os.path.join(handoff_dir, name)
    # Recorded as soon as a document is chosen -- before it is even opened
    # (R1-c10, ruling 9): every reason from here on, write path or not, now
    # says WHICH handoff it was about, instead of only the ones that reached
    # a successful upsert.
    external_id = _external_id(root, doc_path)
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

    if _opted_out(frontmatter):
        return "opted_out"  # checked before the owner check (§3.2 recovery path)

    local_uuid = _identity.aria_uuid()
    doc_uuid = _identity.owner_container_uuid(frontmatter.get("owner-container"))
    if local_uuid is None or doc_uuid is None:
        # Never silently `not_owner`: uuid could not be determined on either
        # side, which is a different (and reportable) condition from "this
        # is someone else's handoff". A pointer-resolved candidate whose
        # frontmatter simply lacks `owner-container` falls through to here
        # too -- it does not get a free pass on the owner check. The raw
        # frontmatter value (R1-c10) says which of the two sides was the
        # problem without having to reproduce the run to find out.
        run["extra"]["detail"] = _short(f"owner-container={frontmatter.get('owner-container')!r}")
        return "identity_unresolved"
    if local_uuid != doc_uuid:
        return "not_owner"

    content, content_reason = _build_content(body)
    if content_reason:
        return content_reason
    content = _cap_for_wire(content)  # R2-c08: the cap must hold post-redaction too

    # ---- write path: owner + content both passed ----
    metadata = _build_metadata(session_id, _current_branch(cwd), frontmatter, doc_uuid)

    state, _state_reasons = _hook_state.read_state(HOOK, cwd)
    drift_reasons = _hook_state.identity_drift(
        previous=state.get("container_id"),
        current=_identity.container_id(),
        state_existed=_hook_state.state_exists(HOOK, cwd),
    )
    if "identity_changed" in drift_reasons:
        # Recorded independently of the scalar `reason` this run ends on
        # (ruling 2, R1-c04): worst_reason can have a higher-priority
        # failure such as http_error outrank identity_changed there, and
        # without this separate flag the drift signal simply disappears
        # from the ledger for that run -- exactly the finding's complaint.
        #
        # Checked by MEMBERSHIP, not `if drift_reasons:` (R2-c06): a
        # corrupted or pre-migration state file (one that exists but
        # carries no `container_id`) makes `identity_drift` return
        # `["unknown"]` -- the previous identity is UNKNOWABLE, a
        # genuinely different condition from "the identity changed", and
        # the old truthiness check stamped `identity_changed: True` on
        # that ledger row too, sending a reader chasing a container swap
        # that never happened. `worst_reason` below still surfaces
        # `unknown` as the scalar `reason` either way.
        run["extra"]["identity_changed"] = True

    client = _ingest_client.IngestClient(
        base_url,
        os.environ.get("NEXUS_API_TOKEN", ""),
        _identity.user_id(cwd),
        _identity.container_id(),
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
    # Persist the NEW container_id for the next run's drift check only when
    # this run's write actually finished (ruling 2, R1-c04): a round-abort
    # outcome (500, 403 ingest_disabled, 429, a client-side timeout) means
    # we do not know whether the server ever saw the new identity, and
    # persisting anyway would make the drift silently unrecoverable -- the
    # next run's `previous` would already read the new value, so a write
    # that in fact never got through is never retried under the old id
    # either. `main()` ALSO gates the actual persist on whether the worker
    # thread was abandoned (`left_behind`): a thread stuck inside `upsert`
    # itself never reaches this line at all, but that gate does not rely on
    # this line's placement to hold.
    run["persist_container_id"] = not outcome.aborts_round
    run["extra"].update(
        # external_id is already in `extra` (set as soon as it was chosen,
        # above) -- not repeated here.
        action=outcome.action,
        redacted=outcome.redacted,
        dedup_merged=outcome.dedup_merged,
        status=outcome.status,
        detail=_short(outcome.detail),
    )
    return _hook_state.worst_reason([*drift_reasons, *outcome.reasons])


def _record(reason, started, run, work_left_behind):
    """Append this run to the ledger, within a budget. Never raises.
    Returns True when the write had to be left behind.

    Mirrors the other two SessionEnd hooks' ``_record``: the write happens
    on a daemon thread abandoned after ``_LEDGER_BUDGET_SECONDS`` (the
    ledger takes a blocking lock, and a stall must not keep this process
    alive past the host's own timeout). One addition here: when the write
    path was reached AND actually finished (``persist``, see below), the
    same budgeted write also persists the new ``container_id`` for the NEXT
    run's drift check -- identical in shape to session_inject.py persisting
    its ``reported`` markers alongside its ledger write.

    ``work_left_behind`` is ``main()``'s own ``left_behind`` for the WORK
    thread (not this function's ledger-write one): ruling 2 (R1-c04) gates
    the container_id persist on the work having actually finished, not just
    on ``run["persist_container_id"]`` -- a thread abandoned mid-``upsert``
    must not have its guess about the outcome trusted.

    ``record_run`` happens FIRST, the persist attempt only AFTER (R2-c01,
    reversing an earlier revision that had it the other way around): both
    steps share this one budgeted ``write()``, and ``write_with_budget``
    abandons whichever step is still running at ``_LEDGER_BUDGET_SECONDS``
    -- the thread is simply left behind, not killed, but the process this
    runs in DOES get killed shortly after, via ``_hook_runner.finish``'s
    ``os._exit(0)`` when a thread was left behind (see ``__main__`` below).
    Persisting first meant a persist slow or contended enough to eat the
    whole budget could commit the new container_id to disk and STILL run
    out of time before record_run ever ran -- os._exit then erasing the row
    for good, not just the persist half of it, because the next run's drift
    check reads the already-updated state and finds nothing changed. With
    record_run first, the row that says what THIS run did is what has to
    survive a slow persist, not the other way around; a persist abandoned
    after it merely means the NEXT run's drift check still sees the old id
    and reports the (by then already known) drift again -- a duplicate,
    not a silent loss.

    A persist failure discovered this way -- after the run's own row is
    already on disk -- can no longer be folded into that row's `reason`
    (ruling 2's literal "must not vanish" used to mean exactly that merge).
    Rewriting the row already appended would need a new ``_hook_state``
    primitive this fix does not add; a second, independent
    ``state_write_failed`` row is what stays inside the existing public
    surface (Amendment A9 -- the exact shape of "must not vanish" is an
    owner question, not decided here) -- but ONLY when the persist
    genuinely failed (R3-c01, see ``write`` below): ``update_state``'s own
    ``reasons`` list is not a pass/fail flag by itself. A degraded lock
    (``lock_unavailable``) or a corrupt state file it just repaired
    (``unknown``) both still finish the write -- ``_hook_state._locked``
    degrades to writing UNLOCKED rather than dropping the write, and a
    corrupt ``read_state`` rebuilds from empty rather than aborting -- so
    treating ANY non-empty ``reasons`` as failure appended a SPURIOUS
    ``state_write_failed`` row even when the write landed, and since
    ``session_inject._failure_report`` only reads the ledger's LAST row,
    that spurious row buried whatever this run's real, correctly-recorded
    reason was (``identity_changed``, ``dedup_merged``) behind a failure
    that never happened.
    """
    elapsed_ms = int((time.monotonic() - started) * 1000)
    # R1-c09: a work thread abandoned mid-`upsert` never reaches the line
    # that assigns `run["calls"]`, so it is still sitting at its initial 0 --
    # which reads as "definitely made no requests" when the truth is "we do
    # not know". `None` says that honestly instead of a false 0.
    calls = None if work_left_behind else run["calls"]
    cwd = run["cwd"]
    extra, persist = dict(run["extra"]), run["persist_container_id"] and not work_left_behind

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
        if not persist:
            return
        try:
            # R4-c05: computed ONCE, here, rather than calling
            # _identity.container_id() again below purely to VERIFY what
            # this same value already told the mutate lambda to write.
            # container_id() only reads an env var or falls back to
            # socket.gethostname(), so a failure here is rare -- but the
            # old code's SECOND, unprotected call sat right after
            # update_state's own try/except, with nothing to catch it: a
            # transient failure there escaped write() entirely, taking the
            # whole "did the persist really land" check -- and the
            # state_write_failed row a genuine persist failure is supposed
            # to guarantee -- down with it. Treated exactly like
            # update_state's own failure below: nothing could be persisted,
            # full stop.
            new_container_id = _identity.container_id()
        except Exception as exc:  # noqa: BLE001 - the net under it; see above
            _warn(f"[{HOOK}] could not determine container_id to persist: {exc!r}")
            new_container_id = None
            _new_state, persist_reasons = {}, ["state_write_failed"]
        else:
            try:
                _new_state, persist_reasons = _hook_state.update_state(
                    HOOK,
                    cwd or os.getcwd(),
                    lambda s: {**s, "container_id": new_container_id},
                )
            except Exception as exc:  # noqa: BLE001 - update_state does not raise by contract; the net under it
                _warn(f"[{HOOK}] could not persist container_id: {exc!r}")
                _new_state, persist_reasons = {}, ["state_write_failed"]
        if new_container_id is not None and _new_state.get("container_id") == new_container_id:
            # The write landed on disk with the identity this mutate always
            # sets, whatever `persist_reasons` says about HOW (R3-c01) --
            # see the docstring above. Trust disk state over the reasons
            # list. The `is not None` guard matters only for the branch
            # right above, where there is nothing trustworthy to compare
            # against at all: without it, an empty `_new_state` reading
            # back `None` for a MISSING key would false-positive against a
            # `new_container_id` that is ALSO `None`, and this would
            # wrongly return early instead of falling through to append
            # the failure row below.
            return
        if "state_write_failed" not in persist_reasons:
            # A degraded lock or a repaired-corrupt-state whose OWN write
            # this call cannot otherwise confirm is not this caller's
            # failure to report (R3-c01): only a genuine
            # `state_write_failed` means persisting itself is what broke.
            return
        # The row above already recorded this run's true reason; a persist
        # failure found only now is a second, independent fact, not a
        # correction of it -- see the "state_write_failed" paragraph above.
        try:
            _hook_state.record_run(
                HOOK,
                ok=False,
                reason="state_write_failed",
                elapsed_ms=0,
                calls=None,
                cwd=cwd,
                extra={"detail": _short(f"container_id persist failed after this run ({reason})")},
            )
        except Exception as exc:  # record_run does not raise by contract; the net under it
            _warn(f"[{HOOK}] could not record state_write_failed: {exc!r}")

    left_behind = _hook_runner.write_with_budget(write, _LEDGER_BUDGET_SECONDS, f"{HOOK}-ledger")
    if left_behind:
        _warn(
            f"[{HOOK}] ledger write still running after {_LEDGER_BUDGET_SECONDS}s; "
            f"leaving it behind, this run ({reason}) may go unrecorded"
        )
    return left_behind


class _StderrGuard:
    """Wraps ``sys.stderr`` so that ANY later write to it cannot raise out
    into its caller (R4-c06).

    ``_warn`` above already protects every stderr write THIS file makes.
    But the work thread ``main()`` starts below calls into
    ``_ingest_client``, which has SEVERAL of its own unguarded
    ``print(..., file=sys.stderr)`` calls (a full lookup page, an
    empty-content caller bug, a missing ``session_id``) -- a closed pipe
    there raises ``BrokenPipeError`` straight out of ``_lookup``/``upsert``,
    which ``_hook_state.reason_for_exception`` reads as ``http_error`` one
    layer up (``BrokenPipeError`` is a ``ConnectionError`` subclass) --
    misreporting a purely LOCAL "could not write a diagnostic" condition
    as a network failure, and silently skipping whatever dedup/write that
    perfectly good response called for. Ruling 15 forbids fixing this
    inside ``_ingest_client.py`` itself, so this wraps the GLOBAL
    ``sys.stderr`` object instead: every module that does
    ``print(..., file=sys.stderr)`` looks up ``sys.stderr`` fresh at call
    time, so replacing the attribute here protects writes from ANY module,
    not just this file's own.

    A write failing here is swallowed and, on the FIRST such failure, the
    underlying file descriptor is redirected to ``os.devnull`` via
    ``_silence_stderr`` -- the same dance ``_warn`` already does for its
    own writes (R3-c03) -- so CPython's own unconditional reflush at
    shutdown lands on a descriptor that accepts anything, instead of
    retrying the exact same failed bytes against the exact same closed
    pipe a second time with no Python-level ``except`` anywhere near it
    (exit 120, R4-c04).

    ``real`` may itself be ``None`` (R4-c07): ``sys.stderr`` -- what this
    wraps -- is ``None``, never a stream, when fd 2 was already closed
    BEFORE the interpreter even started. Calling ``.write``/``.flush`` on
    ``None`` would raise ``AttributeError``, which the ``except OSError``
    below does NOT catch -- that exception would escape this wrapper (and,
    at interpreter shutdown, is exactly as fatal as the closed-pipe
    ``OSError`` this class otherwise protects against). There is no real
    file descriptor behind a ``None`` stream to redirect either, so both
    methods simply no-op in that case, same as after ``_silence_stderr``
    has already run once.
    """

    def __init__(self, real):
        self._real = real

    def write(self, s):
        if self._real is None:
            return len(s)
        try:
            return self._real.write(s)
        except OSError:
            _silence_stderr()
            return len(s)

    def flush(self):
        if self._real is None:
            return
        try:
            self._real.flush()
        except OSError:
            _silence_stderr()

    def fileno(self):
        return self._real.fileno()

    def __getattr__(self, name):
        return getattr(self._real, name)


def main():
    """Run the hook. Returns True when a worker thread had to be left behind."""
    started = time.monotonic()
    if not isinstance(sys.stderr, _StderrGuard):
        # Installed before the work thread starts (R4-c06): _collect, on
        # that thread, calls into _ingest_client, whose own stderr writes
        # this file does not own (ruling 15) but must still not let crash
        # the run. Guarded against double-wrapping across repeated
        # in-process main() calls within the same test process (production
        # runs this once per process, so it never matters there).
        sys.stderr = _StderrGuard(sys.stderr)
    run = {
        "cwd": None,
        "calls": 0,
        "extra": {},
        "persist_container_id": False,
        # An absolute time.monotonic() value, about _DEADLINE_SLACK_SECONDS
        # before the work budget itself expires: IngestClient refuses a
        # request it could not finish in time rather than this whole
        # worker thread being abandoned mid-call with nothing recorded.
        "deadline": started + _WORK_BUDGET_SECONDS - _DEADLINE_SLACK_SECONDS,
    }

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
