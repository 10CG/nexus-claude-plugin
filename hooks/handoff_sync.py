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

try:
    import _identity
except Exception as exc:  # a broken or partial install
    # Imported by a test this must stay loud. Run as a hook it must not be:
    # a traceback is exit 1, reported as a hook error on every session end.
    if __name__ != "__main__":
        raise
    print(f"[handoff-sync] cannot import _identity: {exc!r}", file=sys.stderr)
    sys.exit(0)

try:
    import _hook_runner
except Exception as exc:  # a broken or partial install
    if __name__ != "__main__":
        raise
    print(f"[handoff-sync] cannot import _hook_runner: {exc!r}", file=sys.stderr)
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
    print(f"[handoff-sync] cannot import _ingest_client: {exc!r}", file=sys.stderr)
    sys.exit(0)

import _hook_state  # noqa: E402 - guaranteed importable: _ingest_client already imports it

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
# of this comment claimed it was): two 5 s git calls (project_root's
# memoised one, and _current_branch's own, separate, un-memoised one) plus
# up to three 8 s HTTP calls (lookup + POST/PATCH + dedup DELETE) add up to
# ~29 s on paper, more than this budget. What actually keeps a slow run
# inside it is IngestClient's own ``deadline`` (derived from this same
# budget, see main): it refuses a call it could not finish rather than let
# the wall clock run out from under it. This number only has to clear
# ordinary latency plus that refusal path, and, with the ledger budget,
# stay under the host's timeout.
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
_FRONTMATTER_PROBE_BYTES = 4096
# Read caps (R1-c13): a regular file this small is already generous for any
# real handoff (docs/handoff/*.md run a few KB); the cap exists so a FIFO or
# a device node masquerading as a *.md file cannot block a read indefinitely
# or exhaust memory, not because any real document is expected to hit it.
_MAX_DOCUMENT_BYTES = 1_048_576

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
    """Regular ``*.md`` files directly in ``handoff_dir`` (non-recursive),
    excluding ``latest.md`` and ``README.md`` (case-insensitively -- ruling
    6). ``[]`` when the directory itself does not exist -- the common case,
    most projects keep no handoffs, and must stay quiet (``no_handoff``).

    Any OTHER ``OSError`` (permission denied, ``ELOOP``, a stale NFS handle)
    is RE-RAISED rather than folded into that same ``[]``: ruling 5 (R1-c06)
    -- "cannot tell" is not "no handoff", and reading the two alike is how a
    broken/unreadable directory goes quiet forever. ``_locate`` below turns
    the re-raised error into ``pointer_unresolved`` with a stderr line
    naming it, instead of the silent skip this used to be.
    """
    try:
        names = os.listdir(handoff_dir)
    except (FileNotFoundError, NotADirectoryError):
        return []
    out = []
    for name in names:
        if name.upper() in _HANDOFF_EXCLUDED_NAMES_UPPER or not name.endswith(".md"):
            continue
        if os.path.isfile(os.path.join(handoff_dir, name)):
            out.append(name)
    return sorted(out)


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
    capped (``_MAX_DOCUMENT_BYTES``) for the same reason a regular file that
    is merely huge must not be read in full just to find one pointer line.
    """
    path = os.path.join(handoff_dir, _LATEST_MD)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(_MAX_DOCUMENT_BYTES)
    except OSError:
        return None
    match = _LATEST_POINTER_RE.search(text)
    if not match:
        return None
    target = match.group(1).strip()
    return os.path.basename(target) if target else None


def _probe_frontmatter(path):
    """Just enough of a candidate's head to read its frontmatter, for the
    fallback scan below (which reads every candidate)."""
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(_FRONTMATTER_PROBE_BYTES)
    except OSError:
        return {}
    frontmatter, _body = _split_frontmatter(text)
    return frontmatter


def _locate(handoff_dir):
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
    reason rather than inventing a new one.
    """
    try:
        candidates = _candidates(handoff_dir)
    except OSError as exc:
        print(f"[{HOOK}] docs/handoff exists but could not be listed: {exc!r}", file=sys.stderr)
        return None, "pointer_unresolved"
    if not candidates:
        return None, "no_handoff"
    target = _pointer_target(handoff_dir)
    if target in candidates:  # None never matches a real filename
        return target, None
    newest_name, newest_ts = None, None
    for name in candidates:
        ts = _parse_instant(_probe_frontmatter(os.path.join(handoff_dir, name)).get("updated-at"))
        if ts is not None and (newest_ts is None or ts > newest_ts):
            newest_name, newest_ts = name, ts
    if newest_name:
        return newest_name, None
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


def _parse_instant(value):
    """An aware ``datetime`` from an ISO-8601 string, else ``None``. Accepts
    the trailing ``Z`` frontmatter uses and a naive value (taken as UTC)."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _opted_out(frontmatter):
    """True when ``nexus-ingest`` selects the opt-out (R1-c05, TASK-005 R1
    fix round). Normalises case and strips a trailing inline ``# reason``
    comment before comparing -- this frontmatter dialect is flat
    ``key: string`` only (session-handoff.md §2.3.8.3; Aria's own simple
    parser keeps an inline comment verbatim on the value exactly like
    ``_split_frontmatter`` above does), so ``Skip`` / ``SKIP`` / ``skip  #
    has a secret in §6`` were all being read as "not skip" and ingested
    instead of honouring the recovery path (``docs/architecture/memory-
    layers.md`` §3.2) it exists for. Only the literal ``skip`` is
    recognised once normalised: ``false`` / ``no`` / ``off`` are left
    ingesting, an open question for Amendment A9, not a guess made here.
    """
    value = frontmatter.get("nexus-ingest")
    if not isinstance(value, str):
        return False
    value = re.split(r"\s+#", value, maxsplit=1)[0].strip().lower()
    return value == "skip"


# ── content: H1 -> §6 -> §2, capped ─────────────────────────────────────

def _extract_h1(lines):
    for line in lines:
        if line.startswith("# "):
            return line.strip()
    return ""


def _extract_section(lines, start_re):
    """The heading line matching ``start_re`` through the line before the
    next ``## `` heading (subsections included), or ``""`` when the section
    is not present at all -- OR present only as a bare heading with nothing
    but blank lines under it (ruling 3, TASK-005 R1 fix round, R1-c01): a
    freshly created handoff whose §6/§2 still just carry the template
    heading is not "content", and must not be read as one. Subsection
    headings and any other non-blank line under the heading still count.
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
    if not any(line.strip() for line in lines[start + 1 : end]):
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
    """
    if len(text) <= limit:
        return text
    if limit <= len(_TRUNCATION_MARKER):
        return _TRUNCATION_MARKER[: max(limit, 0)]
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

    name, locate_reason = _locate(handoff_dir)
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
            text = fh.read(_MAX_DOCUMENT_BYTES)
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

    # ---- write path: owner + content both passed ----
    metadata = _build_metadata(session_id, _current_branch(cwd), frontmatter, doc_uuid)

    state, _state_reasons = _hook_state.read_state(HOOK, cwd)
    drift_reasons = _hook_state.identity_drift(
        previous=state.get("container_id"),
        current=_identity.container_id(),
        state_existed=_hook_state.state_exists(HOOK, cwd),
    )
    if drift_reasons:
        # Recorded independently of the scalar `reason` this run ends on
        # (ruling 2, R1-c04): worst_reason can have a higher-priority
        # failure such as http_error outrank identity_changed there, and
        # without this separate flag the drift signal simply disappears
        # from the ledger for that run -- exactly the finding's complaint.
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

    The persist attempt runs BEFORE ``record_run``, and its own failure
    reason (``state_write_failed``) is folded into what gets recorded
    (ruling 2): the two are separate files, but the ledger is the only
    channel a user ever sees, so a failure to persist container_id must
    show up on the very row that would otherwise misreport this run as the
    clean one that resolved the drift.
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
        final_reason = reason
        if persist:
            try:
                _new_state, persist_reasons = _hook_state.update_state(
                    HOOK,
                    cwd or os.getcwd(),
                    lambda s: {**s, "container_id": _identity.container_id()},
                )
            except Exception as exc:  # noqa: BLE001 - update_state does not raise by contract; the net under it
                print(f"[{HOOK}] could not persist container_id: {exc!r}", file=sys.stderr)
                persist_reasons = ["state_write_failed"]
            if persist_reasons:
                final_reason = _hook_state.worst_reason([final_reason, *persist_reasons])
        try:
            _hook_state.record_run(
                HOOK,
                ok=not _hook_state.is_failure_reason(final_reason),
                reason=final_reason,
                elapsed_ms=elapsed_ms,
                calls=calls,
                cwd=cwd,
                extra=extra or None,
            )
        except Exception as exc:  # record_run does not raise by contract; the net under it
            print(f"[{HOOK}] could not record this run ({final_reason}): {exc!r}", file=sys.stderr)

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

    # _collect itself never calls print(): the WORK stays off stdio so an
    # abandoned thread cannot interleave with anything main() writes after
    # giving up on it. Not quite absolute, though (R1-c35): the client it
    # calls into (_ingest_client) still writes a diagnostic line to stderr
    # on a couple of its own paths (a dedup page that filled up, a body
    # that could not be serialised) -- the same "spooky abandoned thread"
    # risk _hook_runner's own docstring already names for stderr, not
    # something this hook adds beyond it.
    outcome, left_behind = _hook_runner.run_with_deadline(
        lambda: _collect(run), _WORK_BUDGET_SECONDS, f"{HOOK}-work"
    )

    reason = "unknown"
    if left_behind:
        reason = "timeout"
        print(
            f"[{HOOK}] {reason}: no result after {_WORK_BUDGET_SECONDS}s; leaving the work behind",
            file=sys.stderr,
        )
    elif "result" not in outcome:
        exc = outcome.get("error", RuntimeError("the worker ended without a result"))
        reason = _hook_state.reason_for_exception(exc)
        print(f"[{HOOK}] {reason}: {exc!r}", file=sys.stderr)
    else:
        reason = outcome["result"]
    return _record(reason, started, run, left_behind) or left_behind


if __name__ == "__main__":
    left_behind = False
    try:
        left_behind = bool(main())
    except Exception:
        # FAIL-OPEN: ANY failure (config, network, timeout, parse, git) -> exit 0
        # with no stdout. Never block session teardown over handoff ingestion.
        pass
    _hook_runner.finish(left_behind)
