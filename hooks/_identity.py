"""Identity primitives shared by every nexus hook.

Two different identities are in play and they are NOT interchangeable:

``container_id``
    Who is writing, from Nexus's point of view. ``NEXUS_CONTAINER_ID`` else the
    hostname — the same derivation ``session_capture.py`` already uses, so the
    observation rows it writes, the aggregator's tagging, and the read side's
    grouping all agree. Diverge here and the injection recipe splits one
    container into two groups.

``aria_uuid``
    Who wrote a handoff document, from Aria's point of view: the ``uuid:`` line
    of ``~/.aria/container-id``. Only used to answer "did *this* container write
    this handoff", because both containers pull the same files and only the
    author may update its row.

    ⚠️ Read the **uuid line**, never a label-over-uuid fallback. Aria's own
    ``get_container_id()`` prefers ``label`` when it is non-empty, and that
    file's header warns that a non-empty label silently changes the machine's
    coordination identity. Handoff frontmatter pins the uuid, so uuid is what
    has to match. If someone "aligns" this with Aria's helper, owner checks
    start failing open on any machine that sets a label.

``plugin_version`` / ``source_header``
    Which release is writing: the ``X-Nexus-Source: <hook-name>/<version>``
    header every remote call carries. The *name* half is what the backend
    attributes by, so it is chosen by each hook and must stay on the backend's
    allowlist; the version half comes from the plugin manifest so there is one
    number to bump, not one per hook.

``project_slug`` / ``user_id``
    Both derived from the git toplevel, deliberately from the same source: the
    ledger/state directory is keyed by project and the server rows are keyed by
    user_id, and the orphan reconciliation in the memory-sync hook compares the
    two. If they could disagree — say, by deriving one from cwd and the other
    from the toplevel — running a hook from a subdirectory would list zero
    local files against a full set of server rows and read that as "everything
    was deleted".

``current_branch``
    The provenance branch tag on every observation / episode row, and half of
    D's per-container episode grouping key (``container_id``, ``branch``).
    session_capture.py and session_inject.py each carried a byte-identical
    copy until TASK-005 (the branch half of the TASK-002 project_slug
    consolidation) — a divergence here would silently split one branch into
    two groups the same way a diverging project_slug used to split one
    project.
"""

import json
import os
import re
import socket
import subprocess

# Aria uuids are hex fragments (8 chars today); full uuids are accepted so a
# future widening does not read as a format violation. Hostnames and labels
# are not -- which is the entire point, see owner_container_uuid.
# The two shapes aria emits: an 8-hex fragment (today) or a full uuid. NOT a
# general "8 to 32 hex chars" -- that also accepts `deadbeef`, `cafebabe` and
# docker's 12-hex default hostname, each of which would sail through as a
# comparable identity and land back in the silent `not_owner` case.
_UUID_SHAPED = re.compile(
    r"^[0-9a-f]{8}$|^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$"
)

ARIA_CONTAINER_ID_FILE = "~/.aria/container-id"

PLUGIN_MANIFEST = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".claude-plugin", "plugin.json"
)

# What a version may look like once it is inside an HTTP header. Deliberately
# narrow: urllib raises on a header value containing a newline, and that would
# fail the remote call -- and the whole run -- over a cosmetic field. `/` is
# excluded because the backend splits the header on the first one.
#
# No anchors, and matched with fullmatch: the first version was `^...$`, and
# `$` also matches just before a trailing newline -- so "0.5.0\n" passed the
# check that exists to keep newlines out.
_VERSION_SHAPED = re.compile(r"[0-9A-Za-z][0-9A-Za-z.+-]{0,31}")


def normalize_slug(text):
    """Normalize a path basename into a project slug (lowercase, safe chars)."""
    slug = "".join(c if (c.isalnum() or c in "-_") else "-" for c in text.strip().lower())
    slug = slug.strip("-")
    return slug or "default"


# One git call per process per cwd. A SessionStart with one failed ledger to
# report was measured at 12 git subprocesses (11 of them `rev-parse
# --show-toplevel`): every ledger path, state path and the user_id each asked
# again. Normally that is tens of milliseconds, but each call carries a 5 s
# timeout, so a git that hangs (NFS, an index lock) could eat the hook's whole
# 25 s deadline. Caching also keeps one run internally consistent: two calls
# that disagreed (git timing out once) used to overwrite the ledger history
# (Amendment A5-5). Amendment A6-4.
#
# Holds the RAW answer -- (toplevel-or-None, degraded) -- not a derived slug:
# project_identity's slug and project_root's path (TASK-005, change 2) are two
# different projections of the same one git call, and caching the raw pair
# lets both derive their answer from it without a second subprocess. A cwd
# that resolves is never absent from the dict (the tuple itself, even
# ``(None, False)``, is not None), so ``.get(cwd) is not None`` still tells
# "already asked" from "never asked" apart.
_SLUG_CACHE = {}  # cwd -> (toplevel_or_None, degraded)


def _resolved_root(cwd):
    """The one ``git rev-parse --show-toplevel`` call this process makes for
    ``cwd``. Shared by project_identity and project_root (TASK-005): a run
    that asks both pays for a single subprocess, not two (Amendment A6-4).
    """
    cached = _SLUG_CACHE.get(cwd)
    if cached is not None:
        return cached
    toplevel = None
    degraded = False
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "--show-toplevel"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            toplevel = result.stdout.decode().strip() or None
        elif b"not a git repository" not in result.stderr.lower():
            # git is there and refused to answer -- `dubious ownership`
            # (a checkout mounted into a container under another uid, this
            # repo's own deployment shape), a corrupt index, permissions.
            # That IS a repository, and the basename is not its name.
            degraded = True
    except Exception:  # timeout, no git binary, a cwd that vanished
        toplevel = None
        degraded = True
    resolved = (toplevel, degraded)
    _SLUG_CACHE[cwd] = resolved
    return resolved


def project_identity(cwd):
    """``(slug, degraded)``: the project slug and whether it is a guess.

    The slug is the git toplevel basename; outside a repository it is the cwd
    basename, and that fallback is fine. ``degraded`` is True when git could
    not be *asked* -- it timed out, is not installed, or refused to answer for
    a directory that is a repository (``dubious ownership``) -- so the
    fallback may name a different project than the one the rows are keyed by. That is not
    fine: ``user_id`` derives from the same call, and one git hiccup would
    file a whole run of writes under a different user_id, invisible to the
    next run and to the orphan reconciliation (which lists rows by the new
    id). A writer must record ``identity_unresolved`` and skip, not guess
    (TASK-010 pre-merge review A8-4). Memoised per process by ``cwd``: one
    git call per run, and one answer per run (Amendment A6-4).
    """
    toplevel, degraded = _resolved_root(cwd)
    base = os.path.basename(toplevel) if toplevel else os.path.basename(cwd.rstrip("/"))
    return normalize_slug(base), degraded


def project_root(cwd):
    """``(toplevel, degraded)``: the git toplevel for ``cwd``, or ``None``.

    TASK-005 (change 2, the handoff-sync hook needs an actual directory to
    list, not just a slug to key rows by). Shares project_identity's single
    memoised git call -- calling both for the same ``cwd`` costs one
    subprocess, not two.

    ``toplevel`` is ``None`` in exactly the two cases project_identity's own
    internal toplevel is: outside a git repository (``degraded`` False, git
    answered and the answer is "no") and git could not be asked at all --
    missing, timed out, or refused (``dubious ownership``) -- (``degraded``
    True). Unlike ``project_slug``, this function does not invent a
    directory for the ``None`` case: the basename fallback belongs to
    project_identity's slug, not to a path callers will pass to ``open()`` or
    ``os.listdir()``. Each caller decides its own fallback (handoff_sync.py
    falls back to ``cwd`` itself either way, so a project outside a
    repository -- or one git could not be asked about -- still gets to look
    for handoff files under its own cwd; only the WRITE is refused when
    ``degraded``, via ``identity_degraded`` on ``IngestClient``).
    """
    return _resolved_root(cwd)


def project_slug(cwd):
    """Derive the project slug: git toplevel basename, else cwd basename.

    The only copy: session_capture.py and session_inject.py each carried a
    byte-identical one until TASK-002, which is two chances for the write side
    and the read side to key the same project differently. See
    ``project_identity`` for the degraded case a writer must check.
    """
    return project_identity(cwd)[0]


def user_id(cwd):
    """The Nexus user_id rows are keyed by — same source as project_slug."""
    return os.environ.get("NEXUS_DEFAULT_USER_ID") or project_slug(cwd)


def current_branch(cwd):
    """The current git branch for ``cwd``, or None.

    None covers three cases callers must not distinguish: not a git
    repository, a detached HEAD (nothing to scope by), and git failing or
    timing out — a hung git (NFS, a stale index lock) must not cost a hook
    more than this call's own 5 s budget. Not memoised like
    ``project_identity``: every existing caller asks once per run already,
    so there is no repeated-call cost here to amortise (contrast
    ``_SLUG_CACHE``'s docstring, where a single ledger-reporting run was
    measured asking 11 times).
    """
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        branch = result.stdout.decode().strip()
        if not branch or branch == "HEAD":  # detached HEAD -> no branch
            return None
        return branch
    except Exception:
        return None


def container_id():
    """Who is writing, for provenance: ``NEXUS_CONTAINER_ID`` else the hostname."""
    return os.environ.get("NEXUS_CONTAINER_ID") or socket.gethostname()


def plugin_version():
    """The plugin's version from its manifest, or ``"unknown"``.

    Never raises and never returns something that is unsafe in a header: this
    feeds ``source_header``, and an attribution field must not be able to fail
    the request it is describing.
    """
    try:
        with open(PLUGIN_MANIFEST, encoding="utf-8") as fh:
            version = json.load(fh).get("version")
    except (OSError, ValueError, AttributeError):
        return "unknown"
    if isinstance(version, str) and _VERSION_SHAPED.fullmatch(version):
        return version
    return "unknown"


def source_header(hook_name):
    """``X-Nexus-Source`` value for one hook: ``<hook-name>/<version>``.

    The backend attributes a request by the part before the first ``/``
    (nexus ``mcp_attribution._normalize_source``), against an allowlist. So the
    name must be one the backend knows, and nothing here may add a second
    slash.
    """
    return f"{hook_name}/{plugin_version()}"


def aria_uuid(path=None):
    """The ``uuid:`` line of Aria's container-id file, or None.

    None means "could not determine", which callers must report as
    ``identity_unresolved`` (a failure reason) rather than quietly treating the
    document as somebody else's — see the two-table split in _hook_state.
    """
    target = os.path.expanduser(path or ARIA_CONTAINER_ID_FILE)
    try:
        with open(target, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return None
    for line in lines:
        stripped = line.strip()
        # No comment-skip: a commented-out line still begins with '#', so it
        # cannot match the prefix below. An explicit skip here would be a dead
        # branch that reads as protection.
        if stripped.startswith("uuid:"):
            value = stripped[len("uuid:"):].strip().lower()
            # Lowercased because owner_container_uuid lowercases the other
            # half. Leaving this one as-is revives exactly the C1 failure from
            # the local side: `uuid: BFE8285D` (macOS uuidgen emits uppercase)
            # would never equal the document's lowercased value, the owner
            # check would be merely false, and ingestion would stop under
            # `not_owner` -- an expected skip that is never reported.
            return value or None
    return None


def owner_container_uuid(owner_container):
    """The uuid half of a handoff's ``<owner>/<uuid>`` frontmatter, or None.

    The owner half drifts (this repo has seen the two containers swap it), so
    only the uuid half is comparable, lowercased on both sides.

    Returns None for a value that is not uuid-shaped, and that matters more
    than it looks: 42 of the 96 handoff documents in this repo carry a
    *hostname* there (``simonfish/dev-claude2``) rather than an aria uuid,
    because Aria's own ``get_container_id()`` prefers ``label`` over ``uuid``
    and the note saying "label MUST stay empty on this machine" is a hand-kept
    convention, not an enforced one. Set a label and the closer writes
    hostnames there again.

    Returning the hostname would make the owner comparison merely false, which
    the caller records as ``not_owner`` -- an expected skip, never reported --
    so handoff ingestion would stop dead and say nothing. "Read it, but it is
    not a comparable identity" is a third case and belongs with
    ``identity_unresolved``, which is reported. (TASK-001 pre-merge audit C1.)
    """
    if not owner_container:
        return None
    value = str(owner_container).strip()
    if not value:
        return None
    candidate = value.rsplit("/", 1)[-1].strip().lower()
    if not candidate or not _UUID_SHAPED.match(candidate):
        return None
    return candidate
