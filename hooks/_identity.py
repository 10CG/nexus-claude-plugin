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

``project_slug`` / ``user_id``
    Both derived from the git toplevel, deliberately from the same source: the
    ledger/state directory is keyed by project and the server rows are keyed by
    user_id, and the orphan reconciliation in the memory-sync hook compares the
    two. If they could disagree — say, by deriving one from cwd and the other
    from the toplevel — running a hook from a subdirectory would list zero
    local files against a full set of server rows and read that as "everything
    was deleted".
"""

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


def normalize_slug(text):
    """Normalize a path basename into a project slug (lowercase, safe chars)."""
    slug = "".join(c if (c.isalnum() or c in "-_") else "-" for c in text.strip().lower())
    slug = slug.strip("-")
    return slug or "default"


def project_slug(cwd):
    """Derive the project slug: git toplevel basename, else cwd basename.

    Kept byte-compatible with the copies in session_capture.py and
    session_inject.py (verified identical modulo docstring); those two collapse
    onto this one in TASK-002.
    """
    toplevel = None
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "--show-toplevel"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            toplevel = result.stdout.decode().strip() or None
    except Exception:
        toplevel = None
    base = os.path.basename(toplevel) if toplevel else os.path.basename(cwd.rstrip("/"))
    return normalize_slug(base)


def user_id(cwd):
    """The Nexus user_id rows are keyed by — same source as project_slug."""
    return os.environ.get("NEXUS_DEFAULT_USER_ID") or project_slug(cwd)


def container_id():
    """Who is writing, for provenance. Same derivation as session_capture.py."""
    return os.environ.get("NEXUS_CONTAINER_ID") or socket.gethostname()


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
