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
import socket
import subprocess

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
            value = stripped[len("uuid:"):].strip()
            return value or None
    return None


def owner_container_uuid(owner_container):
    """Extract the uuid half of a handoff's ``<owner>/<uuid>`` frontmatter value.

    The owner half drifts (this repo has seen the two containers swap it), so
    only the uuid half is comparable. A bare value with no slash is treated as
    the uuid, which is what single-segment legacy documents carry.
    """
    if not owner_container:
        return None
    value = str(owner_container).strip()
    if not value:
        return None
    return value.rsplit("/", 1)[-1].strip() or None
