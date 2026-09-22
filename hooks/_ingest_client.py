"""Idempotent write client shared by the handoff-sync and memory-sync hooks
(TASK-010, workflows B + C).

B and C run the same protocol against the same backend -- "find the row I
wrote for this document, then create / update / leave it" -- and the one
branch in it that destroys data (dedup, orphan deletes) is exactly where two
copies would drift apart (10CG/nexus#441 is that story). So there is one
implementation, and its two callers only decide *what* to send.

The protocol, per document (contract ``docs/architecture/memory-layers.md``
§3.2 / §3.3 / §6.2):

1. Redact. Every outbound string -- the content and every string inside the
   metadata, ``aria.description`` included -- goes through ``_redact`` first,
   and the ``content_hash`` is taken of what is actually sent. A change to the
   redaction rules therefore changes the hash and re-writes the row, which is
   the behaviour you want from a redactor that just learned a new shape --
   **but only for a document the caller sends again**. memory-sync sends
   nothing once every file is synced and clean, so it has to mark its synced
   files dirty itself when the rules change (Amendment A8-2 ruling); this
   client cannot see documents it is not given.
2. Look up by ``(container_id, external_id)`` on the list endpoint, one page
   of five. Then **verify every returned row**: ``metadata.layer`` /
   ``external_id`` / ``container_id`` must equal what was asked for, byte for
   byte. The list endpoint ignores query keys it does not know (FastAPI
   default), so a renamed filter key returns the newest rows instead of an
   error -- and "first row is my row" would then PATCH a stranger, or POST a
   duplicate on every run. A non-empty page with no verified row is
   ``filter_suspect``: the one branch that writes is the one branch that must
   not fail open.
3. No row: POST. Two or more verified rows (only this client's own race can
   make them): the earliest ``created_at`` is canonical, the rest are soft
   deleted, and ``dedup_merged`` is *returned as a reason*, not just a count
   -- it is in the failure table (Amendment A4-4) and the caller collapses it
   with ``worst_reason`` so that a run that merged duplicates and then found
   nothing to change is reported as ``dedup_merged``, not ``unchanged``.
4. One row: same hash **and** every ``aria.*`` key equal to the stored value
   → ``unchanged``; local timestamp older than the server's → ``stale_local``
   (do not clobber a newer copy from the other container's checkout); else
   PATCH ``content_hash`` + the ``aria.*`` keys, plus ``content`` only when
   the hash changed. Never ``session_id`` / ``branch`` / ``container_id`` /
   ``layer`` / ``external_id``: PATCH metadata is a shallow merge (§4) and
   re-sending the identity keys is how an episode gets moved to another
   session. The ``aria.*`` comparison is what makes a metadata-only edit
   travel: a memory file whose description changed but whose body did not,
   or a handoff flipped from active to done, has the same content hash, and
   judging on the hash alone recorded it as ``unchanged`` -- a skip-class
   reason nobody is shown -- while the server kept the old description, the
   one field the SessionStart injection renders (Amendment A8-1 follow-up).
   Content is left out of a metadata-only PATCH because the backend
   re-embeds whenever a PATCH carries content; ``updated_at`` moves either
   way, so incremental readers see the change. A key the caller stops
   sending is *not* removed (shallow merge keeps omitted keys): a caller
   whose flag can turn off must send it as ``False``, not drop it.

Every 2xx is checked for the shape the API promises (Amendment A5-4): a
proxy's login page is a 200 too. GET must answer a memory list, POST and
PATCH a memory, and DELETE **204** (the route declares it; a 200 with a body
is not the API deleting anything -- the first draft counted such a reply as
"deleted 2 rows, run clean", A8-5). ``403`` with ``detail.error ==
STRUCTURED_INGEST_DISABLED`` is ``ingest_disabled`` (the tenant switch, §3.5;
not retried), ``429`` is ``rate_limited`` (``Retry-After`` kept for the
ledger; not retried), ``422`` is ``rejected_422``; network failures map
through ``_hook_state.reason_for_exception``. Nothing here raises for a
remote condition: every path returns an ``Outcome`` the caller records.

Time is bounded twice. ``timeout`` is urllib's, which is per socket
operation, not per request: a server that drips four bytes every 50 ms kept
a "0.2 s" request open for 52 s. So the client also takes an absolute
``deadline`` (``time.monotonic()`` value) from the hook's own budget: a call
that would start past it is refused as ``timeout`` without a request, the
socket timeout never exceeds what is left, and the body is read in chunks
against the same clock. The body is also capped in size.

Stdlib only. This module imports ``_hook_state`` unconditionally (the reason
tables live there); a platform without ``fcntl`` cannot ingest, which the
hooks report on stderr rather than crash over.
"""

import hashlib
import json
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import _hook_state
import _identity
import _redact

DEFAULT_TIMEOUT_SECONDS = 8.0
LOOKUP_LIMIT = 5
MAX_BODY_BYTES = 4 * 1024 * 1024
STRUCTURED_INGEST_DISABLED = "STRUCTURED_INGEST_DISABLED"

# What a PATCH never carries (see the module docstring, step 4).
IDENTITY_KEYS = ("layer", "session_id", "branch", "container_id", "external_id")
# Metadata keys that DO travel on a PATCH: the source's own descriptive keys
# (the description D renders, the status / phase / updated_at of a handoff).
PATCH_PREFIX = "aria."

# Reasons after which a caller's batch loop should stop for this run and
# resume from the same document next time (workflow C's cursor rule). The
# others -- rejected_422, filter_suspect, unchanged, ... -- are per document.
ROUND_ABORT_REASONS = frozenset({"ingest_disabled", "rate_limited", "http_error", "timeout"})


def content_hash(text):
    """``sha256:<hex>`` of the UTF-8 text. One scheme for both hooks and the
    memory-sync state, so a hash can be compared wherever it is found -- but
    mind what goes in. The server row's ``content_hash`` is of the redacted
    body, i.e. what was sent. memory-sync's dirty check must hash the whole
    file, frontmatter included: hashing the body there would never see an
    edit confined to the frontmatter (the description), and the file would
    never be sent for the metadata comparison in ``upsert`` to catch."""
    return "sha256:" + hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def _as_stored(value):
    """``value`` as the server gives it back: the JSON round trip turns a tuple
    into a list and an int dict key into a string, and comparing the raw value
    with what the server returns would never be equal -- a PATCH on every run.
    A value that is not JSON at all is returned as is; the request that would
    carry it fails on its own."""
    try:
        return json.loads(json.dumps(value))
    except (TypeError, ValueError):
        return value


class Outcome:
    """What one upsert / delete did. ``reason`` is the collapsed ledger reason;
    ``reasons`` keeps every reason the call produced, in order, for callers
    that merge several documents into one ledger entry."""

    def __init__(self):
        self.reasons = []
        self.action = None  # created / updated / unchanged / deleted / None
        self.memory_id = None
        self.calls = 0
        self.redacted = 0
        self.dedup_merged = 0
        self.deleted = 0
        self.status = None  # last HTTP status seen
        self.retry_after = None
        self.detail = None  # last error body / message, for stderr

    @property
    def reason(self):
        return _hook_state.worst_reason(self.reasons)

    @property
    def aborts_round(self):
        return self.reason in ROUND_ABORT_REASONS

    def fail(self, reason, detail=None):
        self.reasons.append(reason)
        if detail is not None:
            self.detail = detail
        return self

    def __repr__(self):
        return (
            f"Outcome(reason={self.reason!r}, action={self.action!r}, calls={self.calls}, "
            f"redacted={self.redacted}, dedup_merged={self.dedup_merged}, status={self.status})"
        )


class _Response:
    def __init__(self, status, headers, raw):
        self.status = status
        self.headers = headers
        self.raw = raw

    def json(self):
        """The parsed body, or ``None`` when it is not JSON."""
        if not self.raw:
            return None
        try:
            return json.loads(self.raw)
        except ValueError:
            return None


class _Oversize(Exception):
    """A body past MAX_BODY_BYTES: not an answer this client will parse."""


def _parse_instant(value):
    """An aware datetime from an ISO-8601 string, else ``None``. Accepts the
    trailing ``Z`` the API emits and naive values (taken as UTC)."""
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


def _read_body(resp, deadline, cap, timeout):
    """Read a response body in chunks against a clock and the size cap.

    ``resp.read()`` would honour the socket timeout per chunk and never the
    total; a dripping server keeps it going indefinitely. Without a caller
    deadline the read is still bounded, by ``timeout`` from its start: the
    first draft bounded it only when a deadline was passed, so the default
    construction kept the unbounded behaviour (review R2-a).
    """
    limit = time.monotonic() + timeout
    if deadline is not None:
        limit = min(limit, deadline)
    chunks = []
    size = 0
    # read1: whatever one recv delivers. read(n) would wait for n bytes or
    # the whole declared Content-Length, and against a dripping server that
    # wait is the very thing the deadline exists to cut.
    read = getattr(resp, "read1", None) or resp.read
    while True:
        if time.monotonic() >= limit:
            raise socket.timeout("deadline reached while reading the body")
        chunk = read(65536)
        if not chunk:
            return b"".join(chunks)
        size += len(chunk)
        if size > cap:
            raise _Oversize(f"body exceeds {cap} bytes")
        chunks.append(chunk)


class IngestClient:
    """One instance per hook run. ``source_name`` is the name half of
    ``X-Nexus-Source`` and must be on the backend's allowlist
    (``mcp_attribution._KNOWN_CLIENTS``) or every request lands under
    ``source=unknown``. ``deadline`` is an absolute ``time.monotonic()`` value
    from the hook's own budget; ``None`` means only the per-call timeout."""

    def __init__(
        self,
        base_url,
        token,
        user_id,
        container_id,
        source_name,
        *,
        timeout=DEFAULT_TIMEOUT_SECONDS,
        bulk=False,
        deadline=None,
        identity_degraded=False,
        opener=None,
        max_body_bytes=MAX_BODY_BYTES,
    ):
        # A None here is not "no filter", it is `container_id=None` on the
        # wire and a row without the key passing verification (A8-7). Loud.
        for name, value in (("user_id", user_id), ("container_id", container_id), ("source_name", source_name)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string, got {value!r}")
        self.base_url = (base_url or "").rstrip("/")
        self.token = token or ""
        self.user_id = user_id
        self.container_id = container_id
        self.source_name = source_name
        self.timeout = timeout
        self.bulk = bulk
        self.deadline = deadline
        # `_identity.project_identity(cwd)[1]`: the user_id this client would
        # write under is a guess. Refusing here, in the one module that
        # writes, rather than in each caller's memory (review R2 C).
        self.identity_degraded = bool(identity_degraded)
        self.max_body_bytes = max_body_bytes
        # urllib.request.urlopen unless a test injects something else.
        self._open = opener or urllib.request.urlopen

    # ── HTTP ─────────────────────────────────────────────────────────────

    def remaining(self):
        """Seconds left on the deadline, or ``None`` without one."""
        if self.deadline is None:
            return None
        return self.deadline - time.monotonic()

    def _headers(self, write, has_body):
        headers = {
            # CF 1010 Bot Fight Mode blocks UA-less requests through the proxy.
            "User-Agent": f"nexus-{self.source_name}/{_identity.plugin_version()}",
            "X-Nexus-Source": _identity.source_header(self.source_name),
            "Accept": "application/json",
        }
        if self.token:
            headers["X-API-Key"] = self.token
        if has_body:
            headers["Content-Type"] = "application/json"
        if write and self.bulk:
            # The backend reads `.lower() == "true"` -- the literal word,
            # not `1`, or the write lands in the interactive bucket.
            headers["X-Bulk-Import"] = "true"
        return headers

    def _call(self, outcome, method, path, body=None, query=None):
        """One request. Returns a ``_Response`` for any HTTP status (4xx and
        5xx included -- they are answers, and the caller classifies them), or
        ``None`` after recording the failure reason for a transport error or
        an exhausted deadline (in which case no request is made)."""
        timeout = self.timeout
        left = self.remaining()
        if left is not None:
            if left <= 0:
                outcome.fail("timeout", f"{method} {path}: deadline exhausted before the request")
                return None
            timeout = min(timeout, left)
        url = f"{self.base_url}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        write = method in ("POST", "PATCH", "DELETE")
        req = urllib.request.Request(url, data=data, method=method, headers=self._headers(write, data is not None))
        outcome.calls += 1  # counted before the call: a call that fails was still made
        try:
            with self._open(req, timeout=timeout) as resp:
                raw = _read_body(resp, self.deadline, self.max_body_bytes, timeout)
                headers = {k.lower(): v for k, v in resp.headers.items()}
                response = _Response(resp.status, headers, raw)
        except urllib.error.HTTPError as exc:
            try:
                raw = _read_body(exc, self.deadline, self.max_body_bytes, timeout)
            except Exception:  # noqa: BLE001 - the error body is optional
                raw = b""
            headers = {k.lower(): v for k, v in (exc.headers.items() if exc.headers else [])}
            response = _Response(exc.code, headers, raw)
        except _Oversize as exc:
            outcome.fail("http_error", f"{method} {path}: {exc}")
            return None
        except Exception as exc:  # noqa: BLE001 - every transport failure has a reason
            outcome.fail(_hook_state.reason_for_exception(exc), repr(exc))
            return None
        outcome.status = response.status
        return response

    def _refused(self, outcome, response, what):
        """Classify a non-2xx. Returns True when the caller must stop."""
        status = response.status
        if 200 <= status < 300:
            return False
        body = response.json()
        detail = body.get("detail") if isinstance(body, dict) else None
        if status == 403 and isinstance(detail, dict) and detail.get("error") == STRUCTURED_INGEST_DISABLED:
            outcome.fail("ingest_disabled", f"{what}: {detail.get('reason')}")
        elif status == 429:
            outcome.retry_after = response.headers.get("retry-after")
            outcome.fail("rate_limited", f"{what}: 429, Retry-After={outcome.retry_after}")
        elif status == 422:
            outcome.fail("rejected_422", f"{what}: {body!r}"[:500])
        else:
            outcome.fail("http_error", f"{what}: HTTP {status}"[:500])
        return True

    # ── Protocol ─────────────────────────────────────────────────────────

    def _lookup(self, outcome, layer, external_id):
        """The page of rows for ``(layer, container_id, external_id)``, or
        ``None`` after a failure. Rows come back verified: a page that had rows
        and none of them was ours is ``filter_suspect``."""
        response = self._call(
            outcome,
            "GET",
            "/memories",
            query={
                "user_id": self.user_id,
                "layer": layer,
                "container_id": self.container_id,
                "external_id": external_id,
                "limit": LOOKUP_LIMIT,
            },
        )
        if response is None or self._refused(outcome, response, "lookup"):
            return None
        body = response.json()
        rows = body.get("memories") if isinstance(body, dict) else None
        if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
            outcome.fail("http_error", "lookup: 2xx but not a memory list")
            return None
        verified = [r for r in rows if self._is_ours(r, layer, external_id)]
        if rows and not verified:
            outcome.fail("filter_suspect", f"lookup returned {len(rows)} row(s), none with our keys")
            return None
        if len(rows) >= LOOKUP_LIMIT:
            # More duplicates than one page holds: dedup keeps the earliest
            # of THIS page and converges over runs, but say so (A8-8).
            outcome.detail = f"lookup page full ({len(rows)} rows) for {external_id!r}; dedup continues next run"
            print(f"[{self.source_name}] {outcome.detail}", file=sys.stderr)
        return verified

    def _keys_ok(self, outcome, layer, external_id):
        """`layer` / `external_id` are the lookup keys: a None here is the
        literal string `None` on the wire and a row without the key passing
        verification (review R2-c). A caller bug, loud."""
        for name, value in (("layer", layer), ("external_id", external_id)):
            if not isinstance(value, str) or not value.strip():
                print(f"[{self.source_name}] {name} must be a non-empty string, got {value!r}", file=sys.stderr)
                outcome.fail("unknown", f"{name}={value!r}")
                return False
        return True

    def _is_ours(self, row, layer, external_id):
        meta = row.get("metadata")
        if not isinstance(meta, dict) or not isinstance(row.get("memory_id"), str):
            return False
        # Strings only: a row with no container_id must not match a client
        # whose container_id is somehow None (the constructor refuses that,
        # this is the second lock).
        return (
            isinstance(meta.get("layer"), str)
            and isinstance(meta.get("external_id"), str)
            and isinstance(meta.get("container_id"), str)
            and meta["layer"] == layer
            and meta["external_id"] == external_id
            and meta["container_id"] == self.container_id
        )

    def _delete_row(self, outcome, memory_id, what):
        """Soft-delete one row. Only the route's own answer counts: 204, or
        404 for a row that is already gone. A 200 with a body is somebody
        else's page, not a deletion (A8-5)."""
        response = self._call(outcome, "DELETE", "/memories/" + urllib.parse.quote(memory_id, safe=""))
        if response is None:
            return False
        if response.status in (204, 404):
            return True
        if 200 <= response.status < 300:
            outcome.fail("http_error", f"{what}: HTTP {response.status} with a body is not the API's 204")
            return False
        return not self._refused(outcome, response, what)

    def _dedup(self, outcome, rows):
        """Keep the earliest row, delete the rest. Returns the canonical row,
        or ``None`` when a delete failed (the rest is retried next run)."""
        ordered = sorted(
            rows,
            key=lambda r: (
                _parse_instant(r.get("created_at")) or datetime.max.replace(tzinfo=timezone.utc),
                str(r.get("id", "")),
            ),
        )
        canonical, extras = ordered[0], ordered[1:]
        for row in extras:
            if not self._delete_row(outcome, row["memory_id"], "dedup delete"):
                return None
            outcome.dedup_merged += 1
            if outcome.dedup_merged == 1:
                # A destructive action, and evidence the idempotency protocol
                # lost a race once: in the failure table, so it is reported
                # (A4-4). Recorded at the first deletion, so a later failure
                # in the same loop does not erase it from `reasons` (A8-6).
                outcome.fail("dedup_merged")
        return canonical

    def upsert(self, layer, external_id, content, metadata, *, local_updated_at=None, updated_key="aria.updated_at"):
        """Create or update the row for ``external_id``. Returns an ``Outcome``.

        ``metadata`` is the full metadata for a POST (the identity keys are set
        here from the arguments; ``content_hash`` is computed here). On a PATCH
        only ``content_hash`` and the ``aria.*`` keys are sent, with
        ``content`` added only when the hash changed; a row whose hash and
        ``aria.*`` values all match is ``unchanged``.
        ``local_updated_at`` (ISO-8601) is compared with the stored row's
        ``updated_key`` to refuse writing an older local copy over a newer
        server one.
        """
        outcome = Outcome()
        if not self.base_url:
            return outcome.fail("not_configured")
        if self.identity_degraded:
            return outcome.fail("identity_unresolved", "project identity is a guess; not writing under it")
        if not self._keys_ok(outcome, layer, external_id):
            return outcome
        if not isinstance(content, str) or not content.strip():
            # Nothing to write is a caller bug at this level (the hooks decide
            # `empty_sections` before calling); loud, not silent.
            print(f"[{self.source_name}] upsert({external_id!r}): empty content", file=sys.stderr)
            return outcome.fail("unknown", "empty content")
        if layer == "session_summary" and not (isinstance(metadata, dict) and metadata.get("session_id")):
            # §3.2 hard requirement: without it the aggregator never sees the
            # episode and quietly writes a second row beside it.
            print(f"[{self.source_name}] upsert({external_id!r}): session_summary without session_id", file=sys.stderr)
            return outcome.fail("unknown", "session_summary without session_id")

        content, redacted_content = _redact.redact_text(content)
        meta, redacted_meta = _redact.redact_object(dict(metadata or {}))
        outcome.redacted = redacted_content + redacted_meta
        digest = content_hash(content)
        meta.update(
            {
                "layer": layer,
                "external_id": external_id,
                "container_id": self.container_id,
                "content_hash": digest,
            }
        )
        meta.pop("aggregation_hash", None)  # the aggregator's ownership mark: never ours to set (§3.2)

        rows = self._lookup(outcome, layer, external_id)
        if rows is None:
            return outcome

        if not rows:
            response = self._call(
                outcome,
                "POST",
                "/memories",
                body={"user_id": self.user_id, "content": content, "memory_type": "semantic", "metadata": meta},
            )
            if response is None or self._refused(outcome, response, "create"):
                return outcome
            body = response.json()
            if not isinstance(body, dict) or not isinstance(body.get("memory_id"), str):
                return outcome.fail("http_error", "create: 2xx but not a memory")
            outcome.memory_id = body["memory_id"]
            outcome.action = "created"
            return outcome

        row = rows[0] if len(rows) == 1 else self._dedup(outcome, rows)
        if row is None:
            return outcome
        outcome.memory_id = row["memory_id"]
        stored = row.get("metadata") or {}

        source_meta = {k: v for k, v in meta.items() if k.startswith(PATCH_PREFIX)}
        content_changed = stored.get("content_hash") != digest
        # A key the stored row lacks is a change: a source key the caller
        # starts sending has to reach the rows written before it existed.
        if not content_changed and all(k in stored and stored[k] == _as_stored(v) for k, v in source_meta.items()):
            outcome.reasons.append("unchanged")
            outcome.action = "unchanged"
            return outcome

        local_ts, server_ts = _parse_instant(local_updated_at), _parse_instant(stored.get(updated_key))
        if local_ts is not None and server_ts is not None and local_ts < server_ts:
            outcome.action = None
            return outcome.fail("stale_local", f"local {local_updated_at} < server {stored.get(updated_key)}")

        patch = {"metadata": {"content_hash": digest, **source_meta}}
        if content_changed:
            # The backend re-embeds on any PATCH that carries content; a
            # metadata-only edit (step 4) must not pay for that.
            patch["content"] = content
        response = self._call(
            outcome,
            "PATCH",
            "/memories/" + urllib.parse.quote(row["memory_id"], safe=""),
            body=patch,
        )
        if response is None or self._refused(outcome, response, "update"):
            return outcome
        body = response.json()
        if not isinstance(body, dict) or not isinstance(body.get("memory_id"), str):
            return outcome.fail("http_error", "update: 2xx but not a memory")
        outcome.action = "updated"
        return outcome

    def delete(self, layer, external_id):
        """Soft-delete every row this container wrote for ``external_id``.
        Returns an ``Outcome``; ``outcome.deleted`` counts the rows the server
        confirmed gone (204, or 404 for an already-gone row). The
        ``pending_delete`` bookkeeping belongs to the caller's state: this
        only reports whether the server now agrees the rows are gone."""
        outcome = Outcome()
        if not self.base_url:
            return outcome.fail("not_configured")
        if self.identity_degraded:
            return outcome.fail("identity_unresolved", "project identity is a guess; not deleting under it")
        if not self._keys_ok(outcome, layer, external_id):
            return outcome
        rows = self._lookup(outcome, layer, external_id)
        if rows is None:
            return outcome
        if not rows:
            outcome.reasons.append("nothing_to_do")
            return outcome
        for row in rows:
            if not self._delete_row(outcome, row["memory_id"], "delete"):
                return outcome
            outcome.deleted += 1
        outcome.action = "deleted"
        return outcome
