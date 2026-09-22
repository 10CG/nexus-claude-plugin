"""Tests for hooks/_ingest_client.py (TASK-010, the shared idempotent writer).

Runnable as: python3 hooks/test_ingest_client.py   (stdlib unittest only)

Every test talks to a real HTTP server on 127.0.0.1 that plays a script of
canned replies and records what it was sent -- headers, query string, JSON
body -- because the whole subject here is what goes over the wire: which
rows count as ours, what a PATCH carries, what a 403 body has to look like.
Nothing here touches the ledger or the state directory (the client has no
state), and nothing reads the network beyond the loopback.

The fixtures follow the TASK-010 verification list: empty page → POST; page
with no verified row → filter_suspect and no write; one row → unchanged /
stale_local / PATCH; two rows → one DELETE + dedup_merged as the reason;
redaction before send; 403 / 429 / 422 / transport failures; the bulk header.
"""

import http.server
import json
import os
import threading
import time
import unittest
import urllib.parse
from datetime import datetime, timezone
from unittest import mock

import _hook_state
import _identity
import _ingest_client
import _redact

# urllib honours proxy variables, and a developer machine's cross-border proxy
# answers 502 for 127.0.0.1 -- which reads exactly like the backend being down.
_PROXY_VARS = ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY")
_NO_PROXY = {"no_proxy": "127.0.0.1,localhost", "NO_PROXY": "127.0.0.1,localhost"}


def setUpModule():
    env = {k: v for k, v in os.environ.items() if k not in _PROXY_VARS}
    env.update(_NO_PROXY)
    patcher = mock.patch.dict(os.environ, env, clear=True)
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)


class _Backend:
    """A scripted fake of the memory endpoints on a real loopback socket."""

    def __init__(self):
        self.script = []
        self.requests = []
        self.delay = 0.0
        self.drip = None  # (chunk, delay_seconds, chunks): stream the body slowly
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
                if backend.delay:
                    time.sleep(backend.delay)
                if backend.script:
                    status, body, headers = backend.script.pop(0)
                else:
                    status, body, headers = 599, {"detail": "unscripted request"}, {}
                if backend.drip:
                    chunk, pause, count = backend.drip
                    handler.send_response(status)
                    handler.send_header("Content-Type", "application/json")
                    handler.send_header("Content-Length", str(len(chunk) * count))
                    handler.end_headers()
                    try:
                        for _ in range(count):
                            handler.wfile.write(chunk)
                            handler.wfile.flush()
                            time.sleep(pause)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
                payload = body if isinstance(body, bytes) else (b"" if body is None else json.dumps(body).encode("utf-8"))
                handler.send_response(status)
                for key, value in headers.items():
                    handler.send_header(key, value)
                if "content-type" not in {k.lower() for k in headers}:
                    handler.send_header("Content-Type", "application/json")
                handler.send_header("Content-Length", str(len(payload)))
                handler.end_headers()
                if payload:
                    try:
                        handler.wfile.write(payload)
                    except BrokenPipeError:
                        pass  # the client gave up (the timeout test); nothing to report

            do_GET = do_POST = do_PATCH = do_DELETE = _handle

            def log_message(handler, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_address[1]}/v1"

    def reply(self, status, body=None, headers=None):
        self.script.append((status, body, headers or {}))
        return self

    def close(self):
        self.server.shutdown()
        self.server.server_close()


CONTAINER = "dev-box-a"
USER = "nexus"
HOOK = "memory-sync-hook"


def _row(external_id, *, content_hash, layer="fact", container_id=CONTAINER, created_at="2026-09-22T10:00:00.000001Z", row_id="11111111-1111-4111-8111-111111111111", extra=None):
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


class _ClientCase(unittest.TestCase):
    def setUp(self):
        self.backend = _Backend()
        self.addCleanup(self.backend.close)

    def client(self, **kw):
        kw.setdefault("token", "t0k3n-secret-value")
        return _ingest_client.IngestClient(self.backend.url, kw.pop("token"), USER, CONTAINER, HOOK, **kw)

    @property
    def requests(self):
        return self.backend.requests


class TestLookup(_ClientCase):
    def test_query_is_exactly_the_four_keys_and_a_limit_of_five(self):
        self.backend.reply(200, _page()).reply(201, {"memory_id": "t1::nexus::new"})
        self.client().upsert("fact", "slug-a", "content one", {})
        get = self.requests[0]
        self.assertEqual(get["method"], "GET")
        self.assertEqual(get["path"], "/v1/memories")
        self.assertEqual(
            get["query"],
            {"user_id": USER, "layer": "fact", "container_id": CONTAINER, "external_id": "slug-a", "limit": "5"},
        )
        self.assertNotIn("offset", get["query"])

    def test_headers_carry_source_version_key_and_user_agent(self):
        self.backend.reply(200, _page(_row("s", content_hash="x")))
        self.client().upsert("fact", "s", "c", {})
        headers = self.requests[0]["headers"]
        self.assertEqual(headers["x-nexus-source"], _identity.source_header(HOOK))
        self.assertTrue(headers["x-nexus-source"].startswith(HOOK + "/"))
        self.assertEqual(headers["x-api-key"], "t0k3n-secret-value")
        self.assertIn("user-agent", headers)
        self.assertNotIn("x-bulk-import", headers)  # a read never carries it

    def test_no_token_means_no_key_header(self):
        self.backend.reply(200, _page(_row("s", content_hash="x")))
        self.client(token="").upsert("fact", "s", "c", {})
        self.assertNotIn("x-api-key", self.requests[0]["headers"])


class TestCreate(_ClientCase):
    def test_empty_page_posts_the_full_row(self):
        self.backend.reply(200, _page()).reply(201, {"memory_id": "t1::nexus::new", "id": "x"})
        out = self.client().upsert(
            "fact", "slug-a", "the body", {"aria.memory_slug": "slug-a", "aria.description": "d"}
        )
        self.assertEqual(out.reason, _hook_state.NO_REASON)
        self.assertEqual(out.action, "created")
        self.assertEqual(out.memory_id, "t1::nexus::new")
        self.assertEqual(out.calls, 2)
        post = self.requests[1]
        self.assertEqual((post["method"], post["path"]), ("POST", "/v1/memories"))
        body = post["json"]
        self.assertEqual(body["user_id"], USER)
        self.assertEqual(body["content"], "the body")
        self.assertEqual(body["memory_type"], "semantic")
        self.assertEqual(
            body["metadata"],
            {
                "aria.memory_slug": "slug-a",
                "aria.description": "d",
                "layer": "fact",
                "external_id": "slug-a",
                "container_id": CONTAINER,
                "content_hash": _ingest_client.content_hash("the body"),
            },
        )
        self.assertEqual(post["headers"]["content-type"], "application/json")

    def test_aggregation_hash_is_never_sent(self):
        """§3.2: a row carrying it is treated as the aggregator's own and overwritten."""
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m"})
        self.client().upsert("session_summary", "h.md", "c", {"session_id": "s1", "aggregation_hash": "abc"})
        self.assertNotIn("aggregation_hash", self.requests[1]["json"]["metadata"])
        self.assertEqual(self.requests[1]["json"]["metadata"]["session_id"], "s1")

    def test_bulk_header_is_the_literal_true_on_writes_only(self):
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m"})
        self.client(bulk=True).upsert("fact", "s", "c", {})
        self.assertNotIn("x-bulk-import", self.requests[0]["headers"])
        self.assertEqual(self.requests[1]["headers"]["x-bulk-import"], "true")

    def test_without_bulk_the_header_is_absent(self):
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m"})
        self.client().upsert("fact", "s", "c", {})
        self.assertNotIn("x-bulk-import", self.requests[1]["headers"])

    def test_a_2xx_that_is_not_a_memory_is_http_error(self):
        for body in (b"<html>login</html>", {"accepted": 1}, [], {"memory_id": 5}):
            with self.subTest(repr(body)[:20]):
                backend = _Backend()
                self.addCleanup(backend.close)
                backend.reply(200, _page()).reply(200, body)
                out = _ingest_client.IngestClient(backend.url, "", USER, CONTAINER, HOOK).upsert("fact", "s", "c", {})
                self.assertEqual(out.reason, "http_error")
                self.assertIsNone(out.action)


class TestVerification(_ClientCase):
    def test_a_page_with_no_row_of_ours_is_filter_suspect_and_writes_nothing(self):
        """The list endpoint ignores unknown query keys: a renamed filter returns
        the newest rows. Taking the first would PATCH a stranger's row."""
        cases = {
            "other external_id": _row("other-slug", content_hash="x"),
            "other container": _row("s", content_hash="x", container_id="someone-else"),
            "other layer": _row("s", content_hash="x", layer="observation"),
            "no metadata keys": {"memory_id": "t1::u::x", "id": "x", "metadata": {}, "created_at": "2026-01-01T00:00:00Z"},
        }
        for label, row in cases.items():
            with self.subTest(label):
                backend = _Backend()
                self.addCleanup(backend.close)
                backend.reply(200, _page(row))
                out = _ingest_client.IngestClient(backend.url, "", USER, CONTAINER, HOOK).upsert("fact", "s", "c", {})
                self.assertEqual(out.reason, "filter_suspect")
                self.assertEqual(len(backend.requests), 1)
                self.assertIsNone(out.action)

    def test_a_list_that_is_not_a_list_is_http_error(self):
        for body in (b"<html>", {"results": []}, {"memories": "nope"}, {"memories": [1]}):
            with self.subTest(repr(body)[:20]):
                backend = _Backend()
                self.addCleanup(backend.close)
                backend.reply(200, body)
                out = _ingest_client.IngestClient(backend.url, "", USER, CONTAINER, HOOK).upsert("fact", "s", "c", {})
                self.assertEqual(out.reason, "http_error")
                self.assertEqual(len(backend.requests), 1)


class TestOneRow(_ClientCase):
    def test_same_hash_is_unchanged_and_writes_nothing(self):
        digest = _ingest_client.content_hash("same")
        self.backend.reply(200, _page(_row("s", content_hash=digest)))
        out = self.client().upsert("fact", "s", "same", {})
        self.assertEqual((out.reason, out.action, out.calls), ("unchanged", "unchanged", 1))
        self.assertEqual(out.memory_id, "t1::nexus::11111111-1111-4111-8111-111111111111")

    def test_changed_hash_patches_content_and_only_the_source_keys(self):
        self.backend.reply(200, _page(_row("h.md", content_hash="old", layer="session_summary", extra={"aria.updated_at": "2026-09-20T00:00:00Z"})))
        self.backend.reply(200, {"memory_id": "t1::nexus::11111111-1111-4111-8111-111111111111"})
        meta = {
            "session_id": "sess-1",
            "branch": "main",
            "aria.source": "handoff",
            "aria.status": "done",
            "aria.updated_at": "2026-09-22T00:00:00Z",
        }
        out = self.client().upsert(
            "session_summary", "h.md", "new body", meta, local_updated_at="2026-09-22T00:00:00Z"
        )
        self.assertEqual((out.reason, out.action, out.calls), (_hook_state.NO_REASON, "updated", 2))
        patch = self.requests[1]
        self.assertEqual(patch["method"], "PATCH")
        self.assertEqual(patch["path"], "/v1/memories/t1%3A%3Anexus%3A%3A11111111-1111-4111-8111-111111111111")
        self.assertEqual(set(patch["json"]), {"content", "metadata"})
        self.assertEqual(patch["json"]["content"], "new body")
        self.assertEqual(
            patch["json"]["metadata"],
            {
                "content_hash": _ingest_client.content_hash("new body"),
                "aria.source": "handoff",
                "aria.status": "done",
                "aria.updated_at": "2026-09-22T00:00:00Z",
            },
        )
        for key in _ingest_client.IDENTITY_KEYS:
            self.assertNotIn(key, patch["json"]["metadata"])

    def test_an_older_local_copy_is_stale_local_and_writes_nothing(self):
        self.backend.reply(200, _page(_row("h.md", content_hash="old", layer="session_summary", extra={"aria.updated_at": "2026-09-22T12:00:00Z"})))
        out = self.client().upsert(
            "session_summary", "h.md", "older body", {"session_id": "s"}, local_updated_at="2026-09-21T12:00:00Z"
        )
        self.assertEqual((out.reason, out.action, out.calls), ("stale_local", None, 1))

    def test_updated_key_is_configurable_for_memory_files(self):
        self.backend.reply(200, _page(_row("slug", content_hash="old", extra={"aria.modified": "2026-09-22T12:00:00+00:00"})))
        out = self.client().upsert(
            "fact", "slug", "body", {}, local_updated_at="2026-09-22T11:00:00Z", updated_key="aria.modified"
        )
        self.assertEqual(out.reason, "stale_local")

    def test_unparsable_or_missing_timestamps_do_not_block_the_update(self):
        for server_ts, local_ts in (("garbage", "2026-09-22T00:00:00Z"), (None, "2026-09-22T00:00:00Z"), ("2026-09-22T00:00:00Z", None)):
            with self.subTest((server_ts, local_ts)):
                backend = _Backend()
                self.addCleanup(backend.close)
                extra = {"aria.updated_at": server_ts} if server_ts else {}
                backend.reply(200, _page(_row("s", content_hash="old", extra=extra))).reply(200, {"memory_id": "m"})
                out = _ingest_client.IngestClient(backend.url, "", USER, CONTAINER, HOOK).upsert("fact", "s", "c", {}, local_updated_at=local_ts)
                self.assertEqual(out.action, "updated")


class TestMetadataOnlyChange(_ClientCase):
    """A8-1 follow-up (owner ruling 2026-09-22). The hash covers the content
    only, so an edit confined to the ``aria.*`` keys -- a memory file whose
    description changed but whose body did not, a handoff flipped from active
    to done -- came back ``unchanged`` (a skip-class reason, never shown) and
    the server kept the old values. It must PATCH, and without ``content``:
    the backend re-embeds whenever a PATCH carries content."""

    MEMORY_ID = "t1::nexus::11111111-1111-4111-8111-111111111111"

    def _one_row(self, content, *, layer="fact", external_id="slug", extra=None):
        digest = _ingest_client.content_hash(content)
        self.backend.reply(200, _page(_row(external_id, layer=layer, content_hash=digest, extra=extra)))

    def test_a_description_only_edit_patches_the_metadata_without_content(self):
        # aria.memory_slug is equal on both sides: one differing key is enough.
        stored = {"aria.memory_slug": "slug", "aria.description": "old description", "aria.modified": "2026-09-20T00:00:00Z"}
        self._one_row("same body", extra=stored)
        self.backend.reply(200, {"memory_id": self.MEMORY_ID})
        out = self.client().upsert(
            "fact",
            "slug",
            "same body",
            {"aria.memory_slug": "slug", "aria.description": "new description", "aria.modified": "2026-09-22T00:00:00Z"},
            local_updated_at="2026-09-22T00:00:00Z",
            updated_key="aria.modified",
        )
        self.assertEqual((out.reason, out.action, out.calls), (_hook_state.NO_REASON, "updated", 2))
        patch = self.requests[1]
        self.assertEqual(patch["method"], "PATCH")
        self.assertEqual(set(patch["json"]), {"metadata"})  # no content, so no re-embedding
        self.assertEqual(
            patch["json"]["metadata"],
            {
                "content_hash": _ingest_client.content_hash("same body"),
                "aria.memory_slug": "slug",
                "aria.description": "new description",
                "aria.modified": "2026-09-22T00:00:00Z",
            },
        )

    def test_a_handoff_status_flip_patches_the_metadata_without_content(self):
        stored = {"aria.status": "active", "aria.phase": "B", "aria.updated_at": "2026-09-20T00:00:00Z"}
        self._one_row("# H\n## 6\n## 2", layer="session_summary", external_id="docs/handoff/h.md", extra=stored)
        self.backend.reply(200, {"memory_id": self.MEMORY_ID})
        meta = {
            "session_id": "sess-1",
            "branch": "main",
            "aria.status": "done",
            "aria.phase": "session",
            "aria.updated_at": "2026-09-22T00:00:00Z",
        }
        out = self.client().upsert(
            "session_summary", "docs/handoff/h.md", "# H\n## 6\n## 2", meta, local_updated_at="2026-09-22T00:00:00Z"
        )
        self.assertEqual(out.action, "updated")
        patch = self.requests[1]["json"]
        self.assertEqual(set(patch), {"metadata"})
        self.assertEqual((patch["metadata"]["aria.status"], patch["metadata"]["aria.phase"]), ("done", "session"))
        for key in _ingest_client.IDENTITY_KEYS:
            self.assertNotIn(key, patch["metadata"])

    def test_equal_values_of_every_json_type_are_unchanged_and_write_nothing(self):
        """The comparison runs on every sync: a value that stops comparing
        equal after the JSON round trip would PATCH on every run."""
        stored = {
            "aria.description": "d",
            "aria.truncated": True,
            "aria.count": 3,
            "aria.tags": ["a", "b"],
            "aria.nested": {"k": None},
        }
        self._one_row("body", extra=stored)
        out = self.client().upsert("fact", "slug", "body", dict(stored))
        self.assertEqual((out.reason, out.action, out.calls), ("unchanged", "unchanged", 1))

    def test_the_comparison_is_on_the_redacted_value(self):
        """The server holds what was sent, i.e. the redacted form; comparing
        it with the raw local value would differ on every run for any
        description that carries a secret shape."""
        raw = "rotate it: password: hunter22x9"
        sent, hits = _redact.redact_text(raw)
        self.assertEqual(hits, 1)  # the fixture has to exercise the redactor
        self._one_row("body", extra={"aria.description": sent})
        out = self.client().upsert("fact", "slug", "body", {"aria.description": raw})
        self.assertEqual((out.reason, out.calls), ("unchanged", 1))

    def test_a_metadata_only_edit_from_an_older_local_copy_is_stale_local(self):
        self._one_row("body", extra={"aria.description": "newer on server", "aria.modified": "2026-09-22T12:00:00Z"})
        out = self.client().upsert(
            "fact",
            "slug",
            "body",
            {"aria.description": "older local", "aria.modified": "2026-09-21T00:00:00Z"},
            local_updated_at="2026-09-21T00:00:00Z",
            updated_key="aria.modified",
        )
        self.assertEqual((out.reason, out.calls), ("stale_local", 1))

    def test_a_metadata_only_patch_is_redacted_before_send(self):
        """The PATCH body is built from the redacted metadata, as the POST is;
        built from the caller's own dict it would store the secret."""
        raw = "rotate it: password: hunter22x9"
        self._one_row("body", extra={"aria.description": "old description"})
        self.backend.reply(200, {"memory_id": self.MEMORY_ID})
        out = self.client().upsert("fact", "slug", "body", {"aria.description": raw})
        self.assertEqual((out.action, out.redacted), ("updated", 1))
        sent = self.requests[1]["json"]
        self.assertEqual(sent["metadata"]["aria.description"], _redact.redact_text(raw)[0])
        self.assertEqual(_redact.find(json.dumps(sent)), [])

    def test_values_the_json_round_trip_reshapes_still_compare_equal(self):
        """A tuple comes back as a list and an int key as a string; compared
        raw they would differ on every run, and every run would PATCH."""
        self._one_row("body", extra={"aria.tags": ["a", "b"], "aria.by_id": {"1": "x"}})
        out = self.client().upsert("fact", "slug", "body", {"aria.tags": ("a", "b"), "aria.by_id": {1: "x"}})
        self.assertEqual((out.reason, out.calls), ("unchanged", 1))

    def test_equal_timestamps_do_not_make_a_metadata_edit_stale(self):
        """Only an OLDER local copy is stale. A handoff whose status flipped
        without touching updated-at, or a memory file whose description changed
        under an explicit `modified:`, arrives with equal timestamps."""
        ts = "2026-09-22T00:00:00Z"
        self._one_row("body", extra={"aria.description": "old", "aria.modified": ts})
        self.backend.reply(200, {"memory_id": self.MEMORY_ID})
        out = self.client().upsert(
            "fact", "slug", "body", {"aria.description": "new", "aria.modified": ts},
            local_updated_at=ts, updated_key="aria.modified",
        )
        self.assertEqual(out.action, "updated")

    def test_a_key_the_stored_row_lacks_is_a_change(self):
        """A source key the caller starts sending must reach the rows written
        before it existed."""
        self._one_row("body", extra={"aria.description": "d"})
        self.backend.reply(200, {"memory_id": self.MEMORY_ID})
        out = self.client().upsert("fact", "slug", "body", {"aria.description": "d", "aria.origin_session": "s-1"})
        self.assertEqual(out.action, "updated")
        self.assertEqual(self.requests[1]["json"]["metadata"]["aria.origin_session"], "s-1")

    def test_a_key_the_stored_row_lacks_is_a_change_even_when_its_value_is_none(self):
        """The row says nothing about the key; sending null says something.
        Pinned because dropping the membership test would read the two as
        equal, and the row would never learn the key exists."""
        self._one_row("body", extra={"aria.description": "d"})
        self.backend.reply(200, {"memory_id": self.MEMORY_ID})
        out = self.client().upsert("fact", "slug", "body", {"aria.description": "d", "aria.origin_session": None})
        self.assertEqual(out.action, "updated")
        self.assertIn("aria.origin_session", self.requests[1]["json"]["metadata"])

    def test_a_body_the_caller_made_unsendable_is_a_caller_bug_not_an_exception(self):
        """Raising here would be swallowed by the hooks' blanket handler, and
        the server's 500 would be `http_error` -- a round stop that parks the
        caller's cursor on this document for every round after."""
        cases = {
            "not JSON": ("body", {"aria.when": datetime(2026, 9, 22, tzinfo=timezone.utc)}),
            "NaN": ("body", {"aria.score": float("nan")}),
            "NUL in content": ("a\x00b", {}),
            "lone surrogate": ("a\ud800b", {}),
        }
        for label, (content, meta) in cases.items():
            with self.subTest(label):
                backend = _Backend()
                self.addCleanup(backend.close)
                backend.reply(200, _page())  # empty page -> POST attempt
                with mock.patch("sys.stderr"):
                    out = _ingest_client.IngestClient(backend.url, "", USER, CONTAINER, HOOK).upsert(
                        "fact", "s", content, meta
                    )
                self.assertEqual((out.reason, out.action, out.calls), ("unknown", None, 1))
                self.assertFalse(out.aborts_round, "a caller bug is per document, not a round stop")
                self.assertEqual([r["method"] for r in backend.requests], ["GET"])

    def test_a_key_the_caller_stops_sending_is_not_a_change(self):
        """Shallow merge keeps omitted keys, so dropping a flag cannot clear
        it; only the keys the caller sends are compared. Pinned so nobody reads
        the comparison as covering keys that exist only on the server."""
        self._one_row("body", extra={"aria.truncated": True})
        out = self.client().upsert("fact", "slug", "body", {})
        self.assertEqual(out.reason, "unchanged")

    def test_sending_false_for_a_stored_true_patches_it(self):
        """...which is how a caller clears a flag: send it as False."""
        self._one_row("body", extra={"aria.truncated": True})
        self.backend.reply(200, {"memory_id": self.MEMORY_ID})
        out = self.client().upsert("fact", "slug", "body", {"aria.truncated": False})
        self.assertEqual(out.action, "updated")
        self.assertEqual(
            self.requests[1]["json"],
            {"metadata": {"content_hash": _ingest_client.content_hash("body"), "aria.truncated": False}},
        )


class TestDedup(_ClientCase):
    def _two_rows(self, digest):
        older = _row("s", content_hash=digest, created_at="2026-09-20T00:00:00.000001Z", row_id="aaaaaaaa-1111-4111-8111-111111111111")
        newer = _row("s", content_hash=digest, created_at="2026-09-21T00:00:00.000001Z", row_id="bbbbbbbb-1111-4111-8111-111111111111")
        return older, newer

    def test_the_later_row_is_deleted_and_dedup_merged_is_the_reason_even_when_unchanged(self):
        digest = _ingest_client.content_hash("same")
        older, newer = self._two_rows(digest)
        self.backend.reply(200, _page(newer, older)).reply(204, None)
        out = self.client().upsert("fact", "s", "same", {})
        self.assertEqual(out.reasons, ["dedup_merged", "unchanged"])
        self.assertEqual(out.reason, "dedup_merged")  # A4-4: not `unchanged`
        self.assertEqual(out.dedup_merged, 1)
        self.assertEqual(out.action, "unchanged")
        self.assertEqual(out.memory_id, older["memory_id"])
        delete = self.requests[1]
        self.assertEqual(delete["method"], "DELETE")
        self.assertEqual(delete["path"], "/v1/memories/" + urllib.parse.quote(newer["memory_id"], safe=""))
        self.assertEqual(len(self.requests), 2)

    def test_after_dedup_a_changed_content_patches_the_canonical_row(self):
        older, newer = self._two_rows("old")
        self.backend.reply(200, _page(newer, older)).reply(204, None).reply(200, {"memory_id": older["memory_id"]})
        out = self.client().upsert("fact", "s", "changed", {})
        self.assertEqual(out.reason, "dedup_merged")
        self.assertEqual(out.action, "updated")
        self.assertEqual(self.requests[2]["path"], "/v1/memories/" + urllib.parse.quote(older["memory_id"], safe=""))

    def test_a_failed_delete_stops_the_run_with_its_reason(self):
        older, newer = self._two_rows("old")
        self.backend.reply(200, _page(newer, older)).reply(500, {"detail": "boom"})
        out = self.client().upsert("fact", "s", "changed", {})
        self.assertEqual(out.reason, "http_error")
        self.assertEqual(out.dedup_merged, 0)
        self.assertEqual(len(self.requests), 2)  # no PATCH after a failed delete

    def test_a_404_on_delete_counts_as_gone(self):
        older, newer = self._two_rows(_ingest_client.content_hash("same"))
        self.backend.reply(200, _page(newer, older)).reply(404, {"detail": "Memory not found"})
        out = self.client().upsert("fact", "s", "same", {})
        self.assertEqual(out.dedup_merged, 1)
        self.assertEqual(out.reason, "dedup_merged")


class TestRedaction(_ClientCase):
    def test_content_and_every_metadata_string_are_redacted_before_send_and_counted(self):
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m"})
        content = "db: postgresql://nexus:s3cretpw@db:5432/nexus"
        meta = {"aria.description": "key sk-Ab3Cd4Ef5Gh6Jk7Mn8Pq9Rs0Tu1Vw2Xy", "aria.memory_slug": "slug"}
        out = self.client().upsert("fact", "slug", content, meta)
        self.assertEqual(out.redacted, 2)
        body = self.requests[1]["json"]
        self.assertEqual(body["content"], "db: postgresql://nexus:[redacted:url-userinfo]@db:5432/nexus")
        self.assertEqual(body["metadata"]["aria.description"], "key [redacted:openai-key]")
        self.assertNotIn("s3cretpw", json.dumps(body))
        # the hash is of what was sent, so a redaction change re-writes the row
        self.assertEqual(body["metadata"]["content_hash"], _ingest_client.content_hash(body["content"]))
        self.assertEqual(_redact.find(json.dumps(body)), [])

    def test_the_callers_metadata_is_not_mutated(self):
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m"})
        meta = {"aria.description": "password: hunter22x9"}
        self.client().upsert("fact", "slug", "c", meta)
        self.assertEqual(meta, {"aria.description": "password: hunter22x9"})


class TestRefusals(_ClientCase):
    def _upsert_with(self, status, body, headers=None, on="POST"):
        if on == "POST":
            self.backend.reply(200, _page())
        self.backend.reply(status, body, headers)
        return self.client().upsert("fact", "s", "c", {})

    def test_403_with_the_contract_body_is_ingest_disabled(self):
        out = self._upsert_with(403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "tenant off"}})
        self.assertEqual(out.reason, "ingest_disabled")
        self.assertTrue(out.aborts_round)
        self.assertEqual(out.status, 403)

    def test_any_other_403_is_http_error(self):
        for body in ({"detail": "Forbidden"}, {"detail": {"error": "SCOPE_DENIED"}}, {"error": "STRUCTURED_INGEST_DISABLED"}):
            with self.subTest(repr(body)):
                backend = _Backend()
                self.addCleanup(backend.close)
                backend.reply(200, _page()).reply(403, body)
                out = _ingest_client.IngestClient(backend.url, "", USER, CONTAINER, HOOK).upsert("fact", "s", "c", {})
                self.assertEqual(out.reason, "http_error")

    def test_429_is_rate_limited_and_keeps_retry_after(self):
        out = self._upsert_with(429, {"detail": "Rate limit exceeded"}, {"Retry-After": "60"})
        self.assertEqual((out.reason, out.retry_after), ("rate_limited", "60"))
        self.assertTrue(out.aborts_round)

    def test_422_is_rejected_422_and_does_not_abort_the_round(self):
        out = self._upsert_with(422, {"detail": [{"loc": ["body", "content"], "msg": "too long"}]})
        self.assertEqual(out.reason, "rejected_422")
        self.assertFalse(out.aborts_round)

    def test_500_is_http_error(self):
        self.assertEqual(self._upsert_with(500, {"detail": "boom"}).reason, "http_error")

    def test_a_refused_lookup_is_classified_the_same_way(self):
        out = self._upsert_with(403, {"detail": {"error": "STRUCTURED_INGEST_DISABLED", "reason": "r"}}, on="GET")
        self.assertEqual(out.reason, "ingest_disabled")
        self.assertEqual(len(self.requests), 1)


class TestTransport(_ClientCase):
    def test_connection_refused_is_http_error(self):
        port = self.backend.server.server_address[1]
        self.backend.close()
        out = _ingest_client.IngestClient(f"http://127.0.0.1:{port}/v1", "", USER, CONTAINER, HOOK).upsert("fact", "s", "c", {})
        self.assertEqual(out.reason, "http_error")
        self.assertEqual(out.calls, 1)

    def test_a_stalled_reply_is_timeout(self):
        self.backend.delay = 1.0
        self.backend.reply(200, _page())
        out = self.client(timeout=0.2).upsert("fact", "s", "c", {})
        self.assertEqual(out.reason, "timeout")
        self.assertTrue(out.aborts_round)


class TestCallerBugsAndConfig(_ClientCase):
    def test_not_configured_makes_no_call(self):
        out = _ingest_client.IngestClient("", "", USER, CONTAINER, HOOK).upsert("fact", "s", "c", {})
        self.assertEqual((out.reason, out.calls), ("not_configured", 0))

    def test_empty_content_is_loud_not_silent(self):
        with mock.patch("sys.stderr") as err:
            out = self.client().upsert("fact", "s", "   ", {})
        self.assertEqual((out.reason, out.calls), ("unknown", 0))
        self.assertTrue(err.write.called)

    def test_session_summary_without_session_id_is_refused(self):
        """§3.2 hard requirement: the aggregator would never see the episode."""
        with mock.patch("sys.stderr"):
            out = self.client().upsert("session_summary", "h.md", "c", {"branch": "main"})
        self.assertEqual((out.reason, out.calls), ("unknown", 0))


class TestDelete(_ClientCase):
    def test_deletes_every_verified_row(self):
        a = _row("s", content_hash="x", row_id="aaaaaaaa-1111-4111-8111-111111111111")
        b = _row("s", content_hash="x", row_id="bbbbbbbb-1111-4111-8111-111111111111")
        self.backend.reply(200, _page(a, b)).reply(204, None).reply(404, {"detail": "gone"})
        out = self.client().delete("fact", "s")
        self.assertEqual((out.reason, out.action, out.deleted, out.calls), (_hook_state.NO_REASON, "deleted", 2, 3))
        self.assertEqual([r["method"] for r in self.requests], ["GET", "DELETE", "DELETE"])

    def test_nothing_to_delete(self):
        self.backend.reply(200, _page())
        out = self.client().delete("fact", "s")
        self.assertEqual((out.reason, out.deleted), ("nothing_to_do", 0))

    def test_a_stranger_page_is_filter_suspect_and_deletes_nothing(self):
        self.backend.reply(200, _page(_row("someone-elses", content_hash="x")))
        out = self.client().delete("fact", "s")
        self.assertEqual((out.reason, out.deleted, len(self.requests)), ("filter_suspect", 0, 1))

    def test_a_failed_delete_reports_and_stops(self):
        a = _row("s", content_hash="x", row_id="aaaaaaaa-1111-4111-8111-111111111111")
        b = _row("s", content_hash="x", row_id="bbbbbbbb-1111-4111-8111-111111111111")
        self.backend.reply(200, _page(a, b)).reply(500, {"detail": "x"})
        out = self.client().delete("fact", "s")
        self.assertEqual((out.reason, out.deleted, out.action), ("http_error", 0, None))


class TestContract(unittest.TestCase):
    def test_every_reason_this_module_emits_is_in_the_tables(self):
        import re

        with open(_ingest_client.__file__, encoding="utf-8") as fh:
            source = fh.read()
        emitted = set(re.findall(r"\.fail\(\"([a-z_0-9]+)\"", source))
        emitted |= set(re.findall(r"reasons\.append\(\"([a-z_0-9]+)\"", source))
        self.assertTrue(emitted)
        self.assertTrue(emitted <= _hook_state.ALL_REASONS, emitted - _hook_state.ALL_REASONS)

    def test_round_abort_reasons_are_failures(self):
        for reason in _ingest_client.ROUND_ABORT_REASONS:
            self.assertTrue(_hook_state.is_failure_reason(reason), reason)

    def test_content_hash_is_prefixed_deterministic_and_survives_surrogates(self):
        a = _ingest_client.content_hash("hello")
        self.assertTrue(a.startswith("sha256:"))
        self.assertEqual(a, _ingest_client.content_hash("hello"))
        self.assertNotEqual(a, _ingest_client.content_hash("hello!"))
        self.assertTrue(_ingest_client.content_hash("lone \ud800 surrogate").startswith("sha256:"))

    def test_identity_keys_and_patch_prefix_pin_the_patch_rule(self):
        self.assertEqual(set(_ingest_client.IDENTITY_KEYS), {"layer", "session_id", "branch", "container_id", "external_id"})
        self.assertEqual(_ingest_client.PATCH_PREFIX, "aria.")


class TestReviewRound1(_ClientCase):
    """What the pre-merge adversarial review found (Amendment A8)."""

    def _two_rows(self, digest):
        older = _row("s", content_hash=digest, created_at="2026-09-20T00:00:00.000001Z", row_id="aaaaaaaa-1111-4111-8111-111111111111")
        newer = _row("s", content_hash=digest, created_at="2026-09-21T00:00:00.000001Z", row_id="bbbbbbbb-1111-4111-8111-111111111111")
        return older, newer

    def test_a_200_page_is_not_a_deletion(self):
        """A8-5 (critical): a login page answering 200 was counted as
        'deleted 2 rows, run clean' and the caller would have cleared
        pending_delete for rows still on the server."""
        a = _row("s", content_hash="x", row_id="aaaaaaaa-1111-4111-8111-111111111111")
        b = _row("s", content_hash="x", row_id="bbbbbbbb-1111-4111-8111-111111111111")
        self.backend.reply(200, _page(a, b)).reply(200, b"<html>login</html>", {"Content-Type": "text/html"})
        out = self.client().delete("fact", "s")
        self.assertEqual((out.reason, out.deleted, out.action, out.calls), ("http_error", 0, None, 2))

    def test_a_200_on_dedup_delete_does_not_count_as_merged(self):
        older, newer = self._two_rows("old")
        self.backend.reply(200, _page(newer, older)).reply(200, {"detail": "ok"})
        out = self.client().upsert("fact", "s", "changed", {})
        self.assertEqual((out.reason, out.dedup_merged, out.action), ("http_error", 0, None))
        self.assertNotIn("dedup_merged", out.reasons)
        self.assertEqual(len(self.requests), 2)  # no PATCH after a delete that did not happen

    def test_a_dripping_body_is_cut_at_the_deadline(self):
        """A8-3: urllib's timeout is per socket operation; four bytes every
        50 ms kept a '0.2 s' request open for 52 s."""
        self.backend.drip = (b"    ", 0.05, 200)  # 10 seconds of drip
        self.backend.reply(200, None)
        started = time.monotonic()
        client = self.client(timeout=0.2, deadline=time.monotonic() + 0.6)
        out = client.upsert("fact", "s", "c", {})
        self.assertEqual(out.reason, "timeout")
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertTrue(out.aborts_round)

    def test_an_exhausted_deadline_makes_no_request(self):
        client = self.client(deadline=time.monotonic() - 1)
        out = client.upsert("fact", "s", "c", {})
        self.assertEqual((out.reason, out.calls), ("timeout", 0))
        self.assertEqual(self.requests, [])

    def test_reasons_are_collapsed_by_worst_reason_not_first_wins(self):
        """A8 survivor M12: the only multi-reason fixture had the failure
        first, so `reasons[0]` passed. Here first-wins says dedup_merged
        (does not abort the round) and the collapse says http_error (does)."""
        older, newer = self._two_rows("old")
        self.backend.reply(200, _page(newer, older)).reply(204, None).reply(500, {"detail": "boom"})
        out = self.client().upsert("fact", "s", "changed", {})
        self.assertEqual(out.reasons, ["dedup_merged", "http_error"])
        self.assertEqual(out.reason, "http_error")
        self.assertTrue(out.aborts_round)
        self.assertEqual(out.dedup_merged, 1)

    def test_a_partial_dedup_keeps_dedup_merged_in_the_reasons(self):
        """A8-6: the reason used to be appended after the loop, so a second
        delete failing erased the first deletion from `reasons`."""
        digest = _ingest_client.content_hash("same")
        rows = [
            _row("s", content_hash=digest, created_at=f"2026-09-2{i}T00:00:00Z", row_id=f"{c * 8}-1111-4111-8111-111111111111")
            for i, c in ((0, "a"), (1, "b"), (2, "c"))
        ]
        self.backend.reply(200, _page(*rows)).reply(204, None).reply(500, {"detail": "boom"})
        out = self.client().upsert("fact", "s", "same", {})
        self.assertEqual(out.dedup_merged, 1)
        self.assertEqual(out.reasons, ["dedup_merged", "http_error"])

    def test_the_constructor_refuses_a_missing_identity(self):
        """A8-7: `container_id=None` went on the wire as the literal string
        and a row without the key passed verification."""
        for kw in ({"user_id": None}, {"user_id": ""}, {"container_id": None}, {"container_id": " "}, {"source_name": ""}):
            with self.subTest(kw):
                args = {"user_id": USER, "container_id": CONTAINER, "source_name": HOOK}
                args.update(kw)
                with self.assertRaises(ValueError):
                    _ingest_client.IngestClient(self.backend.url, "", **args)

    def test_a_row_whose_keys_are_not_strings_is_not_ours(self):
        row = _row("s", content_hash="x")
        row["metadata"]["container_id"] = 123
        self.backend.reply(200, _page(row))
        out = self.client().upsert("fact", "s", "c", {})
        self.assertEqual(out.reason, "filter_suspect")

    def test_a_full_page_is_reported_and_still_deduplicated(self):
        """A8-8: with more duplicates than a page holds, dedup keeps the
        earliest of the page and converges over runs -- but says so."""
        digest = _ingest_client.content_hash("same")
        rows = [
            _row("s", content_hash=digest, created_at=f"2026-09-1{i}T00:00:00Z", row_id=f"{c * 8}-1111-4111-8111-111111111111")
            for i, c in ((5, "e"), (4, "d"), (3, "c"), (2, "b"), (1, "a"))
        ]
        self.backend.reply(200, _page(*rows))
        for _ in range(4):
            self.backend.reply(204, None)
        with mock.patch("sys.stderr") as err:
            out = self.client().upsert("fact", "s", "same", {})
        self.assertEqual((out.reason, out.dedup_merged, out.action), ("dedup_merged", 4, "unchanged"))
        self.assertEqual(out.memory_id, rows[-1]["memory_id"])  # the earliest of the page
        self.assertIn("page full", out.detail)
        self.assertTrue(err.write.called)

    def test_delete_carries_no_content_type_and_bulk_on_writes(self):
        """A8-9: `Content-Type: application/json` with no body."""
        self.backend.reply(200, _page(_row("s", content_hash="x"))).reply(204, None)
        self.client(bulk=True).delete("fact", "s")
        delete = self.requests[1]
        self.assertEqual(delete["method"], "DELETE")
        self.assertNotIn("content-type", delete["headers"])
        self.assertEqual(delete["headers"]["x-bulk-import"], "true")

    def test_a_dripping_body_is_bounded_without_a_deadline_too(self):
        """R2-a: the default construction used to keep the unbounded read."""
        self.backend.drip = (b"    ", 0.05, 200)
        self.backend.reply(200, None)
        started = time.monotonic()
        out = self.client(timeout=0.5).upsert("fact", "s", "c", {})
        self.assertEqual(out.reason, "timeout")
        self.assertLess(time.monotonic() - started, 3.0)

    def test_the_socket_timeout_never_exceeds_what_is_left(self):
        """R2 survivor N9: `min(timeout, left)` had no test."""
        seen = []

        def opener(req, timeout=None):
            seen.append(timeout)
            raise ConnectionRefusedError(111, "refused")

        client = _ingest_client.IngestClient(self.backend.url, "", USER, CONTAINER, HOOK, timeout=8.0, deadline=time.monotonic() + 0.3, opener=opener)
        out = client.upsert("fact", "s", "c", {})
        self.assertEqual(out.reason, "http_error")
        self.assertEqual(len(seen), 1)
        self.assertLessEqual(seen[0], 0.3)

    def test_a_row_missing_a_key_is_not_ours_and_the_keys_must_be_given(self):
        """R2 survivor N8: `==` alone matched a missing key against a None."""
        row = _row("s", content_hash="x")
        del row["metadata"]["container_id"]
        self.backend.reply(200, _page(row))
        out = self.client().upsert("fact", "s", "c", {})
        self.assertEqual(out.reason, "filter_suspect")
        for layer, ext in ((None, "s"), ("fact", None), ("", "s"), ("fact", " ")):
            with self.subTest((layer, ext)), mock.patch("sys.stderr"):
                out = self.client().upsert(layer, ext, "c", {})
                self.assertEqual((out.reason, out.calls), ("unknown", 0))
                out = self.client().delete(layer, ext)
                self.assertEqual((out.reason, out.calls), ("unknown", 0))

    def test_a_degraded_identity_refuses_to_write_or_delete(self):
        """R2 C: the check lives in the one module that writes, not in each
        caller's memory. `_identity.project_identity(cwd)[1]` feeds it."""
        client = self.client(identity_degraded=True)
        for out in (client.upsert("fact", "s", "c", {}), client.delete("fact", "s")):
            self.assertEqual((out.reason, out.calls, out.action), ("identity_unresolved", 0, None))
        self.assertEqual(self.requests, [])
        self.assertTrue(_hook_state.is_failure_reason("identity_unresolved"))

    def test_an_oversized_body_is_http_error(self):
        self.backend.reply(200, _page()).reply(201, {"memory_id": "m", "pad": "x" * 4096})
        out = self.client(max_body_bytes=1024).upsert("fact", "s", "c", {})
        self.assertEqual((out.reason, out.action), ("http_error", None))


if __name__ == "__main__":
    unittest.main()
