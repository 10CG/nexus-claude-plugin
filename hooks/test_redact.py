"""Tests for hooks/_redact.py (TASK-004, value-level redaction).

Runnable as: python3 hooks/test_redact.py   (stdlib unittest only)

Two kinds of fixture live here, and the difference is the point of the module:

* POSITIVE shapes are synthetic -- built from a fixed alphabet, never a real
  credential -- and each must be replaced by ``[redacted:<shape>]`` with the
  rest of the line intact.
* NEGATIVE shapes are the false positives found on the real corpora (this
  machine's 109 memory files + 102 handoffs, then all 848 memory files and the
  220 nexus docs as extra evidence; see the PR for the masked scan output) and
  by the pre-merge adversarial review (Amendment A7 in the change proposal).
  Every one is quoted with where it came from and must NOT be touched. The
  spec's acceptance is "0 false positives + every known true positive hit",
  and a redactor that shreds ``task-planner``, ``process.env.X`` or a date
  after ``token:`` is worse than none: it corrupts every synced row and nobody
  notices.

The corpus regression itself (``TestCorpusRegression``) runs against real
files that are not in git, so it is gated on ``NEXUS_REDACT_CORPUS_MANIFEST``
and skips with a message otherwise -- the constants above are what CI runs.
"""

import json
import os
import random
import re
import time
import unittest

import _redact

# ── Synthetic positives (fake by construction) ────────────────────────────────

# Wide enough that a real-looking token has the entropy the placeholder gate
# expects (a repeated three-letter pattern would read as `ghp_xxxx...`).
_ALPHA = "Ab3Cd4Ef5Gh6Jk7Mn8Pq9Rs0Tu1Vw2Xy"


def _fake(prefix, n):
    """``prefix`` + ``n`` chars of a fixed alphabet."""
    return prefix + (_ALPHA * (n // len(_ALPHA) + 1))[:n]


# The example token from jwt.io: public documentation, not a credential.
_EXAMPLE_JWT = (
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0."
    "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

TOKENS = {
    "nexus-key": _fake("nx_live_", 43),
    "nexus-key-test": ("nexus-key", _fake("nx_test_", 43)),
    "openai-key": _fake("sk-", 48),
    "openai-key-anthropic": ("openai-key", _fake("sk-ant-api03-", 60)),
    "openai-key-luxeno": ("openai-key", _fake("sk-silk-", 40)),
    "stripe-key": _fake("sk_live_", 24),
    "stripe-key-restricted": ("stripe-key", _fake("rk_test_", 24)),
    "github-token": _fake("ghp_", 36),
    "github-token-pat": ("github-token", _fake("github_pat_", 60)),
    "gitlab-token": _fake("glpat-", 20),
    "slack-token": "xoxb-1234567890-abcdefghij",
    "slack-token-app": ("slack-token", "xapp-1-A0123456789-abcdefghij"),
    "npm-token": _fake("npm_", 36),
    "aws-key": "AKIA" + "ABCDEFGHIJKLMNOP",
    "google-key": _fake("AIza", 35),
    "jwt": _EXAMPLE_JWT,
}


def _cases():
    """Yield ``(label, shape, value)`` for every synthetic token."""
    for label, entry in TOKENS.items():
        shape, value = entry if isinstance(entry, tuple) else (label, entry)
        yield label, shape, value


# ── Negatives, each with where it was found ───────────────────────────────────

NOT_SECRETS = [
    # sk- inside a word: 60 hits on docs/handoff/, 0 secrets (unanchored scan)
    "2026-05-25-task-025-publish-close.md",
    "feature/us-037-task-026-marketplace-prep",
    "the task-planner skill and the risk-assessment table",
    # a deliberately truncated fingerprint (2026-09-03 handoff)
    "nexus dev luxeno key = `sk-silk-xzYL…`, Kairos 用另一把",
    # too short to be a key of that family
    "sk-abc123 ghp_short xoxb-12 AKIA1234",
    # docs/ README + env-vars + prd-v7: placeholders with the real prefix
    # (review A1: the prefix rules used to skip the placeholder gate)
    "GITHUB_TOKEN=ghp_" + "x" * 36,
    "NEXUS_API_TOKEN=nx_live_" + "x" * 40,
    "OPENAI_API_KEY=sk-" + "X" * 40,
    "GOOGLE_API_KEY=AIza" + "x" * 35,
    "nx_live_" + "0" * 43,
    "xoxb-XXXXXXXXXXXX-XXXXXXXXXXXX glpat-" + "X" * 20 + " sk_live_" + "x" * 24,
    # feedback_mypy_misses_dynamic_getattr_consumers.md: code, not a value
    'api_key = getattr(settings, info["api_key_attr"])',
    "api_key: SecretStr = Field(...)",
    'api_key = key_data.json()["key"]',  # docs/development/testing-guide.md
    # feedback_npm_provenance_needs_repository_url.md: a GitHub Actions permission
    "Also requires `id-token: write` permission",
    "permissions:\n  id-token: write-all",
    # reference_nexus_local_integration_tests_podman.md: below the 8-char floor
    # (documented miss -- the userinfo rule catches the same password in the DSN)
    "-e POSTGRES_USER=nexus -e POSTGRES_PASSWORD=nexus",
    # 2026-06-19 handoffs: a shell variable reference
    "CF_ACCESS_CLIENT_ID=$CF_ACCESS_CLIENT_ID CF_ACCESS_CLIENT_SECRET=$CF_ACCESS_CLIENT_SECRET",
    # reference_aether_dev_catalog_onboarding.md: a template path
    "    ZHIPU_API_KEY: .zhipu_api_key,",
    # sportmonks_no_websocket_rest_only.md: a placeholder in a query string
    "GET /v3/football/livescores/latest?api_token=X&include=events",
    # reference_forgejo_actions_internal_registry_build.md: prose + a name
    "- secret: org-level `FORGEJO_TOKEN` 默认继承所有 repo",
    "the secret: FORGEJO_TOKEN inherits to every repo",
    # docs/integration + docs/guides: code references and placeholders
    "apiKey: process.env.NEXUS_API_KEY!,",
    'api_key=os.getenv("NEXUS_API_KEY")',
    "SECRET_KEY=your-random-secret-key-here",
    "SECRET_KEY=change-this-in-production-please",
    "SMTP_PASSWORD=your-password",
    "STRIPE_SECRET_KEY=sk_live_xxxxx",
    "STRIPE_WEBHOOK_SECRET=whsec_xxxxx",
    "NEXUS_API_TOKEN: sk-nexus-xxxxx",
    '"NEXUS_API_TOKEN": "sk-nexus-xxx…",',  # docs/requirements/prd-v7: a truncated placeholder
    "SMTP_PASSWORD=re_<your-api-key>",  # docs/deployment/env-vars.md
    # numbers and counters are not secrets
    "token: 15000000",
    "token: 1789640666883",  # an epoch in milliseconds (audit report ids)
    "max_tokens=4096",
    "token_count: 12345678",
    "total_tokens: 987654321",
    # review A2: what prose puts after `token:` / `secret:` that is not a secret
    "token: 2026-09-22T10:00:00Z",
    "secret: 4a12ccaa-db22-41c9-bf63-1604dc1efbd6",
    "secret: 4a12ccaa",
    "api-key: application/json",
    "token: refs/aria/coordination",
    "secret: nexus/prod/llm_api_key",
    "pwd: packages/nexus-claude-plugin",
    "token: text-embedding-3-large",
    "token=v1.2.3-rc4.build567",
    "password: README.md#section-12",
    # a hash, masked values, a placeholder header (review E2: same-char gate)
    "password_hash=$2b$12$abcdefghijklmnopqrstuv",
    "password: aaaaaaaaaaaaaaaa",
    "password=****************",
    "header 变成 `Authorization: Bearer **********`",
    "Authorization: Bearer <NEXUS_API_TOKEN>",
    "uses Bearer token authentication",
    "the Bearer authentication-scheme",
    "Bearer Token-Based-Authentication-Scheme",  # review A4
    # YAML: without a block indicator the next line is a nested mapping
    "password:\n  hunter22x9",
    # not a URL with a password in it
    'git push "http://10cg-ci-bot:${FORGEJO_TOKEN}@192.168.69.200:3000/10CG/repo.git"',
    "app DSN `postgres://turfsync_dev:PW@192.168.69.81:5432/turfsync_dev`",
    "DATABASE_URL: postgresql+asyncpg://${POSTGRES_USER:-nexus}:${POSTGRES_PASSWORD:-nexus}@postgres:5432/db",
    "postgresql://user:password@host:5432/db",
    "postgresql://[user]:[password]@[host]:[port]/[database]",
    "ssh://git@forgejo.10cg.pub/10CG/nexus.git",
    "git@github.com:10CG/repo.git",
    "http://localhost:8001/v1/memories?user_id=a:b@c",
    'DATABASE_URL="postgresql://nexus:$(cat pw)@host/db"',
    # review R3: a field whose value is a file path
    "private_key: /etc/nexus/id_ed25519",
    "secret_key: /run/secrets/app_key",
    "access_key: ~/.aws/credentials",
    "smtp_password: /var/run/secrets/smtp",
    "password: ./config/shadow.bak",
    # review R3: hex after a bare prose word is a commit hash, at any length
    "token: 0123456789a",
    "secret: c0ffee1234ab",
    "token: " + "0123456789abcdef" * 2 + "01234567",
    "http://127.0.0.1:8001@proxy",  # review A3: a port is not a password
    # review R2: the userinfo position gets the same prose gates
    "see redis://cache:2026-09-22@node1",
    "amqp://svc:text-embedding-3@rabbit",
    "svn://repo:notes.md@host",
    "Bearer Token-Based-Auth-2026-Scheme",
    "see redis://cache:6379@node1 for the alias",
    # not a private key
    "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----",
]


class TestTokenShapes(unittest.TestCase):
    def test_every_synthetic_token_is_replaced_by_its_shape(self):
        for label, shape, value in _cases():
            with self.subTest(label):
                text, n = _redact.redact_text(f"key {value} end")
                self.assertEqual(n, 1)
                self.assertEqual(text, f"key [redacted:{shape}] end")

    def test_boundary_is_only_identifier_characters(self):
        """Quotes, brackets and a line start are boundaries; letters, digits,
        `_` and `-` are not."""
        value = TOKENS["openai-key"]
        for wrap in ('"{}"', "'{}'", "({})", "`{}`", "{}", "=\t{}", "key={}"):
            with self.subTest(wrap):
                text, n = _redact.redact_text(wrap.format(value))
                self.assertEqual(n, 1, text)
                self.assertNotIn(value, text)
        for glue in ("a", "9", "_", "-"):
            with self.subTest(glue):
                text, n = _redact.redact_text(glue + value)
                self.assertEqual(n, 0)
                self.assertEqual(text, glue + value)

    def test_a_low_entropy_token_is_a_placeholder(self):
        """Review R2 survivor: the distinct-character gate had no test."""
        for value in ("sk-" + "Ab3" * 8, "ghp_" + "ab" * 12, "nx_live_" + "Q" * 30):
            with self.subTest(value[:12]):
                self.assertEqual(_redact.redact_text(value), (value, 0))
        self.assertEqual(_redact.redact_text("sk-" + _ALPHA)[1], 1)

    def test_aws_docs_example_key_is_a_true_positive_by_shape(self):
        """Found in feedback_github_secret_scanning_push_range_blocks_history.md:
        GitHub's own scanner flags it, and so does this -- the shape is the
        shape. Recorded so the corpus run's one extra hit is understood."""
        text, n = _redact.redact_text("AWS example `AKIAIOSFODNN7EXAMPLE`")
        self.assertEqual((text, n), ("AWS example `[redacted:aws-key]`", 1))


class TestKeyValue(unittest.TestCase):
    def _one(self, text, expected):
        redacted, n = _redact.redact_text(text)
        self.assertEqual(redacted, expected)
        self.assertEqual(n, 1)

    def test_value_only_is_replaced_and_shape_names_the_key_word(self):
        self._one("password: hunter22x9", "password: [redacted:password]")
        self._one('"api_key": "k9f8e7d6c5b4a3"', '"api_key": "[redacted:api-key]"')
        self._one("--password=Tr0ub4dor3", "--password=[redacted:password]")
        self._one("client_secret = a1b2c3d4e5f6", "client_secret = [redacted:secret]")
        self._one(
            "aws_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "aws_secret_access_key: [redacted:access-key]",
        )
        self._one('smtp_password="P@ssw0rd!2026"', 'smtp_password="[redacted:password]"')
        self._one("JWT_SECRET_KEY=8f7e6d5c4b3a29181716", "JWT_SECRET_KEY=[redacted:secret-key]")
        self._one("pwd:  q1w2e3r4t5y6", "pwd:  [redacted:pwd]")

    def test_a_long_word_password_is_caught_without_a_digit(self):
        self._one("password=correcthorsebatterystaple", "password=[redacted:password]")

    def test_symbol_first_and_all_digit_passwords_are_caught(self):
        """Review B1 / B3: the first draft required a letter or digit first
        and refused every all-digit value."""
        self._one("password=!Qaz2wsx3edc", "password=[redacted:password]")
        self._one("api_key=_9f8e7d6c5b4a3210", "api_key=[redacted:api-key]")
        self._one("password=$ecretHunter22", "password=[redacted:password]")
        self._one("password=86753091234567", "password=[redacted:password]")
        self._one("NEXUS_API_TOKEN=91827364550918273645", "NEXUS_API_TOKEN=[redacted:token]")

    def test_field_keys_take_passwords_of_any_shape(self):
        """Review R2: the first precision fix applied the prose gates to every
        key and turned these into zero hits with a zero count."""
        self._one("password=P@ssw0rd*2026x", "password=[redacted:password]")
        self._one("password=Tr0ub4dor(3)Xy", "password=[redacted:password]")
        self._one("api_key=abc12345*XYZQWE", "api_key=[redacted:api-key]")
        self._one("the creds (password: hunter22x9) are in the vault", "the creds (password: [redacted:password]) are in the vault")
        self._one("password=prod-db-secret-2026", "password=[redacted:password]")
        self._one("password=correct-horse-battery-staple", "password=[redacted:password]")
        self._one("password=hunter/22x9/abcdef", "password=[redacted:password]")
        self._one("api_key=abc123def456/ghi789jkl012", "api_key=[redacted:api-key]")
        self._one("password=a1b2c3d4e5", "password=[redacted:password]")
        self._one("NEXUS_API_TOKEN=1234567890123456", "NEXUS_API_TOKEN=[redacted:token]")
        self._one("token: abc123456z.", "token: [redacted:token].")  # sentence punctuation stays outside

    def test_a_full_length_hex_after_token_is_a_token(self):
        """Only the abbreviated commit-hash length (7-12) is treated as prose;
        a forgejo PAT is 40 hex characters and `token=` is where it goes."""
        self._one("FORGEJO_TOKEN=" + "0123456789abcdef" * 2 + "01234567", "FORGEJO_TOKEN=[redacted:token]")

    def test_the_value_is_taken_whole_or_not_at_all(self):
        """Review B2: cutting a value at the first odd character shipped the
        tail in the clear while the ledger counted a redaction."""
        self._one("password=Tr0ub4dor&3xtra", "password=[redacted:password]")
        text, n = _redact.redact_text("api_key=abc12345***")
        self.assertEqual((text, n), ("api_key=abc12345***", 0))  # a masked value: left alone, not half-done

    def test_arrow_and_yaml_block_scalars(self):
        """Review B4."""
        self._one("'password' => 'hunter22x9'", "'password' => '[redacted:password]'")
        self._one("password: >\n  hunter22x9long", "password: >\n  [redacted:password]")
        self._one("password: |\n  hunter22x9long\n", "password: |\n  [redacted:password]\n")

    def test_known_prefix_wins_over_the_key_word(self):
        """`NEXUS_API_TOKEN=nx_live_...` is reported under its precise shape,
        and only once."""
        value = TOKENS["nexus-key"]
        self._one(f"NEXUS_API_TOKEN={value}", "NEXUS_API_TOKEN=[redacted:nexus-key]")

    def test_the_rest_of_the_line_survives(self):
        text, n = _redact.redact_text(
            "export NEXUS_API_URL=https://nexus.example/v1 NEXUS_API_TOKEN=a1b2c3d4e5f6g7 # dev"
        )
        self.assertEqual(n, 1)
        self.assertEqual(
            text,
            "export NEXUS_API_URL=https://nexus.example/v1 NEXUS_API_TOKEN=[redacted:token] # dev",
        )

    def test_none_of_the_classified_false_positives_is_touched(self):
        for text in NOT_SECRETS:
            with self.subTest(text[:60]):
                self.assertEqual(_redact.redact_text(text), (text, 0))
                self.assertEqual(_redact.find(text), [])


class TestUserinfo(unittest.TestCase):
    def test_only_the_password_half_is_replaced(self):
        text, n = _redact.redact_text(
            "`DATABASE_URL` = `postgresql+asyncpg://nexus:nexus@localhost:5433/nexus`"
        )
        self.assertEqual(n, 1)
        self.assertEqual(
            text,
            "`DATABASE_URL` = `postgresql+asyncpg://nexus:[redacted:url-userinfo]@localhost:5433/nexus`",
        )

    def test_empty_user_is_still_a_password(self):
        text, n = _redact.redact_text("REDIS_URL=redis://:s3cretpw@redis:6379/0")
        self.assertEqual((text, n), ("REDIS_URL=redis://:[redacted:url-userinfo]@redis:6379/0", 1))

    def test_a_token_in_userinfo_is_reported_once_under_its_own_shape(self):
        value = TOKENS["github-token"]
        text, n = _redact.redact_text(f"https://x-access-token:{value}@github.com/org/repo.git")
        self.assertEqual(n, 1)
        self.assertEqual(text, "https://x-access-token:[redacted:github-token]@github.com/org/repo.git")


class TestBearer(unittest.TestCase):
    def test_jwt_bearer_is_reported_as_jwt(self):
        text, n = _redact.redact_text(f"Authorization: Bearer {TOKENS['jwt']}")
        self.assertEqual((text, n), ("Authorization: Bearer [redacted:jwt]", 1))

    def test_opaque_bearer(self):
        text, n = _redact.redact_text("Authorization: Bearer a1b2c3d4e5f6g7h8i9j0k1")
        self.assertEqual((text, n), ("Authorization: Bearer [redacted:bearer]", 1))


class TestPrivateKey(unittest.TestCase):
    BLOCK = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXkt\nAAAAB3NzaC1\n-----END OPENSSH PRIVATE KEY-----"

    def test_whole_block_is_the_value(self):
        text, n = _redact.redact_text(f"key:\n{self.BLOCK}\nafter")
        self.assertEqual((text, n), ("key:\n[redacted:private-key]\nafter", 1))

    def test_unterminated_block_is_key_material_to_the_end(self):
        head = "-----BEGIN RSA PRIVATE KEY-----\nMIIEow\nMIIEpA"
        text, n = _redact.redact_text(f"before\n{head}")
        self.assertEqual((text, n), ("before\n[redacted:private-key]", 1))


class TestObject(unittest.TestCase):
    def test_every_string_inside_is_redacted_and_counted_and_keys_are_kept(self):
        value = TOKENS["openai-key"]
        payload = {
            "content": f"see {value}",
            "metadata": {
                "layer": "fact",
                "aria.description": f"token {value} in the description",  # D renders only this
                "tags": ["ok", "pw password=hunter22x9"],
                "count": 3,
                "nested": {"password": value},  # a KEY named password stays a key
            },
        }
        before = json.dumps(payload, sort_keys=True)
        out, n = _redact.redact_object(payload)
        self.assertEqual(n, 4)
        self.assertEqual(out["content"], "see [redacted:openai-key]")
        self.assertEqual(out["metadata"]["aria.description"], "token [redacted:openai-key] in the description")
        self.assertEqual(out["metadata"]["tags"], ["ok", "pw password=[redacted:password]"])
        self.assertEqual(out["metadata"]["nested"], {"password": "[redacted:openai-key]"})
        self.assertEqual(out["metadata"]["count"], 3)
        self.assertEqual(out["metadata"]["layer"], "fact")
        # the input is not mutated: callers hash the original
        self.assertEqual(json.dumps(payload, sort_keys=True), before)

    def test_non_strings_tuples_and_sets(self):
        self.assertEqual(_redact.redact_object(None), (None, 0))
        self.assertEqual(_redact.redact_object(42), (42, 0))
        out, n = _redact.redact_object(("a", TOKENS["aws-key"]))
        self.assertEqual((out, n), (("a", "[redacted:aws-key]"), 1))
        out, n = _redact.redact_object(frozenset({"password: hunter22x9"}))  # review C1
        self.assertEqual((out, n), (frozenset({"password: [redacted:password]"}), 1))
        self.assertIsInstance(_redact.redact_object({"x"})[0], set)
        import collections

        Pair = collections.namedtuple("Pair", "a b")
        out, n = _redact.redact_object(Pair("password: hunter22x9", 1))  # review R2: type(obj)(items) raised
        self.assertEqual((out, n), (("password: [redacted:password]", 1), 1))

    def test_a_structure_deeper_than_any_payload_is_refused_visibly(self):
        """Review C2: 20000 nested dicts used to raise RecursionError, which
        the hooks' blanket handler would have turned into a silent skip."""
        deep = {"password: hunter22x9": "leaf"}
        for _ in range(20000):
            deep = {"k": deep}
        out, n = _redact.redact_object(deep)
        self.assertGreaterEqual(n, 1)
        self.assertIn("[redacted:too-deep]", json.dumps(out))
        # and a merely nested payload is untouched by the guard
        shallow = {"a": {"b": {"c": [{"d": "fine"}]}}}
        self.assertEqual(_redact.redact_object(shallow), (shallow, 0))

    def test_redact_text_on_non_string(self):
        self.assertEqual(_redact.redact_text(None), (None, 0))
        self.assertEqual(_redact.redact_text(""), ("", 0))


class TestIdempotentAndCounts(unittest.TestCase):
    def _samples(self):
        return [f"a {value}" for _, _, value in _cases()] + [
            "password: hunter22x9",
            "postgresql://u:s3cretpw@h/db",
            "Bearer a1b2c3d4e5f6g7h8i9j0",
            f"NEXUS_API_TOKEN={TOKENS['nexus-key']}",
            f"https://x-access-token:{TOKENS['github-token']}@github.com/o/r.git",
            "'password' => 'hunter22x9'",
        ]

    def test_second_pass_is_a_no_op(self):
        text = "\n".join(self._samples())
        once, n1 = _redact.redact_text(text)
        twice, n2 = _redact.redact_text(once)
        self.assertEqual(n1, len(TOKENS) + 6)
        self.assertEqual((twice, n2), (once, 0))
        self.assertEqual(_redact.find(once), [])

    def test_find_agrees_with_redact_text_and_reports_a_nested_token_once(self):
        """Review E1: `find` is the corpus test's only oracle. It has to see
        later rules against the text with earlier hits replaced, as
        redact_text does, or it double-reports a token inside a key/value."""
        for text in self._samples() + NOT_SECRETS:
            with self.subTest(text[:50]):
                self.assertEqual(len(_redact.find(text)), _redact.redact_text(text)[1])
        self.assertEqual(
            _redact.find(f"NEXUS_API_TOKEN={TOKENS['nexus-key']}"),
            [("nexus-key", TOKENS["nexus-key"])],
        )
        self.assertEqual(
            [s for s, _ in _redact.find(f"https://x-access-token:{TOKENS['github-token']}@github.com/o/r.git")],
            ["github-token"],
        )

    def test_find_reports_shape_and_value(self):
        value = TOKENS["gitlab-token"]
        self.assertEqual(
            _redact.find(f"x {value} password: hunter22x9"),
            [("gitlab-token", value), ("password", "hunter22x9")],
        )


class TestRuleStructure(unittest.TestCase):
    """The spec's two structural requirements on every known-prefix rule."""

    def test_every_token_rule_has_a_left_boundary_and_a_length_floor(self):
        for shape, pattern in _redact._TOKEN_RULES:
            with self.subTest(shape):
                self.assertTrue(pattern.pattern.startswith(_redact._LB))
                self.assertIn("v", pattern.groupindex)
                floors = [int(m.group(1)) for m in re.finditer(r"\{(\d+)(?:,\d*)?\}", pattern.pattern)]
                self.assertTrue(floors, pattern.pattern)
                self.assertGreaterEqual(min(floors), 10, pattern.pattern)

    def test_every_rule_names_a_value_group(self):
        for pattern, _shape_of, _span_of in _redact._rules():
            self.assertIn("v", pattern.groupindex, pattern.pattern)


class TestLinearTime(unittest.TestCase):
    """Review D1: with `-` / `_` / `.` allowed before a key, every dash in a
    run was a legal start and the non-greedy prefix walked the rest of the
    run from each one -- 8 s for 32 KB of base64, >40 s for 16000 dashes.
    Prose never triggers it, so the corpus runs were green throughout."""

    def test_adversarial_runs_stay_fast(self):
        rng = random.Random(7)
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        soup = "".join(rng.choice(alphabet) for _ in range(32768))
        # Smallest first: a quadratic regression fails on the 4 KB case in
        # well under a second instead of hanging the suite on the 100 KB one.
        inputs = [
            ("dashes 4k", "-" * 4000),
            ("base64url 4k", soup[:4096]),
            ("dashes 20k", "-" * 20000),
            ("base64url 32k", soup),
            ("identifier soup 100k", "a.-_" * 25000),
            ("dots 100k", "." * 100000),
            ("many headers", "-----BEGIN PRIVATE KEY-----\n" * 500),
            ("colons 50k", "http://" + ":" * 50000),
        ]
        for label, text in inputs:
            with self.subTest(label):
                started = time.monotonic()
                _redact.redact_text(text)
                _redact.find(text)
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 0.5 if len(text) <= 4096 else 2.0, f"{label}: {elapsed:.2f}s")


class TestCorpusRegression(unittest.TestCase):
    """0 false positives + every known true positive, on the real files.

    Gated on ``NEXUS_REDACT_CORPUS_MANIFEST`` = path of a JSON file::

        {"dirs": ["/abs/dir", ...],
         "expected": [{"file": "<basename>", "shape": "<shape>", "count": 1}, ...]}

    Every ``*.md`` under ``dirs`` is scanned; the multiset of
    ``(basename, shape)`` hits must equal ``expected`` exactly. Values are
    never printed -- only the first two characters and the length.
    """

    def test_corpus_hits_are_exactly_the_expected_ones(self):
        manifest_path = os.environ.get("NEXUS_REDACT_CORPUS_MANIFEST")
        if not manifest_path:
            self.skipTest("set NEXUS_REDACT_CORPUS_MANIFEST to run the local corpus regression")
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        expected = {}
        for row in manifest.get("expected", []):
            key = (row["file"], row["shape"])
            expected[key] = expected.get(key, 0) + int(row.get("count", 1))
        seen = {}
        files = 0
        for root in manifest["dirs"]:
            for base, _dirs, names in os.walk(root):
                for name in sorted(names):
                    if not name.endswith(".md"):
                        continue
                    files += 1
                    with open(os.path.join(base, name), encoding="utf-8", errors="replace") as fh:
                        text = fh.read()
                    for shape, value in _redact.find(text):
                        seen[(name, shape)] = seen.get((name, shape), 0) + 1
                        print(f"  hit: {name} {shape} {value[:2]}…({len(value)})")
        print(f"  scanned {files} files, {sum(seen.values())} hits")
        self.assertGreater(files, 0, "the manifest dirs contain no .md files")
        self.assertEqual(seen, expected)


if __name__ == "__main__":
    unittest.main()
