"""Value-level redaction shared by every nexus hook (TASK-004, workflow C).

Everything a hook sends to the backend -- handoff episodes, memory-file facts,
their metadata -- passes through here first. The design decision that shapes
this module is **precision over recall**:

* Line-level shapes (any run of >= 20 characters) were measured on the real
  memory corpus at 100% false positives -- 765 hits, not one of them a secret --
  which would have shredded every commit hash, URL and slug in every file.
* So only *value-level* shapes are matched, and only the value is replaced:
  ``password=hunter22x9`` becomes ``password=[redacted:password]``, the key and
  the rest of the line stay legible. The replacement names the shape, so a
  reader of the stored row knows what was there and can go look it up at the
  source.
* Every known-prefix token rule carries a **left boundary** and a **minimum
  length**. Unanchored ``sk-`` matched the ``sk-`` inside ``task-planner`` on
  the handoff corpus 60 times (0 secrets); ``sk-[A-Za-z0-9_-]{20,}`` behind
  ``(?<![A-Za-z0-9_-])`` matched nothing there and still catches every real
  key of that family (``sk-``, ``sk-ant-``, ``sk-proj-``, ``sk-silk-``, ...).
* Placeholders are not secrets, and neither are the other things people write
  where a secret would go. The corpora are full of ``${TOKEN}``,
  ``process.env.NEXUS_API_KEY``, ``your-api-key-here``, ``sk_live_xxxxx``,
  ``nx_live_xxxxxxxx``, ``?api_token=X&...``, ``ZHIPU_API_KEY: .zhipu_api_key``
  and ``secret: FORGEJO_TOKEN``; and *prose* -- a bare ``token:`` or
  ``secret:`` in a sentence -- is followed by dates, UUIDs, short commit
  hashes, paths, MIME types, model names and version strings. Each of those
  shapes is rejected by name in ``_rejected`` and pinned by a test.
* The prose gates apply to prose only. The first fix for the shapes above
  applied them to every key, and turned ``password=prod-db-secret-2026``,
  ``password=hunter/22x9/abcdef`` and ``password=P@ssw0rd*2026x`` into zero
  hits with a zero count -- quieter than the miss it replaced. So a bare
  ``token:`` / ``secret:`` (prose), the userinfo and bearer positions are
  gated on shape; an identifier key (``NEXUS_API_TOKEN=``, ``client_secret=``,
  any ``password=``) takes its value as long as it is not a placeholder or a
  piece of code.
* A value is taken **whole**, up to whitespace or a quote, minus unbalanced
  closing punctuation (``(password: hunter22x9)``), and then either replaced
  whole or left whole. An earlier draft cut a value at the first character
  outside its charset, which shipped the tail in the clear while the ledger
  counted a redaction -- worse than missing it.
* Linear time. The key/value rule's left boundary excludes ``-``, ``_`` and
  ``.``, because with it relaxed a 32 KB run of base64 (or of dashes) took
  eight seconds: every dash was a legal start and the non-greedy identifier
  prefix walked the rest of the run from each one. A test times the
  adversarial shapes, smallest first, so a regression fails in seconds.

The known misses, accepted: a password shorter than 8 characters, a
dictionary-word password after a prose key, a YAML multi-line password
without a ``|`` / ``>`` indicator, a bare string of unknown shape. The
aria-plugin secret scanner already screens these files at write time; this
module is the second gate.

Not reused on purpose: aria-plugin's ``secret-guard.sh`` ``_sg_redact_echo`` --
it redacts whole lines of a BLOCKED-command echo, which is the line-level shape
above.

Stdlib only, pure functions, no I/O. Two entry points: ``redact_text`` for a
string, ``redact_object`` for a JSON-shaped value (every string inside it,
recursively; dict keys are left alone). Both return ``(redacted, hits)`` where
``hits`` is the number of replacements made, for the ledger. ``find`` returns
what *would* be replaced, for the corpus regression test.
"""

import re

REDACTED_FORMAT = "[redacted:{shape}]"

# "Not the tail of a longer identifier": the character before a token must not
# be one that identifiers, slugs and file names are made of. This is what keeps
# `task-planner`, `risk-based` and `2026-05-25-task-025` out of the sk- rule.
_LB = r"(?<![A-Za-z0-9_-])"

# Known-prefix tokens: (shape, pattern). Every pattern has a left boundary and
# either a fixed length or a `{n,}` floor (a test checks both), and exactly one
# group named `v` -- the value to replace. Ordered from most to least specific
# where two could overlap.
_TOKEN_RULES = tuple(
    (shape, re.compile(_LB + pattern))
    for shape, pattern in (
        # The one this plugin actually carries: nexus API keys are
        # `nx_live_` / `nx_test_` + secrets.token_urlsafe(32) (43 chars).
        ("nexus-key", r"(?P<v>nx_(?:live|test)_[A-Za-z0-9_-]{20,})"),
        # OpenAI-style and everyone who copied the prefix (Anthropic sk-ant-,
        # sk-proj-, luxeno sk-silk-). A fingerprint such as `sk-silk-xzYL…`
        # stays: 9 chars after the dash is below the floor.
        ("openai-key", r"(?P<v>sk-[A-Za-z0-9_-]{20,})"),
        ("stripe-key", r"(?P<v>[sr]k_(?:live|test)_[A-Za-z0-9]{20,})"),
        ("github-token", r"(?P<v>gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{22,})"),
        ("gitlab-token", r"(?P<v>glpat-[A-Za-z0-9_-]{20,})"),
        ("slack-token", r"(?P<v>xox[abprs]-[A-Za-z0-9-]{10,}|xapp-[A-Za-z0-9-]{10,})"),
        ("npm-token", r"(?P<v>npm_[A-Za-z0-9]{36})"),
        ("aws-key", r"(?P<v>AKIA[0-9A-Z]{16})"),
        ("google-key", r"(?P<v>AIza[0-9A-Za-z_-]{35})"),
        # Three base64url segments, the first decoding to `{"` -- the JWT
        # header. Long enough that prose never produces it.
        ("jwt", r"(?P<v>eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})"),
    )
)

# A PEM private key: the whole block, header to footer, is the value. An
# unterminated block (a file cut off mid-key) is key material to the end of the
# text, so the second alternative takes everything that follows the header.
_PRIVATE_KEY = re.compile(
    r"(?P<v>-----BEGIN (?:[A-Z]+ )*PRIVATE KEY-----"
    r"(?:[\s\S]*?-----END (?:[A-Z]+ )*PRIVATE KEY-----|[\s\S]*))"
)

# URL userinfo: only the password half of `scheme://user:password@host` is the
# value. The user, the host and the path stay -- a stored row that reads
# `postgresql://nexus:[redacted:url-userinfo]@db-prod:5432/nexus` still tells
# the reader which database it was.
_USERINFO = re.compile(
    r"(?i)(?<![A-Za-z0-9])[a-z][a-z0-9+.-]*://"
    r"[^\s/:@\"'`<>]*:(?P<v>[^\s/@\"'`<>]{4,})@"
)

# key = value / key: value / key => value, where the key is an identifier
# ENDING in one of the sensitive words (`NEXUS_API_TOKEN`, `client_secret`,
# `--password`, `aws_secret_access_key`) and the value runs to the next
# whitespace, quote or list punctuation. `max_tokens=4096`, `token_count`,
# `password_hash` do not match: the word has to be the END of the identifier.
#
# The left boundary excludes `_`, `.` and `-`: see the module docstring on why
# a run of dashes was quadratic without it.
#
# Horizontal whitespace only, except for a YAML block scalar (`password: >`
# / `password: |` and the value on the next, indented line). A bare
# `password:` at the end of a line followed by an indented block is a nested
# mapping, not a value, and is left alone.
_KV_WORDS = (
    r"api[_-]?key|secret[_-]?key|access[_-]?key|private[_-]?key"
    r"|secret|password|passwd|pwd|token"
)
_KV = re.compile(
    r"(?i)(?<![A-Za-z0-9_.-])(?P<key>[A-Za-z0-9_.-]*?(?P<word>" + _KV_WORDS + r"))"
    r"[\"']?[ \t]*(?:=>|[=:])[ \t]*(?:[|>][ \t]*\n[ \t]+|[\"']?)"
    r"(?P<v>[^\s\"'`,;]{8,})"
)

# `Authorization: Bearer <value>`. `<token>`, `**********` and `${TOKEN}` are
# out by charset; prose (`Bearer Token-Based-Authentication`) by the slug
# gate in _rejected.
_BEARER = re.compile(r"(?i)\bBearer[ \t]+(?P<v>[A-Za-z0-9._~+/=-]{16,})")

# Value shapes that are placeholders, references or words, never secrets. Each
# one was found in a real file (see test_redact.py for the file it came from).
_PLACEHOLDER_PREFIXES = (
    "your",  # your-api-key-here, your-random-secret-key
    "changeme",
    "change-me",
    "change_me",
    "change-this",
    "example",
    "placeholder",
    "dummy",
    "replace",
    "redacted",
    "process.env",  # apiKey: process.env.NEXUS_API_KEY!
    "os.environ",
    "os.getenv",
    "settings.",
    "secrets.",
    "config.",
    "env.",
)
_PLACEHOLDER_WORDS = frozenset(
    {
        "password",
        "passwd",
        "pass",
        "pwd",
        "secret",
        "token",
        "apikey",
        "api_key",
        "api-key",
        "username",
        "user",
        "write",
        "read",
        "none",
        "null",
        "true",
        "false",
        "write-all",
        "read-all",
    }
)
_ALL_DIGITS = re.compile(r"[0-9.]+")
_XXX_TAIL = re.compile(r"[xX]{3,}$")
_SAME_CHAR = re.compile(r"(.)\1*$")
_RUN_TAIL = re.compile(r"(.)\1{5,}$")  # nx_live_000000..., ghp_xxxxxxxx
_MASK_RUN = re.compile(r"\*{3,}")  # ********, ***REDACTED***
_ANGLE_PLACEHOLDER = re.compile(r"<[^<>]*>")  # <token>, re_<your-api-key>
_CALL = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*\(")  # getattr(settings, ...), os.getenv(
# FORGEJO_TOKEN, NEXUS_API_KEY: the NAME of a secret, written where a value
# would go ("secret: FORGEJO_TOKEN inherits ..."). Upper-case words joined by
# underscores and nothing else. A real all-caps secret keeps its digits and has
# no underscores, so it is not caught by this.
_ENV_VAR_NAME = re.compile(r"[A-Z0-9]+(?:_[A-Z0-9]+)+")
# What prose puts after `token:` / `secret:` that is not a secret.
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_UUID = re.compile(r"(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# A bare `token:` / `secret:` in prose followed by hex is a commit hash (7-40
# chars); a hex credential is written as a field (`FORGEJO_TOKEN=...`), which
# this gate does not touch (review R3).
_PROSE_HEX = re.compile(r"(?i)[0-9a-f]{7,40}")
_VERSION = re.compile(r"v?\d+\.\d+(?:\.\d+)?(?:[-+][A-Za-z0-9.-]{0,24})?")
_FILE_LIKE = re.compile(r"\.(?:md|py|json|ya?ml|toml|txt|hcl|sh|ts|js|html|sql|csv|log|env)(?:#\S*)?$")
_QUERY_CONTINUES = re.compile(r"&[A-Za-z_][A-Za-z0-9_]*=")
_SLUG_SEGMENT = re.compile(r"[A-Za-z]+|\d+")

# Where a bare sensitive word sits in a sentence rather than naming a field.
PROSE = "prose"  # `token: ...` / `secret: ...` / `api-key: ...` / `pwd: ...` with nothing before the word
FIELD = "field"  # `NEXUS_API_TOKEN=`, `client_secret:`, and any `password=` (bare or not)
# `pwd:` in prose is the shell's working directory far more often than a
# password; `password:` in prose is a password. So the bare-word list is
# explicit rather than "every sensitive word".
_PROSE_WORDS = frozenset({"token", "secret", "api_key", "api-key", "apikey", "pwd"})
USERINFO = "userinfo"
BEARER = "bearer"


def _has(value, predicate):
    return any(predicate(c) for c in value)


def _rejected(value, kind):
    """The structural reasons a value is not a secret.

    The first block applies everywhere: placeholders, references, code, masks,
    names, URLs, dates, versions, file names. The second only where a bare
    word sits in prose, or in a URL / header position: paths, slugs, short
    commit hashes and small numbers are what those positions carry when they
    do not carry a secret -- while ``password=prod-db-secret-2026`` or
    ``api_key=hunter/22x9/abcdef`` are secrets and must stay caught.
    """
    first = value[0]
    if first in "<[{." or "${" in value or "$(" in value:
        return True  # <token>, [password], {{ x }}, .template.path, ${TOKEN}, $(cat pw)
    if first == "$" and (len(value) == 1 or _ENV_VAR_NAME.fullmatch(value[1:])):
        return True  # $CF_ACCESS_CLIENT_SECRET
    if value[-1] in "([{" or (_CALL.match(value) and value.count("(") != value.count(")")):
        return True  # os.getenv( / key_data.json()[ / getattr(settings -- code cut at a quote or comma; `Tr0ub4dor(3)Xy` is balanced
    if _MASK_RUN.search(value) or _ANGLE_PLACEHOLDER.search(value):
        return True
    if value.startswith(("/", "./", "../", "~/")):
        return True  # private_key: /etc/nexus/id_ed25519, access_key: ~/.aws/credentials (review R3)
    if "…" in value or value.endswith("..."):
        return True  # a truncated display (`sk-nexus-xxx…`) is never the whole secret
    lowered = value.lower()
    if lowered in _PLACEHOLDER_WORDS or lowered.startswith(_PLACEHOLDER_PREFIXES):
        return True
    if "://" in value or _QUERY_CONTINUES.search(value):  # a URL, or `X&include=events`
        return True
    if _SAME_CHAR.fullmatch(value) or _XXX_TAIL.search(value) or _ENV_VAR_NAME.fullmatch(value):
        return True
    if _ISO_DATE.match(value) or _UUID.fullmatch(value) or _VERSION.fullmatch(value):
        return True
    if _FILE_LIKE.search(value):
        return True
    if kind == FIELD:
        return False
    # Prose, userinfo and bearer positions: shape gates.
    segments = re.split(r"[-_]", value)
    if len(segments) >= 3 and all(_SLUG_SEGMENT.fullmatch(s) for s in segments):
        return True  # text-embedding-3-large, Token-Based-Auth-2026-Scheme
    if kind == PROSE:
        if _PROSE_HEX.fullmatch(value):
            return True  # `secret: 4a12ccaa`, `token: <40 hex>` -- a commit hash
        if "/" in value and not _has(value, str.isdigit):
            return True  # refs/aria/coordination, nexus/prod/llm_api_key, application/json
    return False


def _value_ok(value, kind, floor=8):
    """The precision gate for key/value and bearer hits."""
    if len(value) < floor or _rejected(value, kind):
        return False
    if _ALL_DIGITS.fullmatch(value):
        # `token: 15000000` is a budget; `password=86753091234567` and
        # `NEXUS_API_TOKEN=91827364550918273645` are secrets. Only a bare
        # word in prose carries numbers, and those are short.
        return not (kind == PROSE and len(value) <= 16)
    # A short run of letters is a word (`org-level`, `SecretStr`, `write-all`);
    # secrets are either long or carry digits.
    return _has(value, str.isdigit) or len(value) >= 12


def _token_ok(value):
    """A known-prefix token that is not a placeholder: `ghp_xxxxxxxx...`,
    `nx_live_0000...` and `sk-XXXX...` keep the prefix and repeat one
    character; a real one is drawn from a 60+ symbol alphabet."""
    if _XXX_TAIL.search(value) or _RUN_TAIL.search(value):
        return False
    return len(set(value)) >= 8


def _userinfo_ok(value):
    if _rejected(value, USERINFO) or _ALL_DIGITS.fullmatch(value):
        return False  # http://127.0.0.1:8001@proxy: a port, not a password
    # No entropy floor: `nexus`, `s3cret` -- userinfo is unambiguous about
    # where the value is, unlike `token:`.
    return True


_UNBALANCED = ((")", "("), ("]", "["), ("}", "{"))


def _trim(value):
    """Drop sentence punctuation and unbalanced closers from the end of a
    value taken up to whitespace: `(password: hunter22x9)`, `token: abc123456.`"""
    while value:
        last = value[-1]
        if last in ".,;:":
            value = value[:-1]
            continue
        for closer, opener in _UNBALANCED:
            if last == closer and opener not in value:
                value = value[:-1]
                break
        else:
            return value
    return value


def _kv_span(m):
    """The span to replace for a key/value hit, or None."""
    value = _trim(m.group("v"))
    if not value:
        return None
    key, word = m.group("key"), m.group("word")
    if m.string[max(0, m.start("key") - 2) : m.start("key")] == "${":
        return None  # ${POSTGRES_PASSWORD:-nexus}: a shell expansion with a default, not a value
    kind = PROSE if key.lower() == word.lower() and word.lower() in _PROSE_WORDS else FIELD
    # A path is a path: `token: refs/aria/coordination`. But a base64 key can
    # carry `/` too (`api_key=abc123def456/ghi789jkl012`), and the difference
    # is the digits -- a path of words has none.
    if not _value_ok(value, kind):
        return None
    return m.start("v"), m.start("v") + len(value)


def _kv_shape(m):
    return m.group("word").lower().replace("_", "-")


def _whole(check):
    """A span callback for rules whose value is the whole `v` group."""

    def span(m):
        value = m.group("v")
        return (m.start("v"), m.end("v")) if check(value) else None

    return span


def _sub_value(pattern, text, shape_of, span_of):
    """Replace the span `span_of(match)` of every match. Returns (text, n)."""
    out = []
    last = 0
    count = 0
    for m in pattern.finditer(text):
        span = span_of(m)
        if span is None:
            continue
        start, end = span
        out.append(text[last:start])
        out.append(REDACTED_FORMAT.format(shape=shape_of(m)))
        last = end
        count += 1
    if not count:
        return text, 0
    out.append(text[last:])
    return "".join(out), count


# Applied in this order. The PEM block first because key material can contain
# anything; the prefix tokens before key/value so that `OPENAI_API_KEY=sk-...`
# is reported under its precise shape; userinfo before key/value so that
# `DATABASE_URL=postgresql://u:p@h` is handled as a URL, not as `URL=` (it is
# not a sensitive key anyway).
def _rules():
    yield _PRIVATE_KEY, (lambda _m: "private-key"), _whole(lambda _v: True)
    for shape, pattern in _TOKEN_RULES:
        yield pattern, (lambda _m, s=shape: s), _whole(_token_ok)
    yield _USERINFO, (lambda _m: "url-userinfo"), _whole(_userinfo_ok)
    yield _KV, _kv_shape, _kv_span
    # Real bearer tokens (JWT, base64, hex) carry digits; `Bearer
    # authentication-scheme` does not, and the slug gate covers the rest.
    yield _BEARER, (lambda _m: "bearer"), _whole(
        lambda v: _value_ok(v, BEARER, floor=16) and _has(v, str.isdigit)
    )


def redact_text(text):
    """Redact every secret-shaped value in ``text``. Returns ``(text, hits)``.

    Anything that is not a string comes back untouched with 0 hits.
    Idempotent: the replacement text matches no rule, so a second pass is a
    no-op (a test pins this -- a redactor that re-redacts its own output would
    inflate the ledger count on every sync).
    """
    if not isinstance(text, str) or not text:
        return text, 0
    total = 0
    for pattern, shape_of, span_of in _rules():
        text, n = _sub_value(pattern, text, shape_of, span_of)
        total += n
    return text, total


def find(text):
    """What ``redact_text`` would replace, as ``[(shape, value), ...]``.

    For tests and the corpus scan. The values are returned in the clear to the
    caller, who is responsible for masking them before printing anything.
    ``len(find(t)) == redact_text(t)[1]`` always: later rules see the text
    with earlier hits replaced, exactly as ``redact_text`` does, so a token
    inside a key/value pair is reported once (a test pins this).
    """
    found = []
    if not isinstance(text, str) or not text:
        return found
    for pattern, shape_of, span_of in _rules():
        for m in pattern.finditer(text):
            span = span_of(m)
            if span is not None:
                found.append((shape_of(m), text[span[0] : span[1]]))
        text, _ = _sub_value(pattern, text, shape_of, span_of)
    return found


# Deeper than any real payload; a structure past this is not data to sync but
# something to refuse -- and refusing must be visible, not an exception the
# hooks' blanket handler turns into a silent skip.
_MAX_DEPTH = 64


def redact_object(obj, _depth=0):
    """Redact every string inside a JSON-shaped value. Returns ``(copy, hits)``.

    Dicts are walked by value (keys are field names, never secrets, and
    changing them would break the row); lists, tuples and sets element-wise
    (a namedtuple comes back as a plain tuple: rebuilding it by type raises,
    and raising here is the silent failure this module exists to prevent);
    strings through ``redact_text``; everything else is returned as is. The
    input is not mutated -- the caller may still need the original for
    hashing. Past ``_MAX_DEPTH`` the subtree is replaced by a marker string
    and counted as a hit: fail closed, and say so.
    """
    if isinstance(obj, str):
        return redact_text(obj)
    if isinstance(obj, (dict, list, tuple, set, frozenset)):
        if _depth >= _MAX_DEPTH:
            return REDACTED_FORMAT.format(shape="too-deep"), 1
        total = 0
        if isinstance(obj, dict):
            out = {}
            for key, value in obj.items():
                out[key], n = redact_object(value, _depth + 1)
                total += n
            return out, total
        items = []
        for value in obj:
            item, n = redact_object(value, _depth + 1)
            items.append(item)
            total += n
        if isinstance(obj, list):
            return items, total
        if isinstance(obj, tuple):
            return tuple(items), total
        return (frozenset(items) if isinstance(obj, frozenset) else set(items)), total
    return obj, 0
