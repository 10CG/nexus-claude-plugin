"""Tests for the shared identity primitives.

Isolation rule for this file (TASK-011 pre-merge audit): CI runs every hook
test in ONE process, so use mock.patch / addCleanup only — never
save-and-restore by hand, and never leave an env var popped.
"""

import json
import os
import socket
import subprocess
import tempfile
import unittest
from unittest import mock

import _identity

# The real file on this machine, for the shape test below.
ARIA_FILE_SAMPLE = """\
# Aria container identity (auto-generated 2026-05-24T16:59:24Z)
# Edit the `label` line to add a human-readable tag
# NOTE: label MUST stay empty on this machine
uuid: bfe8285d
label:
created_at: 2026-05-24T16:59:24Z
"""


def _mentions(source, needle):
    """`needle in source`, as a bool.

    Not assertIn / assertNotIn: on failure those print the whole haystack, and
    the haystack here is an entire hook source file -- 30 KB of output hiding a
    one-line answer.
    """
    return needle in source


def _read_sibling(name):
    """Read a hook module's source for a derivation-parity assertion."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class TestContainerId(unittest.TestCase):
    def test_env_wins(self):
        """Use a value that cannot equal the hostname.

        The first draft used this machine's actual hostname (dev-claude2), so
        an implementation that ignored the variable entirely still passed --
        the injection matrix caught it.
        """
        sentinel = "container-from-env-not-hostname"
        self.assertNotEqual(sentinel, socket.gethostname())
        with mock.patch.dict(os.environ, {"NEXUS_CONTAINER_ID": sentinel}):
            self.assertEqual(_identity.container_id(), sentinel)

    def test_falls_back_to_hostname(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NEXUS_CONTAINER_ID", None)
            with mock.patch("socket.gethostname", return_value="some-host"):
                self.assertEqual(_identity.container_id(), "some-host")

    def test_the_hooks_no_longer_carry_their_own_derivation(self):
        """One rule, one place (TASK-002).

        Until TASK-002 this test pinned the *text* of session_capture's copy,
        because two independent derivations that merely agree are the setup
        for the read side grouping one container as two. The copies are gone;
        what is left to guard is somebody re-growing one. That each hook really
        routes through this module is asserted behaviourally in its own suite.
        """
        for name in ("session_capture.py", "session_inject.py"):
            with self.subTest(hook=name):
                source = _read_sibling(name)
                self.assertFalse(
                    _mentions(source, "socket.gethostname"), "derives the hostname itself"
                )
                self.assertFalse(
                    _mentions(source, 'NEXUS_CONTAINER_ID")'), "reads NEXUS_CONTAINER_ID itself"
                )
                self.assertTrue(
                    _mentions(source, "_identity.container_id()"),
                    "does not call _identity.container_id()",
                )


class TestAriaUuid(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _write(self, text):
        path = os.path.join(self.tmp.name, "container-id")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        return path

    def test_reads_the_uuid_line(self):
        self.assertEqual(_identity.aria_uuid(self._write(ARIA_FILE_SAMPLE)), "bfe8285d")

    def test_label_never_overrides_uuid(self):
        """Aria's own helper prefers a non-empty label; this one must not.

        Handoff frontmatter pins the uuid, so a machine that sets a label would
        otherwise stop recognising its own documents and silently fail the
        owner check open.
        """
        labelled = ARIA_FILE_SAMPLE.replace("label:", "label: devbox-A")
        self.assertEqual(_identity.aria_uuid(self._write(labelled)), "bfe8285d")

    def test_comment_mentioning_uuid_is_not_parsed(self):
        text = "# uuid: deadbeef is the old one\nuuid: bfe8285d\n"
        self.assertEqual(_identity.aria_uuid(self._write(text)), "bfe8285d")

    def test_uuid_line_is_lowercased(self):
        """The other half of the case-insensitive comparison.

        owner_container_uuid lowercases the document side; leaving this side
        alone revives the same silent `not_owner` from the local side. macOS
        uuidgen emits uppercase, and the shape check already anticipates a
        move to full uuids.
        """
        self.assertEqual(_identity.aria_uuid(self._write("uuid: BFE8285D\n")), "bfe8285d")

    def test_missing_file_is_none(self):
        self.assertIsNone(_identity.aria_uuid(os.path.join(self.tmp.name, "nope")))

    def test_empty_uuid_value_is_none(self):
        self.assertIsNone(_identity.aria_uuid(self._write("uuid:\nlabel:\n")))

    def test_no_uuid_line_is_none(self):
        self.assertIsNone(_identity.aria_uuid(self._write("label: x\ncreated_at: y\n")))


class TestOwnerContainerUuid(unittest.TestCase):
    def test_two_segment_form(self):
        self.assertEqual(_identity.owner_container_uuid("simonfish/bfe8285d"), "bfe8285d")

    def test_owner_half_is_ignored(self):
        """The owner half drifts — this repo has seen the two containers swap
        it — so only the uuid half is comparable."""
        self.assertEqual(_identity.owner_container_uuid("someone-else/bfe8285d"),
                         _identity.owner_container_uuid("simonfish/bfe8285d"))

    def test_comparison_is_case_insensitive(self):
        """Frontmatter casing must not decide ownership.

        An uppercased uuid would otherwise be unequal to aria_uuid()'s
        lowercase one, land in `not_owner`, and stop ingestion silently.
        """
        self.assertEqual(_identity.owner_container_uuid("SIMONFISH/BFE8285D"), "bfe8285d")
        self.assertEqual(
            _identity.owner_container_uuid("simonfish/BFE8285D"),
            _identity.owner_container_uuid("simonfish/bfe8285d"),
        )

    def test_hostname_form_is_not_comparable(self):
        """42 of this repo's 96 handoffs carry a hostname here.

        Returning it would make the owner check merely false -> `not_owner` ->
        an expected skip that is never reported, so handoff ingestion would
        stop dead and say nothing. None sends the caller to
        `identity_unresolved`, which is reported.
        """
        for value in ("simonfish/dev-claude", "simonfish/dev-claude2",
                      "creationhikari/dev-claude2", "simonfish/devbox-A"):
            self.assertIsNone(_identity.owner_container_uuid(value), value)

    def test_hex_shaped_hostnames_that_can_be_rejected_are(self):
        """The widths a general 8-32 hex rule would have let through.

        Docker's default hostname is its 12-hex short id, and a sha prefix is
        another common one; both would otherwise read as comparable identities
        and land back in the silent not_owner case.
        """
        for value in ("a1b2c3d4e5f6", "0123456789abcdef0123", "abcdef1234567"):
            self.assertIsNone(_identity.owner_container_uuid(f"owner/{value}"), value)

    def test_an_eight_hex_hostname_is_indistinguishable_and_accepted(self):
        """The residual ambiguity, recorded rather than papered over.

        `deadbeef` is exactly the shape aria emits, so no check can reject it
        without rejecting real uuids. A host named that would compare unequal
        and fall into the quiet `not_owner` path -- the failure this shape
        check narrows but cannot close. Narrowing further would trade a rare
        silent skip for routine false `identity_unresolved` noise on every
        legitimate uuid, which is the worse deal.
        """
        self.assertEqual(_identity.owner_container_uuid("owner/deadbeef"), "deadbeef")

    def test_the_two_shapes_aria_emits_are_accepted(self):
        self.assertEqual(_identity.owner_container_uuid("o/bfe8285d"), "bfe8285d")
        full = "bfe8285d-1234-5678-9abc-def012345678"
        self.assertEqual(_identity.owner_container_uuid(f"o/{full}"), full)

    def test_bare_uuid(self):
        self.assertEqual(_identity.owner_container_uuid("bfe8285d"), "bfe8285d")

    def test_empty_and_none(self):
        self.assertIsNone(_identity.owner_container_uuid(None))
        self.assertIsNone(_identity.owner_container_uuid(""))
        self.assertIsNone(_identity.owner_container_uuid("   "))
        self.assertIsNone(_identity.owner_container_uuid("owner/"))


class TestProjectSlugAndUserId(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_slug_normalization(self):
        self.assertEqual(_identity.normalize_slug("My Project!"), "my-project")
        self.assertEqual(_identity.normalize_slug("  nexus  "), "nexus")
        self.assertEqual(_identity.normalize_slug("!!!"), "default")

    def test_slug_uses_git_toplevel_not_cwd(self):
        """Derived from the toplevel so a subdirectory yields the same project.

        This is the guard behind the orphan reconciliation: if a hook started
        from a subdirectory produced a different project than the one the rows
        are keyed by, it would list zero local files and read that as mass
        deletion.
        """
        repo = os.path.join(self.tmp.name, "therepo")
        sub = os.path.join(repo, "deep", "nested")
        os.makedirs(sub)
        subprocess.run(["git", "init", "-q", repo], check=True, capture_output=True)
        self.assertEqual(_identity.project_slug(sub), "therepo")
        self.assertEqual(_identity.project_slug(repo), "therepo")

    def test_slug_falls_back_to_basename_outside_a_repo(self):
        plain = os.path.join(self.tmp.name, "NotARepo")
        os.makedirs(plain)
        with mock.patch("subprocess.run", side_effect=OSError("no git")):
            self.assertEqual(_identity.project_slug(plain), "notarepo")

    def test_user_id_env_wins(self):
        with mock.patch.dict(os.environ, {"NEXUS_DEFAULT_USER_ID": "pinned"}):
            self.assertEqual(_identity.user_id(self.tmp.name), "pinned")

    def test_user_id_defaults_to_project_slug(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NEXUS_DEFAULT_USER_ID", None)
            with mock.patch.object(_identity, "project_slug", return_value="theproj"):
                self.assertEqual(_identity.user_id("/anywhere"), "theproj")

    def test_user_id_and_project_share_one_source(self):
        """Not just equal today — the same call.

        Two independent derivations that happen to agree are the setup for the
        mass-deletion failure above; the point is that there is one.
        """
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NEXUS_DEFAULT_USER_ID", None)
            with mock.patch.object(_identity, "project_slug",
                                   return_value="x") as slug:
                _identity.user_id("/somewhere")
        slug.assert_called_once_with("/somewhere")

    def test_the_hooks_no_longer_carry_their_own_slug(self):
        """The three byte-identical copies collapsed onto this module in
        TASK-002. A `def _project_slug` reappearing in a hook is the mass-
        deletion failure above being set up again."""
        for name in ("session_capture.py", "session_inject.py"):
            with self.subTest(hook=name):
                source = _read_sibling(name)
                self.assertFalse(_mentions(source, "def _project_slug"), "re-grew _project_slug")
                self.assertFalse(
                    _mentions(source, "def _normalize_slug"), "re-grew _normalize_slug"
                )
                self.assertFalse(
                    _mentions(source, 'NEXUS_DEFAULT_USER_ID")'),
                    "reads NEXUS_DEFAULT_USER_ID itself",
                )
                self.assertTrue(
                    _mentions(source, "_identity.user_id("), "does not call _identity.user_id()"
                )


class TestPluginVersion(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.manifest = os.path.join(self.tmp.name, "plugin.json")

    def _with_manifest(self, text):
        with open(self.manifest, "w", encoding="utf-8") as fh:
            fh.write(text)
        return mock.patch.object(_identity, "PLUGIN_MANIFEST", self.manifest)

    def test_reads_the_real_manifest(self):
        """Read independently here, so the two can only agree by both being
        right -- not by sharing a parser."""
        real = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", ".claude-plugin", "plugin.json"
        )
        with open(real, encoding="utf-8") as fh:
            expected = json.load(fh)["version"]
        self.assertRegex(expected, r"^\d+\.\d+\.\d+")
        self.assertEqual(_identity.plugin_version(), expected)

    def test_missing_manifest_is_unknown_not_an_exception(self):
        with mock.patch.object(_identity, "PLUGIN_MANIFEST", self.manifest + ".absent"):
            self.assertEqual(_identity.plugin_version(), "unknown")

    def test_unparsable_or_wrongly_shaped_manifest_is_unknown(self):
        for text in ("{not json", "[]", '{"version": 7}', '{"version": ""}', "{}"):
            with self.subTest(manifest=text), self._with_manifest(text):
                self.assertEqual(_identity.plugin_version(), "unknown")

    def test_a_version_that_is_not_header_safe_is_unknown(self):
        """This value goes into an HTTP header. urllib rejects a header value
        containing a newline by raising, which would fail the whole remote
        call -- and the run with it -- over a cosmetic field."""
        # "0.5.0\n" is the one that got through the first version of this
        # check: `$` matches before a trailing newline, so an anchored pattern
        # accepted it and the request died on "Invalid header value". Found by
        # the pre-merge review, not by the cases that were here.
        for bad in ("1.0\nX-Evil: 1", "1.0 beta", "1.0/2", "v" * 40, "1.0\r",
                    "0.5.0\n", "\n0.5.0", "0.5.0\t"):
            with self.subTest(version=bad), self._with_manifest(json.dumps({"version": bad})):
                self.assertEqual(_identity.plugin_version(), "unknown")

    def test_ordinary_prerelease_versions_survive(self):
        for good in ("0.5.0", "1.2.3-rc.1", "1.2.3+build5"):
            with self.subTest(version=good), self._with_manifest(json.dumps({"version": good})):
                self.assertEqual(_identity.plugin_version(), good)


class TestSourceHeader(unittest.TestCase):
    def test_shape_is_name_slash_version(self):
        with mock.patch.object(_identity, "plugin_version", return_value="9.9.9"):
            self.assertEqual(
                _identity.source_header("sessionstart-hook"), "sessionstart-hook/9.9.9"
            )

    def test_the_name_half_survives_the_backend_split(self):
        """The backend attributes a request by `raw.split("/", 1)[0]` against
        an allowlist (nexus `mcp_attribution._normalize_source`). Whatever this
        returns, that split has to give back the bare hook name -- including
        when the version could not be read."""
        with mock.patch.object(_identity, "plugin_version", return_value="unknown"):
            header = _identity.source_header("session-capture-hook")
        self.assertEqual(header.split("/", 1)[0], "session-capture-hook")
        self.assertEqual(header.count("/"), 1)


if __name__ == "__main__":
    unittest.main()
