"""Tests for the shared identity primitives.

Isolation rule for this file (TASK-011 pre-merge audit): CI runs every hook
test in ONE process, so use mock.patch / addCleanup only — never
save-and-restore by hand, and never leave an env var popped.
"""

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

    def test_matches_session_capture_derivation(self):
        """Same rule as the hook that writes observation rows.

        If these two ever diverge, the read side groups one container as two
        and the aggregator tags rows the injection recipe will not match.
        """
        source = _read_sibling("session_capture.py")
        self.assertIn('os.environ.get("NEXUS_CONTAINER_ID") or socket.gethostname()', source)


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

    def test_matches_session_capture_user_id_derivation(self):
        source = _read_sibling("session_capture.py")
        self.assertIn('os.environ.get("NEXUS_DEFAULT_USER_ID") or _project_slug(cwd)', source)


if __name__ == "__main__":
    unittest.main()
