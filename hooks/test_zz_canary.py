"""TEMPORARY canary — proves the glob picks up a file the old lists never named.

The previous workflow enumerated nine `paths:` entries and three test
invocations by hand. A new file was in neither, so it could land with CI green
and its tests never run. This file exists only to make that visible: it fails
on purpose, so a RED run proves both halves (the push/PR trigger fired AND the
test actually executed).

Deleted in the next commit on this branch; the red/green pair is recorded in
the PR description.
"""

import unittest


class TestCanaryMustFail(unittest.TestCase):
    def test_this_file_is_executed_by_ci(self):
        self.fail(
            "canary: if you see this in CI, the glob picked up a file that the "
            "old hand-written lists did not name -- which is the point"
        )


if __name__ == "__main__":
    unittest.main()
