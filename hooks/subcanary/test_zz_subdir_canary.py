"""TEMPORARY canary #2 — proves the new predicate runs IN CI, not just locally.

This file sits in a subdirectory with no __init__.py, so `unittest discover`
does not recurse into it and would report success having never run it. The
gate is supposed to notice the file contributed nothing and fail.

If CI goes RED naming this path, the set-comparison predicate is live in CI.
If CI goes GREEN, the predicate is not doing what the local injection showed.
Deleted in the next commit; both readings recorded in the PR.
"""

import unittest


class TestSubdirCanary(unittest.TestCase):
    def test_should_never_be_silently_skipped(self):
        self.fail("if this ran, discover recursed after all")


if __name__ == "__main__":
    unittest.main()
