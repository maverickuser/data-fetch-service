"""Tests for check_docs.py."""
import contextlib
import io
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check_docs  # noqa: E402


class CheckDocsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def write(self, name, text=""):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def check(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            status = check_docs.main(self.root)
        return status, output.getvalue()

    def test_existing_relative_link_passes(self):
        self.write("docs/a.md", "a")
        self.write("README.md", "[a](docs/a.md#part)")
        self.assertEqual(self.check()[0], 0)

    def test_missing_relative_link_fails(self):
        self.write("README.md", "[a](docs/missing.md)")
        self.assertEqual(self.check()[0], 1)

    def test_links_in_fences_and_external_copies_are_skipped(self):
        self.write("README.md", "```\n[a](missing.md)\n```\n[b](https://example.com)")
        self.write("docs/specs/structured-file-processing-lld.md", "[a](../../src/missing.md)")
        self.assertEqual(self.check()[0], 0)

    def test_agents_paths_that_exist_and_placeholders_pass(self):
        self.write("internal/pull/pull.go")
        self.write("AGENTS.md", "`internal/pull`, `docs/<slug>`, `{exchangeName}/x.csv`, `/docs`.")
        self.assertEqual(self.check()[0], 0)

    def test_agents_path_that_is_gone_fails(self):
        self.write("internal/pull/pull.go")
        self.write("AGENTS.md", "`internal/pull`, `internal/orchestration`.")
        status, output = self.check()
        self.assertEqual(status, 1)
        self.assertIn("`internal/orchestration` does not exist", output)

    def test_unlisted_internal_package_fails(self):
        self.write("internal/pull/pull.go")
        self.write("internal/queue/queue.go")
        self.write("AGENTS.md", "`internal/pull`")
        status, output = self.check()
        self.assertEqual(status, 1)
        self.assertIn("`internal/queue` is not in the repository map", output)

    def test_make_target_that_exists_passes(self):
        self.write("Makefile", "lint:\n\tgo vet ./...\nGO ?= go\n")
        self.write("AGENTS.md", "Run `make lint`.")
        self.assertEqual(self.check()[0], 0)

    def test_make_target_that_is_missing_fails_but_completed_plans_are_history(self):
        self.write("Makefile", "lint:\n\tgo vet ./...\nGO ?= go\n")
        self.write("docs/plans/completed/old-pr.md", "Ran `make retired`.")
        self.assertEqual(self.check()[0], 0)
        self.write("docs/runbook.md", "Run `make GO`.")
        self.assertEqual(self.check()[0], 1)

    def test_indexed_decisions_with_unique_numbers_pass(self):
        self.write("docs/decisions/0001-a.md", "# 0001")
        self.write("docs/decisions/README.md", "- [0001](0001-a.md)\n")
        self.assertEqual(self.check()[0], 0)

    def test_decision_missing_from_the_index_fails(self):
        self.write("docs/decisions/0001-a.md", "# 0001")
        self.write("docs/decisions/README.md", "# Decisions\n")
        self.assertEqual(self.check()[0], 1)

    def test_duplicate_decision_number_fails(self):
        self.write("docs/decisions/0001-a.md", "# 0001")
        self.write("docs/decisions/0001-b.md", "# 0001")
        self.write("docs/decisions/README.md", "- [a](0001-a.md)\n- [b](0001-b.md)\n")
        status, output = self.check()
        self.assertEqual(status, 1)
        self.assertIn("number 0001 is used by", output)


if __name__ == "__main__":
    unittest.main()
