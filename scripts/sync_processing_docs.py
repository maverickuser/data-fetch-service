#!/usr/bin/env python3
"""Copy the processing service's design documents into docs/specs, or check that the copies are current.

Usage: sync_processing_docs.py [--check] [--ref REF] PROCESSING_CHECKOUT

Files are read from REF (default origin/main) of the processing-service git checkout, never from its working
tree, so only merged content is copied. The source commit is recorded in docs/specs/processing-service-copies.md.
"""

import argparse
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
RECORD = ROOT / "docs/specs/processing-service-copies.md"
COPIES = [
    "docs/specs/structured-file-processing-lld.md",
    "docs/specs/structured-file-processing-spec.md",
    "docs/specs/data-processing-service-openapi.yaml",
    "docs/specs/data-processing-service-openapi.json",
]
SOURCE_LINE = re.compile(r"^Source commit: .*$", re.MULTILINE)


def git(checkout, *arguments):
    """Return the stdout bytes of a git command run in the processing checkout."""
    return subprocess.run(["git", "-C", str(checkout), *arguments], capture_output=True, check=True).stdout


def main(argv):
    """Sync or check the copies; return the process exit status."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=pathlib.Path, help="path to a data-processing-service git checkout")
    parser.add_argument("--ref", default="origin/main", help="commit or branch to copy from (default origin/main)")
    parser.add_argument("--check", action="store_true", help="report stale copies without changing anything")
    options = parser.parse_args(argv)

    commit = git(options.checkout, "rev-parse", "--short=12", options.ref).decode().strip()
    date = git(options.checkout, "show", "-s", "--format=%cs", commit).decode().strip()
    stale = []
    for path in COPIES:
        source = git(options.checkout, "show", f"{commit}:{path}")
        target = ROOT / path
        if not target.exists() or target.read_bytes() != source:
            stale.append(path)
            if not options.check:
                target.write_bytes(source)

    if options.check:
        if stale:
            print(f"Stale copies of data-processing-service {options.ref} ({commit}):", *stale, sep="\n  ")
            print(f"Run: python3 scripts/sync_processing_docs.py {options.checkout}")
            return 1
        print(f"Processing-service copies match {options.ref} ({commit}).")
        return 0

    record = RECORD.read_text(encoding="utf-8")
    RECORD.write_text(SOURCE_LINE.sub(f"Source commit: `{commit}` ({options.ref}, committed {date}).", record, count=1),
                      encoding="utf-8")
    print(f"Copied {len(stale)} changed file(s) from {options.ref} ({commit}).", *stale, sep="\n  ")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
