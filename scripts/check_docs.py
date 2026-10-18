"""Check that the repository's documentation still matches the checkout.

Checked, outside fenced examples:
- Every relative link in README.md, AGENTS.md, and docs/**/*.md points at an existing path.
- Every repository path written in backticks in AGENTS.md exists, and AGENTS.md names every `internal/` package.
- Every `make <target>` named in those files is a Makefile target. Completed plans are history and are not checked.
- Decision records in docs/decisions have unique numbers and are all listed in docs/decisions/README.md.

The copies of processing-service documents (docs/specs/processing-service-copies.md) are byte-for-byte external
references whose links point into the processing repository, so they are skipped.
Files that git ignores are left out of the repository-path and decision checks.
"""
from pathlib import Path
import fnmatch
import re
import subprocess
import sys
from urllib.parse import unquote

EXTERNAL_COPIES = {"structured-file-processing-lld.md", "structured-file-processing-spec.md"}
BACKTICK_TOKEN = re.compile(r"`([^`\s]+)`")
MAKE_COMMAND = re.compile(r"`make ([A-Za-z0-9_-]+)")
MAKE_TARGET = re.compile(r"^([A-Za-z0-9_-]+)\s*:(?!=)", re.MULTILINE)
DECISION_FILE = re.compile(r"^(\d{4})-.+\.md$")


def documents(root):
    """Return the Markdown files this check covers, without the external processing-service copies."""
    paths = [root / "README.md", root / "AGENTS.md", *sorted(root.glob("docs/**/*.md"))]
    return [path for path in paths if path.exists() and path.name not in EXTERNAL_COPIES]


def prose(path):
    """Return the text of a Markdown file with fenced examples removed."""
    return re.sub(r"```.*?```", "", path.read_text(encoding="utf-8"), flags=re.S)


def link_findings(root):
    """Return relative links that point at missing paths."""
    findings = []
    for path in documents(root):
        for target in re.findall(r"\]\(([^)]+)\)", prose(path)):
            target = unquote(target.strip("<>").split("#")[0])
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            if not (path.parent / target).exists():
                findings.append(f"{path.relative_to(root)}: broken link {target}")
    return findings


def visible_files(root):
    """Return the repository-relative paths of files git does not ignore, or of every file outside a git checkout."""
    try:
        listed = subprocess.run(
            ["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            capture_output=True, check=True, text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return {path.relative_to(root).as_posix() for path in root.rglob("*")
                if path.is_file() and ".git" not in path.parts}
    # A tracked file deleted in the working tree no longer counts as present.
    return {path for path in listed.split("\0") if path and (root / path).exists()}


def path_exists(path, files):
    """Return whether a repository path, directory, or glob matches at least one visible file."""
    path = path.rstrip("/")
    if any(character in path for character in "*?["):
        return any(fnmatch.fnmatch(name, path) for name in files)
    return path in files or any(name.startswith(path + "/") for name in files)


def agents_findings(root, files):
    """Return AGENTS.md paths that do not exist and internal packages that AGENTS.md does not name."""
    path = root / "AGENTS.md"
    if not path.exists():
        return []
    tokens = set(BACKTICK_TOKEN.findall(prose(path)))
    top_level = {name.split("/")[0] for name in files}
    findings = []
    for token in sorted(tokens):
        placeholder = any(character in token for character in "<>{}")
        if "/" in token and not placeholder and token.split("/")[0] in top_level and not path_exists(token, files):
            findings.append(f"AGENTS.md: `{token}` does not exist. Update AGENTS.md to match the checkout, "
                            "or restore the path.")
    named = {token.rstrip("/") for token in tokens}
    packages = {"internal/" + name.split("/")[1] for name in files
                if name.startswith("internal/") and name.count("/") >= 2}
    for package in sorted(packages - named):
        findings.append(f"AGENTS.md: package `{package}` is not in the repository map. "
                        "Add it with a one-line description of what it owns.")
    return findings


def make_findings(root):
    """Return `make` targets named in current documents that the Makefile does not define."""
    makefile = root / "Makefile"
    if not makefile.exists():
        return []
    targets = set(MAKE_TARGET.findall(makefile.read_text(encoding="utf-8")))
    findings = []
    for path in documents(root):
        if "plans/completed" in path.as_posix():
            continue
        for target in sorted(set(MAKE_COMMAND.findall(path.read_text(encoding="utf-8")))):
            if target not in targets:
                findings.append(f"{path.relative_to(root)}: `make {target}` is not a Makefile target. "
                                "Fix the command in the document, or add the target and list it in AGENTS.md.")
    return findings


def decision_findings(root, files):
    """Return duplicate decision numbers and decision records missing from the decisions index."""
    index_path = root / "docs/decisions/README.md"
    records = sorted(name.split("/")[-1] for name in files
                     if name.startswith("docs/decisions/") and DECISION_FILE.match(name.split("/")[-1]))
    index = index_path.read_text(encoding="utf-8") if index_path.exists() else ""
    findings = []
    numbers = {}
    for name in records:
        numbers.setdefault(DECISION_FILE.match(name).group(1), []).append(name)
        if f"({name})" not in index:
            findings.append(f"docs/decisions/{name} is not listed in docs/decisions/README.md. "
                            f"Add a line: - [{name[:4]} — Title]({name})")
    for number, names in sorted(numbers.items()):
        if len(names) > 1:
            findings.append(f"docs/decisions: number {number} is used by {', '.join(names)}. "
                            "Give the newer record the next free number and update its links.")
    return findings


def main(root=None):
    """Print every documentation problem under root and return 1 if there is any, else 0."""
    root = Path(root) if root else Path(__file__).resolve().parents[1]
    files = visible_files(root)
    findings = link_findings(root) + agents_findings(root, files) + make_findings(root) + decision_findings(root, files)
    for finding in findings:
        print(finding)
    if findings:
        return 1
    print("Documentation matches the checkout: links, AGENTS.md paths and packages, make targets, and decision "
          "records (processing-service copies excluded).")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
