"""Check relative Markdown file links outside fenced examples."""
from pathlib import Path
import re
from urllib.parse import unquote

root = Path(__file__).resolve().parents[1]
missing = []
for path in [root / "README.md", root / "AGENTS.md", *root.glob("docs/**/*.md")]:
    # External processor LLD is preserved byte-for-byte and has upstream-relative links.
    if path.name == "structured-file-processing-lld.md":
        continue
    text = re.sub(r"```.*?```", "", path.read_text(), flags=re.S)
    for target in re.findall(r"\]\(([^)]+)\)", text):
        target = unquote(target.strip("<>").split("#")[0])
        if not target or "://" in target or target.startswith("mailto:"):
            continue
        if not (path.parent / target).exists():
            missing.append(f"{path.relative_to(root)}: {target}")
if missing:
    raise SystemExit("\n".join(missing))
print("Relative document links passed (external processor LLD excluded).")
