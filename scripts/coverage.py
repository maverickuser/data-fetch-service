"""Enforce weighted unit statement coverage without rounding."""
import sys
from pathlib import Path

def check(text):
    """Reject empty or malformed profiles and require strictly more than 95%."""
    lines = text.splitlines()
    if not lines or lines[0] != "mode: atomic":
        raise ValueError("expected atomic coverage profile")
    covered = total = 0
    seen = set()
    for line in lines[1:]:
        location, statements, count = line.split()
        statements, count = int(statements), int(count)
        if location in seen or statements < 0 or count < 0:
            raise ValueError("invalid or duplicate coverage block")
        seen.add(location)
        total += statements
        if count:
            covered += statements
    if total == 0 or covered * 100 <= total * 95:
        raise ValueError(f"coverage must exceed 95%: {covered}/{total}")
    return covered, total

if __name__ == "__main__":
    print("Covered/total statements:", check(Path(sys.argv[1]).read_text()))
