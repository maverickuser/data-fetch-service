"""Enforce weighted unit statement coverage without rounding."""
import sys
from pathlib import Path

def check(text):
    """Reject empty or malformed profiles and require strictly more than 95%."""
    lines = text.splitlines()
    if not lines or lines[0] != "mode: atomic":
        raise ValueError("expected atomic coverage profile")
    covered = total = 0
    blocks = {}
    for line in lines[1:]:
        location, statements, count = line.split()
        statements, count = int(statements), int(count)
        if statements < 0 or count < 0:
            raise ValueError("invalid coverage block")
        previous = blocks.get(location)
        if previous is not None and previous[0] != statements:
            raise ValueError("duplicate coverage block has conflicting statement counts")
        blocks[location] = (statements, max(count, previous[1] if previous else 0))
    for statements, count in blocks.values():
        total += statements
        if count:
            covered += statements
    if total == 0 or covered * 100 <= total * 95:
        raise ValueError(f"coverage must exceed 95%: {covered}/{total}")
    return covered, total

if __name__ == "__main__":
    print("Covered/total statements:", check(Path(sys.argv[1]).read_text()))
