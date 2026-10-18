# Context management for agents

The repository, not the conversation, holds durable state: [`implementation-status.md`](../plans/implementation-status.md), any [active plan](../plans/active/README.md), the branch, and the PR. Shrinking the context is therefore always safe once that state is written down.

- **Compact** (`/compact` in Claude Code or Codex) when the context is roughly two-thirds full, after a PR merges, or after a review pass ends. Do it between steps, never in the middle of an edit or while chasing a failing check.
- **Keep** in the summary: the branch, its parent branch, and the PR link; the active plan and step, if any; files changed and why; checks run with their result and the exact failing test or error; open review findings; and user decisions or open questions not yet recorded in the repository.
- **Drop**: full file contents, passing test output, old command logs, superseded attempts, and documents that can simply be re-read.
- **Purge** (`/clear` or a new session) instead of compacting when switching to an unrelated PR or stack, or when the context is mostly a dead end. Before purging, record progress in `implementation-status.md`, the active plan, or the PR description.
- If a summary would lose an unrecorded decision, write it to the LLD, a decision record, or the status file first.
- After any compact or purge, re-read `AGENTS.md` and the status file, and check the branch and `git status` before editing.
