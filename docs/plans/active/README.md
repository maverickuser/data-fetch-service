# Active plans

A change that takes more than one PR, or more than one session, gets a plan here before work starts. A one-PR change needs no plan; its PR description is enough.

- Name the file after the change: `<slug>.md`, matching the branch slug.
- Start with the goal, the decision record it implements (if any), and the PRs in dependency order. Under each PR, list its scope, the tests it adds, and any operational effect.
- Keep a short progress log at the end: what merged, what was found, and what changed in the plan. Update it in every PR of the change, so a new session can resume from the file alone.
- When the last PR merges, move the file to [`../completed/`](../completed) in that PR and record the outcome in [`implementation-status.md`](../implementation-status.md).

There are no active plans. The original build plan and its per-PR descriptions are in [completed](../completed/data-fetch-service-implementation-plan.md).
