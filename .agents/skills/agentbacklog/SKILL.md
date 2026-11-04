---
name: agentbacklog
description: Use the AgentBacklog feature queue — shared FIFO queue between a user and dev agents (add/pop/claim/done/release/bump/depend/submit-plan/claim-review/verdict/list)
argument-hint: "[add|pop|claim|done|release|bump|depend|submit-plan|verdict|list] ..."
allowed-tools:
  - exec
---

# AgentBacklog — shared feature queue

AgentBacklog is a queue of features shared between a user and dev agents.
Features are enqueued, popped (FIFO, tunable via `--bump` priority and
`--depend` dependencies), implemented, then archived.

- CLI: `agentbacklog` (alias `AgentBacklog`)
- Storage: SQLite `$AGENTBACKLOG_DB` or `~/.agentbacklog/backlog.db`
- No daemon: each command is an atomic SQLite transaction — safe under
  concurrent agents

## Commands

| Command | Role |
|---|---|
| `agentbacklog --add "Title" [--desc "..."] [--depends-on ID]… [--repo NAME]` | Enqueue a feature (→ pending), optionally dep-blocked (`--depends-on` repeatable) and/or repo-scoped |
| `agentbacklog --pop --agent-id <id> [--repo NAME] [--json]` | Take the highest-priority/oldest eligible pending → wip; `--repo` restricts to NAME + untagged |
| `agentbacklog --done <ID> --agent-id <id>` | wip → done (archived) |
| `agentbacklog --release <ID> --agent-id <id>` | wip → back to pending |
| `agentbacklog --bump <ID>` | Move a pending feature to the front |
| `agentbacklog --depend <ID> <DEP> [DEP…]` | Set pending ID's whole dependency set — poppable once ALL are done (`0` clears) |
| `agentbacklog --claim <ID> --agent-id <id> [--repo NAME]` | Assign a specific pending ID → wip (orchestrator-driven; same eligibility as pop) |
| `agentbacklog --submit-plan <ID> --agent-id <id> --plan-file <path> [--plan-content <path>]` | wip → plan_review; plan content travels in the db |
| `agentbacklog --claim-review <ID> --reviewer <sid>` | plan_review → reviewing (atomic claim) |
| `agentbacklog --unclaim-review <ID>` | reviewing → plan_review (dead reviewer) |
| `agentbacklog --verdict <ID> --reviewer <sid> --outcome approved\|denied\|conditional --comment "…"` | reviewing → wip; logs a review round |
| `agentbacklog --reassign <ID> --agent-id <id>` | Re-owner a wip item (dead agent hand-off; `--force` covers review states) |
| `agentbacklog --reviews <ID>` | Review history of an item |
| `agentbacklog --edit <ID> [--title T] [--desc TXT|--desc-file F] [--append-desc TXT|--append-desc-file F] [--repo NAME] [--no-plan\|--plan]` | Update a pending item's fields (`--repo -` clears the tag; `--force` for other statuses) |
| `agentbacklog --rm <ID>` | Delete an item (pending/done; `--force` for active states or to detach dependents) |
| `agentbacklog --list [--all] [--repo NAME] [--json]` | List pending+wip (`--all` includes done) |

`--json` gives machine output — prefer it as an agent. `--force` bypasses
ownership checks on done/release: never use without an explicit ask.

Semantics: service order is `priority DESC`, then `id`. `--bump` puts an
item first. A feature with `depends_on` is only eligible once its
dependencies are `done` (list `DEP` shows per-dep markers: `!ID` blocked,
`ID` satisfied). The `--repo NAME`
tag is informational — it tells the agent which repo to work on. `--pop`
without `--repo` takes **any** eligible feature (tagged or not);
`--pop --repo NAME` restricts to `NAME` + untagged (repo-dedicated
session). Export `AGENTBACKLOG_REPO=<name>` to scope a whole session.
`--workspace` is an alias of `--repo`.

Pipeline `pending → wip → plan_review → reviewing → wip → done`: an
orchestrator `--claim`s an item for a planner; the planner `--submit-plan`s
its plan (content is stored in the db, so any reviewer reads it via
`--list --json`); a reviewer `--claim-review`s then `--verdict`s it —
every round is logged (`--reviews ID`) and sends the item back to `wip`
for implementation or revision (resubmit again after a
conditional/denied). Items added with `--add --no-plan` skip the pipeline.

## Agent protocol

1. Pick a stable `--agent-id` for the session (session name, hostname, or
   a user-provided id); reuse it for pop/done/release. Export
   `AGENTBACKLOG_REPO` if the session is dedicated to one repo.
2. `agentbacklog --pop --agent-id <id> --json`
   - exit 0 → a feature is assigned (wip); JSON has `id`, `title`,
     `description`, `repo`
   - exit 1 « queue is empty » → nothing to take: say so and stop
   - exit 1 « blocked by dependencies » → pending items exist but their
     dep isn't `done`: report (`--list` shows `!ID`), don't pop
   - exit 1 « queue is empty for repo 'X' » → nothing eligible for that
     repo: report and stop
3. Implement the feature: `title` + `description` is the spec.
4. `agentbacklog --done <ID> --agent-id <id>` once the work is finished
   **and verified** (tests/build OK).
5. Abandoning (blocked, out of scope): `agentbacklog --release <ID>
   --agent-id <id>` and explain why.

## Rules

- Never `--done` a feature owned by another agent — the CLI refuses;
  don't bypass with `--force`.
- Don't `--pop` just to peek: `--list`/`--list --json` inspects without
  assigning.
- A feature is only `--done` after the work is actually verified.
