# AgentBacklog

A shared FIFO queue of features between a user and autonomous dev agents.

The user (or an agent) enqueues feature requests; agents pick them up in
order, implement them, then archive them. Single-file Python CLI backed by
SQLite — **no daemon**: every command is an atomic SQLite transaction
(`BEGIN IMMEDIATE` + WAL journal), so concurrent agents can safely race
for the next item.

## Install

```bash
# as a uv tool
uv tool install .

# or symlink the script — stdlib only, always reflects the checkout
ln -sf "$PWD/agentbacklog.py" ~/.local/bin/agentbacklog
ln -sf "$PWD/agentbacklog.py" ~/.local/bin/AgentBacklog

# or just run it in place
./agentbacklog.py --help
```

Provides two entry points: `agentbacklog` and `AgentBacklog`.

## Usage

```bash
agentbacklog --add "Add retry logic" --desc "exponential backoff, max 3"
agentbacklog --add "Docs" --depends-on 1 --depends-on 2  # poppable once #1 AND #2 are done
agentbacklog --add "AC theme" --repo AgentControl  # scoped to a repo
agentbacklog --pop --agent-id devin-1 --repo AgentControl --json
agentbacklog --done 3 --agent-id devin-1       # wip → done (archived)
agentbacklog --release 3 --agent-id devin-1    # wip → back to pending
agentbacklog --bump 4                          # pending → front of queue
agentbacklog --depend 5 2 3                    # #5 depends on #2 AND #3 (0 clears)
agentbacklog --list                            # pending + wip
agentbacklog --list --all                      # include archived (done)

# plan → review → implementation pipeline
agentbacklog --claim 7 --agent-id s-planner    # assign pending #7 (deps-checked)
agentbacklog --submit-plan 7 --agent-id s-planner --plan-file plan.md
agentbacklog --claim-review 7 --reviewer s-rev # plan_review → reviewing
agentbacklog --verdict 7 --reviewer s-rev --outcome approved --comment "ok"
agentbacklog --unclaim-review 7                # reviewer died → plan_review
agentbacklog --reassign 7 --agent-id s-new     # dead owner hand-off
agentbacklog --reviews 7                       # verdict history
```

| Command | Description |
|---|---|
| `--add TITLE [--desc TXT] [--depends-on ID]… [--repo NAME]` | Enqueue a feature (→ `pending`), optionally dep-blocked (repeat `--depends-on`) and/or repo-scoped |
| `--depend ID DEP [DEP…]` | Set `pending` feature `ID`'s dependencies — poppable once **all** are done (`0` clears all). Rejects self-deps, cycles, non-pending items |
| `--pop --agent-id A [--repo NAME]` | Take the highest-priority/oldest eligible `pending` → `wip`; `--repo` restricts to that repo + untagged |
| `--done ID --agent-id A` | Mark a `wip` feature `done` (archived). Ownership-checked, `--force` overrides |
| `--release ID --agent-id A` | Give a `wip` feature back to `pending` |
| `--bump ID` | Move a `pending` feature to the front of the queue |
| `--list [--all] [--repo NAME]` | List non-`done` items (`--all` includes `done`), optionally filtered by repo |
| `--claim ID --agent-id A [--repo NAME]` | Assign a **specific** `pending` feature → `wip`. Same eligibility as `--pop` (deps all done, repo scope) but no queue scan |
| `--submit-plan ID --agent-id A --plan-file PATH [--plan-content PATH]` | `wip` → `plan_review`. Stores `plan_file`; plan content read from `--plan-content`, else `plan_file` itself, else NULL. Owner-checked |
| `--claim-review ID --reviewer SID` | `plan_review` → `reviewing`, sets `reviewer_id`. Atomic — concurrent claims race safely |
| `--unclaim-review ID` | `reviewing` → `plan_review`, clears `reviewer_id` (dead reviewer / timeout) |
| `--verdict ID --reviewer SID --outcome approved\|denied\|conditional --comment TXT` | `reviewing` → `wip`, appends a `feature_reviews` row (round = current `review_round`), `review_round++`. Reviewer-checked, `--force` overrides |
| `--reassign ID --agent-id A` | Re-point a `wip` feature to a new agent (dead owner hand-off). `--force` also covers `plan_review`/`reviewing` items |
| `--reviews ID` | Show the `feature_reviews` history of a feature |
| `--no-plan` | With `--add`: item skips the plan→review pipeline (`no_plan=1`) |
| `--json` | Machine-readable output |
| `--db PATH` | Override database path |
| `--version` | Print version |

`--workspace` is an alias for `--repo`.

### Priority

`--pop` orders by `priority DESC, id`. `--bump` sets the feature's priority
to `max+1`, so the most recently bumped item is served first; previously
bumped items stay ahead of unprioritized ones.

### Dependencies

A feature can depend on one or **several** others (`--depends-on` repeated
at `--add`, or `--depend ID DEP DEP…` afterwards — the command replaces the
whole set). A dependent feature stays `pending` but is **not eligible for
`--pop`** until **all** its dependencies are `done` — `--pop` skips it and
the `--list` `DEP` column shows per-dep markers (`!ID` = blocking, `ID` =
satisfied, e.g. `!2,5`). If every pending feature is blocked, `--pop` says
so explicitly. Cycles (including multi-hop), self-dependencies and
dependencies on non-existent or non-`pending` features are rejected.

The JSON output exposes `dependencies` as `[{"id": N, "status": …}]`.

### Repo/workspace scoping

One backlog can serve several repos or workspaces. Tag a feature with
`--repo NAME` at `--add` time (the value is a free-form string — repo
name, path, workspace id…). Then:

- `agentbacklog --pop --agent-id A` takes **any** eligible feature — the
  tag is informational, telling the agent which repo to work on.
- `--pop --repo NAME` restricts to features tagged `NAME` **or untagged** —
  use it for sessions dedicated to one repo.
- `--list --repo NAME` filters the listing; the `REPO` column shows the tag.
- `--done`/`--release`/`--bump`/`--depend` address features by id and are
  repo-agnostic.

Set `AGENTBACKLOG_REPO` in the environment to scope `--add`/`--pop` for a
whole session without repeating the flag.

### Plan → review → implementation pipeline

Extended statuses let an orchestrator drive a plan-then-review flow:

```
pending → wip → plan_review → reviewing → wip → done
```

- `--claim` assigns a chosen item to a planner session (`--pop` is
  unchanged: it only ever serves `pending` items).
- The planner works on a plan, then `--submit-plan` sends the item to
  `plan_review` with the plan stored in the db (`plan_content` — a
  reviewer on another machine gets it straight from `--list --json`).
- A reviewer `--claim-review`s it (→ `reviewing`), then `--verdict`s:
  every round is logged in `feature_reviews` and sends the item back to
  `wip` for implementation or plan revision (`conditional`/`denied`
  verdict → resubmit with `--submit-plan` again).
- `--unclaim-review` frees a review whose reviewer died;
  `--reassign` hands a `wip` item to a new agent when its owner is gone.
- `--add --no-plan` marks items that bypass the pipeline (ops notes,
  urgencies).

## Agent workflow

1. Choose a stable `--agent-id` for the session (session name, hostname…)
   and reuse it for `pop`/`done`/`release`. Set `AGENTBACKLOG_REPO` if the
   session is dedicated to one repo.
2. `agentbacklog --pop --agent-id <id> --json` — exit `0` returns the
   assigned feature (`id`, `title`, `description`, `repo`); exit `1` means
   the queue is empty (or only holds blocked/out-of-scope items — the
   message says which).
3. Implement the feature (`title`+`description` is the spec).
4. `agentbacklog --done <ID> --agent-id <id>` once the work is verified.
   Abandoning? `--release` returns it to the queue.
5. Never `--done` a feature owned by another agent (the CLI refuses; don't
   bypass with `--force`).

## Storage

Default database: `~/.agentbacklog/backlog.db`. Override with the
`AGENTBACKLOG_DB` environment variable or `--db PATH`.

Schema: `features` (`id`, `title`, `description`,
`status` ∈ `pending|wip|plan_review|reviewing|done`, `priority`, `repo`,
`agent_id`, `plan_file`, `plan_content`, `reviewer_id`, `review_round`,
`no_plan`, `created_at`, `started_at`, `done_at`) plus `feature_deps`
(`feature_id`, `depends_on`) and `feature_reviews` (`feature_id`, `round`,
`reviewer_id`, `verdict`, `comment`, `created_at`). Migrations run
automatically on connect — the v0.7 status CHECK upgrade rebuilds the
`features` table and drops a `backlog.db.bak-<ts>` backup next to the db
first.

## Contributing

`main` is protected — contributions go through pull requests from a fork.
See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).
