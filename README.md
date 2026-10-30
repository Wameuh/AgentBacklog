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
agentbacklog --add "Docs" --depends-on 1       # only poppable once #1 is done
agentbacklog --add "AC theme" --repo AgentControl  # scoped to a repo
agentbacklog --pop --agent-id devin-1 --repo AgentControl --json
agentbacklog --done 3 --agent-id devin-1       # wip → done (archived)
agentbacklog --release 3 --agent-id devin-1    # wip → back to pending
agentbacklog --bump 4                          # pending → front of queue
agentbacklog --depend 5 2                      # pending #5 depends on #2 (0 clears)
agentbacklog --list                            # pending + wip
agentbacklog --list --all                      # include archived (done)
```

| Command | Description |
|---|---|
| `--add TITLE [--desc TXT] [--depends-on ID] [--repo NAME]` | Enqueue a feature (→ `pending`), optionally dep-blocked and/or repo-scoped |
| `--depend ID DEP` | Make `pending` feature `ID` depend on `DEP` (`DEP=0` clears). Rejects self-deps, cycles, non-pending items |
| `--pop --agent-id A [--repo NAME]` | Take the highest-priority/oldest eligible `pending` → `wip`; `--repo` restricts to that repo + untagged |
| `--done ID --agent-id A` | Mark a `wip` feature `done` (archived). Ownership-checked, `--force` overrides |
| `--release ID --agent-id A` | Give a `wip` feature back to `pending` |
| `--bump ID` | Move a `pending` feature to the front of the queue |
| `--list [--all] [--repo NAME]` | List `pending`+`wip` (`--all` includes `done`), optionally filtered by repo |
| `--json` | Machine-readable output |
| `--db PATH` | Override database path |
| `--version` | Print version |

`--workspace` is an alias for `--repo`.

### Priority

`--pop` orders by `priority DESC, id`. `--bump` sets the feature's priority
to `max+1`, so the most recently bumped item is served first; previously
bumped items stay ahead of unprioritized ones.

### Dependencies

A feature can depend on another one. A dependent feature stays `pending`
but is **not eligible for `--pop`** until its dependency is `done` —
`--pop` skips it and the `--list` `DEP` column shows it as `!ID` (versus
`ID` when satisfied). If every pending feature is blocked, `--pop` says so
explicitly. Cycles, self-dependencies and dependencies on non-existent or
non-`pending` features are rejected.

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

Schema: one `features` table (`id`, `title`, `description`,
`status` ∈ `pending|wip|done`, `priority`, `depends_on`, `repo`,
`agent_id`, `created_at`, `started_at`, `done_at`). New columns are added
automatically to older databases.

## Contributing

`main` is protected — contributions go through pull requests from a fork.
See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).
