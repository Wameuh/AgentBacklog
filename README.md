# AgentBacklog

A shared FIFO queue of features between a user and autonomous dev agents.

The user (or an agent) enqueues feature requests; agents pick them up in
order, implement them, then archive them. Single-file Python CLI backed by
SQLite — **no daemon**: every command is an atomic SQLite transaction
(`BEGIN IMMEDIATE` + WAL journal), so concurrent agents can safely race
for the next item.

## Install

```bash
# as a uv tool (editable install reflects repo changes immediately)
uv tool install --editable .

# or just run the script — stdlib only, no dependencies
./agentbacklog.py --help
```

Provides two entry points: `agentbacklog` and `AgentBacklog`.

## Usage

```bash
agentbacklog --add "Add retry logic" --desc "exponential backoff, max 3"
agentbacklog --pop --agent-id devin-1 --json   # take oldest pending → wip
agentbacklog --done 3 --agent-id devin-1       # wip → done (archived)
agentbacklog --release 3 --agent-id devin-1    # wip → back to pending
agentbacklog --bump 4                          # pending → front of queue
agentbacklog --list                            # pending + wip
agentbacklog --list --all                      # include archived (done)
```

| Command | Description |
|---|---|
| `--add TITLE [--desc TXT]` | Enqueue a feature (→ `pending`) |
| `--pop --agent-id A` | Take the highest-priority/oldest `pending` → `wip`, assigned to agent `A` |
| `--done ID --agent-id A` | Mark a `wip` feature `done` (archived). Ownership-checked, `--force` overrides |
| `--release ID --agent-id A` | Give a `wip` feature back to `pending` |
| `--bump ID` | Move a `pending` feature to the front of the queue |
| `--list [--all]` | List `pending`+`wip` (`--all` includes `done`) |
| `--json` | Machine-readable output |
| `--db PATH` | Override database path |
| `--version` | Print version |

### Priority

`--pop` orders by `priority DESC, id`. `--bump` sets the feature's priority
to `max+1`, so the most recently bumped item is served first; previously
bumped items stay ahead of unprioritized ones.

## Agent workflow

1. Choose a stable `--agent-id` for the session (session name, hostname…)
   and reuse it for `pop`/`done`/`release`.
2. `agentbacklog --pop --agent-id <id> --json` — exit `0` returns the
   assigned feature (`id`, `title`, `description`); exit `1` means the
   queue is empty.
3. Implement the feature (`title`+`description` is the spec).
4. `agentbacklog --done <ID> --agent-id <id>` once the work is verified.
   Abandoning? `--release` returns it to the queue.
5. Never `--done` a feature owned by another agent (the CLI refuses; don't
   bypass with `--force`).

## Storage

Default database: `~/.agentbacklog/backlog.db`. Override with the
`AGENTBACKLOG_DB` environment variable or `--db PATH`.

Schema: one `features` table (`id`, `title`, `description`,
`status` ∈ `pending|wip|done`, `priority`, `agent_id`, `created_at`,
`started_at`, `done_at`). The `priority` column is added automatically to
older databases.

## License

MIT — see [LICENSE](LICENSE).
