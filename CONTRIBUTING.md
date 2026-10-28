# Contributing

Contributions go through **pull requests only** — `main` is protected and
direct pushes are rejected, including for the repository owner.

## Workflow

1. **Fork** the repository on GitHub.
2. Clone your fork:
   ```bash
   git clone git@github.com:<you>/AgentBacklog.git
   cd AgentBacklog
   ```
3. Create a branch:
   ```bash
   git switch -c feat/my-change   # or fix/, docs/, chore/…
   ```
4. Make your changes.
5. Test them (see below).
6. Commit with a **conventional** message (`feat:`, `fix:`, `docs:`,
   `chore:`…) focused on *why*, not *what*.
7. Push to your fork and open a PR against `main`:
   ```bash
   git push -u origin feat/my-change
   gh pr create
   ```
8. Describe the motivation and the test plan in the PR body. Once merged,
   the branch can be deleted.

## Testing changes

The CLI is a single stdlib-only script — run it directly against a
throwaway database:

```bash
export AGENTBACKLOG_DB=/tmp/backlog_test.db
rm -f "$AGENTBACKLOG_DB"*

./agentbacklog.py --add "first" && ./agentbacklog.py --add "second"
./agentbacklog.py --bump 2
./agentbacklog.py --pop --agent-id test --json   # must return "second"
./agentbacklog.py --list --all
```

Also exercise concurrency if you touched the transaction logic:

```bash
for a in a1 a2 a3; do ./agentbacklog.py --pop --agent-id "$a" & done; wait
```

## Style

- Single-file, **stdlib-only** script — no new dependencies without
  discussing it in an issue first.
- Keep the concurrency model intact: reads may be casual, every
  read-modify-write must run inside `BEGIN IMMEDIATE`.
- Schema changes must stay backward-compatible (see how `priority` was
  added via `ALTER TABLE` when missing).

## Reporting issues

Open a GitHub issue with the command you ran, the output, and
`agentbacklog --version`.
