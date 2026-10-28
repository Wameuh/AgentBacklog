#!/usr/bin/env python3
"""AgentBacklog — shared FIFO queue of features between a user and agents.

A single-file CLI backed by SQLite. Concurrent invocations are safe:
writers are serialized with BEGIN IMMEDIATE transactions and WAL journal.

Storage: $AGENTBACKLOG_DB or ~/.agentbacklog/backlog.db
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

__version__ = "0.1.0"

DEFAULT_DB = Path.home() / ".agentbacklog" / "backlog.db"
BUSY_TIMEOUT_MS = 10_000

SCHEMA = """\
CREATE TABLE IF NOT EXISTS features (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'wip', 'done')),
    agent_id    TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at  TEXT,
    done_at     TEXT
);
"""

LIST_ORDER = (
    "CASE status WHEN 'pending' THEN 0 WHEN 'wip' THEN 1 ELSE 2 END, id"
)


def db_path() -> Path:
    return Path(os.environ.get("AGENTBACKLOG_DB", DEFAULT_DB)).expanduser()


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # explicit transactions
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    conn.executescript(SCHEMA)
    return conn


class BacklogError(Exception):
    pass


def feature_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def cmd_add(conn: sqlite3.Connection, title: str, description: str) -> dict:
    cur = conn.execute(
        "INSERT INTO features (title, description) VALUES (?, ?)",
        (title, description),
    )
    return feature_dict(
        conn.execute(
            "SELECT * FROM features WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
    )


def cmd_pop(conn: sqlite3.Connection, agent_id: str) -> dict | None:
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM features WHERE status = 'pending'"
            " ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            return None
        conn.execute(
            "UPDATE features SET status = 'wip', agent_id = ?,"
            " started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
            " WHERE id = ?",
            (agent_id, row["id"]),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return feature_dict(
        conn.execute(
            "SELECT * FROM features WHERE id = ?", (row["id"],)
        ).fetchone()
    )


def _transition(
    conn: sqlite3.Connection,
    feature_id: int,
    agent_id: str | None,
    force: bool,
    action: str,
) -> dict:
    """Mark a WIP feature done or release it back to pending."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM features WHERE id = ?", (feature_id,)
        ).fetchone()
        if row is None:
            raise BacklogError(f"feature {feature_id} not found")
        if row["status"] != "wip":
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not wip"
            )
        if not force and row["agent_id"] != agent_id:
            raise BacklogError(
                f"feature {feature_id} is owned by agent "
                f"{row['agent_id']!r} (use --force to override)"
            )
        if action == "done":
            conn.execute(
                "UPDATE features SET status = 'done',"
                " done_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
                " WHERE id = ?",
                (feature_id,),
            )
        else:  # release
            conn.execute(
                "UPDATE features SET status = 'pending',"
                " agent_id = NULL, started_at = NULL WHERE id = ?",
                (feature_id,),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return feature_dict(
        conn.execute(
            "SELECT * FROM features WHERE id = ?", (feature_id,)
        ).fetchone()
    )


def cmd_list(conn: sqlite3.Connection, include_done: bool) -> list[dict]:
    where = "" if include_done else "WHERE status != 'done'"
    rows = conn.execute(
        f"SELECT * FROM features {where} ORDER BY {LIST_ORDER}"
    ).fetchall()
    return [feature_dict(r) for r in rows]


def print_list(features: list[dict]) -> None:
    if not features:
        print("queue is empty")
        return
    print(f"{'ID':<5}{'STATUS':<9}{'AGENT':<16}{'CREATED':<26}TITLE")
    for f in features:
        title = f["title"]
        if len(title) > 60:
            title = title[:57] + "..."
        print(
            f"{f['id']:<5}{f['status']:<9}{(f['agent_id'] or '-'):<16}"
            f"{f['created_at']:<26}{title}"
        )


def print_feature(f: dict, header: str) -> None:
    print(header)
    print(f"  id:          {f['id']}")
    print(f"  title:       {f['title']}")
    if f["description"]:
        print(f"  description: {f['description']}")
    print(f"  status:      {f['status']}")
    if f["agent_id"]:
        print(f"  agent:       {f['agent_id']}")


def emit(obj, as_json: bool, human) -> None:
    if as_json:
        print(json.dumps(obj, indent=2))
    else:
        human()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="agentbacklog",
        description="Shared FIFO queue of features for dev agents.",
    )
    ops = parser.add_mutually_exclusive_group(required=True)
    ops.add_argument("--add", metavar="TITLE", help="enqueue a feature")
    ops.add_argument(
        "--pop",
        action="store_true",
        help="take the oldest pending feature as --agent-id (marks it wip)",
    )
    ops.add_argument(
        "--done",
        metavar="ID",
        type=int,
        help="mark a wip feature done and archive it",
    )
    ops.add_argument(
        "--release",
        metavar="ID",
        type=int,
        help="give a wip feature back to the pending queue",
    )
    ops.add_argument(
        "--list",
        action="store_true",
        help="list pending and wip features",
    )
    parser.add_argument(
        "--agent-id", help="identity of the calling agent (pop/done/release)"
    )
    parser.add_argument(
        "--desc",
        "--description",
        dest="description",
        default="",
        help="longer description for --add",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="with --list: also show archived (done) features",
    )
    parser.add_argument(
        "--json", action="store_true", dest="as_json", help="JSON output"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="bypass agent ownership check for --done/--release",
    )
    parser.add_argument(
        "--db", type=Path, help="database path (default: $AGENTBACKLOG_DB"
        " or ~/.agentbacklog/backlog.db)"
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    args = parser.parse_args(argv)

    if (args.pop or args.done is not None) and not args.agent_id:
        parser.error("--agent-id is required for --pop/--done")
    if args.release is not None and not args.agent_id and not args.force:
        parser.error("--release needs --agent-id (or --force)")
    if args.all and not args.list:
        parser.error("--all only makes sense with --list")

    path = args.db or db_path()
    try:
        conn = connect(path)
    except sqlite3.Error as e:
        print(f"agentbacklog: cannot open {path}: {e}", file=sys.stderr)
        return 1

    try:
        if args.add is not None:
            f = cmd_add(conn, args.add, args.description)
            emit(f, args.as_json,
                 lambda: print(f"queued feature #{f['id']}: {f['title']}"))
        elif args.pop:
            f = cmd_pop(conn, args.agent_id)
            if f is None:
                print("queue is empty", file=sys.stderr)
                return 1
            emit(f, args.as_json,
                 lambda: print_feature(f, f"popped feature #{f['id']}"))
        elif args.done is not None:
            f = _transition(conn, args.done, args.agent_id,
                            args.force, "done")
            emit(f, args.as_json,
                 lambda: print(f"feature #{f['id']} done (archived)"))
        elif args.release is not None:
            f = _transition(conn, args.release, args.agent_id,
                            args.force, "release")
            emit(f, args.as_json,
                 lambda: print(f"feature #{f['id']} back to pending"))
        else:  # --list
            fs = cmd_list(conn, args.all)
            emit(fs, args.as_json, lambda: print_list(fs))
    except BacklogError as e:
        print(f"agentbacklog: {e}", file=sys.stderr)
        return 1
    except sqlite3.Error as e:
        print(f"agentbacklog: database error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
