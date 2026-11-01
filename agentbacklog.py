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

__version__ = "0.6.0"

DEFAULT_DB = Path.home() / ".agentbacklog" / "backlog.db"
BUSY_TIMEOUT_MS = 10_000

SCHEMA = """\
CREATE TABLE IF NOT EXISTS features (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'wip', 'done')),
    priority    INTEGER NOT NULL DEFAULT 0,
    repo        TEXT,
    agent_id    TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at  TEXT,
    done_at     TEXT
);

CREATE TABLE IF NOT EXISTS feature_deps (
    feature_id  INTEGER NOT NULL REFERENCES features(id),
    depends_on  INTEGER NOT NULL REFERENCES features(id),
    PRIMARY KEY (feature_id, depends_on)
);
"""

LIST_ORDER = (
    "CASE f.status WHEN 'pending' THEN 0 WHEN 'wip' THEN 1 ELSE 2 END,"
    " f.priority DESC, f.id"
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
    cols = {r[1] for r in conn.execute("PRAGMA table_info(features)")}
    if "priority" not in cols:
        conn.execute(
            "ALTER TABLE features ADD COLUMN priority"
            " INTEGER NOT NULL DEFAULT 0"
        )
    if "repo" not in cols:
        conn.execute("ALTER TABLE features ADD COLUMN repo TEXT")
    if "depends_on" in cols:
        conn.execute(
            "INSERT OR IGNORE INTO feature_deps (feature_id, depends_on)"
            " SELECT id, depends_on FROM features"
            " WHERE depends_on IS NOT NULL"
        )
        try:
            conn.execute("ALTER TABLE features DROP COLUMN depends_on")
        except sqlite3.Error:
            pass  # sqlite < 3.35: column stays but is unused
    return conn


class BacklogError(Exception):
    pass


def feature_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def _require_feature(conn: sqlite3.Connection, feature_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM features WHERE id = ?", (feature_id,)
    ).fetchone()
    if row is None:
        raise BacklogError(f"feature {feature_id} not found")
    return row


def _deps_of(conn: sqlite3.Connection, feature_id: int) -> list[dict]:
    return [
        {"id": r["depends_on"], "status": r["status"]}
        for r in conn.execute(
            "SELECT fd.depends_on, d.status FROM feature_deps fd"
            " LEFT JOIN features d ON fd.depends_on = d.id"
            " WHERE fd.feature_id = ? ORDER BY fd.depends_on",
            (feature_id,),
        )
    ]


def _with_deps(conn: sqlite3.Connection, f: dict) -> dict:
    f["dependencies"] = _deps_of(conn, f["id"])
    return f


def _get_feature(conn: sqlite3.Connection, feature_id: int) -> dict:
    return _with_deps(
        conn,
        feature_dict(
            conn.execute(
                "SELECT * FROM features WHERE id = ?", (feature_id,)
            ).fetchone()
        ),
    )


def _would_cycle(
    conn: sqlite3.Connection, feature_id: int, dep_ids: set[int]
) -> bool:
    stack, seen = list(dep_ids), set()
    while stack:
        cur = stack.pop()
        if cur == feature_id:
            return True
        if cur in seen:
            continue
        seen.add(cur)
        for r in conn.execute(
            "SELECT depends_on FROM feature_deps WHERE feature_id = ?",
            (cur,),
        ):
            stack.append(r["depends_on"])
    return False


def cmd_add(
    conn: sqlite3.Connection,
    title: str,
    description: str,
    depends_on: list[int] | None,
    repo: str | None,
) -> dict:
    deps = set(depends_on or [])
    for dep in deps:
        _require_feature(conn, dep)
    cur = conn.execute(
        "INSERT INTO features (title, description, repo) VALUES (?, ?, ?)",
        (title, description, repo),
    )
    fid = cur.lastrowid
    for dep in deps:
        conn.execute(
            "INSERT INTO feature_deps (feature_id, depends_on)"
            " VALUES (?, ?)",
            (fid, dep),
        )
    return _get_feature(conn, fid)


def cmd_pop(
    conn: sqlite3.Connection, agent_id: str, repo: str | None
) -> tuple[dict | None, int]:
    try:
        conn.execute("BEGIN IMMEDIATE")
        scope = (
            "AND (f.repo IS NULL OR f.repo = ?)" if repo is not None else ""
        )
        row = conn.execute(
            "SELECT f.* FROM features f"
            " WHERE f.status = 'pending'"
            " AND NOT EXISTS ("
            "   SELECT 1 FROM feature_deps fd"
            "   LEFT JOIN features d ON fd.depends_on = d.id"
            "   WHERE fd.feature_id = f.id"
            "   AND (d.status IS NULL OR d.status != 'done')"
            " )"
            f" {scope}"
            " ORDER BY f.priority DESC, f.id LIMIT 1",
            (repo,) if repo is not None else (),
        ).fetchone()
        if row is None:
            blocked = conn.execute(
                "SELECT COUNT(*) AS n FROM features f"
                " WHERE f.status = 'pending'"
                " AND EXISTS ("
                "   SELECT 1 FROM feature_deps fd"
                "   LEFT JOIN features d ON fd.depends_on = d.id"
                "   WHERE fd.feature_id = f.id"
                "   AND (d.status IS NULL OR d.status != 'done')"
                " )"
                f" {scope}",
                (repo,) if repo is not None else (),
            ).fetchone()["n"]
            conn.execute("ROLLBACK")
            return None, blocked
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
    return (_get_feature(conn, row["id"]), 0)


def cmd_depend(
    conn: sqlite3.Connection, feature_id: int, dep_ids: list[int]
) -> dict:
    """Replace a feature's dependency set; dep_ids == [0] clears it."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        if row["status"] != "pending":
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not pending"
            )
        deps = set(dep_ids)
        if deps != {0}:
            if 0 in deps:
                raise BacklogError("0 clears dependencies — don't mix it"
                                   " with real ids")
            if feature_id in deps:
                raise BacklogError("a feature cannot depend on itself")
            for dep in deps:
                _require_feature(conn, dep)
            if _would_cycle(conn, feature_id, deps):
                raise BacklogError(
                    f"dependency cycle: {feature_id} would depend"
                    " on itself through the chain"
                )
        conn.execute(
            "DELETE FROM feature_deps WHERE feature_id = ?", (feature_id,)
        )
        if deps != {0}:
            conn.executemany(
                "INSERT INTO feature_deps (feature_id, depends_on)"
                " VALUES (?, ?)",
                [(feature_id, d) for d in deps],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


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
    return _get_feature(conn, feature_id)


def cmd_bump(conn: sqlite3.Connection, feature_id: int) -> dict:
    """Move a pending feature to the front of the queue."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM features WHERE id = ?", (feature_id,)
        ).fetchone()
        if row is None:
            raise BacklogError(f"feature {feature_id} not found")
        if row["status"] != "pending":
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not pending"
            )
        conn.execute(
            "UPDATE features SET priority ="
            " (SELECT COALESCE(MAX(priority), 0) + 1 FROM features)"
            " WHERE id = ?",
            (feature_id,),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_list(
    conn: sqlite3.Connection, include_done: bool, repo: str | None
) -> list[dict]:
    conds = [] if include_done else ["f.status != 'done'"]
    params: tuple = ()
    if repo is not None:
        conds.append("f.repo = ?")
        params = (repo,)
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    rows = conn.execute(
        f"SELECT f.* FROM features f {where} ORDER BY {LIST_ORDER}",
        params,
    ).fetchall()
    dep_map: dict[int, list[dict]] = {}
    for r in conn.execute(
        "SELECT fd.feature_id, fd.depends_on, d.status"
        " FROM feature_deps fd"
        " LEFT JOIN features d ON fd.depends_on = d.id"
        " ORDER BY fd.depends_on"
    ):
        dep_map.setdefault(r["feature_id"], []).append(
            {"id": r["depends_on"], "status": r["status"]}
        )
    features = [feature_dict(r) for r in rows]
    for f in features:
        f["dependencies"] = dep_map.get(f["id"], [])
    return features


def print_list(features: list[dict]) -> None:
    if not features:
        print("queue is empty")
        return
    print(
        f"{'ID':<5}{'STATUS':<9}{'PRIO':<6}{'DEP':<11}{'REPO':<16}"
        f"{'AGENT':<16}{'CREATED':<26}TITLE"
    )
    for f in features:
        title = f["title"]
        if len(title) > 60:
            title = title[:57] + "..."
        prio = str(f["priority"]) if f["priority"] else "-"
        repo = f["repo"] or "-"
        if len(repo) > 15:
            repo = repo[:12] + "..."
        deps = f.get("dependencies") or []
        if not deps:
            dep = "-"
        else:
            dep = ",".join(
                str(d["id"]) if d["status"] == "done" else f"!{d['id']}"
                for d in deps
            )
            if len(dep) > 10:
                dep = dep[:9] + "…"
        print(
            f"{f['id']:<5}{f['status']:<9}{prio:<6}{dep:<11}{repo:<16}"
            f"{(f['agent_id'] or '-'):<16}{f['created_at']:<26}{title}"
        )


def print_feature(f: dict, header: str) -> None:
    print(header)
    print(f"  id:          {f['id']}")
    print(f"  title:       {f['title']}")
    if f["description"]:
        print(f"  description: {f['description']}")
    print(f"  status:      {f['status']}")
    if f["priority"]:
        print(f"  priority:    {f['priority']}")
    deps = f.get("dependencies") or []
    if deps:
        ids = ", ".join(f"#{d['id']}" for d in deps)
        print(f"  depends on:  {ids}")
    if f["repo"]:
        print(f"  repo:        {f['repo']}")
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
        "--bump",
        metavar="ID",
        type=int,
        help="move a pending feature to the front of the queue",
    )
    ops.add_argument(
        "--depend",
        metavar=("ID", "DEP"),
        type=int,
        nargs="+",
        help="make pending feature ID depend on DEP(s) — it can only"
        " pop once they are all done; single DEP=0 clears",
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
        "--repo",
        "--workspace",
        dest="repo",
        metavar="NAME",
        help="with --add: tag the feature for a repo/workspace;"
        " with --pop: restrict to features for it (plus untagged);"
        " without it, pop takes anything; with --list: filter."
        " Default: $AGENTBACKLOG_REPO",
    )
    parser.add_argument(
        "--depends-on",
        metavar="ID",
        type=int,
        action="append",
        help="with --add: the new feature depends on feature ID"
        " (repeatable for several dependencies)",
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
    if args.depends_on is not None and args.add is None:
        parser.error("--depends-on only makes sense with --add")
    if args.depend is not None and len(args.depend) < 2:
        parser.error("--depend needs an ID and at least one DEP (0 clears)")
    if args.repo is not None and not (
        args.add is not None or args.pop or args.list
    ):
        parser.error("--repo only makes sense with --add/--pop/--list")

    resolved_repo = args.repo or os.environ.get("AGENTBACKLOG_REPO")

    path = args.db or db_path()
    try:
        conn = connect(path)
    except sqlite3.Error as e:
        print(f"agentbacklog: cannot open {path}: {e}", file=sys.stderr)
        return 1

    try:
        if args.add is not None:
            f = cmd_add(
                conn, args.add, args.description, args.depends_on,
                resolved_repo,
            )
            emit(f, args.as_json,
                 lambda: print(f"queued feature #{f['id']}: {f['title']}"))
        elif args.pop:
            f, blocked = cmd_pop(conn, args.agent_id, resolved_repo)
            if f is None:
                scope_txt = (
                    f" for repo {resolved_repo!r}" if resolved_repo else ""
                )
                if blocked:
                    print(
                        f"queue empty — {blocked} pending feature(s)"
                        f" blocked by dependencies{scope_txt}",
                        file=sys.stderr,
                    )
                else:
                    print(f"queue is empty{scope_txt}", file=sys.stderr)
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
        elif args.bump is not None:
            f = cmd_bump(conn, args.bump)
            emit(f, args.as_json,
                 lambda: print(f"feature #{f['id']} moved to front"))
        elif args.depend is not None:
            fid, deps = args.depend[0], args.depend[1:]
            f = cmd_depend(conn, fid, deps)
            msg = (
                f"feature #{fid} dependencies cleared"
                if deps == [0]
                else f"feature #{fid} now depends on"
                f" {', '.join(f'#{d}' for d in sorted(set(deps)))}"
            )
            emit(f, args.as_json, lambda: print(msg))
        else:  # --list
            fs = cmd_list(conn, args.all, args.repo)
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
