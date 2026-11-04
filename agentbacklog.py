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
import time
from pathlib import Path

__version__ = "0.8.0"

DEFAULT_DB = Path.home() / ".agentbacklog" / "backlog.db"
BUSY_TIMEOUT_MS = 10_000

FEATURES_DEF = """\
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'wip', 'plan_review',
                                  'reviewing', 'done')),
    priority    INTEGER NOT NULL DEFAULT 0,
    repo        TEXT,
    agent_id    TEXT,
    plan_file   TEXT,
    plan_content TEXT,
    reviewer_id TEXT,
    review_round INTEGER NOT NULL DEFAULT 0,
    no_plan     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at  TEXT,
    done_at     TEXT"""

# column order used by the schema rebuild — keep in sync with FEATURES_DEF
FEATURE_COLUMNS = [
    "id", "title", "description", "status", "priority", "repo",
    "agent_id", "plan_file", "plan_content", "reviewer_id",
    "review_round", "no_plan", "created_at", "started_at", "done_at",
]

SCHEMA = f"""\
CREATE TABLE IF NOT EXISTS features (
{FEATURES_DEF}
);

CREATE TABLE IF NOT EXISTS feature_deps (
    feature_id  INTEGER NOT NULL REFERENCES features(id),
    depends_on  INTEGER NOT NULL REFERENCES features(id),
    PRIMARY KEY (feature_id, depends_on)
);

CREATE TABLE IF NOT EXISTS feature_reviews (
    feature_id  INTEGER NOT NULL REFERENCES features(id),
    round       INTEGER NOT NULL,
    reviewer_id TEXT,
    verdict     TEXT NOT NULL
                CHECK (verdict IN ('approved', 'denied', 'conditional')),
    comment     TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (feature_id, round)
);
"""

LIST_ORDER = (
    "CASE f.status WHEN 'pending' THEN 0 WHEN 'wip' THEN 1"
    " WHEN 'plan_review' THEN 2 WHEN 'reviewing' THEN 3 ELSE 4 END,"
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
    # v0.7: the status CHECK is baked into the table definition, so adding
    # plan_review/reviewing (and the plan columns) needs a rebuild.
    table_sql = conn.execute(
        "SELECT sql FROM sqlite_master"
        " WHERE type = 'table' AND name = 'features'"
    ).fetchone()["sql"] or ""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(features)")}
    if "'plan_review'" not in table_sql or not set(FEATURE_COLUMNS) <= cols:
        _rebuild_features(conn, path)
    return conn


def _rebuild_features(conn: sqlite3.Connection, path: Path) -> None:
    """Rebuild `features` with the current schema (CHECK can't be altered).

    The database file is backed up next to the db first; rows are copied
    column-by-column so older bases (extra depends_on, missing plan_*)
    migrate cleanly.
    """
    bak = path.with_name(
        f"{path.name}.bak-{time.strftime('%Y%m%d-%H%M%S')}"
    )
    dst = sqlite3.connect(str(bak))
    try:
        conn.backup(dst)
    finally:
        dst.close()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(features)")}
    keep = [c for c in FEATURE_COLUMNS if c in cols]
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f"CREATE TABLE features_new (\n{FEATURES_DEF}\n)")
        conn.execute(
            f"INSERT INTO features_new ({', '.join(keep)})"
            f" SELECT {', '.join(keep)} FROM features"
        )
        conn.execute("DROP TABLE features")
        conn.execute("ALTER TABLE features_new RENAME TO features")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


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
    no_plan: bool,
) -> dict:
    deps = set(depends_on or [])
    for dep in deps:
        _require_feature(conn, dep)
    cur = conn.execute(
        "INSERT INTO features (title, description, repo, no_plan)"
        " VALUES (?, ?, ?, ?)",
        (title, description, repo, int(no_plan)),
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


def _has_unfinished_deps(
    conn: sqlite3.Connection, feature_id: int
) -> bool:
    return bool(
        conn.execute(
            "SELECT EXISTS ("
            " SELECT 1 FROM feature_deps fd"
            " LEFT JOIN features d ON fd.depends_on = d.id"
            " WHERE fd.feature_id = ?"
            " AND (d.status IS NULL OR d.status != 'done'))",
            (feature_id,),
        ).fetchone()[0]
    )


def cmd_claim(
    conn: sqlite3.Connection,
    feature_id: int,
    agent_id: str,
    repo: str | None,
) -> dict:
    """Assign a specific pending feature to an agent (-> wip).

    Unlike pop there is no queue scan — the caller picks the item, but
    the eligibility rules are the same: pending, all deps done, repo ok.
    """
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
        if (
            repo is not None
            and row["repo"] is not None
            and row["repo"] != repo
        ):
            raise BacklogError(
                f"feature {feature_id} is tagged for repo"
                f" {row['repo']!r}, not {repo!r}"
            )
        if _has_unfinished_deps(conn, feature_id):
            raise BacklogError(
                f"feature {feature_id} is blocked by unfinished"
                " dependencies"
            )
        conn.execute(
            "UPDATE features SET status = 'wip', agent_id = ?,"
            " started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
            " WHERE id = ?",
            (agent_id, feature_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_submit_plan(
    conn: sqlite3.Connection,
    feature_id: int,
    agent_id: str,
    plan_file: str,
    plan_content_path: str | None,
    force: bool,
) -> dict:
    """wip -> plan_review: attach the plan document for the reviewer."""
    content = None
    for p in (plan_content_path, plan_file):
        if not p:
            continue
        try:
            content = Path(p).expanduser().read_text()
            break
        except OSError:
            continue
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        if row["status"] != "wip":
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not wip"
            )
        if not force and row["agent_id"] != agent_id:
            raise BacklogError(
                f"feature {feature_id} is owned by agent "
                f"{row['agent_id']!r} (use --force to override)"
            )
        conn.execute(
            "UPDATE features SET status = 'plan_review', plan_file = ?,"
            " plan_content = ?, reviewer_id = NULL WHERE id = ?",
            (plan_file, content, feature_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_claim_review(
    conn: sqlite3.Connection, feature_id: int, reviewer: str
) -> dict:
    """plan_review -> reviewing: a reviewer atomically takes the review."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        if row["status"] != "plan_review":
            raise BacklogError(
                f"feature {feature_id} is {row['status']},"
                " not plan_review"
            )
        conn.execute(
            "UPDATE features SET status = 'reviewing', reviewer_id = ?"
            " WHERE id = ?",
            (reviewer, feature_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_unclaim_review(conn: sqlite3.Connection, feature_id: int) -> dict:
    """reviewing -> plan_review: the reviewer died or timed out."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        if row["status"] != "reviewing":
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not reviewing"
            )
        conn.execute(
            "UPDATE features SET status = 'plan_review',"
            " reviewer_id = NULL WHERE id = ?",
            (feature_id,),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_verdict(
    conn: sqlite3.Connection,
    feature_id: int,
    reviewer: str,
    outcome: str,
    comment: str,
    force: bool,
) -> dict:
    """reviewing -> wip: record the verdict and bump review_round."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        if row["status"] != "reviewing":
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not reviewing"
            )
        if not force and row["reviewer_id"] != reviewer:
            raise BacklogError(
                f"feature {feature_id} is reviewed by "
                f"{row['reviewer_id']!r} (use --force to override)"
            )
        conn.execute(
            "INSERT INTO feature_reviews"
            " (feature_id, round, reviewer_id, verdict, comment)"
            " VALUES (?, ?, ?, ?, ?)",
            (feature_id, row["review_round"], reviewer, outcome, comment),
        )
        conn.execute(
            "UPDATE features SET status = 'wip',"
            " review_round = review_round + 1 WHERE id = ?",
            (feature_id,),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_reassign(
    conn: sqlite3.Connection,
    feature_id: int,
    agent_id: str,
    force: bool,
) -> dict:
    """Change agent_id of a wip feature (dead planner hand-off).

    --force also allows plan_review/reviewing items stuck on a dead
    planner — the pipeline then resumes from its current state.
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        ok = row["status"] == "wip" or (
            force and row["status"] in ("plan_review", "reviewing")
        )
        if not ok:
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not wip"
                + ("" if force else " (use --force for review states)")
            )
        conn.execute(
            "UPDATE features SET agent_id = ? WHERE id = ?",
            (agent_id, feature_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_reviews(conn: sqlite3.Connection, feature_id: int) -> list[dict]:
    _require_feature(conn, feature_id)
    return [
        feature_dict(r)
        for r in conn.execute(
            "SELECT * FROM feature_reviews WHERE feature_id = ?"
            " ORDER BY round",
            (feature_id,),
        )
    ]


def cmd_edit(
    conn: sqlite3.Connection,
    feature_id: int,
    *,
    title: str | None,
    description: str | None,
    append_desc: str | None,
    repo_set: bool,
    repo: str | None,
    no_plan: bool,
    plan: bool,
    force: bool,
) -> dict:
    """Update fields of a feature (pending; --force for other statuses)."""
    sets: list[str] = []
    params: list = []
    if title is not None:
        sets.append("title = ?")
        params.append(title)
    if description is not None:
        sets.append("description = ?")
        params.append(description)
    if append_desc is not None:
        sets.append("description = description || ?")
        params.append(append_desc)
    if repo_set:
        sets.append("repo = ?")
        params.append(repo)
    if no_plan:
        sets.append("no_plan = 1")
    if plan:
        sets.append("no_plan = 0")
    if not sets:
        raise BacklogError(
            "nothing to update — pass --title/--desc/--desc-file/"
            "--append-desc/--append-desc-file/--repo/--no-plan/--plan"
        )
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        if row["status"] != "pending" and not force:
            raise BacklogError(
                f"feature {feature_id} is {row['status']}, not pending"
                " (use --force)"
            )
        params.append(feature_id)
        conn.execute(
            f"UPDATE features SET {', '.join(sets)} WHERE id = ?", params
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return _get_feature(conn, feature_id)


def cmd_rm(conn: sqlite3.Connection, feature_id: int, force: bool) -> int:
    """Delete a feature plus its dep edges and review history.

    Refuses on active statuses or when other features depend on it,
    unless --force — the dependents' edges are then dropped, which
    unblocks them. Returns the number of dependents that were detached.
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _require_feature(conn, feature_id)
        if (
            row["status"] in ("wip", "plan_review", "reviewing")
            and not force
        ):
            raise BacklogError(
                f"feature {feature_id} is {row['status']}"
                " (use --force to delete)"
            )
        dependents = [
            r["feature_id"]
            for r in conn.execute(
                "SELECT feature_id FROM feature_deps WHERE depends_on = ?",
                (feature_id,),
            )
        ]
        if dependents and not force:
            raise BacklogError(
                f"feature {feature_id} is depended on by "
                f"{', '.join(f'#{d}' for d in sorted(dependents))}"
                " (use --force — their dep edges will be removed)"
            )
        conn.execute(
            "DELETE FROM feature_deps"
            " WHERE feature_id = ? OR depends_on = ?",
            (feature_id, feature_id),
        )
        conn.execute(
            "DELETE FROM feature_reviews WHERE feature_id = ?",
            (feature_id,),
        )
        conn.execute("DELETE FROM features WHERE id = ?", (feature_id,))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return len(dependents)


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
        f"{'ID':<5}{'STATUS':<12}{'PRIO':<6}{'DEP':<11}{'REPO':<16}"
        f"{'AGENT':<14}{'REVIEWER':<14}{'CREATED':<26}TITLE"
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
            f"{f['id']:<5}{f['status']:<12}{prio:<6}{dep:<11}{repo:<16}"
            f"{(f['agent_id'] or '-'):<14}"
            f"{(f['reviewer_id'] or '-'):<14}"
            f"{f['created_at']:<26}{title}"
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
    if f["reviewer_id"]:
        print(f"  reviewer:    {f['reviewer_id']}")
    if f["plan_file"]:
        print(f"  plan file:   {f['plan_file']}")
    if f["review_round"]:
        print(f"  reviews:     {f['review_round']} round(s)")
    if f["no_plan"]:
        print(f"  no plan:     yes")


def _read_desc_file(path: str) -> str:
    try:
        return Path(path).expanduser().read_text()
    except OSError as e:
        raise BacklogError(f"cannot read {path}: {e}") from e


def print_reviews(feature_id: int, reviews: list[dict]) -> None:
    if not reviews:
        print(f"feature #{feature_id}: no reviews yet")
        return
    print(f"reviews for feature #{feature_id}:")
    for r in reviews:
        by = f" by {r['reviewer_id']}" if r["reviewer_id"] else ""
        print(
            f"  round {r['round']}: {r['verdict']}{by}"
            f" ({r['created_at']})"
        )
        if r["comment"]:
            for line in r["comment"].splitlines():
                print(f"      {line}")


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
    ops.add_argument(
        "--claim",
        metavar="ID",
        type=int,
        help="assign pending feature ID to --agent-id (marks it wip)",
    )
    ops.add_argument(
        "--submit-plan",
        metavar="ID",
        type=int,
        help="submit the plan of a wip feature (→ plan_review)",
    )
    ops.add_argument(
        "--claim-review",
        metavar="ID",
        type=int,
        help="claim the plan review of a feature for --reviewer"
        " (→ reviewing)",
    )
    ops.add_argument(
        "--unclaim-review",
        metavar="ID",
        type=int,
        help="release a claimed review back to plan_review",
    )
    ops.add_argument(
        "--verdict",
        metavar="ID",
        type=int,
        help="record the review --outcome for a reviewing feature"
        " (→ back to wip)",
    )
    ops.add_argument(
        "--reassign",
        metavar="ID",
        type=int,
        help="reassign a wip feature to --agent-id (dead owner hand-off)",
    )
    ops.add_argument(
        "--reviews",
        metavar="ID",
        type=int,
        help="show the review history of a feature",
    )
    ops.add_argument(
        "--edit",
        metavar="ID",
        type=int,
        help="update fields of a pending feature (--title/--desc/"
        "--desc-file/--append-desc/--append-desc-file/--repo/"
        "--no-plan/--plan; --force for other statuses)",
    )
    ops.add_argument(
        "--rm",
        "--delete",
        dest="rm",
        metavar="ID",
        type=int,
        help="delete a feature (pending/done; --force for active"
        " states or when other features depend on it)",
    )
    parser.add_argument(
        "--agent-id", help="identity of the calling agent (pop/done/release)"
    )
    parser.add_argument(
        "--desc",
        "--description",
        dest="description",
        help="with --add: longer description;"
        " with --edit: replace the description",
    )
    parser.add_argument(
        "--title",
        metavar="TITLE",
        help="with --edit: new title",
    )
    parser.add_argument(
        "--desc-file",
        metavar="PATH",
        help="with --add/--edit: read the description from a file",
    )
    parser.add_argument(
        "--append-desc",
        metavar="TEXT",
        help="with --edit: append text to the description",
    )
    parser.add_argument(
        "--append-desc-file",
        metavar="PATH",
        help="with --edit: append a file's content to the description",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="with --edit: require the plan→review pipeline"
        " (clears no_plan)",
    )
    parser.add_argument(
        "--repo",
        "--workspace",
        dest="repo",
        metavar="NAME",
        help="with --add: tag the feature for a repo/workspace;"
        " with --pop: restrict to features for it (plus untagged);"
        " without it, pop takes anything; with --list: filter;"
        " with --edit: set the tag ('-' clears it)."
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
        "--no-plan",
        action="store_true",
        help="with --add/--edit: the feature skips the plan→review"
        " pipeline",
    )
    parser.add_argument(
        "--reviewer",
        metavar="SID",
        help="reviewer session id (--claim-review/--verdict)",
    )
    parser.add_argument(
        "--outcome",
        choices=("approved", "denied", "conditional"),
        help="verdict outcome (--verdict)",
    )
    parser.add_argument(
        "--comment", default="", metavar="TEXT",
        help="verdict comment (--verdict)",
    )
    parser.add_argument(
        "--plan-file", metavar="PATH",
        help="plan file path (required by --submit-plan)",
    )
    parser.add_argument(
        "--plan-content", metavar="PATH",
        help="file whose content is stored as the plan"
        " (--submit-plan; falls back to reading --plan-file)",
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
        help="bypass agent/reviewer ownership and status checks"
        " (--done/--release/--submit-plan/--verdict/--reassign/"
        "--edit/--rm)",
    )
    parser.add_argument(
        "--db", type=Path, help="database path (default: $AGENTBACKLOG_DB"
        " or ~/.agentbacklog/backlog.db)"
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    args = parser.parse_args(argv)

    needs_agent = (
        args.pop or args.done is not None or args.claim is not None
        or args.submit_plan is not None or args.reassign is not None
    )
    if needs_agent and not args.agent_id:
        parser.error(
            "--agent-id is required for"
            " --pop/--done/--claim/--submit-plan/--reassign"
        )
    if args.release is not None and not args.agent_id and not args.force:
        parser.error("--release needs --agent-id (or --force)")
    if args.all and not args.list:
        parser.error("--all only makes sense with --list")
    if args.depends_on is not None and args.add is None:
        parser.error("--depends-on only makes sense with --add")
    if args.no_plan and args.add is None and args.edit is None:
        parser.error("--no-plan only makes sense with --add/--edit")
    if args.no_plan and args.plan:
        parser.error("--no-plan and --plan are mutually exclusive")
    if args.title is not None and args.edit is None:
        parser.error("--title only makes sense with --edit")
    if (
        args.description is not None or args.desc_file is not None
    ) and args.add is None and args.edit is None:
        parser.error("--desc/--desc-file only make sense with --add/--edit")
    if args.description is not None and args.desc_file is not None:
        parser.error("--desc and --desc-file are mutually exclusive")
    if args.append_desc is not None and args.edit is None:
        parser.error("--append-desc only makes sense with --edit")
    if args.append_desc_file is not None and args.edit is None:
        parser.error("--append-desc-file only makes sense with --edit")
    if args.plan and args.edit is None:
        parser.error("--plan only makes sense with --edit")
    if args.depend is not None and len(args.depend) < 2:
        parser.error("--depend needs an ID and at least one DEP (0 clears)")
    if args.submit_plan is not None and not args.plan_file:
        parser.error("--submit-plan requires --plan-file")
    if (
        args.claim_review is not None or args.verdict is not None
    ) and not args.reviewer:
        parser.error("--reviewer is required for --claim-review/--verdict")
    if args.verdict is not None and args.outcome is None:
        parser.error("--verdict requires --outcome")
    if args.repo is not None and not (
        args.add is not None or args.pop or args.list
        or args.claim is not None or args.edit is not None
    ):
        parser.error(
            "--repo only makes sense with --add/--pop/--list/--claim/--edit"
        )

    resolved_repo = args.repo or os.environ.get("AGENTBACKLOG_REPO")

    path = args.db or db_path()
    try:
        conn = connect(path)
    except sqlite3.Error as e:
        print(f"agentbacklog: cannot open {path}: {e}", file=sys.stderr)
        return 1

    try:
        if args.add is not None:
            desc = args.description
            if desc is None and args.desc_file:
                desc = _read_desc_file(args.desc_file)
            f = cmd_add(
                conn, args.add, desc or "", args.depends_on,
                resolved_repo, args.no_plan,
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
        elif args.claim is not None:
            f = cmd_claim(
                conn, args.claim, args.agent_id, resolved_repo
            )
            emit(
                f, args.as_json,
                lambda: print_feature(f, f"claimed feature #{f['id']}"),
            )
        elif args.submit_plan is not None:
            f = cmd_submit_plan(
                conn, args.submit_plan, args.agent_id, args.plan_file,
                args.plan_content, args.force,
            )
            emit(
                f, args.as_json,
                lambda: print(
                    f"feature #{f['id']} plan submitted → plan_review"
                ),
            )
        elif args.claim_review is not None:
            f = cmd_claim_review(conn, args.claim_review, args.reviewer)
            emit(
                f, args.as_json,
                lambda: print(
                    f"feature #{f['id']} review claimed"
                    f" by {f['reviewer_id']}"
                ),
            )
        elif args.unclaim_review is not None:
            f = cmd_unclaim_review(conn, args.unclaim_review)
            emit(
                f, args.as_json,
                lambda: print(
                    f"feature #{f['id']} review released → plan_review"
                ),
            )
        elif args.verdict is not None:
            f = cmd_verdict(
                conn, args.verdict, args.reviewer, args.outcome,
                args.comment, args.force,
            )
            emit(
                f, args.as_json,
                lambda: print(
                    f"feature #{f['id']} verdict '{args.outcome}'"
                    " recorded → wip"
                ),
            )
        elif args.reassign is not None:
            f = cmd_reassign(
                conn, args.reassign, args.agent_id, args.force
            )
            emit(
                f, args.as_json,
                lambda: print(
                    f"feature #{f['id']} reassigned to {f['agent_id']}"
                ),
            )
        elif args.reviews is not None:
            rs = cmd_reviews(conn, args.reviews)
            emit(
                rs, args.as_json,
                lambda: print_reviews(args.reviews, rs),
            )
        elif args.edit is not None:
            desc = args.description
            if desc is None and args.desc_file:
                desc = _read_desc_file(args.desc_file)
            append = args.append_desc or ""
            if args.append_desc_file:
                append += _read_desc_file(args.append_desc_file)
            f = cmd_edit(
                conn, args.edit,
                title=args.title, description=desc,
                append_desc=append or None,
                repo_set=args.repo is not None,
                repo=None if args.repo == "-" else args.repo,
                no_plan=args.no_plan, plan=args.plan, force=args.force,
            )
            emit(
                f, args.as_json,
                lambda: print_feature(f, f"updated feature #{f['id']}"),
            )
        elif args.rm is not None:
            detached = cmd_rm(conn, args.rm, args.force)
            msg = f"feature #{args.rm} deleted"
            if detached:
                msg += (
                    f" ({detached} dependent dep edge(s) removed)"
                )
            emit(
                {"id": args.rm, "deleted": True}, args.as_json,
                lambda: print(msg),
            )
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
