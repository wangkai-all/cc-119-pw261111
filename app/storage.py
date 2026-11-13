"""SQLite metadata for tasks and resumable shards (WAL mode, short txns)."""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    input_path TEXT NOT NULL,
    input_size INTEGER NOT NULL,
    input_sha256 TEXT NOT NULL,
    input_mtime_ns INTEGER NOT NULL DEFAULT 0,
    delimiter TEXT NOT NULL,
    encoding TEXT NOT NULL,
    header_mode TEXT NOT NULL,
    columns_json TEXT NOT NULL DEFAULT '[]',
    rules_json TEXT NOT NULL,
    chunk_bytes INTEGER NOT NULL,
    chunk_rows INTEGER NOT NULL,
    fan_in INTEGER NOT NULL,
    work_dir TEXT NOT NULL DEFAULT '',
    output_path TEXT NOT NULL DEFAULT '',
    bad_path TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT '',
    merged_from_count INTEGER NOT NULL DEFAULT 0,
    total_records INTEGER NOT NULL DEFAULT 0,
    dedupe_done INTEGER NOT NULL DEFAULT 0,
    records_read INTEGER NOT NULL DEFAULT 0,
    bytes_read INTEGER NOT NULL DEFAULT 0,
    physical_lines INTEGER NOT NULL DEFAULT 0,
    good_rows INTEGER NOT NULL DEFAULT 0,
    bad_rows INTEGER NOT NULL DEFAULT 0,
    dup_removed INTEGER NOT NULL DEFAULT 0,
    shards_written INTEGER NOT NULL DEFAULT 0,
    shard_bytes_written INTEGER NOT NULL DEFAULT 0,
    merge_parts_total INTEGER NOT NULL DEFAULT 0,
    merge_parts_done INTEGER NOT NULL DEFAULT 0,
    merge_level INTEGER NOT NULL DEFAULT 0,
    output_rows INTEGER NOT NULL DEFAULT 0,
    output_bytes INTEGER NOT NULL DEFAULT 0,
    output_sha256 TEXT NOT NULL DEFAULT '',
    peak_shard_rows INTEGER NOT NULL DEFAULT 0,
    peak_shard_bytes INTEGER NOT NULL DEFAULT 0,
    peak_rss_mb REAL NOT NULL DEFAULT 0,
    elapsed_ms INTEGER NOT NULL DEFAULT 0,
    shard_summary_json TEXT NOT NULL DEFAULT '[]',
    bad_samples_json TEXT NOT NULL DEFAULT '[]',
    out_samples_json TEXT NOT NULL DEFAULT '[]',
    error TEXT,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shards (
    task_id INTEGER NOT NULL,
    dataset TEXT NOT NULL,
    level INTEGER NOT NULL,
    part INTEGER NOT NULL,
    path TEXT NOT NULL,
    rows INTEGER NOT NULL DEFAULT 0,
    bad_rows INTEGER NOT NULL DEFAULT 0,
    bytes INTEGER NOT NULL DEFAULT 0,
    sha256 TEXT NOT NULL DEFAULT '',
    verified INTEGER NOT NULL DEFAULT 0,
    offset_end INTEGER NOT NULL DEFAULT 0,
    lines_end INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (task_id, dataset, level, part)
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rowdict(row):
    return {k: row[k] for k in row.keys()} if row is not None else None


class Store:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._writer: sqlite3.Connection | None = None

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init(self):
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    @contextmanager
    def tx(self):
        with self._lock:
            if self._writer is None:
                self._writer = self._connect()
            conn = self._writer
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    def read(self, fn):
        conn = self._connect()
        try:
            return fn(conn)
        finally:
            conn.close()

    # tasks ------------------------------------------------------------
    def create_task(self, values: dict) -> int:
        ts = utcnow()
        cols = list(values) + ["created_at", "updated_ts"]
        sql = f"INSERT INTO tasks ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
        with self.tx() as c:
            return c.execute(sql, tuple(values.values()) + (ts, ts)).lastrowid

    def get_task(self, task_id: int):
        return self.read(lambda c: rowdict(c.execute(
            "SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()))

    def find_by_key(self, task_key: str):
        return self.read(lambda c: rowdict(c.execute(
            "SELECT * FROM tasks WHERE task_key=?", (task_key,)).fetchone()))

    def list_tasks(self):
        return self.read(lambda c: [rowdict(r) for r in c.execute(
            "SELECT * FROM tasks ORDER BY id DESC").fetchall()])

    def update(self, task_id: int, values: dict | None = None, **kw):
        values = dict(values or {})
        values.update(kw)
        if not values:
            return
        values["updated_ts"] = utcnow()
        sql = "UPDATE tasks SET " + ", ".join(f"{k}=?" for k in values) + " WHERE id=?"
        with self.tx() as c:
            c.execute(sql, tuple(values.values()) + (task_id,))

    def delete_task(self, task_id: int):
        with self.tx() as c:
            c.execute("DELETE FROM shards WHERE task_id=?", (task_id,))
            c.execute("DELETE FROM tasks WHERE id=?", (task_id,))

    # shards -----------------------------------------------------------
    def put_shard(self, task_id: int, dataset: str, level: int, part: int,
                  path: str, rows: int, nbytes: int, sha256: str,
                  verified: bool, offset_end: int = 0, lines_end: int = 0,
                  bad_rows: int = 0):
        with self.tx() as c:
            c.execute(
                """INSERT INTO shards (task_id,dataset,level,part,path,rows,bytes,
                       sha256,verified,offset_end,lines_end,bad_rows,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(task_id,dataset,level,part) DO UPDATE SET
                     path=excluded.path, rows=excluded.rows,
                     bytes=excluded.bytes, sha256=excluded.sha256,
                     verified=excluded.verified, offset_end=excluded.offset_end,
                     lines_end=excluded.lines_end, bad_rows=excluded.bad_rows""",
                (task_id, dataset, level, part, path, rows, nbytes, sha256,
                 1 if verified else 0, offset_end, lines_end, bad_rows, utcnow()))

    def shards(self, task_id: int, dataset: str | None = None,
               level: int | None = None) -> list[dict]:
        sql = "SELECT * FROM shards WHERE task_id=?"
        args: list = [task_id]
        if dataset is not None:
            sql += " AND dataset=?"
            args.append(dataset)
        if level is not None:
            sql += " AND level=?"
            args.append(level)
        sql += " ORDER BY dataset, level, part"
        return self.read(lambda c: [rowdict(r) for r in c.execute(sql, tuple(args))])

    def delete_shards(self, task_id: int, dataset: str | None = None,
                      level: int | None = None,
                      level_from: int | None = None,
                      part: int | None = None):
        sql = "DELETE FROM shards WHERE task_id=?"
        args: list = [task_id]
        if dataset is not None:
            sql += " AND dataset=?"
            args.append(dataset)
        if level is not None:
            sql += " AND level=?"
            args.append(level)
        if level_from is not None:
            sql += " AND level>=?"
            args.append(level_from)
        if part is not None:
            sql += " AND part=?"
            args.append(part)
        with self.tx() as c:
            c.execute(sql, tuple(args))
