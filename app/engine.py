"""Bounded-memory CSV cleaning engine: chunked sort + k-way merge.

Stages
------
scan    The input is read as physical lines.  Lines are grouped into logical
        records by ``RecordSplitter`` (a newline inside a quoted field never
        ends a record), typed, buffered and sorted up to the configured chunk
        limit, then sealed as a JSONL shard plus a bad-row sidecar.  Memory is
        bounded by ``chunk_rows`` / ``chunk_bytes``, never by input size.
dedupe  Optional.  Scan shards are sorted by business key and k-way merged into
        one grouped stream; the first row of each contiguous key group wins
        (``latest`` = timestamp desc, ``first`` = original line asc; ties are
        broken by the original physical line).  Winners are re-buffered, sorted
        by the display order and sealed as value shards.
merge   Value shards are k-way merged; while more than ``fan_in`` parts remain,
        each level collapses ``fan_in`` parts into one, so at most ``fan_in``
        files are open at once.
final   The last merge writes a temp CSV and atomically renames it.  Bad rows
        are concatenated in input order into a sidecar CSV.

Determinism
-----------
Every comparator ends with ``(source byte offset, physical line no)``.  Rows
that tie on business data therefore fall back to their original position, so a
rerun of the same input+rules is byte-identical, and a resumed task that reuses
a verified input prefix produces exactly the file a full run would.
"""
from __future__ import annotations

import csv
import hashlib
import heapq
import json
import os
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterator

from .typed import (SEMANTICS_VERSION, MalformedRow, RecordSplitter, Rules,
                    digest, is_null, parse_timestamp_seconds, typed_value)

ENGINE_VERSION = "engine-2608.1"
JSON_SEP = (",", ":")


class TaskCancelled(Exception):
    pass


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------
@dataclass
class Spec:
    task_id: int
    input_path: str
    input_size: int
    encoding: str
    delimiter: str
    header_mode: str            # present | absent | custom
    columns: list[str]
    rules: Rules
    chunk_bytes: int
    chunk_rows: int
    fan_in: int
    work_dir: str
    output_path: str
    bad_path: str

    @property
    def width(self) -> int:
        return len(self.columns)

    @property
    def sort_idx(self) -> int:
        return self.columns.index(self.rules.sort_by)

    @property
    def key_indices(self) -> list[int]:
        names = [p.strip() for p in self.rules.business_key.split("|") if p.strip()]
        if not names:
            names = [self.rules.sort_by]
        return [self.columns.index(n) for n in names]

    @property
    def time_idx(self) -> int | None:
        name = self.rules.updated_at
        if name and name in self.columns:
            return self.columns.index(name)
        return None

    @property
    def dedupe(self) -> bool:
        return self.rules.dup_policy in ("latest", "first")


def build_task_key(payload: dict) -> str:
    body = dict(payload)
    body["engine"] = ENGINE_VERSION
    body["semantics"] = SEMANTICS_VERSION
    return digest(body)


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Comparators
# ---------------------------------------------------------------------------
def _payload_key(value, desc: bool):
    """Native, precomputable sort token for one typed value.

    The returned objects compare with plain Python operators (int/Decimal
    tuples), so ``list.sort`` and ``heapq`` stay in C instead of calling back
    into Python for every comparison:

    * Decimal  -> itself (negated for descending)
    * datetime -> integer seconds since ordinal 1 (negated for descending)
    * str      -> the string, or for descending a tuple of negated codepoints
                  with a sentinel that sorts after every prefix
    * tuple    -> auto mode's ``(type_rank, payload)``: the rank stays
                  ascending so types stay grouped, the payload flips
    """
    if isinstance(value, tuple):
        return (value[0], _payload_key(value[1], desc))
    import decimal
    if isinstance(value, decimal.Decimal):
        return -value if desc else value
    if isinstance(value, datetime):
        secs = (value.toordinal() * 86400 + value.hour * 3600
                + value.minute * 60 + value.second)
        return -secs if desc else secs
    if isinstance(value, str):
        if not desc:
            return value
        # Inverted space: append a sentinel larger than any codepoint so a
        # shorter string sorts *after* its extensions (correct for desc).
        return tuple(-ord(c) for c in value) + (0x110000,)
    return value


class Row:
    __slots__ = ("fields", "value", "null", "key", "time", "offset", "line")

    def __init__(self, fields, value, null, key, time, offset, line):
        self.fields = fields
        self.value = value
        self.null = null
        self.key = key
        self.time = time
        self.offset = offset
        self.line = line


def value_key(r: Row, rules: Rules) -> tuple:
    """Display order: nulls at the configured end, typed values, then position.

    Null rows never compare against non-null rows (the bucket differs), so the
    placeholder in their second slot is never reached.
    """
    null_first = rules.nulls == "first"
    bucket = 0 if (r.null == null_first) else 1
    if r.null:
        return (bucket, 0, r.offset, r.line)
    return (bucket, _payload_key(r.value, rules.sort_dir == "desc"),
            r.offset, r.line)


def group_key(r: Row, policy: str) -> tuple:
    """Duplicate selection order: business key asc, then latest timestamp /
    earliest input line, then original position.  ``r.time`` is integer
    seconds, so descending is a negation and stays native."""
    key = r.key if isinstance(r.key, tuple) else tuple(r.key)
    if policy == "latest":
        return (key, -int(r.time or 0), r.line, r.offset)
    return (key, r.line, r.offset)


# ---------------------------------------------------------------------------
# Shard serialisation
# ---------------------------------------------------------------------------
def _enc(v):
    """Lossless, typed encoding for shard round trips.

    Every scalar carries its type tag and tuples carry their structure, so a
    value decoded from a shard compares exactly like the value that was
    originally typed (a Decimal never degrades to a string).
    """
    import decimal
    if v is None:
        return ["z"]
    if isinstance(v, tuple):
        return ["tup", [_enc(x) for x in v]]
    if isinstance(v, decimal.Decimal):
        return ["d", str(v)]
    if isinstance(v, datetime):
        return ["dt", v.isoformat(sep=" ")]
    if isinstance(v, bool):
        return ["b", v]
    if isinstance(v, int):
        return ["i", v]
    if isinstance(v, float):
        return ["f", v]
    return ["s", str(v)]


def _dec(obj):
    import decimal
    if not isinstance(obj, list) or not obj:
        return obj
    tag = obj[0]
    if tag == "z":
        return None
    if tag == "tup":
        return tuple(_dec(x) for x in obj[1])
    if tag == "d":
        return decimal.Decimal(obj[1])
    if tag == "dt":
        return datetime.fromisoformat(obj[1])
    if tag == "b":
        return bool(obj[1])
    if tag == "i":
        return int(obj[1])
    if tag == "f":
        return float(obj[1])
    if tag == "s":
        return str(obj[1])
    return obj


def encode_row(r: Row) -> str:
    return json.dumps(
        {"f": r.fields, "v": _enc(r.value), "n": r.null,
         "k": [_enc(x) for x in r.key], "t": r.time, "o": r.offset, "l": r.line},
        ensure_ascii=False, separators=JSON_SEP, sort_keys=True)


def decode_row(line: str) -> Row:
    p = json.loads(line)
    key = p["k"]
    key = tuple(_dec(x) for x in key) if isinstance(key, list) else key
    return Row(p["f"], _dec(p["v"]), p["n"], key, p["t"], p["o"], p["l"])


BAD_MAX_RAW = 4000


def encode_bad(offset: int, start_line: int, end_line: int, reason: str,
               raw: str) -> str:
    return json.dumps({"o": offset, "a": start_line, "b": end_line,
                       "r": reason, "raw": raw[:BAD_MAX_RAW]},
                      ensure_ascii=False, separators=JSON_SEP)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(1 << 20)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def file_digest(path: str) -> tuple[str, int, int]:
    """(sha256, size, mtime_ns) — task identity and change detection."""
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            block = fh.read(1 << 20)
            if not block:
                break
            h.update(block)
            size += len(block)
        mtime_ns = os.fstat(fh.fileno()).st_mtime_ns
    return h.hexdigest(), size, mtime_ns


# ---------------------------------------------------------------------------
# k-way merge
# ---------------------------------------------------------------------------
def merge_stream(paths: list[str], key_fn: Callable[[Row], tuple],
                 on_part_done: Callable[[], None] | None = None,
                 checkpoint: Callable[[], None] | None = None) -> Iterator[Row]:
    readers: list = []
    try:
        heap: list = []
        for idx, path in enumerate(paths):
            fh = open(path, "r", encoding="utf-8", newline="\n")
            readers.append(fh)
            line = fh.readline()
            if not line:
                if on_part_done:
                    on_part_done()
                continue
            row = decode_row(line)
            heapq.heappush(heap, (key_fn(row), idx, row, fh))
        while heap:
            if checkpoint:
                checkpoint()
            _k, idx, row, fh = heapq.heappop(heap)
            yield row
            nxt = fh.readline()
            if nxt:
                r2 = decode_row(nxt)
                heapq.heappush(heap, (key_fn(r2), idx, r2, fh))
            elif on_part_done:
                on_part_done()
    finally:
        for fh in readers:
            fh.close()


# ---------------------------------------------------------------------------
# Control
# ---------------------------------------------------------------------------
class Control:
    def __init__(self, on_pause=None, on_resume=None):
        import threading
        self._ev = threading.Event()
        self._ev.set()
        self.cancelled = False
        self.on_pause = on_pause
        self.on_resume = on_resume

    def pause(self):
        self._ev.clear()

    def resume(self):
        self._ev.set()

    def cancel(self):
        self.cancelled = True
        self._ev.set()

    @property
    def paused(self) -> bool:
        return not self._ev.is_set()

    def checkpoint(self):
        if self.cancelled:
            raise TaskCancelled()
        if not self._ev.is_set():
            if self.on_pause:
                self.on_pause()
            while not self._ev.wait(0.1):
                if self.cancelled:
                    raise TaskCancelled()
            if self.on_resume:
                self.on_resume()
        if self.cancelled:
            raise TaskCancelled()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
class Runner:
    def __init__(self, store, spec: Spec, control: Control):
        self.store = store
        self.spec = spec
        self.control = control
        self.tid = spec.task_id
        self.started = time.time()
        self._last_write = 0.0
        self.records = 0
        self.good = 0
        self.bad = 0
        self.dup_removed = 0
        self.shards_written = 0
        self.shard_bytes = 0
        self.merge_total = 0
        self.merge_done = 0
        self.summary: list[dict] = []
        self.peak_rows = 0
        self.peak_bytes = 0
        self.bytes_read = 0
        self.lines_read = 0
        self.out_rows = 0
        self.out_bytes = 0
        self.out_sha = ""
        self.bad_samples: list[dict] = []

    # -- paths ---------------------------------------------------------
    def part_path(self, kind: str, part: int) -> str:
        return os.path.join(self.spec.work_dir, kind, f"{kind}-{part:05d}.jsonl")

    # -- progress ------------------------------------------------------
    def db(self, force=False, **extra):
        now = time.time()
        if not force and now - self._last_write < 0.4:
            return
        self._last_write = now
        payload = dict(
            records_read=self.records, good_rows=self.good, bad_rows=self.bad,
            bytes_read=self.bytes_read, physical_lines=self.lines_read,
            dup_removed=self.dup_removed, shards_written=self.shards_written,
            shard_bytes_written=self.shard_bytes,
            merge_parts_total=self.merge_total, merge_parts_done=self.merge_done,
            peak_shard_rows=self.peak_rows, peak_shard_bytes=self.peak_bytes,
            peak_rss_mb=peak_rss_mb(),
            elapsed_ms=int((time.time() - self.started) * 1000))
        payload.update(extra)
        self.store.update(self.tid, payload)

    def stage(self, name: str):
        self.db(force=True, stage=name)

    # -- shard verification -------------------------------------------
    def _verified_chain(self, dataset: str, level: int = 0) -> list[dict]:
        """Contiguous verified parts [1..k] whose files still match their hash."""
        shards = self.store.shards(self.tid, dataset=dataset, level=level)
        chain: list[dict] = []
        expect = 1
        for shard in shards:
            if shard["part"] != expect or not shard["verified"]:
                break
            path = shard["path"]
            try:
                if os.path.getsize(path) != shard["bytes"]:
                    break
                if sha256_file(path) != shard["sha256"]:
                    break
            except OSError:
                break
            chain.append(shard)
            expect += 1
        return chain

    def _drop_unverified(self, dataset: str, keep_parts: set[int],
                         level: int | None = 0):
        for shard in self.store.shards(self.tid, dataset=dataset, level=level):
            if shard["part"] not in keep_parts:
                unlink(shard["path"])
                self.store.delete_shards(self.tid, dataset=dataset,
                                         level=level, part=shard["part"])

    def _clear_dataset(self, dataset: str):
        for shard in self.store.shards(self.tid, dataset=dataset):
            unlink(shard["path"])
        self.store.delete_shards(self.tid, dataset=dataset)

    # ------------------------------------------------------------------
    def run(self) -> None:
        spec = self.spec
        os.makedirs(spec.work_dir, exist_ok=True)
        for kind in ("scan", "bad", "value", "merge"):
            os.makedirs(os.path.join(spec.work_dir, kind), exist_ok=True)
        for path in (spec.output_path, spec.bad_path):
            os.makedirs(os.path.dirname(path), exist_ok=True)

        self.store.update(self.tid, status="running", stage="recover",
                          error=None, started_at=utcnow())
        self.db(force=True)

        scan = self._recover_scan()
        if scan:
            self.records = sum(s["rows"] + s["bad_rows"] for s in scan)
            self.good = sum(s["rows"] for s in scan)
            self.bad = sum(s["bad_rows"] for s in scan)
            self.bytes_read = scan[-1]["offset_end"]
            self.lines_read = scan[-1]["lines_end"]
            self.shards_written = len(scan)
            self.shard_bytes = sum(s["bytes"] for s in scan)
            for s in scan:
                self._note_shard("scan", 0, s["part"], s["rows"], s["bytes"])

        if not scan or scan[-1]["offset_end"] < spec.input_size:
            self.stage("scan")
            scan = self._scan(scan)

        self.db(force=True, stage="dedupe" if spec.dedupe else "merge",
                total_records=self.records, records_read=self.records,
                good_rows=self.good, bad_rows=self.bad,
                bytes_read=spec.input_size, physical_lines=self.lines_read,
                shards_written=self.shards_written,
                shard_bytes_written=self.shard_bytes,
                peak_shard_rows=self.peak_rows,
                peak_shard_bytes=self.peak_bytes,
                shard_summary_json=json.dumps(self.summary),
                bad_samples_json=json.dumps(self.bad_samples[:8],
                                            ensure_ascii=False))

        if spec.dedupe:
            self.stage("dedupe")
            value = self._dedupe(scan)
        else:
            value = scan

        self.stage("merge")
        self._merge(value)
        self._write_bad(scan)
        self._finish(scan)

    # ------------------------------------------------------------------
    # recovery
    # ------------------------------------------------------------------
    def _recover_scan(self) -> list[dict]:
        """Reuse only the verified, checksum-matching prefix of the input.

        The prefix must also have its bad-row sidecars; otherwise the whole
        scan is discarded rather than mixing old and new results.
        """
        spec = self.spec
        scan = self._verified_chain("scan")
        bad_shards = {s["part"]: s for s in
                      self.store.shards(self.tid, dataset="bad", level=0)}
        prefix: list[dict] = []
        for shard in scan:
            b = bad_shards.get(shard["part"])
            if b is None or not b["verified"]:
                break
            try:
                if os.path.getsize(b["path"]) != b["bytes"]:
                    break
                if sha256_file(b["path"]) != b["sha256"]:
                    break
            except OSError:
                break
            if shard["offset_end"] > spec.input_size:
                break
            prefix.append({**shard, "bad_rows": b["rows"]})
        if prefix and prefix[-1]["offset_end"] == 0:
            prefix = []
        keep = {s["part"] for s in prefix}
        self._drop_unverified("scan", keep)
        self._drop_unverified("bad", keep)
        # Downstream results are invalid unless the *whole* scan survived and
        # the dedupe stage had completed.  A partial value chain is never a
        # valid result, so it is dropped rather than reused.
        scan_complete = bool(prefix) and prefix[-1]["offset_end"] >= spec.input_size
        self._clear_dataset("merge")
        if not scan_complete:
            self._clear_dataset("value")
            self.store.update(self.tid, dedupe_done=0, dup_removed=0)
        return prefix

    def _note_shard(self, dataset: str, level: int, part: int, rows: int,
                    nbytes: int, bad: int = 0):
        self.summary.append({"dataset": dataset, "level": level, "part": part,
                             "rows": rows, "bad_rows": bad, "bytes": nbytes})
        if dataset in ("scan", "value"):       # chunk shards, not merge parts
            self.peak_rows = max(self.peak_rows, rows)
            self.peak_bytes = max(self.peak_bytes, nbytes)

    # ------------------------------------------------------------------
    # scan
    # ------------------------------------------------------------------
    def _scan(self, scan: list[dict]) -> list[dict]:
        spec = self.spec
        part = scan[-1]["part"] + 1 if scan else 1
        offset = scan[-1]["offset_end"] if scan else 0
        line_no = scan[-1]["lines_end"] if scan else 0
        skip_header = (offset == 0 and spec.header_mode == "present")

        buf: list[tuple[tuple, str]] = []      # (sort key, encoded JSON row)
        buf_est = 0
        bad_lines: list[str] = []
        bad_count = 0
        parts_written = 0
        splitter = RecordSplitter(spec.delimiter)
        sort_idx = spec.sort_idx
        key_indices = spec.key_indices
        time_idx = spec.time_idx
        order = (lambda r: group_key(r, spec.rules.dup_policy)) if spec.dedupe \
            else (lambda r: value_key(r, spec.rules))
        sort_type = spec.rules.sort_type
        width = spec.width

        def flush(end_offset: int, end_line: int):
            nonlocal buf, buf_est, bad_lines, bad_count, part, parts_written
            path = self.part_path("scan", part)
            bad_path = self.part_path("bad", part)
            buf.sort(key=lambda entry: entry[0])
            # one buffered write per shard: rows are already encoded, so the
            # file sees a single write instead of one per record
            payload = b"\n".join(text for _, text in buf)
            with open(path, "wb") as fh:
                fh.write(payload)
                if payload:
                    fh.write(b"\n")
            write_lines(bad_path, bad_lines)
            p_bytes = os.path.getsize(path)
            b_bytes = os.path.getsize(bad_path)
            self.store.put_shard(self.tid, "scan", 0, part, path, len(buf),
                                 p_bytes, sha256_file(path), True,
                                 end_offset, end_line)
            self.store.put_shard(self.tid, "bad", 0, part, bad_path, bad_count,
                                 b_bytes, sha256_file(bad_path), True,
                                 end_offset, end_line)
            self._note_shard("scan", 0, part, len(buf), p_bytes, bad_count)
            self.good += len(buf)
            self.bad += bad_count
            self.shards_written += 1
            self.shard_bytes += p_bytes + b_bytes
            self.bytes_read = end_offset
            self.lines_read = end_line
            self.db(force=True, shard_summary_json=json.dumps(self.summary),
                    bad_samples_json=json.dumps(self.bad_samples[:8],
                                                ensure_ascii=False))
            buf = []
            buf_est = 0
            bad_lines = []
            bad_count = 0
            part += 1
            parts_written += 1

        def isolate(pos, start_line, end_line, reason, raw):
            nonlocal bad_count
            bad_lines.append(encode_bad(pos, start_line, end_line, reason, raw))
            bad_count += 1
            if len(self.bad_samples) < 8:
                self.bad_samples.append({"start_line": start_line,
                                         "end_line": end_line,
                                         "reason": reason,
                                         "raw": raw[:300]})

        def handle(events, end_offset: int, end_line: int):
            nonlocal buf, buf_est, skip_header
            for ev in events:
                if ev.error:
                    self.records += 1
                    isolate(ev.payload, ev.start_line, ev.end_line, ev.error, ev.raw)
                    continue
                if skip_header:
                    skip_header = False
                    continue
                self.records += 1
                fields = ev.fields or []
                if len(fields) != width:
                    isolate(ev.payload, ev.start_line, ev.end_line,
                            f"列数不符：期望 {width}，实际 {len(fields)}", ev.raw)
                    continue
                try:
                    null, value = typed_value(fields[sort_idx], sort_type)
                except MalformedRow as exc:
                    isolate(ev.payload, ev.start_line, ev.end_line,
                            f"排序字段 {spec.rules.sort_by}：{exc.reason}", ev.raw)
                    continue
                tm = 0
                if time_idx is not None:
                    tm = parse_timestamp_seconds(fields[time_idx])
                    if fields[time_idx].strip() and not tm:
                        isolate(ev.payload, ev.start_line, ev.end_line,
                                f"更新字段 {spec.rules.updated_at}：非法日期 "
                                f"{fields[time_idx]!r}", ev.raw)
                        continue
                key = tuple("" if is_null(fields[i]) else fields[i].strip()
                            for i in key_indices)
                row = Row(fields, value, null, key, tm, ev.payload,
                          ev.start_line)
                # encode once and count the bytes we will actually write, so
                # the configured chunk size bounds the shard and the buffer
                text = encode_row(row).encode("utf-8")
                buf.append((order(row), text))
                buf_est += len(text) + 1
            if buf and (len(buf) >= spec.chunk_rows or buf_est >= spec.chunk_bytes) \
                    and splitter.at_boundary():
                flush(end_offset, end_line)

        self.db(force=True, bytes_read=offset, physical_lines=line_no,
                records_read=self.records)
        with open(spec.input_path, "rb") as fh:
            fh.seek(offset)
            cur = offset
            while True:
                self.control.checkpoint()
                raw = fh.readline()
                if not raw:
                    break
                pos = cur
                cur += len(raw)
                line_no += 1
                try:
                    text = raw.decode(spec.encoding)
                except UnicodeDecodeError:
                    handle(splitter.close(), pos, line_no - 1)
                    splitter = RecordSplitter(spec.delimiter)
                    self.records += 1
                    isolate(pos, line_no, line_no, "解码失败", repr(raw[:80]))
                    self.db(bytes_read=cur, physical_lines=line_no,
                            records_read=self.records)
                    continue
                handle(splitter.feed(line_no, text, payload=pos), cur, line_no)
                if line_no % 4096 == 0:
                    self.db(bytes_read=cur, physical_lines=line_no,
                            records_read=self.records)
            handle(splitter.close(), cur, line_no)
            self.bytes_read = cur
            self.lines_read = line_no
            if buf or bad_lines or parts_written == 0:
                flush(cur, line_no)
        scan.extend(self.store.shards(self.tid, dataset="scan", level=0))
        self.db(force=True, bytes_read=self.bytes_read,
                physical_lines=self.lines_read, records_read=self.records,
                good_rows=self.good, bad_rows=self.bad,
                shard_summary_json=json.dumps(self.summary),
                bad_samples_json=json.dumps(self.bad_samples[:8],
                                            ensure_ascii=False))
        return self.store.shards(self.tid, dataset="scan", level=0)

    # ------------------------------------------------------------------
    # dedupe
    # ------------------------------------------------------------------
    def _dedupe(self, scan: list[dict]) -> list[dict]:
        spec = self.spec
        task = self.store.get_task(self.tid)
        value = self._verified_chain("value") if task["dedupe_done"] else []
        whole_scan = bool(scan) and scan[-1]["offset_end"] >= spec.input_size
        if value and whole_scan:
            self.dup_removed = self.store.get_task(self.tid)["dup_removed"]
            for s in value:
                self._note_shard("value", 0, s["part"], s["rows"], s["bytes"])
            return value
        self._clear_dataset("value")
        self._clear_dataset("merge")
        self.store.update(self.tid, dedupe_done=0, dup_removed=0)

        policy = spec.rules.dup_policy
        paths = [s["path"] for s in scan]
        part = 1
        buf: list[tuple[tuple, str]] = []      # (sort key, encoded JSON row)
        buf_est = 0
        removed = 0
        last_key: tuple | None = None
        self.merge_total = len(paths)
        self.merge_done = 0
        self.db(force=True, merge_parts_total=self.merge_total,
                merge_parts_done=0)

        def flush():
            nonlocal buf, buf_est, part
            buf.sort(key=lambda entry: entry[0])
            path = self.part_path("value", part)
            payload = b"\n".join(text for _, text in buf)
            with open(path, "wb") as fh:
                fh.write(payload)
                if payload:
                    fh.write(b"\n")
            nbytes = os.path.getsize(path)
            self.store.put_shard(self.tid, "value", 0, part, path, len(buf),
                                 nbytes, sha256_file(path), True)
            self._note_shard("value", 0, part, len(buf), nbytes)
            self.shards_written += 1
            self.shard_bytes += nbytes
            buf = []
            buf_est = 0
            part += 1

        def part_done():
            self.merge_done += 1
            self.db()

        for row in merge_stream(paths, lambda r: group_key(r, policy),
                                on_part_done=part_done,
                                checkpoint=self.control.checkpoint):
            if last_key is not None and row.key == last_key:
                removed += 1
                continue
            last_key = row.key
            text = encode_row(row).encode("utf-8")
            buf.append((value_key(row, spec.rules), text))
            buf_est += len(text) + 1
            if len(buf) >= spec.chunk_rows or buf_est >= spec.chunk_bytes:
                flush()
                self.dup_removed = removed
                self.db(dup_removed=removed)
        if buf or not value:
            flush()
        self.dup_removed = removed
        self.merge_done = self.merge_total
        self.store.update(self.tid, dedupe_done=1, dup_removed=removed)
        self.db(force=True, dup_removed=removed,
                merge_parts_done=self.merge_total)
        return self.store.shards(self.tid, dataset="value", level=0)

    # ------------------------------------------------------------------
    # merge
    # ------------------------------------------------------------------
    def _merge(self, value: list[dict]) -> None:
        spec = self.spec
        # intermediate merges are cheap and always rebuilt: only fully
        # verified *scan/value* shards are trusted across interruptions.
        self._clear_dataset("merge")

        level = 1
        current = list(value)
        while len(current) > spec.fan_in:
            total = len(current)
            done = 0
            self.merge_total = total
            self.merge_done = 0
            self.db(force=True, merge_level=level, merge_parts_total=total,
                    merge_parts_done=0)
            produced: list[dict] = []
            for start in range(0, total, spec.fan_in):
                group = current[start:start + spec.fan_in]
                idx = start // spec.fan_in + 1
                out = os.path.join(spec.work_dir, "merge",
                                   f"merge-L{level}-{idx:05d}.jsonl")
                rows = 0
                with open(out, "w", encoding="utf-8", newline="\n") as fh:
                    def one():
                        nonlocal done
                        done += 1
                        self.merge_done = done
                        self.db(merge_parts_done=done)
                    for row in merge_stream([s["path"] for s in group],
                                            lambda r: value_key(r, spec.rules),
                                            on_part_done=one,
                                            checkpoint=self.control.checkpoint):
                        fh.write(encode_row(row))
                        fh.write("\n")
                        rows += 1
                nbytes = os.path.getsize(out)
                self.store.put_shard(self.tid, "merge", level, idx, out, rows,
                                     nbytes, sha256_file(out), True)
                self._note_shard("merge", level, idx, rows, nbytes)
                produced.append({"path": out, "rows": rows, "bytes": nbytes})
                self.db(merge_parts_done=done)
            current = produced
            level += 1

        # final merge -> CSV
        total = len(current)
        self.merge_total = total
        self.merge_done = 0
        self.db(force=True, merge_level=level, merge_parts_total=total,
                merge_parts_done=0)
        tmp = os.path.join(spec.work_dir, "output.csv.tmp")
        digest_ = hashlib.sha256()
        rows = 0
        with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            writer.writerow(spec.columns)

            def one():
                self.merge_done += 1
                self.db(merge_parts_done=self.merge_done)

            for row in merge_stream([s["path"] for s in current],
                                    lambda r: value_key(r, spec.rules),
                                    on_part_done=one,
                                    checkpoint=self.control.checkpoint):
                writer.writerow(row.fields)
                rows += 1
                if rows % 4096 == 0:
                    self.db(merge_parts_done=self.merge_done)
        with open(tmp, "rb") as fh:
            while True:
                block = fh.read(1 << 20)
                if not block:
                    break
                digest_.update(block)
        self.out_rows = rows
        self.out_sha = digest_.hexdigest()
        self.out_bytes = os.path.getsize(tmp)
        self.merge_done = total
        os.replace(tmp, spec.output_path)
        self.db(force=True, output_rows=rows, output_bytes=self.out_bytes,
                output_sha256=self.out_sha, merge_parts_total=total,
                merge_parts_done=self.merge_done)

    # ------------------------------------------------------------------
    def _write_bad(self, scan: list[dict]) -> None:
        tmp = os.path.join(self.spec.work_dir, "bad_rows.csv.tmp")
        with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            writer.writerow(["start_line", "end_line", "byte_offset", "reason", "raw"])
            for shard in scan:
                path = self.part_path("bad", shard["part"])
                if not os.path.isfile(path):
                    continue
                with open(path, "r", encoding="utf-8") as reader:
                    for line in reader:
                        if not line.strip():
                            continue
                        self.control.checkpoint()
                        p = json.loads(line)
                        writer.writerow([p["a"], p["b"], p["o"], p["r"], p["raw"]])
        os.replace(tmp, self.spec.bad_path)

    def _finish(self, scan: list[dict]) -> None:
        self.cleanup_temp()
        self.store.update(
            self.tid, status="completed", stage="done",
            records_read=self.records, good_rows=self.good, bad_rows=self.bad,
            dup_removed=self.dup_removed, bytes_read=self.spec.input_size,
            physical_lines=self.lines_read, total_records=self.records,
            output_rows=self.out_rows, output_bytes=self.out_bytes,
            output_sha256=self.out_sha, shards_written=self.shards_written,
            shard_bytes_written=self.shard_bytes,
            merge_parts_total=self.merge_total, merge_parts_done=self.merge_done,
            merge_level=self.store.get_task(self.tid)["merge_level"],
            peak_shard_rows=self.peak_rows, peak_shard_bytes=self.peak_bytes,
            peak_rss_mb=peak_rss_mb(),
            shard_summary_json=json.dumps(self.summary),
            bad_samples_json=json.dumps(self.bad_samples[:8], ensure_ascii=False),
            elapsed_ms=int((time.time() - self.started) * 1000),
            finished_at=utcnow())

    def cleanup_temp(self) -> None:
        """Delete only this task's temporary shards and work directory."""
        for shard in self.store.shards(self.tid):
            unlink(shard["path"])
        self.store.delete_shards(self.tid)
        shutil.rmtree(self.spec.work_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def write_lines(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for line in lines:
            fh.write(line)
            fh.write("\n")


def unlink(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def peak_rss_mb() -> float:
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)
    except Exception:
        return 0.0
