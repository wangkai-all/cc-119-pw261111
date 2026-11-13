"""Task lifecycle: inspect uploads, create/resume tasks, preview and search.

The web layer is thin; everything that touches task state lives here so the
same operations are testable without HTTP.

Rules for reuse
---------------
* A task is identified by the *content hash* of the input file plus the full
  rule set, delimiter, encoding, header mode and chunk configuration.
* When a task with the same identity already exists it is reused — completed
  tasks are served as-is, interrupted ones resume from their verified shards.
* Different input bytes or different rules always produce a different task
  identity, so stale shards can never be mixed into a new result.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import threading
import uuid
from datetime import datetime, timezone

from .engine import (Control, Runner, Spec, TaskCancelled, build_task_key,
                     file_digest, peak_rss_mb, unlink)
from .typed import (RecordSplitter, Rules, digest, normalize_columns,
                    stable_json)

DEFAULT_CHUNK_BYTES = 4 * 1024 * 1024
DEFAULT_CHUNK_ROWS = 200_000
DEFAULT_FAN_IN = 16
MAX_CHUNK_BYTES = 512 * 1024 * 1024
MAX_CHUNK_ROWS = 5_000_000
MAX_FAN_IN = 256


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clamp(value, low, high, default):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


def parse_rules(payload: dict, columns: list[str]) -> Rules:
    sort_by = str(payload.get("sort_by") or "").strip()
    if sort_by not in columns:
        raise ApiError(400, f"排序字段 {sort_by!r} 不在列名中")
    business_key = str(payload.get("business_key") or sort_by).strip()
    for name in [p.strip() for p in business_key.split("|") if p.strip()]:
        if name not in columns:
            raise ApiError(400, f"业务键字段 {name!r} 不在列名中")
    updated_at = payload.get("updated_at") or None
    if updated_at and updated_at not in columns:
        raise ApiError(400, f"更新时间和字段 {updated_at!r} 不在列名中")
    dup = str(payload.get("dup_policy") or "keep")
    if dup in ("latest", "first") and not updated_at:
        # latest needs a timestamp; without one it degrades to input order
        if dup == "latest":
            raise ApiError(400, "按更新时间保留重复记录时需要指定更新时间列")
    rules = Rules(
        sort_by=sort_by,
        sort_dir=str(payload.get("sort_dir") or "asc"),
        nulls=str(payload.get("nulls") or "last"),
        business_key=business_key,
        updated_at=updated_at,
        dup_policy=dup,
        sort_type=str(payload.get("sort_type") or "auto"),
    ).normalize()
    return rules


class Manager:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.uploads_dir = os.path.join(self.data_dir, "uploads")
        self.tasks_dir = os.path.join(self.data_dir, "tasks")
        os.makedirs(self.uploads_dir, exist_ok=True)
        os.makedirs(self.tasks_dir, exist_ok=True)
        from .storage import Store
        self.store = Store(os.path.join(self.data_dir, "tasks.sqlite3"))
        self.store.init()
        self._lock = threading.RLock()
        self._workers: dict[int, tuple[threading.Thread, Control]] = {}
        self._recover_interrupted()

    # ------------------------------------------------------------------
    # startup
    # ------------------------------------------------------------------
    def _recover_interrupted(self):
        """Tasks that were running when the process stopped become resumable."""
        for task in self.store.list_tasks():
            if task["status"] in ("running", "pausing"):
                self.store.update(task["id"], status="interrupted",
                                  stage=task["stage"] or "interrupted")

    # ------------------------------------------------------------------
    # uploads
    # ------------------------------------------------------------------
    def save_upload(self, stream, filename: str) -> dict:
        """Stream a request body to disk; identify it by its sha256."""
        tmp = os.path.join(self.uploads_dir, f"tmp-{uuid.uuid4().hex}")
        h = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as fh:
                while True:
                    block = stream.read(1 << 20)
                    if not block:
                        break
                    size += len(block)
                    if size > 8 * 1024 ** 3:
                        raise ApiError(413, "文件超过 8 GiB 限制")
                    h.update(block)
                    fh.write(block)
        except BaseException:
            unlink(tmp)
            raise
        if size == 0:
            unlink(tmp)
            raise ApiError(400, "上传文件为空")
        sha = h.hexdigest()
        final = os.path.join(self.uploads_dir, f"{sha}.csv")
        if os.path.exists(final):
            unlink(tmp)
        else:
            os.replace(tmp, final)
        return {"upload_id": sha, "size": size, "filename": filename or "upload.csv"}

    def upload_path(self, upload_id: str) -> str:
        if not upload_id or any(ch not in "0123456789abcdef" for ch in upload_id):
            raise ApiError(400, "非法的上传标识")
        path = os.path.join(self.uploads_dir, f"{upload_id}.csv")
        if not os.path.isfile(path):
            raise ApiError(404, "上传文件不存在")
        return path

    def inspect(self, upload_id: str, encoding: str, delimiter: str,
                header_mode: str, limit: int = 20) -> dict:
        path = self.upload_path(upload_id)
        encoding, bom = self._effective_encoding(path, encoding)
        delimiter = delimiter if delimiter else ","
        if len(delimiter) != 1:
            raise ApiError(400, "分隔符必须是单个字符")
        header_mode = header_mode if header_mode in ("present", "absent") else "present"
        splitter = RecordSplitter(delimiter)
        records: list[list[str]] = []
        width = 0
        try:
            with open(path, "rb") as fh:
                line_no = 0
                for raw in fh:
                    line_no += 1
                    try:
                        text = raw.decode(encoding)
                    except UnicodeDecodeError:
                        raise ApiError(400, f"文件不是有效的 {encoding} 编码")
                    for ev in splitter.feed(line_no, text):
                        if ev.error or not ev.fields:
                            continue
                        if not width:
                            width = len(ev.fields)
                        if len(ev.fields) != width:
                            continue
                        records.append(ev.fields)
                        if len(records) >= limit + 1:
                            break
                    if len(records) >= limit + 1:
                        break
            for ev in splitter.close():
                if ev.fields and not ev.error and len(ev.fields) == width:
                    records.append(ev.fields)
        except ApiError:
            raise
        if not records:
            raise ApiError(400, "文件中没有可解析的记录")
        if header_mode == "present":
            columns = normalize_columns(records[0])
            data = records[1:limit + 1]
        else:
            columns = [f"col_{i}" for i in range(1, width + 1)]
            data = records[:limit]
        preview = [dict(zip(columns, row)) for row in data]
        return {
            "upload_id": upload_id,
            "encoding": encoding,
            "bom": bom,
            "delimiter": delimiter,
            "header_mode": header_mode,
            "columns": columns,
            "width": width,
            "sample_rows": preview,
            "size": os.path.getsize(path),
        }

    @staticmethod
    def _effective_encoding(path: str, encoding: str) -> tuple[str, bool]:
        encoding = (encoding or "utf-8").strip().lower()
        if encoding in ("utf-8", "utf8", "auto"):
            with open(path, "rb") as fh:
                head = fh.read(3)
            if head == b"\xef\xbb\xbf":
                return "utf-8-sig", True
            return "utf-8", False
        if encoding in ("gbk", "gb18030", "gb2312", "latin-1", "latin1",
                        "iso-8859-1"):
            return encoding, False
        raise ApiError(400, f"不支持的编码 {encoding!r}")

    # ------------------------------------------------------------------
    # create / reuse
    # ------------------------------------------------------------------
    def create_task(self, payload: dict) -> dict:
        upload_id = str(payload.get("upload_id") or "")
        path = self.upload_path(upload_id)
        encoding = str(payload.get("encoding") or "utf-8")
        encoding, _bom = self._effective_encoding(path, encoding)
        delimiter = str(payload.get("delimiter") or ",")
        if len(delimiter) != 1:
            raise ApiError(400, "分隔符必须是单个字符")
        header_mode = str(payload.get("header_mode") or "present")
        if header_mode not in ("present", "absent", "custom"):
            raise ApiError(400, "header_mode 无效")
        chunk_bytes = _clamp(payload.get("chunk_bytes"), 64 * 1024,
                             MAX_CHUNK_BYTES, DEFAULT_CHUNK_BYTES)
        chunk_rows = _clamp(payload.get("chunk_rows"), 100,
                            MAX_CHUNK_ROWS, DEFAULT_CHUNK_ROWS)
        fan_in = _clamp(payload.get("fan_in"), 2, MAX_FAN_IN, DEFAULT_FAN_IN)

        info = self.inspect(upload_id, encoding, delimiter,
                            "absent" if header_mode == "absent" else "present")
        if header_mode == "custom":
            names = normalize_columns(list(payload.get("header_names") or []))
            if len(names) != info["width"]:
                raise ApiError(400, f"自定义表头有 {len(names)} 列，"
                                    f"但文件有 {info['width']} 列")
            columns = names
        elif header_mode == "absent":
            columns = [f"col_{i}" for i in range(1, info["width"] + 1)]
        else:
            columns = info["columns"]

        rules = parse_rules(payload.get("rules") or {}, columns)
        sha = upload_id
        size = os.path.getsize(path)
        mtime_ns = os.stat(path).st_mtime_ns
        identity = {
            "input_sha256": sha,
            "input_size": size,
            "encoding": encoding,
            "delimiter": delimiter,
            "header_mode": header_mode,
            "columns": columns,
            "rules": rules.to_dict(),
            "chunk_bytes": chunk_bytes,
            "chunk_rows": chunk_rows,
            "fan_in": fan_in,
        }
        task_key = build_task_key(identity)

        with self._lock:
            existing = self.store.find_by_key(task_key)
            if existing:
                return {"task_id": existing["id"], "reused": True,
                        "status": existing["status"]}
            task_id = self.store.create_task(dict(
                task_key=task_key,
                name=str(payload.get("name") or f"清理任务 {utcnow()}"),
                input_path=path,
                input_size=size,
                input_sha256=sha,
                input_mtime_ns=mtime_ns,
                delimiter=delimiter,
                encoding=encoding,
                header_mode=header_mode,
                columns_json=json.dumps(columns, ensure_ascii=False),
                rules_json=json.dumps(rules.to_dict(), ensure_ascii=False),
                chunk_bytes=chunk_bytes,
                chunk_rows=chunk_rows,
                fan_in=fan_in,
                work_dir="",
                output_path="",
                bad_path="",
                status="queued",
                stage="",
            ))
            base = os.path.join(self.tasks_dir, str(task_id))
            self.store.update(task_id, work_dir=os.path.join(base, "work"),
                              output_path=os.path.join(base, "cleaned.csv"),
                              bad_path=os.path.join(base, "bad_rows.csv"))
            self.start(task_id, restart=True)
            return {"task_id": task_id, "reused": False, "status": "queued"}

    # ------------------------------------------------------------------
    # spec materialisation
    # ------------------------------------------------------------------
    def spec_for(self, task) -> Spec:
        rules = Rules.from_dict(json.loads(task["rules_json"]))
        return Spec(
            task_id=task["id"],
            input_path=task["input_path"],
            input_size=task["input_size"],
            encoding=task["encoding"],
            delimiter=task["delimiter"],
            header_mode=task["header_mode"],
            columns=json.loads(task["columns_json"]),
            rules=rules,
            chunk_bytes=task["chunk_bytes"],
            chunk_rows=task["chunk_rows"],
            fan_in=task["fan_in"],
            work_dir=task["work_dir"],
            output_path=task["output_path"],
            bad_path=task["bad_path"],
        )

    # ------------------------------------------------------------------
    # worker lifecycle
    # ------------------------------------------------------------------
    def start(self, task_id: int, restart: bool = False) -> dict:
        with self._lock:
            if task_id in self._workers and self._workers[task_id][0].is_alive():
                return {"ok": True, "already_running": True}
            task = self.store.get_task(task_id)
            if task is None:
                raise ApiError(404, "任务不存在")
            if task["status"] == "completed" and not restart:
                return {"ok": True, "already_completed": True}
            self._check_input(task)
            control = Control(
                on_pause=lambda tid=task_id: self.store.update(tid, status="paused"),
                on_resume=lambda tid=task_id: self.store.update(tid, status="running"),
            )
            self.store.update(task_id, status="running", error=None,
                              stage=task["stage"] or "starting")
            spec = self.spec_for(task)
            thread = threading.Thread(target=self._work, args=(task_id, spec, control),
                                      name=f"clean-{task_id}", daemon=True)
            self._workers[task_id] = (thread, control)
            thread.start()
            return {"ok": True, "started": True}

    def _check_input(self, task):
        if not os.path.isfile(task["input_path"]):
            raise ApiError(410, "输入文件已不存在，请重新上传")
        sha, size, _ = file_digest(task["input_path"])
        if sha != task["input_sha256"] or size != task["input_size"]:
            # the content this task was created for is gone: never resume onto
            # a different file, and never mix its old shards into a new result.
            self.store.update(task["id"], status="input_changed", error="输入文件内容已改变")
            raise ApiError(409, "输入文件内容已改变，请重新创建任务")

    def _work(self, task_id: int, spec: Spec, control: Control):
        try:
            Runner(self.store, spec, control).run()
        except TaskCancelled:
            self._on_cancelled(task_id, spec)
        except ApiError as exc:
            self.store.update(task_id, status="failed", error=exc.message)
        except Exception as exc:  # noqa: BLE001 - surface the message to the UI
            import traceback
            self.store.update(task_id, status="failed",
                              error=f"{type(exc).__name__}: {exc}",
                              stage="failed")
            traceback.print_exc()
        finally:
            with self._lock:
                self._workers.pop(task_id, None)

    def _on_cancelled(self, task_id: int, spec: Spec):
        """Cancelled tasks clean up only their own temporary files."""
        runner = Runner(self.store, spec, Control())
        try:
            runner.cleanup_temp()
        finally:
            self.store.update(task_id, status="cancelled", stage="cancelled",
                              merge_parts_done=0, merge_parts_total=0)

    # ------------------------------------------------------------------
    # controls
    # ------------------------------------------------------------------
    def pause(self, task_id: int) -> dict:
        with self._lock:
            entry = self._workers.get(task_id)
            if not entry or not entry[0].is_alive():
                raise ApiError(409, "任务未在运行")
            entry[1].pause()
            self.store.update(task_id, status="pausing")
            return {"ok": True}

    def resume(self, task_id: int) -> dict:
        with self._lock:
            task = self.store.get_task(task_id)
            if task is None:
                raise ApiError(404, "任务不存在")
            entry = self._workers.get(task_id)
            if entry and entry[0].is_alive():
                entry[1].resume()
                self.store.update(task_id, status="running")
                return {"ok": True, "resumed": True}
        self.start(task_id)
        return {"ok": True, "resumed": True}

    def cancel(self, task_id: int) -> dict:
        with self._lock:
            task = self.store.get_task(task_id)
            if task is None:
                raise ApiError(404, "任务不存在")
            entry = self._workers.get(task_id)
            if entry and entry[0].is_alive():
                entry[1].cancel()
                return {"ok": True, "cancelling": True}
            # not running: clean up whatever this task left behind
            spec = self.spec_for(task)
            Runner(self.store, spec, Control()).cleanup_temp()
            self.store.update(task_id, status="cancelled", stage="cancelled")
            return {"ok": True}

    def delete_task(self, task_id: int) -> dict:
        with self._lock:
            task = self.store.get_task(task_id)
            if task is None:
                raise ApiError(404, "任务不存在")
            entry = self._workers.get(task_id)
            if entry and entry[0].is_alive():
                entry[1].cancel()
                entry[0].join(timeout=10)
                if entry[0].is_alive():
                    raise ApiError(409, "任务正在停止，请稍后再删除")
            Runner(self.store, self.spec_for(task), Control()).cleanup_temp()
            for path in (task["output_path"], task["bad_path"]):
                unlink(path)
            shutil.rmtree(os.path.dirname(task["work_dir"]), ignore_errors=True)
            self.store.delete_task(task_id)
            return {"ok": True}

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------
    def task_detail(self, task_id: int) -> dict:
        task = self.store.get_task(task_id)
        if task is None:
            raise ApiError(404, "任务不存在")
        columns = json.loads(task["columns_json"])
        rules = json.loads(task["rules_json"])
        worker = self._workers.get(task_id)
        running = bool(worker and worker[0].is_alive())
        status = task["status"]
        if running and status == "pausing" and worker[1].paused:
            status = "paused"
        shards = self.store.shards(task_id)
        summary = json.loads(task["shard_summary_json"] or "[]")
        shard_disk = sum(s["bytes"] for s in shards)
        return {
            "id": task["id"],
            "name": task["name"],
            "status": status,
            "stage": task["stage"],
            "running": running,
            "columns": columns,
            "rules": rules,
            "input": {
                "path": task["input_path"],
                "size": task["input_size"],
                "sha256": task["input_sha256"],
                "encoding": task["encoding"],
                "delimiter": task["delimiter"],
                "header_mode": task["header_mode"],
            },
            "config": {
                "chunk_rows": task["chunk_rows"],
                "chunk_bytes": task["chunk_bytes"],
                "fan_in": task["fan_in"],
            },
            "progress": {
                "records_read": task["records_read"],
                "physical_lines": task["physical_lines"],
                "bytes_read": task["bytes_read"],
                "good_rows": task["good_rows"],
                "bad_rows": task["bad_rows"],
                "dup_removed": task["dup_removed"],
                "shards_written": task["shards_written"],
                "shard_bytes_written": task["shard_bytes_written"],
                "merge_parts_total": task["merge_parts_total"],
                "merge_parts_done": task["merge_parts_done"],
                "merge_level": task["merge_level"],
                "output_rows": task["output_rows"],
                "output_bytes": task["output_bytes"],
                "output_sha256": task["output_sha256"],
                "elapsed_ms": task["elapsed_ms"],
                "peak_shard_rows": task["peak_shard_rows"],
                "peak_shard_bytes": task["peak_shard_bytes"],
                "peak_rss_mb": task["peak_rss_mb"],
                "temp_shards_on_disk": len(shards),
                "temp_shard_bytes_on_disk": shard_disk,
            },
            "shards": summary,
            "bad_samples": json.loads(task["bad_samples_json"] or "[]"),
            "error": task["error"],
            "created_at": task["created_at"],
            "started_at": task["started_at"],
            "finished_at": task["finished_at"],
        }

    def list_tasks(self) -> list[dict]:
        out = []
        for task in self.store.list_tasks():
            out.append({
                "id": task["id"], "name": task["name"], "status": task["status"],
                "stage": task["stage"], "input_size": task["input_size"],
                "records_read": task["records_read"],
                "good_rows": task["good_rows"], "bad_rows": task["bad_rows"],
                "dup_removed": task["dup_removed"],
                "shards_written": task["shards_written"],
                "merge_parts_done": task["merge_parts_done"],
                "merge_parts_total": task["merge_parts_total"],
                "output_rows": task["output_rows"],
                "elapsed_ms": task["elapsed_ms"],
                "created_at": task["created_at"],
            })
        return out

    # ------------------------------------------------------------------
    # result access
    # ------------------------------------------------------------------
    def preview(self, task_id: int, offset: int = 0, limit: int = 50,
                bad: bool = False) -> dict:
        task = self.store.get_task(task_id)
        if task is None:
            raise ApiError(404, "任务不存在")
        if task["status"] != "completed":
            raise ApiError(409, "任务完成后才能预览结果")
        path = task["bad_path"] if bad else task["output_path"]
        if not os.path.isfile(path):
            raise ApiError(404, "结果文件不存在")
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 500))
        with open(path, encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            try:
                columns = next(reader)
            except StopIteration:
                return {"columns": [], "rows": [], "offset": offset,
                        "has_more": False, "bad": bad}
            rows = []
            skipped = 0
            has_more = False
            for row in reader:
                if skipped < offset:
                    skipped += 1
                    continue
                if len(rows) >= limit:
                    has_more = True
                    break
                rows.append(row)
        return {"columns": columns, "rows": rows, "offset": offset,
                "has_more": has_more, "bad": bad}

    def search(self, task_id: int, query: str, limit: int = 100,
               max_scan: int = 2_000_000) -> dict:
        task = self.store.get_task(task_id)
        if task is None:
            raise ApiError(404, "任务不存在")
        if task["status"] != "completed":
            raise ApiError(409, "任务完成后才能搜索")
        query = (query or "").strip()
        if not query:
            raise ApiError(400, "搜索内容为空")
        limit = max(1, min(int(limit), 500))
        needle = query.casefold()
        path = task["output_path"]
        hits = []
        scanned = 0
        truncated = False
        with open(path, encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            try:
                columns = next(reader)
            except StopIteration:
                columns = []
            for row_no, row in enumerate(reader):
                scanned += 1
                if scanned > max_scan:
                    truncated = True
                    break
                if any(needle in cell.casefold() for cell in row):
                    hits.append({"row": row_no + 1, "fields": row})
                    if len(hits) >= limit:
                        truncated = True
                        break
        return {"columns": columns, "query": query, "hits": hits,
                "scanned": scanned, "truncated": truncated}

    def bad_preview(self, task_id: int, limit: int = 50) -> dict:
        result = self.preview(task_id, 0, limit, bad=True)
        return result

    def result_path(self, task_id: int, bad: bool = False) -> tuple[str, str]:
        task = self.store.get_task(task_id)
        if task is None:
            raise ApiError(404, "任务不存在")
        if task["status"] != "completed":
            raise ApiError(409, "任务完成后才能下载")
        path = task["bad_path"] if bad else task["output_path"]
        if not os.path.isfile(path):
            raise ApiError(404, "结果文件不存在")
        name = f"task-{task_id}-" + ("bad-rows.csv" if bad else "cleaned.csv")
        return path, name
