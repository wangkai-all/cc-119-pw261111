"""HTTP front end: JSON API + static page.  Stdlib only, no dependencies.

Endpoints
---------
POST /api/uploads                 raw body -> {upload_id}
POST /api/inspect                 {upload_id, encoding, delimiter, header_mode}
POST /api/tasks                   create or reuse a task and start it
GET  /api/tasks                   task list
GET  /api/tasks/{id}              detail: progress, shards, samples
POST /api/tasks/{id}/pause|resume|cancel
DEL  /api/tasks/{id}
GET  /api/tasks/{id}/preview?offset=&limit=&bad=0|1
GET  /api/tasks/{id}/search?q=
GET  /api/tasks/{id}/download?kind=clean|bad
GET  /api/meta                   limits and defaults
"""
from __future__ import annotations

import json
import mimetypes
import os
import re
import shutil
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .service import (ApiError, DEFAULT_CHUNK_BYTES, DEFAULT_CHUNK_ROWS,
                      DEFAULT_FAN_IN, MAX_CHUNK_BYTES, MAX_CHUNK_ROWS,
                      MAX_FAN_IN, Manager)

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class Api(BaseHTTPRequestHandler):
    server_version = "CsvCleaner/1.0"
    protocol_version = "HTTP/1.1"

    # -- plumbing ------------------------------------------------------
    @property
    def manager(self) -> Manager:
        return self.server.manager  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):
        if os.environ.get("CSV_CLEAN_QUIET") != "1":
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, exc: ApiError):
        self._send_json({"error": exc.message, "status": exc.status}, exc.status)

    def _read_json(self, limit=4 * 1024 * 1024):
        length = int(self.headers.get("Content-Length") or 0)
        if length > limit:
            raise ApiError(413, "请求体过大")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            raise ApiError(400, "请求体不是合法 JSON")

    # -- routing -------------------------------------------------------
    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def do_DELETE(self):
        self._route("DELETE")

    def _route(self, method: str):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        query = parse_qs(parsed.query)
        try:
            if path.startswith("/api/"):
                self._handle_api(method, path, query)
            elif method == "GET":
                self._serve_static(path)
            else:
                raise ApiError(405, "不支持的方法")
        except ApiError as exc:
            self._send_error_json(exc)
        except BrokenPipeError:
            pass
        except Exception as exc:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            self._send_json({"error": f"服务器错误：{exc}", "status": 500}, 500)

    def _handle_api(self, method: str, path: str, query: dict):
        m = self.manager

        if path == "/api/meta" and method == "GET":
            return self._send_json({
                "defaults": {"chunk_bytes": DEFAULT_CHUNK_BYTES,
                             "chunk_rows": DEFAULT_CHUNK_ROWS,
                             "fan_in": DEFAULT_FAN_IN},
                "limits": {"chunk_bytes": MAX_CHUNK_BYTES,
                           "chunk_rows": MAX_CHUNK_ROWS,
                           "fan_in": MAX_FAN_IN},
                "encodings": ["utf-8", "gbk", "gb18030", "latin-1"],
                "sort_types": ["auto", "number", "date", "text"],
                "machine": machine_info(),
            })

        if path == "/api/uploads" and method == "POST":
            filename = self.headers.get("X-Filename") or "upload.csv"
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                raise ApiError(400, "上传内容为空")
            result = m.save_upload(_LimitedReader(self.rfile, length), filename)
            return self._send_json(result)

        if path == "/api/inspect" and method == "POST":
            body = self._read_json()
            return self._send_json(m.inspect(
                str(body.get("upload_id") or ""),
                body.get("encoding") or "utf-8",
                body.get("delimiter") or ",",
                body.get("header_mode") or "present",
                int(body.get("limit") or 20)))

        if path == "/api/tasks" and method == "POST":
            return self._send_json(m.create_task(self._read_json() or {}),
                                   status=201)

        if path == "/api/tasks" and method == "GET":
            return self._send_json({"tasks": m.list_tasks()})

        match = re.fullmatch(r"/api/tasks/(\d+)(?:/([a-z]+))?", path)
        if not match:
            raise ApiError(404, "接口不存在")
        task_id = int(match.group(1))
        action = match.group(2)

        if action is None and method == "GET":
            return self._send_json(m.task_detail(task_id))
        if action is None and method == "DELETE":
            return self._send_json(m.delete_task(task_id))
        if action in ("pause", "resume", "cancel") and method == "POST":
            return self._send_json(getattr(m, action)(task_id))
        if action == "preview" and method == "GET":
            return self._send_json(m.preview(
                task_id,
                int(_one(query, "offset", "0")),
                int(_one(query, "limit", "50")),
                _one(query, "bad", "0") in ("1", "true")))
        if action == "search" and method == "GET":
            return self._send_json(m.search(
                task_id, _one(query, "q", ""),
                int(_one(query, "limit", "100"))))
        if action == "download" and method == "GET":
            kind = _one(query, "kind", "clean")
            path_, name = m.result_path(task_id, bad=(kind == "bad"))
            return self._send_file(path_, name)
        raise ApiError(404, "接口不存在")

    # -- static --------------------------------------------------------
    def _serve_static(self, path: str):
        if path in ("/", ""):
            path = "/index.html"
        safe = os.path.normpath(path).lstrip("/")
        if safe.startswith(".."):
            raise ApiError(403, "禁止访问")
        target = os.path.join(STATIC_DIR, safe)
        if not os.path.isfile(target):
            raise ApiError(404, "页面不存在")
        with open(target, "rb") as fh:
            body = fh.read()
        ctype = mimetypes.guess_type(target)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype + ("; charset=utf-8"
                                                  if ctype.startswith("text/")
                                                  or ctype.endswith("javascript")
                                                  else ""))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: str, download_name: str):
        size = os.path.getsize(path)
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Disposition",
                         f'attachment; filename="{download_name}"')
        self.end_headers()
        with open(path, "rb") as fh:
            shutil.copyfileobj(fh, self.wfile)


class _LimitedReader:
    def __init__(self, rfile, remaining: int):
        self.rfile = rfile
        self.remaining = remaining

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        if size is None or size < 0 or size > self.remaining:
            size = self.remaining
        data = self.rfile.read(size)
        self.remaining -= len(data)
        return data


def _one(query: dict, key: str, default: str) -> str:
    values = query.get(key)
    return values[0] if values else default


def machine_info() -> dict:
    from .engine import peak_rss_mb
    try:
        cpus = os.cpu_count() or 1
    except Exception:  # noqa: BLE001
        cpus = 1
    return {"cpus": cpus, "peak_rss_mb": peak_rss_mb()}


def make_server(manager: Manager, host="127.0.0.1", port=8000) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), Api)
    server.daemon_threads = True
    server.manager = manager  # type: ignore[attr-defined]
    return server


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="CSV 清理服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir", default=os.environ.get("CSV_CLEAN_DATA", "./data"))
    args = parser.parse_args(argv)
    manager = Manager(args.data_dir)
    server = make_server(manager, args.host, args.port)
    print(f"CSV 清理页面: http://{args.host}:{args.port}/  (数据目录 {manager.data_dir})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在停止…")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
