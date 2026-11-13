"""Service + HTTP API tests: lifecycle, reuse, pause/resume, cancel cleanup."""
from __future__ import annotations

import io
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from app.server import make_server
from app.service import ApiError, Manager


BODY = ("order_id,name,amount,updated_at\n"
        + "".join(f"o{i % 40},n{i},{i % 97}.{i % 10:02d},2024-01-{(i % 28) + 1:02d}\n"
                  for i in range(400)))


def body_bytes(text=BODY):
    return text.encode("utf-8")


class ServiceBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="csvclean-svc-")
        self.manager = Manager(self.dir)

    def tearDown(self):
        import shutil
        for task in self.manager.store.list_tasks():
            if task["status"] in ("running", "pausing", "queued", "paused"):
                try:
                    self.manager.cancel(task["id"])
                except ApiError:
                    pass
        for thread, control in list(self.manager._workers.values()):
            control.cancel()
            thread.join(timeout=10)
        shutil.rmtree(self.dir, ignore_errors=True)

    def upload(self, text=BODY):
        return self.manager.save_upload(io.BytesIO(body_bytes(text)), "t.csv")

    def create(self, upload_id, **overrides):
        payload = {
            "upload_id": upload_id, "encoding": "utf-8", "delimiter": ",",
            "header_mode": "present", "chunk_rows": 64, "chunk_bytes": 16384,
            "fan_in": 3,
            "rules": {"sort_by": "amount", "sort_dir": "asc", "nulls": "last",
                      "business_key": "order_id", "updated_at": "updated_at",
                      "dup_policy": "keep", "sort_type": "number"},
        }
        payload.update(overrides)
        return self.manager.create_task(payload)

    def wait(self, task_id, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            detail = self.manager.task_detail(task_id)
            if detail["status"] in ("completed", "failed", "cancelled",
                                    "input_changed"):
                return detail
            time.sleep(0.03)
        self.fail(f"task {task_id} did not settle: "
                  f"{self.manager.task_detail(task_id)['status']}")


class TestService(ServiceBase):
    def test_inspect_reports_columns_and_samples(self):
        up = self.upload("a,b,c\n1,2,3\n4,5,6\n")
        info = self.manager.inspect(up["upload_id"], "utf-8", ",", "present")
        self.assertEqual(info["columns"], ["a", "b", "c"])
        self.assertEqual(len(info["sample_rows"]), 2)
        self.assertEqual(info["sample_rows"][0], {"a": "1", "b": "2", "c": "3"})

    def test_inspect_without_header(self):
        up = self.upload("1,2,3\n4,5,6\n")
        info = self.manager.inspect(up["upload_id"], "utf-8", ",", "absent")
        self.assertEqual(info["columns"], ["col_1", "col_2", "col_3"])
        self.assertEqual(len(info["sample_rows"]), 2)

    def test_inspect_rejects_bad_encoding(self):
        up = self.upload()
        # latin-1 decodes anything, gbk may fail only on some bytes; use a
        # deliberately invalid utf-8 sequence instead
        bad = self.manager.save_upload(io.BytesIO(b"a,b\n1,\xff\xfe\n"), "bad.csv")
        with self.assertRaises(ApiError):
            self.manager.inspect(bad["upload_id"], "utf-8", ",", "present")

    def test_duplicate_upload_stored_once(self):
        a = self.upload()
        b = self.upload()
        self.assertEqual(a["upload_id"], b["upload_id"])
        files = [f for f in os.listdir(self.manager.uploads_dir) if f.endswith(".csv")]
        self.assertEqual(len(files), 1)

    def test_task_completes_and_reuses_identity(self):
        up = self.upload()
        first = self.create(up["upload_id"])
        detail = self.wait(first["task_id"])
        self.assertEqual(detail["status"], "completed")
        self.assertEqual(detail["progress"]["output_rows"], 400)
        second = self.create(up["upload_id"])
        self.assertTrue(second["reused"])
        self.assertEqual(second["task_id"], first["task_id"])

    def test_rules_change_creates_new_task(self):
        up = self.upload()
        first = self.create(up["upload_id"])
        self.wait(first["task_id"])
        second = self.create(up["upload_id"],
                             rules={"sort_by": "amount", "sort_dir": "desc",
                                    "business_key": "order_id",
                                    "updated_at": "updated_at",
                                    "dup_policy": "latest",
                                    "sort_type": "number"})
        self.assertFalse(second["reused"])
        self.assertNotEqual(second["task_id"], first["task_id"])

    def test_latest_dedupe_reduces_rows(self):
        up = self.upload()
        task = self.create(up["upload_id"], rules={
            "sort_by": "amount", "business_key": "order_id",
            "updated_at": "updated_at", "dup_policy": "latest",
            "sort_type": "number"})
        detail = self.wait(task["task_id"])
        self.assertEqual(detail["progress"]["output_rows"], 40)
        self.assertEqual(detail["progress"]["dup_removed"], 360)

    def test_preview_and_search(self):
        up = self.upload()
        task = self.create(up["upload_id"])
        self.wait(task["task_id"])
        page = self.manager.preview(task["task_id"], 0, 10)
        self.assertEqual(len(page["rows"]), 10)
        self.assertTrue(page["has_more"])
        result = self.manager.search(task["task_id"], "o39")
        self.assertTrue(all(any("o39" == cell for cell in hit["fields"])
                            for hit in result["hits"]))

    def test_preview_requires_completion(self):
        up = self.upload()
        task = self.create(up["upload_id"], chunk_rows=400)
        self.manager.pause(task["task_id"])
        with self.assertRaises(ApiError):
            self.manager.preview(task["task_id"])

    def test_pause_then_resume(self):
        up = self.upload()
        task = self.create(up["upload_id"], chunk_rows=8)
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.manager.task_detail(task["task_id"])["progress"]["records_read"] > 20:
                break
            time.sleep(0.01)
        self.manager.pause(task["task_id"])
        time.sleep(0.4)
        paused = self.manager.task_detail(task["task_id"])
        self.assertIn(paused["status"], ("paused", "pausing"))
        frozen = paused["progress"]["records_read"]
        time.sleep(0.4)
        again = self.manager.task_detail(task["task_id"])
        self.assertEqual(again["progress"]["records_read"], frozen)
        self.manager.resume(task["task_id"])
        detail = self.wait(task["task_id"])
        self.assertEqual(detail["status"], "completed")
        self.assertEqual(detail["progress"]["output_rows"], 400)

    def test_cancel_cleans_temp_files_only_for_its_task(self):
        up = self.upload()
        task = self.create(up["upload_id"], chunk_rows=8)
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.manager.task_detail(task["task_id"])["progress"]["records_read"] > 10:
                break
            time.sleep(0.01)
        self.manager.cancel(task["task_id"])
        detail = self.wait(task["task_id"])
        self.assertEqual(detail["status"], "cancelled")
        self.assertFalse(os.path.exists(os.path.join(
            self.dir, "tasks", str(task["task_id"]), "work")))
        self.assertEqual(self.manager.store.shards(task["task_id"]), [])
        # the upload itself is shared input, not task scratch: keep it
        self.assertTrue(os.path.isfile(detail["input"]["path"]))

    def test_delete_task_removes_outputs(self):
        up = self.upload()
        task = self.create(up["upload_id"])
        detail = self.wait(task["task_id"])
        self.manager.delete_task(task["task_id"])
        self.assertFalse(os.path.exists(detail["progress"]["output_sha256"] and
                                        os.path.join(self.dir, "tasks",
                                                     str(task["task_id"]))))
        with self.assertRaises(ApiError):
            self.manager.task_detail(task["task_id"])

    def test_changed_upload_blocks_resume(self):
        up = self.upload()
        task = self.create(up["upload_id"], chunk_rows=8)
        detail = self.wait(task["task_id"])
        self.assertEqual(detail["status"], "completed")
        path = self.manager.upload_path(up["upload_id"])
        with open(path, "ab") as fh:
            fh.write(b"o999,x,1,2024-01-01\n")
        with self.assertRaises(ApiError) as ctx:
            self.manager.start(task["task_id"], restart=True)
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(self.manager.task_detail(task["task_id"])["status"],
                         "input_changed")


class TestBedInput(ServiceBase):
    def test_bad_rows_sidecar_and_counts(self):
        text = ("1,2\n3\n4,x\n5,6\n7,8\n")
        up = self.upload(text)
        task = self.manager.create_task({
            "upload_id": up["upload_id"], "encoding": "utf-8",
            "delimiter": ",", "header_mode": "absent", "chunk_rows": 8,
            "rules": {"sort_by": "col_1", "business_key": "col_1",
                      "sort_type": "number", "dup_policy": "keep"}})
        detail = self.wait(task["task_id"])
        self.assertEqual(detail["progress"]["bad_rows"], 1)
        self.assertEqual(detail["progress"]["output_rows"], 4)
        bad = self.manager.preview(task["task_id"], 0, 10, bad=True)
        self.assertEqual(len(bad["rows"]), 1)
        self.assertIn("列数不符", bad["rows"][0][3])


class TestHttp(ServiceBase):
    def setUp(self):
        super().setUp()
        self.server = make_server(self.manager, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def request(self, method, path, body=None, headers=None, raw=False):
        data = None
        hdrs = dict(headers or {})
        if body is not None:
            if isinstance(body, bytes):
                data = body
            else:
                data = json.dumps(body).encode()
                hdrs.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=hdrs,
            method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                payload = resp.read()
                return resp.status, payload if raw else json.loads(payload or b"{}")
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            try:
                return exc.code, json.loads(payload)
            except json.JSONDecodeError:
                return exc.code, payload

    def test_full_http_flow(self):
        status, up = self.request("POST", "/api/uploads", body_bytes(),
                                  {"X-Filename": "t.csv", "Content-Type": "text/csv"})
        self.assertEqual(status, 200)
        status, info = self.request("POST", "/api/inspect",
                                    {"upload_id": up["upload_id"]})
        self.assertEqual(status, 200)
        self.assertIn("order_id", info["columns"])
        status, created = self.request("POST", "/api/tasks", {
            "upload_id": up["upload_id"],
            "chunk_rows": 32,
            "rules": {"sort_by": "amount", "business_key": "order_id",
                      "updated_at": "updated_at", "dup_policy": "latest",
                      "sort_type": "number"}})
        self.assertEqual(status, 201)
        task_id = created["task_id"]
        deadline = time.time() + 30
        detail = None
        while time.time() < deadline:
            _, detail = self.request("GET", f"/api/tasks/{task_id}")
            if detail["status"] in ("completed", "failed"):
                break
            time.sleep(0.05)
        self.assertEqual(detail["status"], "completed")
        _, prev = self.request("GET", f"/api/tasks/{task_id}/preview?limit=5")
        self.assertEqual(len(prev["rows"]), 5)
        _, found = self.request("GET", f"/api/tasks/{task_id}/search?q=o39")
        self.assertTrue(found["hits"])
        status, data = self.request("GET", f"/api/tasks/{task_id}/download", raw=True)
        self.assertEqual(status, 200)
        self.assertTrue(data.startswith(b"order_id,name,amount,updated_at"))
        status, _ = self.request("GET", f"/api/tasks/{task_id}/download?kind=bad",
                                 raw=True)
        self.assertEqual(status, 200)
        _, tasks = self.request("GET", "/api/tasks")
        self.assertEqual(len(tasks["tasks"]), 1)

    def test_http_errors_are_json(self):
        status, payload = self.request("POST", "/api/inspect", {"upload_id": "nope"})
        self.assertEqual(status, 400)
        self.assertIn("error", payload)
        status, payload = self.request("POST", "/api/tasks", {"upload_id": "nope"})
        self.assertEqual(status, 400)
        self.assertIn("error", payload)
        status, payload = self.request("GET", "/api/tasks/9999")
        self.assertEqual(status, 404)

    def test_static_page_is_served(self):
        status, data = self.request("GET", "/", raw=True)
        self.assertEqual(status, 200)
        self.assertIn(b"CSV", data)
        status, data = self.request("GET", "/app.js", raw=True)
        self.assertEqual(status, 200)
        self.assertIn(b"renderShardChart", data)

    def test_rejects_path_traversal(self):
        status, _ = self.request("GET", "/../app.py", raw=True)
        self.assertIn(status, (403, 404))
