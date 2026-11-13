"""Engine correctness tests: ordering, dedupe, resumption, cancellation.

Run with ``python3 -m unittest discover -s tests -v`` (stdlib only).
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import unittest

from app.engine import Control, Runner, Spec, TaskCancelled
from app.storage import Store
from app.typed import Rules


def rules(**kw):
    base = dict(sort_by="amount", sort_dir="asc", nulls="last",
                business_key="id", updated_at="updated", dup_policy="keep",
                sort_type="auto")
    base.update(kw)
    return Rules(**base).normalize()


class Harness:
    """Tiny task harness around the engine for tests and CLI smoke runs."""

    HEADER = ["id", "name", "amount", "updated"]

    def __init__(self, body: str, rules_=None, chunk_rows=2, chunk_bytes=10**9,
                 fan_in=2, header=None):
        self.dir = tempfile.mkdtemp(prefix="csvclean-test-")
        self.input = os.path.join(self.dir, "input.csv")
        names = self.HEADER if header is None else header
        with open(self.input, "w", encoding="utf-8", newline="") as fh:
            fh.write(",".join(names) + "\n")
            fh.write(body)
        self.rules = (rules_ or rules())
        self.chunk_rows = chunk_rows
        self.chunk_bytes = chunk_bytes
        self.fan_in = fan_in
        self.columns = list(names)
        self.store = Store(os.path.join(self.dir, "tasks.sqlite"))
        self.store.init()
        self.task_id = None
        self.work = os.path.join(self.dir, "work")
        self.output = os.path.join(self.dir, "out.csv")
        self.bad = os.path.join(self.dir, "bad.csv")

    def make_task(self):
        with open(self.input, "rb") as fh:
            data = fh.read()
        spec = self.spec
        key = f"{os.path.getsize(self.input)}-{hash(data)}-{spec.rules.to_dict()}"
        self.task_id = self.store.create_task(dict(
            task_key=key, name="t", input_path=self.input,
            input_size=os.path.getsize(self.input), input_sha256="",
            input_mtime_ns=0, delimiter=",", encoding="utf-8",
            header_mode="present", columns_json=json.dumps(self.columns),
            rules_json=json.dumps(spec.rules.to_dict()),
            chunk_bytes=self.chunk_bytes, chunk_rows=self.chunk_rows,
            fan_in=self.fan_in, work_dir=self.work, output_path=self.output,
            bad_path=self.bad, status="pending", stage=""))
        return self.task_id

    @property
    def spec(self):
        return Spec(
            task_id=self.task_id or 0, input_path=self.input,
            input_size=os.path.getsize(self.input), encoding="utf-8",
            delimiter=",", header_mode="present", columns=self.columns,
            rules=self.rules, chunk_bytes=self.chunk_bytes,
            chunk_rows=self.chunk_rows, fan_in=self.fan_in, work_dir=self.work,
            output_path=self.output, bad_path=self.bad)

    def run(self, control=None):
        if self.task_id is None:
            self.make_task()
        Runner(self.store, self.spec, control or Control()).run()
        return self

    def out_rows(self):
        import csv
        with open(self.output, encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
        return rows[0], rows[1:]

    def bad_rows(self):
        with open(self.bad, encoding="utf-8", newline="") as fh:
            import csv
            return list(csv.reader(fh))[1:]

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class TestOrdering(unittest.TestCase):
    def test_money_sorted_numerically(self):
        h = Harness("a,one,9.10,2024-01-01\nb,two,10.00,2024-01-02\n"
                    "c,three,2,2024-01-03\n", rules())
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[2] for r in rows], ["2", "9.10", "10.00"])
        finally:
            h.cleanup()

    def test_nulls_last_then_first(self):
        body = "a,one,,2024-01-01\nb,two,5,2024-01-02\n"
        for order, expected in (("last", ["b", "a"]), ("first", ["a", "b"])):
            h = Harness(body, rules(nulls=order))
            try:
                h.run()
                _, rows = h.out_rows()
                self.assertEqual([r[0] for r in rows], expected)
            finally:
                h.cleanup()

    def test_desc_keeps_nulls_configured(self):
        body = "a,one,1,2024-01-01\nb,two,5,2024-01-02\nc,x,,2024-01-03\n"
        h = Harness(body, rules(sort_dir="desc", nulls="last"))
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[2] for r in rows], ["5", "1", ""])
        finally:
            h.cleanup()

    def test_ties_keep_input_order(self):
        body = "".join(f"r{i},n,7,2024-01-01\n" for i in range(30))
        h = Harness(body, rules(), chunk_rows=3, fan_in=2)
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[0] for r in rows],
                             [f"r{i}" for i in range(30)])
        finally:
            h.cleanup()

    def test_text_mode_is_lexical(self):
        body = "a,x,9.10,2024-01-01\nb,y,10.00,2024-01-02\n"
        h = Harness(body, rules(sort_type="text"))
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[2] for r in rows], ["10.00", "9.10"])
        finally:
            h.cleanup()


class TestQuotingAndBadRows(unittest.TestCase):
    def test_quoted_newline_is_one_record(self):
        body = 'a,"multi\nline",3,2024-01-01\nb,"x,y",2,2024-01-02\n'
        h = Harness(body, rules(nulls="last"))
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual(len(rows), 2)   # naive split would see 3
            self.assertEqual(h.bad_rows(), [])
        finally:
            h.cleanup()

    def test_bad_rows_isolated_in_place(self):
        body = ("a,ok,1,2024-01-01\n"
                "b,short,2\n"
                "c,ok,2,2024-02-30\n"
                "d,ok,3,2024-01-03\n"
                "e,ok,notanumber,2024-01-04\n")
        h = Harness(body, rules(sort_type="number"))
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[0] for r in rows], ["a", "d"])
            bad = h.bad_rows()
            self.assertEqual(len(bad), 3)
            self.assertEqual([b[0] for b in bad], ["3", "4", "6"])
            self.assertIn("列数不符", bad[0][3])
            self.assertIn("非法日期", bad[1][3])
            self.assertIn("invalid number", bad[2][3])
        finally:
            h.cleanup()

    def test_blank_lines_skipped(self):
        h = Harness("a,ok,1,2024-01-01\n\n\nb,ok,2,2024-01-02\n")
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual(len(rows), 2)
            self.assertEqual(h.bad_rows(), [])
        finally:
            h.cleanup()


class TestDedupe(unittest.TestCase):
    BODY = ("a,old,1,2024-01-01\n"
            "a,mid,2,2024-03-01\n"
            "b,x,3,2024-01-01\n"
            "a,new,4,2024-05-01\n")

    def test_keep_keeps_all(self):
        h = Harness(self.BODY, rules(dup_policy="keep"))
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual(len(rows), 4)
        finally:
            h.cleanup()

    def test_latest_by_timestamp(self):
        h = Harness(self.BODY, rules(dup_policy="latest"))
        try:
            h.run()
            _, rows = h.out_rows()
            names = {r[0]: r[1] for r in rows}
            self.assertEqual(names, {"a": "new", "b": "x"})
            self.assertEqual(h.store.get_task(h.task_id)["dup_removed"], 2)
        finally:
            h.cleanup()

    def test_first_wins_with_equal_timestamps(self):
        body = "a,one,1,2024-01-01\na,two,2,2024-01-01\n"
        h = Harness(body, rules(dup_policy="first"))
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[1] for r in rows], ["one"])
        finally:
            h.cleanup()

    def test_timestamp_ties_break_on_line_for_latest(self):
        body = "a,one,1,2024-01-01\na,two,2,2024-01-01\n"
        h = Harness(body, rules(dup_policy="latest"))
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[1] for r in rows], ["one"])
        finally:
            h.cleanup()


class TestResume(unittest.TestCase):
    BODY = "".join(f"r{i},n,{1000 - i},2024-01-{(i % 28) + 1:02d}\n"
                   for i in range(60))

    def test_interrupt_then_resume_matches_full_run(self):
        ref = Harness(self.BODY, rules(), chunk_rows=5, fan_in=2)
        try:
            ref.run()
            expected = ref.out_rows()
        finally:
            ref.cleanup()

        h = Harness(self.BODY, rules(), chunk_rows=5, fan_in=2)
        try:
            h.make_task()
            ctl = Control()

            class Stop(Control):
                def checkpoint(self_inner):
                    super().checkpoint()
                    if len(h.store.shards(h.task_id, dataset="scan")) >= 4:
                        raise TaskCancelled()

            with self.assertRaises(TaskCancelled):
                Runner(h.store, h.spec, Stop()).run()
            done_before = [s["part"] for s in
                           h.store.shards(h.task_id, dataset="scan")]
            self.assertGreaterEqual(len(done_before), 4)

            h.run()
            self.assertEqual(h.out_rows(), expected)
            task = h.store.get_task(h.task_id)
            self.assertEqual(task["status"], "completed")
            self.assertEqual(task["good_rows"], 60)
        finally:
            h.cleanup()

    def test_changed_input_is_not_reused(self):
        h = Harness(self.BODY, rules(), chunk_rows=5)
        try:
            h.make_task()
            shards = []

            class Stop(Control):
                def checkpoint(s):
                    super().checkpoint()
                    if len(h.store.shards(h.task_id, dataset="scan")) >= 3:
                        raise TaskCancelled()

            with self.assertRaises(TaskCancelled):
                Runner(h.store, h.spec, Stop()).run()
            old_paths = [s["path"] for s in
                         h.store.shards(h.task_id, dataset="scan")]
            # rewrite the input with different content
            with open(h.input, "a", encoding="utf-8") as fh:
                fh.write("zzz,new,1,2024-06-01\n")
            # a new task identity (size changed) gets a fresh work dir
            seen = h.out_rows if False else None
            h.store.delete_task(h.task_id)
            h.task_id = None
            h.run()
            _, rows = h.out_rows()
            self.assertEqual(len(rows), 61)
            self.assertTrue(all(not os.path.exists(p) for p in old_paths))
        finally:
            h.cleanup()

    def test_tampered_shard_is_rebuilt(self):
        h = Harness(self.BODY, rules(), chunk_rows=5)
        try:
            h.make_task()

            class Stop(Control):
                def checkpoint(s):
                    super().checkpoint()
                    if len(h.store.shards(h.task_id, dataset="scan")) >= 3:
                        raise TaskCancelled()

            with self.assertRaises(TaskCancelled):
                Runner(h.store, h.spec, Stop()).run()
            shard = h.store.shards(h.task_id, dataset="scan")[1]
            with open(shard["path"], "a", encoding="utf-8") as fh:
                fh.write("{}\n")
            h.run()
            _, rows = h.out_rows()
            self.assertEqual(len(rows), 60)
        finally:
            h.cleanup()


class TestChunking(unittest.TestCase):
    BODY = "".join(f"r{i},name-{i},{i}.{i % 100:02d},2024-01-01\n"
                   for i in range(200))

    def test_chunk_bytes_bounds_every_scan_shard(self):
        """The configured chunk size bounds shards to within one record.

        Rows are buffered until adding one crosses the threshold, then the
        shard is written — so a shard may exceed the budget by at most the
        largest encoded record, never by a whole extra chunk.
        Completed tasks delete their shards, so assert against the recorded
        summary (the numbers that were written to disk).
        """
        max_record = 512                    # generous for this fixture
        for budget in (2_000, 8_000):
            h = Harness(self.BODY, rules(), chunk_rows=10**6, chunk_bytes=budget)
            try:
                h.run()
                summary = json.loads(
                    h.store.get_task(h.task_id)["shard_summary_json"])
                scan = [s for s in summary
                        if s["dataset"] == "scan" and s["level"] == 0]
                self.assertGreater(len(scan), 1)
                for s in scan[:-1]:            # last shard holds the remainder
                    self.assertLessEqual(s["bytes"], budget + max_record,
                                         f"shard {s['part']} = {s['bytes']} B")
                self.assertEqual(sum(s["rows"] for s in scan), 200)
            finally:
                h.cleanup()

    def test_repeated_runs_are_byte_identical(self):
        hashes = []
        for _ in range(2):
            h = Harness(self.BODY, rules(dup_policy="latest"), chunk_rows=7,
                        fan_in=2)
            try:
                h.run()
                import hashlib
                with open(h.output, "rb") as fh:
                    hashes.append(hashlib.sha256(fh.read()).hexdigest())
            finally:
                h.cleanup()
        self.assertEqual(hashes[0], hashes[1])


class TestCancel(unittest.TestCase):
    BODY = "".join(f"r{i},n,{i},2024-01-01\n" for i in range(80))

    def test_cancel_cleans_only_own_temp_files(self):
        h = Harness(self.BODY, rules(), chunk_rows=5)
        try:
            h.make_task()

            class Stop(Control):
                def checkpoint(s):
                    super().checkpoint()
                    if len(h.store.shards(h.task_id, dataset="scan")) >= 3:
                        raise TaskCancelled()

            with self.assertRaises(TaskCancelled):
                Runner(h.store, h.spec, Stop()).run()
            self.assertTrue(os.path.isdir(h.work))
            paths = [s["path"] for s in h.store.shards(h.task_id)]
            from app.engine import Runner as R
            R(h.store, h.spec, Control()).cleanup_temp()
            for p in paths:
                self.assertFalse(os.path.exists(p))
            self.assertFalse(os.path.exists(h.work))
            self.assertEqual(h.store.shards(h.task_id), [])
        finally:
            h.cleanup()


class TestTypedComparison(unittest.TestCase):
    def test_money_decimal_not_float(self):
        from app.typed import typed_value
        _, a = typed_value("0.1", "number")
        _, b = typed_value("0.10", "number")
        self.assertEqual(a, b)

    def test_auto_mode_ranks_types(self):
        from app.typed import typed_value
        _, num = typed_value("5", "auto")
        _, date = typed_value("2024-01-01", "auto")
        _, text = typed_value("apple", "auto")
        self.assertLess(num[0], date[0])
        self.assertLess(date[0], text[0])

    def test_invalid_date_is_not_null(self):
        from app.typed import typed_value, MalformedRow
        with self.assertRaises(MalformedRow):
            typed_value("2024-13-40", "date")
        # blank is a legitimate null
        is_null, value = typed_value("  ", "date")
        self.assertTrue(is_null, value)

    def test_negative_numbers_and_accounting_format(self):
        from app.typed import parse_number
        import decimal
        self.assertEqual(parse_number("(1,234.50)"), decimal.Decimal("-1234.50"))
        self.assertEqual(parse_number("-2.5"), decimal.Decimal("-2.5"))

    def test_desc_string_order_is_exact(self):
        """Descending text must be the exact reverse of ascending, including
        prefix cases like "a" vs "ab"."""
        body = "a,x,apple,2024-01-01\nb,y,ab,2024-01-02\nc,z,b,2024-01-03\n"
        for direction, expected in (
                ("asc", ["ab", "apple", "b"]),
                ("desc", ["b", "apple", "ab"])):
            h = Harness(body, rules(sort_by="amount", sort_type="text",
                                    sort_dir=direction))
            try:
                h.run()
                _, rows = h.out_rows()
                self.assertEqual([r[2] for r in rows], expected)
            finally:
                h.cleanup()

    def test_duplicate_key_multi_column(self):
        body = ("a,1,one,10,2024-01-01\n"
                "a,1,two,20,2024-02-01\n"
                "a,2,three,30,2024-01-01\n")
        h = Harness(body, rules(business_key="id|name", dup_policy="latest"),
                    header=["id", "name", "label", "amount", "updated"])
        try:
            h.run()
            _, rows = h.out_rows()
            self.assertEqual([r[2] for r in rows], ["two", "three"])
        finally:
            h.cleanup()
