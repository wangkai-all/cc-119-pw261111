#!/usr/bin/env python3
"""Run one end-to-end clean and report what the engine actually did.

    python3 tools/bench.py --rows 300000
    python3 tools/bench.py --rows 2000000 --chunk-bytes 8388608 --fan-in 8

This is an *observation* tool, not a benchmark contract: it prints the shard
layout, the temporary bytes written, wall-clock time and the process peak RSS
on this machine for this run.  Numbers depend on disk, CPU and the sample's
shape, so none of them is promised for other machines or other inputs.

Uses a throw-away data directory unless ``--data-dir`` is given; pass
``--keep`` to leave the generated input and the result on disk.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

from app.service import Manager                        # noqa: E402
from make_samples import write_big                     # noqa: E402


DEFAULT_RULES = {
    "sort_by": "amount", "sort_dir": "asc", "nulls": "last",
    "business_key": "order_id", "updated_at": "updated_at",
    "dup_policy": "latest", "sort_type": "number",
}


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} GiB"


def group_shards(summary: list[dict]) -> list[tuple[str, list[dict]]]:
    buckets: dict[str, list[dict]] = {}
    for s in summary:
        label = s["dataset"] if s["dataset"] != "merge" else \
            f"merge L{s['level']}"
        buckets.setdefault(label, []).append(s)
    return list(buckets.items())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Clean one generated CSV and report actual sharding + "
                    "resource use on this machine.")
    ap.add_argument("--rows", type=int, default=300_000,
                    help="data rows to generate (default 300000)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--chunk-bytes", type=int, default=4 * 1024 * 1024)
    ap.add_argument("--chunk-rows", type=int, default=200_000)
    ap.add_argument("--fan-in", type=int, default=16)
    ap.add_argument("--dup-policy", choices=("keep", "latest", "first"),
                    default="latest")
    ap.add_argument("--data-dir", default=None,
                    help="reuse this directory instead of a temp one")
    ap.add_argument("--keep", action="store_true",
                    help="keep generated input/result (implies --data-dir temp "
                         "is printed)")
    args = ap.parse_args(argv)

    data_dir = args.data_dir or tempfile.mkdtemp(prefix="csvclean-bench-")
    os.makedirs(data_dir, exist_ok=True)
    input_path = os.path.join(data_dir, "input.csv")
    rules = dict(DEFAULT_RULES, dup_policy=args.dup_policy)

    print("machine")
    print(f"  platform   {platform.platform()}")
    print(f"  cpus       {os.cpu_count()}")
    print(f"  python     {platform.python_version()}")
    print(f"  data dir   {data_dir}")

    t0 = time.perf_counter()
    write_big(input_path, args.rows, args.seed)
    gen_s = time.perf_counter() - t0
    size = os.path.getsize(input_path)
    print("\ninput")
    print(f"  rows       {args.rows} generated in {gen_s:.2f} s")
    print(f"  file       {input_path} ({fmt_bytes(size)})")

    manager = Manager(data_dir)
    with open(input_path, "rb") as fh:
        upload = manager.save_upload(fh, "bench.csv")
    created = manager.create_task({
        "upload_id": upload["upload_id"], "encoding": "utf-8",
        "delimiter": ",", "header_mode": "present",
        "chunk_rows": args.chunk_rows, "chunk_bytes": args.chunk_bytes,
        "fan_in": args.fan_in, "rules": rules, "name": "bench",
    })
    task_id = created["task_id"]
    print("\nconfig")
    print(f"  chunk      {args.chunk_rows} rows / {fmt_bytes(args.chunk_bytes)}")
    print(f"  fan-in     {args.fan_in}")
    print(f"  rules      {json.dumps(rules, ensure_ascii=False, sort_keys=True)}")

    t0 = time.perf_counter()
    while True:
        detail = manager.task_detail(task_id)
        if detail["status"] in ("completed", "failed", "cancelled"):
            break
        time.sleep(0.05)
    wall = time.perf_counter() - t0
    if detail["status"] != "completed":
        print(f"\nFAILED: {detail['status']} {detail.get('error')}")
        return 1

    p = detail["progress"]
    summary = detail["shards"]
    print("\nresult")
    print(f"  status     completed in {wall:.2f} s wall "
          f"(engine-reported {p['elapsed_ms'] / 1000:.2f} s)")
    print(f"  input      {p['records_read']} records from "
          f"{p['physical_lines']} physical lines, "
          f"{fmt_bytes(p['bytes_read'])} read")
    print(f"  rows       {p['good_rows']} good, {p['bad_rows']} isolated, "
          f"{p['dup_removed']} duplicates removed -> {p['output_rows']} output")
    print(f"  output     {fmt_bytes(p['output_bytes'])}, sha256 "
          f"{p['output_sha256'][:16]}…")
    print(f"  peak RSS   {p['peak_rss_mb']:.1f} MiB (this process, "
          f"includes the benchmark harness)")

    print("\nshard layout actually used")
    total_shards = total_bytes = 0
    for label, shards in group_shards(summary):
        nbytes = sum(s["bytes"] for s in shards)
        rows = sum(s["rows"] for s in shards)
        total_shards += len(shards)
        total_bytes += nbytes
        print(f"  {label:<10} {len(shards):>4} shards  {rows:>9} rows  "
              f"{fmt_bytes(nbytes):>10}")
    sizes = [s["bytes"] for s in summary if s["bytes"] > 0]
    if sizes:
        print(f"  shard size min/median/max: {fmt_bytes(min(sizes))} / "
              f"{fmt_bytes(statistics.median(sizes))} / {fmt_bytes(max(sizes))}")
    print(f"  temporary shards: {total_shards} files, "
          f"{fmt_bytes(total_bytes)} written "
          f"({total_bytes / size:.2f}x the input size)")
    print(f"  largest chunk shard: {p['peak_shard_rows']} rows, "
          f"{fmt_bytes(p['peak_shard_bytes'])} (scan/value shards stop at the "
          f"configured chunk size; merge parts are copies of the value data)")
    print("\nnote: timings and memory are observations from this run on this "
          "machine,\n      not throughput guarantees for other hardware or "
          "input shapes.")

    if args.keep:
        print(f"\nkept: {data_dir}")
        print(f"  clean output: {data_dir}/tasks/{task_id}/cleaned.csv")
        print(f"  bad rows:     {data_dir}/tasks/{task_id}/bad_rows.csv")
    else:
        manager.delete_task(task_id)
        if not args.data_dir:
            shutil.rmtree(data_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
