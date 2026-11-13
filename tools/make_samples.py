#!/usr/bin/env python3
"""Generate CSV samples for testing the cleaner.

    python3 tools/make_samples.py --rows 2000000 --out samples/generated/big.csv
    python3 tools/make_samples.py --edge --out samples/generated/edge.csv

The big generator streams its output; it never holds the file in memory and
takes roughly the same disk space as the result.  It deliberately mixes in
quoted newlines, empty fields, duplicate business keys and a few bad rows so
the cleaner's behaviour is visible end to end.
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import random
import sys
from datetime import datetime, timedelta, timezone

HEADER = ["order_id", "customer", "amount", "currency", "updated_at", "note"]

CUSTOMERS = ["alice", "bob", "carol", "dave", "eve", "张三", "李四", "王五",
             "a very long customer name used to widen rows", "o'brien"]
NOTES = ["", "gift wrap", "call before delivery", "multi line:\nsecond line",
         'contains "quotes" and, commas', "  padded  ", None, "urgent"]


def make_rows(count: int, seed: int = 7, bad_every: int = 501):
    """Yield CSV lines; deterministic for a given seed."""
    rng = random.Random(seed)
    base = datetime(2024, 1, 1)
    bugger = io.StringIO()
    writer = csv.writer(bugger, lineterminator="\n")

    for i in range(count):
        if bad_every and i and i % bad_every == 0:
            # a deliberately malformed row: wrong field count / bad date / bad number
            kind = (i // bad_every) % 3
            if kind == 0:
                yield f"{i},broken\n"
            elif kind == 1:
                yield f"o{i},cust,12.00,USD,2024-02-30,oops\n"
            else:
                yield f"o{i},cust,not-a-number,USD,2024-01-01,bad amount\n"
            continue
        order_id = f"o{i // 3}"                 # 3 rows share a business key
        customer = rng.choice(CUSTOMERS)
        amount = round(rng.uniform(1, 5000), 2)
        amount_text = "" if i % 97 == 0 else f"{amount:.2f}"
        currency = rng.choice(["USD", "CNY", "EUR"])
        stamp = base + timedelta(minutes=rng.randrange(0, 60 * 24 * 400))
        note = rng.choice(NOTES)
        bugger.seek(0)
        bugger.truncate(0)
        writer.writerow([order_id, customer, amount_text or "", currency,
                         stamp.strftime("%Y-%m-%d %H:%M:%S"), note or ""])
        yield bugger.getvalue()


def write_big(path: str, count: int, seed: int):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(HEADER)
        for line in make_rows(count, seed):
            fh.write(line)


EDGE = """order_id,customer,amount,currency,updated_at,note
o1,alice,9.10,USD,2024-01-01 10:00:00,plain
o2,"multi
line",10.00,USD,2024-01-02 10:00:00,"note with ""quotes"" and, comma"
o3,bob,,USD,2024-01-03 10:00:00,empty amount
o4,carol,2,CNY,2024-02-30 10:00:00,bad date should be isolated
o5,dave,10.00,USD,2024-01-05 10:00:00,duplicate key older
o5,dave,10.00,USD,2024-03-05 10:00:00,duplicate key newer
o6,eve,not-a-number,USD,2024-01-06 10:00:00,bad amount should be isolated
o7,frank,3.5,USD,2024-01-07 10:00:00,short
o8,gina,7.25,USD,2024-01-08 10:00:00,trailing fields,extra,oops
o9,henry,0.01,USD,2024-01-09 10:00:00,ok
"""

EDGE_HEADERLESS = """o1,alice,9.10,USD,2024-01-01
o2,bob,10.00,USD,2024-01-02
o3,carol,1,USD,2024-01-03
"""


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=0,
                        help="number of data rows for the big sample")
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--edge", action="store_true",
                        help="write the small hand-made edge-case fixture")
    args = parser.parse_args(argv)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    if args.edge:
        with open(args.out, "w", encoding="utf-8", newline="") as fh:
            fh.write(EDGE)
        print(f"wrote {args.out} ({os.path.getsize(args.out)} bytes)")
        return
    if args.rows <= 0:
        parser.error("--rows must be positive (or pass --edge)")
    write_big(args.out, args.rows, args.seed)
    print(f"wrote {args.out} ({os.path.getsize(args.out) / 1024 ** 2:.1f} MiB, "
          f"{args.rows} data rows)")


if __name__ == "__main__":
    main()
