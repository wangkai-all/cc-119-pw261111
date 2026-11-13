"""Typed CSV field handling: record splitting, typed values, dates.

Design notes
------------
* ``RecordSplitter`` is an incremental physical-line state machine.  It tracks
  quote state itself (rather than handing a whole file to ``csv.reader``) so
  callers get *physical* line numbers, exact byte offsets and recovery after a
  malformed record.  It mirrors ``csv.reader`` record boundaries for
  well-formed input.
* Values are compared as *real* types: Decimal for money, datetime for dates,
  ints as Decimal — never as raw strings — so ``9.10`` does not sort after
  ``10.00``.
"""
from __future__ import annotations

import csv
import decimal
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

NULL_TOKENS = {"", "-", "n/a", "na", "null", "none", "nan", "nil"}

# Bump when parsing/comparison semantics change: shard reuse is keyed on it.
SEMANTICS_VERSION = "csv-clean-v5"

DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%Y%m%d",
)

MAX_BAD_RAW = 4000          # cap raw text kept for a malformed record
MAX_QUOTED_LINES = 64       # runaway quote becomes a bad row instead of data


class MalformedRow(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Rules:
    sort_by: str
    sort_dir: str = "asc"
    nulls: str = "last"            # last | first
    business_key: str = ""
    updated_at: str | None = None  # column used to choose between duplicates
    dup_policy: str = "keep"       # keep | latest | first
    sort_type: str = "auto"        # auto | text | number | date

    def normalize(self) -> "Rules":
        sort_dir = "desc" if str(self.sort_dir).lower() == "desc" else "asc"
        nulls = "first" if str(self.nulls).lower() == "first" else "last"
        dup = str(self.dup_policy).lower()
        if dup not in ("keep", "latest", "first"):
            dup = "keep"
        sort_type = str(self.sort_type).lower()
        if sort_type not in ("auto", "text", "number", "date"):
            sort_type = "auto"
        return Rules(sort_by=self.sort_by, sort_dir=sort_dir, nulls=nulls,
                     business_key=self.business_key or self.sort_by,
                     updated_at=self.updated_at or None, dup_policy=dup,
                     sort_type=sort_type)

    def to_dict(self) -> dict:
        return {"sort_by": self.sort_by, "sort_dir": self.sort_dir,
                "nulls": self.nulls, "business_key": self.business_key,
                "updated_at": self.updated_at, "dup_policy": self.dup_policy,
                "sort_type": self.sort_type}

    @staticmethod
    def from_dict(d: dict) -> "Rules":
        return Rules(
            sort_by=d.get("sort_by", ""),
            sort_dir=d.get("sort_dir", "asc"),
            nulls=d.get("nulls", "last"),
            business_key=d.get("business_key", ""),
            updated_at=d.get("updated_at") or None,
            dup_policy=d.get("dup_policy", "keep"),
            sort_type=d.get("sort_type", "auto"),
        ).normalize()


def is_null(value: str) -> bool:
    return value.strip().lower() in NULL_TOKENS


_FAST_DATE = re.compile(
    r"^([0-9]{4})[-/]([0-9]{1,2})[-/]([0-9]{1,2})"
    r"(?:[T ]([0-9]{1,2}):([0-9]{1,2})(?::([0-9]{1,2}))?"
    r"(?:\.([0-9]+))?)?$")
_FAST_DMY = re.compile(
    r"^([0-9]{1,2})[-/.]([0-9]{1,2})[-/.]([0-9]{4})$")
_FAST_COMPACT = re.compile(r"^([0-9]{4})([0-9]{2})([0-9]{2})$")


def parse_date(value: str) -> datetime | None:
    """Naive datetime; None for empty; MalformedRow for garbage.

    The common ISO-like shapes take a regex + ``datetime()`` fast path;
    ``strptime`` is only a fallback, which keeps row scanning cheap.
    """
    v = value.strip()
    if v == "":
        return None
    if v.endswith("Z"):
        v = v[:-1]
    plus = v.find("+")
    if plus > 10:
        v = v[:plus].strip()
    m = _FAST_DATE.match(v)
    if m:
        y, mo, d, hh, mi, ss, _frac = m.groups()
        try:
            return datetime(int(y), int(mo), int(d), int(hh or 0),
                            int(mi or 0), int(ss or 0))
        except ValueError:
            raise MalformedRow(f"invalid date {value!r}")
    m = _FAST_DMY.match(v)
    if m:
        d, mo, y = m.groups()
        try:
            return datetime(int(y), int(mo), int(d))
        except ValueError:
            raise MalformedRow(f"invalid date {value!r}")
    m = _FAST_COMPACT.match(v)
    if m:
        y, mo, d = m.groups()
        try:
            return datetime(int(y), int(mo), int(d))
        except ValueError:
            raise MalformedRow(f"invalid date {value!r}")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    raise MalformedRow(f"invalid date {value!r}")


def parse_number(value: str) -> decimal.Decimal:
    v = value.strip().replace(",", "").replace("$", "").replace("¥", "")
    v = v.replace("€", "").replace("£", "").strip()
    if v.startswith("(") and v.endswith(")"):     # accounting negative
        v = "-" + v[1:-1]
    try:
        d = decimal.Decimal(v)
    except decimal.InvalidOperation:
        raise MalformedRow(f"invalid number {value!r}")
    if not d.is_finite():
        raise MalformedRow(f"non-finite number {value!r}")
    return d


_NUM_RE = re.compile(r"^-?[0-9]+(?:[.,][0-9]+)?$")
_MONEY_RE = re.compile(r"^(?:[$¥€£]|-\s)?\s*\(?-?[0-9][0-9,]*(?:\.\d+)?\)?$")


def typed_value(raw: str, sort_type: str) -> tuple[bool, Any]:
    """(is_null, comparable typed value).

    In ``auto`` mode values are tagged by type rank so tuples only compare
    like-with-like: (0, Decimal) numbers, (1, datetime) dates, (2, str) text.
    """
    if is_null(raw):
        return True, None
    v = raw.strip()
    if sort_type == "text":
        return False, v
    if sort_type == "number":
        return False, parse_number(v)
    if sort_type == "date":
        return False, parse_date(v)
    if _NUM_RE.match(v):
        return False, (0, decimal.Decimal(v.replace(",", "")))
    if _MONEY_RE.match(v) and any(ch.isdigit() for ch in v):
        try:
            return False, (0, parse_number(v))
        except MalformedRow:
            pass
    try:
        return False, (1, parse_date(v))
    except MalformedRow:
        return False, (2, v)


def parse_timestamp_seconds(raw: str) -> int:
    """Timestamp as integer seconds (0 when missing or invalid).

    Integer seconds keep duplicate ordering native (descending is a negation)
    and avoid per-comparison datetime calls.
    """
    try:
        dt = parse_date(raw)
    except MalformedRow:
        return 0
    if dt is None:
        return 0
    return int(dt.timestamp()) if dt.tzinfo else (
        dt.toordinal() * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second)


def parse_timestamp(raw: str) -> str:
    """Canonical, lexicographically sortable timestamp ('' when missing/bad).

    Bad timestamps never raise here: they only lose the duplicate tie-break.
    """
    try:
        dt = parse_date(raw)
    except MalformedRow:
        return ""
    return dt.isoformat(sep=" ") if dt else ""


# ---------------------------------------------------------------------------
# Incremental record splitter
# ---------------------------------------------------------------------------
@dataclass
class RecordEvent:
    raw: str                 # complete logical record text (capped when bad)
    start_line: int          # first physical line
    end_line: int            # last physical line
    lines: int               # physical lines consumed
    payload: Any = None      # opaque marker of the first physical line
    fields: list[str] | None = None
    error: str = ""          # non-empty => isolated / malformed row


class RecordSplitter:
    """Feed physical lines, receive completed logical records.

    Keeps at most one in-flight record.  A quote left open for more than
    ``max_quoted_lines`` physical lines (or at EOF) is a malformed row; the
    following lines are re-scanned as fresh records so one broken row cannot
    swallow the rest of the file.
    """

    def __init__(self, delimiter: str = ",", max_quoted_lines: int = MAX_QUOTED_LINES):
        self.delimiter = delimiter
        self.max_quoted_lines = max_quoted_lines
        self._buf: list[tuple[int, str, Any, str]] = []
        self._inbox: list[tuple[int, str, Any, str]] = []
        self._in_quotes = False
        self._field_start = True

    # -- public -------------------------------------------------------
    def feed(self, line_no: int, text: str, payload: Any = None,
             line_error: str = "") -> list[RecordEvent]:
        self._inbox.append((line_no, text, payload, line_error))
        return self._drain()

    def close(self) -> list[RecordEvent]:
        events: list[RecordEvent] = []
        if self._buf:
            raw = "".join(t for _, t, _, _ in self._buf)
            first, last = self._buf[0][0], self._buf[-1][0]
            nlines = len(self._buf)
            err = next((e for _, _, _, e in self._buf if e), "")
            payload = self._buf[0][2]
            if self._in_quotes:
                events.append(_bad(raw, first, last, nlines, payload,
                                   err or "unterminated quoted field"))
                self._inbox[0:0] = self._buf[1:]
                self._buf.clear()
                self._reset_quote_state()
            else:
                ev = self._complete(raw, first, last, nlines, payload, err)
                self._buf.clear()
                if ev is not None:
                    events.append(ev)
        events.extend(self._drain())
        return events

    def at_boundary(self) -> bool:
        """True when no partial record is buffered."""
        return not self._buf and not self._inbox

    # -- internals ----------------------------------------------------
    def _drain(self) -> list[RecordEvent]:
        events: list[RecordEvent] = []
        while self._inbox:
            line_no, text, payload, line_error = self._inbox.pop(0)
            if not self._buf:
                self._reset_quote_state()
            self._buf.append((line_no, text, payload, line_error))
            self._scan_line(text)
            if self._in_quotes and len(self._buf) > self.max_quoted_lines:
                raw = "".join(t for _, t, _, _ in self._buf)
                events.append(_bad(raw, self._buf[0][0], line_no,
                                   len(self._buf), self._buf[0][2],
                                   "unterminated quoted field"))
                self._inbox[0:0] = self._buf[1:]
                self._buf.clear()
                self._reset_quote_state()
                continue
            if not self._in_quotes:
                raw = "".join(t for _, t, _, _ in self._buf)
                first, last = self._buf[0][0], line_no
                nlines = len(self._buf)
                err = next((e for _, _, _, e in self._buf if e), "")
                payload0 = self._buf[0][2]
                self._buf.clear()
                event = self._complete(raw, first, last, nlines, payload0, err)
                if event is not None:
                    events.append(event)
        return events

    def _complete(self, raw, first, last, nlines, payload, line_error) -> RecordEvent | None:
        if not raw.strip():
            return None                    # blank physical line, not a record
        fields: list[str] | None = None
        err = line_error
        try:
            rows = list(csv.reader([raw], delimiter=self.delimiter))
            if rows:
                fields = rows[0]
        except (csv.Error, TypeError) as exc:
            err = err or f"CSV parse error: {exc}"
        if err:
            fields = None
        return RecordEvent(raw=raw[:MAX_BAD_RAW], start_line=first,
                           end_line=last, lines=nlines, payload=payload,
                           fields=fields, error=err)

    def _reset_quote_state(self) -> None:
        self._in_quotes = False
        self._field_start = True

    def _scan_line(self, text: str) -> None:
        in_q, field_start = self._in_quotes, self._field_start
        i, n = 0, len(text)
        while i < n:
            c = text[i]
            if in_q:
                if c == '"':
                    if i + 1 < n and text[i + 1] == '"':
                        i += 2              # escaped quote, still inside
                        continue
                    in_q = False
            else:
                if c == '"' and field_start:
                    in_q = True
                    field_start = False
                elif c == self.delimiter:
                    field_start = True
                elif not c.isspace():
                    field_start = False
            i += 1
        self._in_quotes = in_q
        self._field_start = field_start


def _bad(raw, first, last, nlines, payload, reason) -> RecordEvent:
    return RecordEvent(raw=raw[:MAX_BAD_RAW], start_line=first, end_line=last,
                       lines=nlines, payload=payload, fields=None, error=reason)


def split_fields(raw: str, delimiter: str = ",") -> list[str]:
    return next(csv.reader([raw], delimiter=delimiter), [])


def normalize_columns(names: list[str]) -> list[str]:
    """Trim a BOM/whitespace, fill empty names, make duplicates unique."""
    out: list[str] = []
    seen: dict[str, int] = {}
    for i, raw in enumerate(names, 1):
        name = raw.replace("﻿", "").strip()
        if not name:
            name = f"col_{i}"
        base = name
        n = seen.get(base, 0)
        while name in seen and n:
            n += 1
            name = f"{base}_{n}"
        seen[name] = seen.get(name, 0) + 1
        out.append(name)
    return out


def detect_columns(path: str, encoding: str, delimiter: str,
                   header_mode: str, custom_names: list[str] | None = None,
                   max_preview_lines: int = 1) -> tuple[list[str], int]:
    """Return (column names, field width of the first record).

    ``header_mode``: ``present`` (first record is the header), ``absent``
    (generate ``col_1..col_n`` from the first record), ``custom`` (names come
    from the caller; every record is data).
    """
    from .typed import iter_lines
    splitter = RecordSplitter(delimiter)
    events: list[RecordEvent] = []
    first_width = 0
    for line_no, text, _off in iter_lines(path, encoding):
        events = splitter.feed(line_no, text)
        for ev in events:
            if ev.error:
                continue
            if not first_width:
                first_width = len(ev.fields or [])
        if first_width:
            break
    if not first_width:
        raise ValueError("输入文件中没有可解析的记录")
    if header_mode == "present":
        return normalize_columns(events[0].fields or []), first_width
    if header_mode == "custom":
        names = normalize_columns(list(custom_names or []))
        if len(names) != first_width:
            raise ValueError(f"自定义表头有 {len(names)} 列，但首行有 {first_width} 列")
        return names, first_width
    return [f"col_{i}" for i in range(1, first_width + 1)], first_width


def iter_lines(path: str, encoding: str) -> Iterable[tuple[int, str, int]]:
    """Yield (physical line no, decoded text, byte length) from a file.

    Lines split on the raw ``\\n`` byte, which keeps byte offsets exact for
    UTF-8/GBK/latin-1 style encodings (a newline byte can never appear inside
    a multi-byte sequence of those encodings).
    """
    lineno = 0
    with open(path, "rb") as fh:
        while True:
            data = fh.readline()
            if not data:
                return
            lineno += 1
            yield lineno, data.decode(encoding), len(data)


# ---------------------------------------------------------------------------
# Fingerprints
# ---------------------------------------------------------------------------
def stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=True, default=str)


def digest(payload: dict) -> str:
    return hashlib.sha256(stable_json(payload).encode("utf-8")).hexdigest()


def file_sha256(path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
