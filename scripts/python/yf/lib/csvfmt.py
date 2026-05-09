"""OHLCV CSV format matching scripts/go/macrotrends (csvdata.go)."""

from __future__ import annotations

import csv
import datetime as dt
import io
import math
import os
import sys
from pathlib import Path
from typing import Iterable


OHLCV_HEADER = "date,open,high,low,close,volume"


def fmt_float(f: float) -> str:
    """Approximate Go strconv.FormatFloat(x, 'f', -1, 64) for OHLCV rows."""
    if not math.isfinite(f):
        raise ValueError(f"non-finite float: {f}")
    i = int(round(f))
    if abs(f - i) < 1e-9:
        return str(i)
    s = f"{f:.12f}".rstrip("0").rstrip(".")
    return s if s else "0"


def write_ohlcv_csv(rows: Iterable[tuple[str, float, float, float, float, float]], out: io.TextIOBase) -> None:
    w = csv.writer(out, lineterminator="\n")
    w.writerow(OHLCV_HEADER.split(","))
    for date, o, h, lo, c, vol in rows:
        w.writerow([date, fmt_float(o), fmt_float(h), fmt_float(lo), fmt_float(c), fmt_float(vol)])
    out.flush()


def read_last_date(path: Path) -> dt.date | None:
    """Return the date on the last data row of an existing CSV, or None."""
    if not path.exists():
        return None
    last_line = ""
    with path.open("r") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                last_line = stripped
    if not last_line or last_line.startswith("date"):
        return None
    date_str = last_line.split(",")[0]
    try:
        return dt.date.fromisoformat(date_str)
    except ValueError:
        return None


def write_ohlcv_csv_atomic(rows: list[tuple[str, float, float, float, float, float]], path: Path) -> None:
    """Write rows to path via a .tmp file, replacing atomically on success."""
    tmp = path.with_suffix(".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", newline="") as f:
            write_ohlcv_csv(rows, f)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def append_ohlcv_rows(rows: Iterable[tuple[str, float, float, float, float, float]], path: Path) -> None:
    with path.open("a", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        for date, o, h, lo, c, vol in rows:
            w.writerow([date, fmt_float(o), fmt_float(h), fmt_float(lo), fmt_float(c), fmt_float(vol)])


def read_ohlcv_rows(path: Path) -> list[tuple[str, float, float, float, float, float]]:
    """Read all data rows from an existing CSV into memory."""
    if not path.exists():
        return []
    rows = []
    with path.open("r", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0 or not row:
                continue
            try:
                rows.append((row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])))
            except (ValueError, IndexError):
                continue
    return rows


def err(msg: str) -> None:
    print(msg, file=sys.stderr)
