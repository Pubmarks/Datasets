#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Update the stocks table in README.md from data/stocks/."""

from __future__ import annotations

import sys
from pathlib import Path


def _year_range(ticker_dir: Path) -> str | None:
    years = sorted(
        d.name
        for d in ticker_dir.iterdir()
        if d.is_dir() and len(d.name) == 4 and d.name.isdigit()
        and (d / "ohlcv.csv").exists()
    )
    if not years:
        return None
    return f"{years[0]}–{years[-1]}" if len(years) > 1 else years[0]


def build_rows(data_root: Path) -> list[str]:
    rows: list[str] = []
    if not data_root.is_dir():
        return rows
    for ticker_dir in sorted(data_root.iterdir()):
        if not ticker_dir.is_dir():
            continue
        flat = ticker_dir / "ohlcv.csv"
        if not flat.exists():
            continue
        ticker = ticker_dir.name.upper()
        link_path = f"data/stocks/{ticker_dir.name}/ohlcv.csv"
        label = _year_range(ticker_dir) or "data"
        rows.append(f"| {ticker} | [{label}]({link_path}) |")
    return rows


def update_readme(readme: Path, rows: list[str]) -> None:
    lines = readme.read_text(encoding="utf-8").splitlines(keepends=True)

    # Locate ### Stocks heading
    start = next(
        (i for i, l in enumerate(lines) if l.rstrip("\n") == "### Stocks"), None
    )
    if start is None:
        print("README.md: '### Stocks' heading not found", file=sys.stderr)
        sys.exit(1)

    # Find first table row after heading (skip blanks and non-table lines)
    table_found = False
    j = start + 1
    while j < len(lines):
        if lines[j].strip() == "":
            j += 1
            continue
        if lines[j].lstrip().startswith("|"):
            table_found = True
            break
        j += 1

    # Find end of existing table (k == j when no table was present)
    k = j
    while k < len(lines) and lines[k].lstrip().startswith("|"):
        k += 1

    header = ["| Ticker | OHLCV |\n", "| ------ | ----- |\n"]
    body = [r + "\n" for r in rows]

    if not table_found and j > 0 and lines[j - 1].strip() != "":
        header = ["\n"] + header

    readme.write_text("".join(lines[:j] + header + body + lines[k:]), encoding="utf-8")
    print(f"Updated README.md: {len(rows)} ticker(s)")


def main() -> None:
    update_readme(Path("README.md"), build_rows(Path("data/stocks")))


if __name__ == "__main__":
    main()
