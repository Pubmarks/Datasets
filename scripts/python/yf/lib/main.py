"""CLI entrypoint: macrotrends-compatible `yf ohlcv`."""

from __future__ import annotations

import datetime as dt
import subprocess
from pathlib import Path

import click

from lib.csvfmt import append_ohlcv_rows, err, read_last_date, write_ohlcv_csv
from lib.ohlcv_fetch import fetch_daily_ohlcv
from lib.ticker_year import parse_ticker_year


def _repo_root() -> Path:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=subprocess.DEVNULL,
        )
        return Path(out.decode().strip())
    except subprocess.CalledProcessError:
        pass
    # Walk up looking for a data/ directory
    here = Path(__file__).resolve().parent
    for parent in [here, *here.parents]:
        if (parent / "data").is_dir():
            return parent
    raise RuntimeError("cannot locate repo root (no git root and no data/ directory found)")


def _output_path(root: Path, ticker: str, year: int, year_set: bool) -> Path:
    if year_set:
        return root / "data" / "stocks" / ticker / str(year) / "ohlcv.csv"
    return root / "data" / "stocks" / ticker / "ohlcv.csv"


def _need_subcommand() -> None:
    err(
        "specify a subcommand: ohlcv\n\n"
        "Examples:\n"
        "  yf ohlcv AAPL\n"
        "  yf ohlcv MSFT 2024\n"
        "  TICKER=AAPL yf ohlcv 2024\n\n"
        'Run "yf --help" for full usage.'
    )
    raise SystemExit(1)


@click.group(invoke_without_command=True)
@click.pass_context
def yf(ctx: click.Context) -> None:
    if ctx.invoked_subcommand is None:
        _need_subcommand()


@yf.command(
    "ohlcv",
    context_settings={"show_default": True},
    help="Download daily OHLCV as CSV and write to data/stocks/TICKER[/YEAR]/ohlcv.csv (Yahoo Finance via yfinance).",
)
@click.argument("parts", nargs=-1)
def ohlcv_cmd(parts: tuple[str, ...]) -> None:
    """
    Ticker: TICKER environment variable and/or first argument.

    Year: YEAR environment and/or last argument; a single 4-digit argument sets year only (ticker from env).

    If year is omitted, writes all history to data/stocks/TICKER/ohlcv.csv.
    If the file already exists, only missing dates at the end are appended.
    """
    try:
        ticker, year, year_set = parse_ticker_year(parts)
    except ValueError as e:
        err(str(e))
        raise SystemExit(1) from e

    try:
        root = _repo_root()
    except RuntimeError as e:
        err(str(e))
        raise SystemExit(1) from e

    path = _output_path(root, ticker, year, year_set)
    last_d = read_last_date(path)
    from_d = last_d + dt.timedelta(days=1) if last_d else None

    today = dt.date.today()
    cutoff = dt.date(year, 12, 31) if year_set else today
    if today.weekday() == 5:
        cutoff = min(cutoff, today - dt.timedelta(days=1))
    elif today.weekday() == 6:
        cutoff = min(cutoff, today - dt.timedelta(days=2))
    if from_d and from_d > cutoff:
        err(f"{path}: already up to date")
        return

    try:
        rows = fetch_daily_ohlcv(
            ticker,
            year=year if year_set else None,
            year_set=year_set,
            from_date=from_d,
        )
    except Exception as e:
        err(str(e))
        raise SystemExit(1) from e

    path.parent.mkdir(parents=True, exist_ok=True)
    if last_d is None:
        with path.open("w", newline="") as f:
            write_ohlcv_csv(rows, f)
    else:
        append_ohlcv_rows(rows, path)

    err(f"wrote {len(rows)} row(s) to {path}")


def main() -> None:
    yf()


if __name__ == "__main__":
    main()
