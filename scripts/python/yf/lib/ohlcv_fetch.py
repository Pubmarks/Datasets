"""Fetch daily OHLCV via yfinance."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import yfinance as yf

Row = tuple[str, float, float, float, float, float]


def _flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.columns, pd.MultiIndex):
        return frame
    # Single-ticker download produces columns like ('Open', 'AAPL') — drop the ticker level.
    if frame.columns.nlevels >= 2:
        tickers = frame.columns.get_level_values(1).unique()
        if len(tickers) == 1:
            return frame.droplevel(1, axis=1)
    return frame


def _download_kwargs(
    year: int | None,
    year_set: bool,
    from_date: dt.date | None,
) -> dict:
    base: dict = {"interval": "1d", "auto_adjust": False, "actions": False, "progress": False, "threads": False}
    if year_set and year is not None:
        base["start"] = from_date.isoformat() if from_date else f"{year}-01-01"
        base["end"] = f"{year + 1}-01-01"
    elif from_date is not None:
        base["start"] = from_date.isoformat()
        base["end"] = (dt.date.today() + dt.timedelta(days=1)).isoformat()
    else:
        base["period"] = "max"
    return base


def fetch_daily_ohlcv(
    ticker: str,
    *,
    year: int | None,
    year_set: bool,
    from_date: dt.date | None = None,
) -> list[Row]:
    symbol = ticker.upper().strip()
    df = yf.download(symbol, **_download_kwargs(year, year_set, from_date))

    if df is None or df.empty:
        return []

    df = _flatten_columns(df)
    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(df.columns):
        raise RuntimeError(f"unexpected columns from yfinance: {list(df.columns)}")

    year_start = dt.date(year, 1, 1) if (year_set and year) else None
    year_end = dt.date(year, 12, 31) if (year_set and year) else None

    rows: list[Row] = []
    for idx, row in df.iterrows():
        ts = pd.Timestamp(idx)
        d = ts.tz_convert("UTC").date() if ts.tzinfo else ts.date()
        if year_start and year_end and (d < year_start or d > year_end):
            continue
        o = float(row["Open"])
        h = float(row["High"])
        lo = float(row["Low"])
        c = float(row["Close"])
        v = float(row["Volume"])
        if pd.isna(o) or pd.isna(h) or pd.isna(lo) or pd.isna(c) or pd.isna(v):
            continue
        rows.append((d.isoformat(), round(o, 3), round(h, 3), round(lo, 3), round(c, 3), float(int(v))))

    rows.sort(key=lambda r: r[0])
    return rows
