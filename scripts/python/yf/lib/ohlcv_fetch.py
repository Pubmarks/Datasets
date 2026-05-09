"""Fetch daily OHLCV via yfinance."""

from __future__ import annotations

import datetime as dt
from typing import List, Tuple

import pandas as pd
import yfinance as yf


def _flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.columns, pd.MultiIndex):
        return frame
    # Single-ticker download: columns like ('Open', 'AAPL')
    if frame.columns.nlevels >= 2:
        tickers = frame.columns.get_level_values(1).unique()
        if len(tickers) == 1:
            return frame.droplevel(1, axis=1)
    return frame


def fetch_daily_ohlcv(
    ticker: str,
    *,
    year: int | None,
    year_set: bool,
    from_date: dt.date | None = None,
) -> List[Tuple[str, float, float, float, float, float]]:
    symbol = ticker.upper().strip()
    if year_set and year is not None:
        start = from_date.isoformat() if from_date else f"{year}-01-01"
        end = f"{year + 1}-01-01"
        df = yf.download(
            symbol,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )
    elif from_date is not None:
        df = yf.download(
            symbol,
            start=from_date.isoformat(),
            end=(dt.date.today() + dt.timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )
    else:
        df = yf.download(
            symbol,
            period="max",
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )

    if df is None or df.empty:
        return []

    df = _flatten_columns(df)
    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(df.columns):
        raise RuntimeError(f"unexpected columns from yfinance: {list(df.columns)}")

    from_d = dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc).date() if year_set and year else None
    to_d = dt.datetime(year, 12, 31, tzinfo=dt.timezone.utc).date() if year_set and year else None

    rows: List[Tuple[str, float, float, float, float, float]] = []
    for idx, row in df.iterrows():
        ts = pd.Timestamp(idx)
        d = ts.tz_convert("UTC").date() if ts.tzinfo else ts.date()
        if year_set and from_d is not None and to_d is not None:
            if d < from_d or d > to_d:
                continue
        o = float(row["Open"])
        h = float(row["High"])
        lo = float(row["Low"])
        c = float(row["Close"])
        v = float(row["Volume"])
        if pd.isna(o) or pd.isna(h) or pd.isna(lo) or pd.isna(c) or pd.isna(v):
            continue
        # Match typical pubmarks OHLCV files (e.g. datasets/stocks/*/ohlcv.csv): 3dp prices, whole volume.
        rows.append(
            (
                d.isoformat(),
                round(o, 3),
                round(h, 3),
                round(lo, 3),
                round(c, 3),
                float(int(v)),
            )
        )

    rows.sort(key=lambda r: r[0])
    return rows
