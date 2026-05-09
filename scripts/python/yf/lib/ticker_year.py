"""Resolve ticker/year from env and args (same rules as macrotrends)."""

from __future__ import annotations

import os
from datetime import date


def parse_year_token(s: str) -> int | None:
    s = s.strip()
    if not s.isdigit() or len(s) != 4:
        return None
    y = int(s)
    if y < 1900 or y > 2100:
        return None
    return y


def year_from_env() -> tuple[int, bool]:
    s = os.environ.get("YEAR", "").strip()
    if not s:
        return 0, False
    y = parse_year_token(s)
    if y is None:
        raise ValueError("YEAR: use a 4-digit year between 1900 and 2100")
    return y, True


def parse_ticker_year(args: tuple[str, ...]) -> tuple[str, int, bool]:
    """
    Ticker: TICKER env and/or first argument.
    Year: YEAR env and/or last argument; a single 4-digit arg sets year only (ticker from env).
    If year is never set, year_set is False (no date filter).
    """
    ticker = os.environ.get("TICKER", "").strip()
    year, year_set = year_from_env()

    if len(args) == 0:
        pass
    elif len(args) == 1:
        y_one = parse_year_token(args[0])
        if y_one is not None:
            year = y_one
            year_set = True
        else:
            ticker = args[0]
    elif len(args) == 2:
        ticker = args[0]
        y = parse_year_token(args[1])
        if y is None:
            raise ValueError(
                f"second argument must be a 4-digit year (e.g. {date.today().year})"
            )
        year = y
        year_set = True
    else:
        raise ValueError("at most 2 arguments allowed [ticker] [year]")

    if not ticker.strip():
        raise ValueError(
            "set TICKER or pass ticker (e.g. yf ohlcv AAPL or yf ohlcv AAPL 2024)"
        )

    return ticker.strip().upper(), year, year_set
