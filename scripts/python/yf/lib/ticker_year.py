"""Resolve ticker and year from CLI arguments."""

from __future__ import annotations

from datetime import date


def parse_year_token(s: str) -> int | None:
    s = s.strip()
    if not s.isdigit() or len(s) != 4:
        return None
    y = int(s)
    if y < 1900 or y > 2100:
        return None
    return y


def parse_ticker_year(args: tuple[str, ...]) -> tuple[str, int, bool]:
    """Return (ticker, year, year_set) from CLI positional args [TICKER] [YEAR]."""
    ticker = ""
    year = 0
    year_set = False

    if len(args) == 1:
        y = parse_year_token(args[0])
        if y is not None:
            year, year_set = y, True
        else:
            ticker = args[0]
    elif len(args) == 2:
        ticker = args[0]
        y = parse_year_token(args[1])
        if y is None:
            raise ValueError(f"second argument must be a 4-digit year (e.g. {date.today().year})")
        year, year_set = y, True
    elif len(args) > 2:
        raise ValueError("at most 2 arguments allowed: [ticker] [year]")

    if not ticker.strip():
        raise ValueError("pass a ticker (e.g. yf ohlcv AAPL or yf ohlcv AAPL 2024)")

    return ticker.strip().upper(), year, year_set
