package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// parseTickerYear resolves ticker and year from env (TICKER, YEAR) and optional args [ticker] [year].
// Command-line values override environment. If year is never set, yearSet is false (no date filter).
func parseTickerYear(args []string) (ticker string, year int, yearSet bool, err error) {
	ticker = strings.TrimSpace(os.Getenv("TICKER"))
	year, yearSet, err = yearFromEnv()
	if err != nil {
		return "", 0, false, err
	}

	switch len(args) {
	case 0:
	case 1:
		if y, ok := parseYearToken(args[0]); ok {
			year = y
			yearSet = true
		} else {
			ticker = args[0]
		}
	case 2:
		ticker = args[0]
		y, ok := parseYearToken(args[1])
		if !ok {
			return "", 0, false, fmt.Errorf("second argument must be a 4-digit year (e.g. %d)", time.Now().Year())
		}
		year = y
		yearSet = true
	default:
		return "", 0, false, fmt.Errorf("at most 2 arguments allowed [ticker] [year]")
	}

	if strings.TrimSpace(ticker) == "" {
		return "", 0, false, fmt.Errorf("set TICKER or pass ticker (e.g. macrotrends ohlcv AAPL or macrotrends peratio AAPL)")
	}
	return ticker, year, yearSet, nil
}

func yearFromEnv() (year int, set bool, err error) {
	s := strings.TrimSpace(os.Getenv("YEAR"))
	if s == "" {
		return 0, false, nil
	}
	y, ok := parseYearToken(s)
	if !ok {
		return 0, false, fmt.Errorf("YEAR: use a 4-digit year between 1900 and 2100")
	}
	return y, true, nil
}

func parseYearToken(s string) (year int, ok bool) {
	y, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || y < 1900 || y > 2100 {
		return 0, false
	}
	return y, true
}

func yearsBackForYear(targetYear int) int {
	cy := time.Now().Year()
	if targetYear > cy {
		return 1
	}
	n := cy - targetYear + 2
	if n < 1 {
		n = 1
	}
	if n > 40 {
		n = 40
	}
	return n
}

func normalizeSymbol(s string) string {
	return strings.ToUpper(strings.TrimSpace(s))
}
