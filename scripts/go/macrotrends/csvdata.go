package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const ohlcvHeader = "date,open,high,low,close,volume"

type candle struct {
	date   string
	open   float64
	high   float64
	low    float64
	close  float64
	volume float64
}

func parseMacroTrendsCSV(r io.Reader) ([]candle, error) {
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	cr.FieldsPerRecord = -1
	var headerFound bool
	var out []candle
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) == 0 {
			continue
		}
		if !headerFound {
			if strings.EqualFold(joinCSVRecord(rec), ohlcvHeader) {
				headerFound = true
			}
			continue
		}
		if len(rec) != 6 {
			continue
		}
		c, err := rowToCandle(rec)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if !headerFound {
		return nil, fmt.Errorf("missing CSV header %q", ohlcvHeader)
	}
	return out, nil
}

func joinCSVRecord(rec []string) string {
	parts := make([]string, len(rec))
	for i, s := range rec {
		parts[i] = strings.TrimSpace(s)
	}
	return strings.Join(parts, ",")
}

func rowToCandle(rec []string) (candle, error) {
	date := strings.TrimSpace(rec[0])
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return candle{}, fmt.Errorf("bad date %q: %w", date, err)
	}
	open, err := strconv.ParseFloat(strings.TrimSpace(rec[1]), 64)
	if err != nil {
		return candle{}, err
	}
	high, err := strconv.ParseFloat(strings.TrimSpace(rec[2]), 64)
	if err != nil {
		return candle{}, err
	}
	low, err := strconv.ParseFloat(strings.TrimSpace(rec[3]), 64)
	if err != nil {
		return candle{}, err
	}
	close, err := strconv.ParseFloat(strings.TrimSpace(rec[4]), 64)
	if err != nil {
		return candle{}, err
	}
	vol, err := strconv.ParseFloat(strings.TrimSpace(rec[5]), 64)
	if err != nil {
		return candle{}, err
	}
	return candle{date: date, open: open, high: high, low: low, close: close, volume: vol}, nil
}

func filterByRange(rows []candle, from, to time.Time) []candle {
	out := rows[:0]
	for _, c := range rows {
		d, err := time.Parse("2006-01-02", c.date)
		if err != nil {
			continue
		}
		if d.Before(from) || d.After(to) {
			continue
		}
		out = append(out, c)
	}
	return out
}

func writeCSV(w io.Writer, rows []candle) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(strings.Split(ohlcvHeader, ",")); err != nil {
		return err
	}
	for _, c := range rows {
		rec := []string{
			c.date,
			fmtFloat(c.open),
			fmtFloat(c.high),
			fmtFloat(c.low),
			fmtFloat(c.close),
			fmtFloat(c.volume),
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func fmtFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
