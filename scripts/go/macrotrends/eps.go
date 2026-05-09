package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// epsHistoryTableHeaders must match MacroTrends' PE history <thead> (order-sensitive).
var epsHistoryTableHeaders = []string{"Date", "Stock Price", "TTM Net EPS", "PE Ratio"}

const epsCSVHeader = "date,stock_price,ttm_net_eps,pe_ratio"

type epsRow struct {
	date       string
	stockPrice *float64
	ttmNetEPS  *float64
	peRatio    *float64
}

func fetchEPSHistory(ctx context.Context, client *http.Client, symbol string) ([]epsRow, error) {
	pageURL, err := resolveEPSPageURL(ctx, client, symbol)
	if err != nil {
		return nil, err
	}
	html, err := fetchHTML(ctx, client, pageURL)
	if err != nil {
		return nil, err
	}
	rows := parseEPSHistoryFromHTML(html)
	if len(rows) == 0 {
		return nil, fmt.Errorf("no EPS history table found (expected thead %v)", epsHistoryTableHeaders)
	}
	return rows, nil
}

// parseEPSHistoryFromHTML finds the first table.table whose thead matches MacroTrends EPS headers.
func parseEPSHistoryFromHTML(html string) []epsRow {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil
	}
	var out []epsRow
	done := false
	doc.Find("table.table").Each(func(_ int, table *goquery.Selection) {
		if done {
			return
		}
		theads := table.Find("thead")
		if theads.Length() == 0 {
			return
		}
		lastThead := theads.Eq(theads.Length() - 1)
		var headers []string
		lastThead.Find("th").Each(func(_ int, th *goquery.Selection) {
			headers = append(headers, strings.TrimSpace(th.Text()))
		})
		if !stringSliceEqual(headers, epsHistoryTableHeaders) {
			return
		}
		tbody := table.Find("tbody").First()
		if tbody.Length() == 0 {
			return
		}
		var rows []epsRow
		tbody.Find("tr").Each(func(_ int, tr *goquery.Selection) {
			var cells []string
			tr.Find("td").Each(func(_ int, td *goquery.Selection) {
				cells = append(cells, strings.TrimSpace(td.Text()))
			})
			if len(cells) != 4 {
				return
			}
			row := epsRow{date: cells[0]}
			if v, ok := parseNumericEPS(cells[1], false); ok {
				row.stockPrice = ptrFloat(v)
			}
			if v, ok := parseNumericEPS(cells[2], true); ok {
				row.ttmNetEPS = ptrFloat(v)
			}
			if v, ok := parseNumericEPS(cells[3], false); ok {
				row.peRatio = ptrFloat(v)
			}
			rows = append(rows, row)
		})
		out = rows
		done = true
	})
	return out
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func parseNumericEPS(raw string, stripCurrency bool) (float64, bool) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return 0, false
	}
	if stripCurrency {
		text = strings.ReplaceAll(text, "$", "")
		text = strings.ReplaceAll(text, ",", "")
	} else {
		text = strings.ReplaceAll(text, ",", "")
	}
	v, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func ptrFloat(v float64) *float64 {
	return &v
}

func filterEPSRowsByRange(rows []epsRow, from, to time.Time) []epsRow {
	out := rows[:0]
	for _, r := range rows {
		d, err := time.Parse("2006-01-02", r.date)
		if err != nil {
			continue
		}
		if d.Before(from) || d.After(to) {
			continue
		}
		out = append(out, r)
	}
	return out
}

func writeEPSCSV(w io.Writer, rows []epsRow) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(strings.Split(epsCSVHeader, ",")); err != nil {
		return err
	}
	for _, r := range rows {
		rec := []string{
			r.date,
			fmtEPSFloatPtr(r.stockPrice),
			fmtEPSFloatPtr(r.ttmNetEPS),
			fmtEPSFloatPtr(r.peRatio),
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func fmtEPSFloatPtr(p *float64) string {
	if p == nil {
		return ""
	}
	return strconv.FormatFloat(*p, 'f', -1, 64)
}

func lastEPSCSVDate(path string) (time.Time, bool) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, false
	}
	defer f.Close()
	var last string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			last = line
		}
	}
	if last == "" || strings.HasPrefix(last, "date") {
		return time.Time{}, false
	}
	d, err := time.Parse("2006-01-02", strings.SplitN(last, ",", 2)[0])
	if err != nil {
		return time.Time{}, false
	}
	return d, true
}

func writeEPSCSVAtomic(rows []epsRow, path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := writeEPSCSV(f, rows); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func appendEPSCSVRows(rows []epsRow, path string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	cw := csv.NewWriter(f)
	for _, r := range rows {
		if err := cw.Write([]string{
			r.date,
			fmtEPSFloatPtr(r.stockPrice),
			fmtEPSFloatPtr(r.ttmNetEPS),
			fmtEPSFloatPtr(r.peRatio),
		}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func readEPSCSVRows(path string) ([]epsRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	cr := csv.NewReader(f)
	cr.FieldsPerRecord = 4
	var rows []epsRow
	first := true
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if first {
			first = false
			continue
		}
		row := epsRow{date: rec[0]}
		if v, e := strconv.ParseFloat(rec[1], 64); e == nil {
			row.stockPrice = ptrFloat(v)
		}
		if v, e := strconv.ParseFloat(rec[2], 64); e == nil {
			row.ttmNetEPS = ptrFloat(v)
		}
		if v, e := strconv.ParseFloat(rec[3], 64); e == nil {
			row.peRatio = ptrFloat(v)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func syncEPSYearFiles(root, ticker string, allRows []epsRow, w io.Writer) error {
	byYear := map[int][]epsRow{}
	for _, r := range allRows {
		y, _ := strconv.Atoi(r.date[:4])
		byYear[y] = append(byYear[y], r)
	}
	years := make([]int, 0, len(byYear))
	for y := range byYear {
		years = append(years, y)
	}
	sort.Ints(years)

	for _, year := range years {
		yearRows := byYear[year]
		yearPath := filepath.Join(root, "data", "stocks", ticker, strconv.Itoa(year), "eps.csv")
		yearLastD, hasYearLast := lastEPSCSVDate(yearPath)
		flatYearLastD, _ := time.Parse("2006-01-02", yearRows[len(yearRows)-1].date)

		if hasYearLast && yearLastD.Equal(flatYearLastD) {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(yearPath), 0755); err != nil {
			return err
		}
		if !hasYearLast {
			if err := writeEPSCSVAtomic(yearRows, yearPath); err != nil {
				return err
			}
			fmt.Fprintf(w, "wrote %d row(s) to %s\n", len(yearRows), yearPath)
		} else {
			var missing []epsRow
			for _, r := range yearRows {
				d, _ := time.Parse("2006-01-02", r.date)
				if d.After(yearLastD) {
					missing = append(missing, r)
				}
			}
			if len(missing) > 0 {
				if err := appendEPSCSVRows(missing, yearPath); err != nil {
					return err
				}
				fmt.Fprintf(w, "appended %d row(s) to %s\n", len(missing), yearPath)
			}
		}
	}
	return nil
}
