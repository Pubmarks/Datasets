package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newPeratioCmd() *cobra.Command {
	var write bool
	cmd := &cobra.Command{
		Use:   "peratio [ticker] [year]",
		Short: "Download historical P/E table as CSV to stdout",
		Long: `Download price-to-earnings history from MacroTrends (chart redirect + /pe-ratio HTML table).

Ticker: TICKER environment variable and/or first argument.
Year:   YEAR environment and/or last argument; a single 4-digit argument sets year only (ticker from env).
If year is omitted, all parsed rows are printed.

Columns: date, stock_price, ttm_net_eps, pe_ratio (empty cells when MacroTrends omits a value).

With --write: writes/updates data/stocks/TICKER/peratio.csv and per-year files in the repo root.`,
		Example: `  macrotrends peratio AAPL
  macrotrends peratio MSFT 2024
  TICKER=AAPL macrotrends peratio 2024
  macrotrends peratio --write AAPL`,
		Args:          cobra.RangeArgs(0, 2),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ticker, year, yearSet, err := parseTickerYear(args)
			if err != nil {
				return err
			}

			ctx := context.Background()
			client := newHTTPClient(httpTimeout)

			rows, err := fetchPEHistory(ctx, client, normalizeSymbol(ticker))
			if err != nil {
				return err
			}
			out := rows
			if yearSet {
				from := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
				to := time.Date(year, 12, 31, 23, 59, 59, 0, time.UTC)
				out = filterPeRowsByRange(rows, from, to)
			}

			if !write {
				return writePeCSV(cmd.OutOrStdout(), out)
			}

			root, err := repoRoot()
			if err != nil {
				return err
			}
			outPath := filepath.Join(root, "data", "stocks", normalizeSymbol(ticker), "peratio.csv")
			if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
				return err
			}

			// store oldest-first so lastPeCSVDate (last line) gives the newest date
			sort.Slice(out, func(i, j int) bool { return out[i].date < out[j].date })

			lastD, hasLast := lastPeCSVDate(outPath)
			if !hasLast {
				if err := writePeCSVAtomic(out, outPath); err != nil {
					return err
				}
				fmt.Fprintf(cmd.ErrOrStderr(), "wrote %d row(s) to %s\n", len(out), outPath)
			} else {
				var newRows []peRow
				for _, r := range out {
					d, err := time.Parse("2006-01-02", r.date)
					if err != nil {
						continue
					}
					if d.After(lastD) {
						newRows = append(newRows, r)
					}
				}
				if len(newRows) == 0 {
					fmt.Fprintf(cmd.ErrOrStderr(), "%s: already up to date\n", outPath)
				} else {
					if err := appendPeCSVRows(newRows, outPath); err != nil {
						return err
					}
					fmt.Fprintf(cmd.ErrOrStderr(), "appended %d row(s) to %s\n", len(newRows), outPath)
				}
			}

			allRows, err := readPeCSVRows(outPath)
			if err != nil {
				return err
			}
			return syncPeYearFiles(root, normalizeSymbol(ticker), allRows, cmd.ErrOrStderr())
		},
	}
	cmd.Flags().BoolVar(&write, "write", false, "write to data/stocks/TICKER/peratio.csv + per-year files (update or create)")
	return cmd
}

func repoRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("cannot find repo root: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
