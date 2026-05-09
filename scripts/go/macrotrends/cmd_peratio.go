package main

import (
	"context"
	"time"

	"github.com/spf13/cobra"
)

func newPeratioCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "peratio [ticker] [year]",
		Short: "Download historical P/E table as CSV to stdout",
		Long: `Download price-to-earnings history from MacroTrends (chart redirect + /pe-ratio HTML table).

Ticker: TICKER environment variable and/or first argument.
Year:   YEAR environment and/or last argument; a single 4-digit argument sets year only (ticker from env).
If year is omitted, all parsed rows are printed.

Columns: date, stock_price, ttm_net_eps, pe_ratio (empty cells when MacroTrends omits a value).`,
		Example: `  macrotrends peratio AAPL
  macrotrends peratio MSFT 2024
  TICKER=AAPL macrotrends peratio 2024`,
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
			return writePeCSV(cmd.OutOrStdout(), out)
		},
	}
}
