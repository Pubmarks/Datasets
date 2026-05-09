package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "macrotrends",
		Short: "MacroTrends stock data fetchers",
		Long: `CLI tools that scrape MacroTrends for stock series.

Subcommands:
  ohlcv   daily OHLCV CSV to stdout
  peratio historical P/E table as CSV to stdout`,
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNeedSubcommand(cmd)
		},
	}

	root.CompletionOptions.DisableDefaultCmd = true
	root.AddCommand(newOhlcvCmd(), newPeratioCmd())
	return root
}

func errNeedSubcommand(cmd *cobra.Command) error {
	path := cmd.CommandPath()
	var b strings.Builder
	fmt.Fprintf(&b, "specify a subcommand: ohlcv, peratio\n\n")
	fmt.Fprintf(&b, "Examples:\n")
	fmt.Fprintf(&b, "  %s ohlcv AAPL\n", path)
	fmt.Fprintf(&b, "  %s ohlcv MSFT 2024\n", path)
	fmt.Fprintf(&b, "  TICKER=AAPL %s ohlcv 2024\n", path)
	fmt.Fprintf(&b, "  %s peratio AAPL\n\n", path)
	fmt.Fprintf(&b, "Run \"%s --help\" for full usage.\n", path)
	return errors.New(strings.TrimRight(b.String(), "\n"))
}
