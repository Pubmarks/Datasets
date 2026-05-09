package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// chartRedirectURL is the first-hop URL MacroTrends uses to redirect to
// /stocks/charts/{TICKER}/{company-slug}/...
func chartRedirectURL(symbol string) string {
	return "https://www.macrotrends.net/stocks/charts/" + normalizeSymbol(symbol)
}

// resolveChartBaseURL follows redirects from /stocks/charts/{SYMBOL} to the canonical
// .../stocks/charts/{SYMBOL}/{slug} URL (no trailing slash).
func resolveChartBaseURL(ctx context.Context, client *http.Client, stockSymbol string) (string, error) {
	symbol := normalizeSymbol(stockSymbol)
	if symbol == "" {
		return "", fmt.Errorf("empty stock symbol")
	}
	initial := chartRedirectURL(symbol)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, initial, nil)
	if err != nil {
		return "", err
	}
	applyDefaultHeaders(req)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	final := strings.TrimRight(resp.Request.URL.String(), "/")
	if err := validateChartCanonicalURL(final, symbol); err != nil {
		return "", err
	}
	return final, nil
}

// resolvePERatioPageURL is resolveChartBaseURL + "/pe-ratio".
func resolvePERatioPageURL(ctx context.Context, client *http.Client, stockSymbol string) (string, error) {
	base, err := resolveChartBaseURL(ctx, client, stockSymbol)
	if err != nil {
		return "", err
	}
	return base + "/pe-ratio", nil
}

func validateChartCanonicalURL(finalURL, expectedSymbol string) error {
	u, err := url.Parse(finalURL)
	if err != nil {
		return fmt.Errorf("invalid final URL %q: %w", finalURL, err)
	}
	segments := pathSegments(u.Path)
	if len(segments) < 4 ||
		segments[0] != "stocks" ||
		segments[1] != "charts" ||
		strings.ToUpper(segments[2]) != expectedSymbol {
		return fmt.Errorf(
			"MacroTrends did not redirect to a /stocks/charts/{ticker}/{slug} URL; got %q",
			finalURL,
		)
	}
	return nil
}

func pathSegments(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
