package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

const (
	chartIframePath = "https://www.macrotrends.net/production/stocks/desktop/PRODUCTION/stock_price_history.php"
	downloadBase    = "https://www.macrotrends.net/assets/php/stock_data_download.php"
	httpTimeout     = 30 * time.Second
)

var stockDataDownloadRE = regexp.MustCompile(`stock_data_download\.php\?s=([^&']+)&t=([^']+)`)

func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			return nil
		},
	}
}

func applyDefaultHeaders(req *http.Request) {
	h := req.Header
	h.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "+
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	h.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	h.Set("Accept-Language", "en-US,en;q=0.9")
}

func fetchHTML(ctx context.Context, client *http.Client, urlStr string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return "", err
	}
	applyDefaultHeaders(req)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("GET %s: %s", urlStr, resp.Status)
	}
	return string(body), nil
}

// chartIframeURL builds the chart iframe URL. If yearsBack is nil, the yb query param is omitted (MacroTrends default range).
func chartIframeURL(symbol string, yearsBack *int) string {
	sym := normalizeSymbol(symbol)
	q := url.Values{}
	q.Set("t", sym)
	if yearsBack != nil {
		q.Set("yb", fmt.Sprintf("%d", *yearsBack))
	}
	return chartIframePath + "?" + q.Encode()
}

func downloadURLFromChartHTML(chartHTML string) (string, error) {
	m := stockDataDownloadRE.FindStringSubmatch(chartHTML)
	if len(m) != 3 {
		return "", fmt.Errorf("stock_data_download URL not found in chart HTML")
	}
	u, err := url.Parse(downloadBase)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("s", m[1])
	q.Set("t", m[2])
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func fetchOHLCVCSV(ctx context.Context, client *http.Client, symbol string, yearsBack *int) ([]byte, error) {
	chartURL := chartIframeURL(symbol, yearsBack)
	html, err := fetchHTML(ctx, client, chartURL)
	if err != nil {
		return nil, err
	}
	dl, err := downloadURLFromChartHTML(html)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dl, nil)
	if err != nil {
		return nil, err
	}
	applyDefaultHeaders(req)
	req.Header.Set("Accept", "text/csv,*/*;q=0.9")
	req.Header.Set("Referer", chartURL)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: %s", dl, resp.Status)
	}
	return body, nil
}
