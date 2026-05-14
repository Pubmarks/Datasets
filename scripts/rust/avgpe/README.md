# avgpe

Reads every `data/stocks/<TICKER>/combined.csv`, cuts to the last N years of
rows, computes P/E stats, and writes `avgpe_<N>.json` per ticker. All tickers
are processed in parallel (10 at a time).

## Usage

```
cargo run -- <YEARS>
```

Example: `cargo run -- 10` writes `avgpe_10.json` in each ticker directory.

## Pipeline

```
read combined.csv

// validation
if combined is missing any of: date, pe_ratio  →  error

// cut
cutoff = today - YEARS (same month/day, N years prior)
keep rows where date >= cutoff

// stats
compute min/max P/E for positive band, min_lossy/max_lossy for loss periods

write avgpe_<N>.json
```

## Output shape (`avgpe_<N>.json`)

| Field | Type | Description |
|---|---|---|
| `ticker` | string | Ticker symbol |
| `start_date` | string | First date in the cut window |
| `end_date` | string | Last date in the cut window |
| `p_e_max_lossy` | float (4dp) \| null | Highest negative P/E (closest to zero); null if no loss periods |
| `p_e_max_lossy_date` | string \| null | Date of `p_e_max_lossy` |
| `p_e_min_lossy` | float (4dp) \| null | Lowest negative P/E (most negative); null if no loss periods |
| `p_e_min_lossy_date` | string \| null | Date of `p_e_min_lossy` |
| `p_e_min` | float (4dp) | Lowest positive P/E in the window |
| `p_e_min_date` | string | Date of `p_e_min` |
| `p_e_max` | float (4dp) | Highest positive P/E in the window |
| `p_e_max_date` | string | Date of `p_e_max` |
| `p_e_last` | float (4dp) | P/E of the last row (`close / ttm_net_eps`) |
| `price_last` | float | Last close price |
| `eps_last` | float | Last TTM net EPS |

Negative P/E (loss periods) are tracked separately from the positive band.
The last row's values are recorded regardless of sign.
