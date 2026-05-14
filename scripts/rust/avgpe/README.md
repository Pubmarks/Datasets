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
compute low/high P/E for positive band, high_neg/low_neg for loss periods

write avgpe_<N>.json
```

## Output shape (`avgpe_<N>.json`)

| Field | Type | Description |
|---|---|---|
| `ticker` | string | Ticker symbol |
| `start_date` | string | First date in the cut window |
| `end_date` | string | Last date in the cut window |
| `p_e_high_neg` | float (4dp) \| null | Highest negative P/E (closest to zero); null if no loss periods |
| `p_e_high_neg_date` | string \| null | Date of `p_e_high_neg` |
| `p_e_low_neg` | float (4dp) \| null | Lowest negative P/E (most negative); null if no loss periods |
| `p_e_low_neg_date` | string \| null | Date of `p_e_low_neg` |
| `p_e_low` | float (4dp) | Lowest positive P/E in the window |
| `p_e_low_date` | string | Date of `p_e_low` |
| `p_e_high` | float (4dp) | Highest positive P/E in the window |
| `p_e_high_date` | string | Date of `p_e_high` |
| `p_e_last` | float (4dp) | P/E of the last row (`close / ttm_net_eps`) |
| `price_last` | float | Last close price |
| `eps_last` | float | Last TTM net EPS |

Negative P/E (loss periods) are tracked separately from the positive band.
The last row's values are recorded regardless of sign.
