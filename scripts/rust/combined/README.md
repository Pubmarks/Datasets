# combined

Accepts a ticker symbol, finds `data/stocks/<TICKER>/` by walking up from the current directory, and produces two output files.

## Validation

Before any processing, the following checks run and exit on failure:

1. No zero `ttm_net_eps` values (negative/positive allowed, zero is not)
2. No stock splits detected — consecutive close prices must not match common split ratios (2:1, 3:1, 4:1, 5:1)

## Output

**`eps_temp.csv`**
1. Copy of `eps.csv`
2. Any missing `ttm_net_eps` values are back-calculated as `stock_price / pe_ratio`

**`combined_temp.csv`**
1. Full outer join of `ohlcv.csv` and `eps_temp.csv` on date
2. `ttm_net_eps` column appended from eps data
3. EPS-only rows have ohlcv fields forward-filled from the last available trading day
4. Rows without any prior eps data keep `ttm_net_eps` empty
5. `ttm_net_eps` is linearly interpolated between known quarterly values
6. `pe_ratio` column added as `close / ttm_net_eps` (empty if either is missing)

## Usage

```
cargo run -- <TICKER>
```
