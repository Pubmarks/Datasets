# combined

Accepts a ticker symbol, finds `data/stocks/<TICKER>/` by walking up from the current directory, and produces `combined.csv`.

## Validation

Before any processing, the following checks run and exit on failure:

1. No zero `ttm_net_eps` values (negative/positive allowed, zero is not)
2. No stock splits detected — consecutive close prices must not match common split ratios (2:1, 3:1, 4:1, 5:1)

## Output

**`combined.csv`**
1. Full outer join of `ohlcv.csv` and `eps.csv` on date
2. Missing `ttm_net_eps` values back-calculated as `stock_price / pe_ratio`
3. `ttm_net_eps` column appended from eps data
4. EPS-only rows have ohlcv fields forward-filled from the last available trading day
5. Rows without any prior eps data keep `ttm_net_eps` empty
6. `ttm_net_eps` is linearly interpolated between known quarterly values
7. `pe_ratio` column added as `close / ttm_net_eps` (empty if either is missing)

## Usage

```
cargo run -- <TICKER>
```
