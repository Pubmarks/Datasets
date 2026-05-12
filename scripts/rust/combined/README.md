# combined

Accepts a ticker symbol, finds `data/stocks/<TICKER>/` by walking up from the current directory, and produces two output files.

**`eps_temp.csv`**
1. Copy of `eps.csv`
2. Any missing `ttm_net_eps` values are back-calculated as `price / pe_ratio`

**`combined_temp.csv`**
1. Full outer join of `ohlcv.csv` and `eps_temp.csv` on date
2. `ttm_net_eps` column appended from eps data
3. Eps-only rows (e.g. quarter-end dates with no trading) have ohlcv fields forward-filled from the last available trading day

## Usage

```
cargo run -- <TICKER>
```
