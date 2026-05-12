# combined

Accepts a ticker symbol, finds `data/stocks/<TICKER>/` by walking up from the current directory, and produces two output files:

- **`eps_temp.csv`** — copy of `eps.csv` with any missing `ttm_net_eps` values back-calculated as `price / pe_ratio`
- **`combined_temp.csv`** — `ohlcv.csv` with a `ttm_net_eps` column appended, full outer join on date (empty cells where no match)

## Usage

```
cargo run -- <TICKER>
```
