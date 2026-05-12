# combined

Accepts a ticker symbol, finds `data/stocks/<TICKER>/` by walking up from the current directory, and writes `combined.csv`.

## Usage

```
cargo run -- <TICKER>
```

## Pipeline

```
read eps.csv, ohlcv.csv

// validation
if eps is missing any of: date, stock_price, ttm_net_eps, pe_ratio  →  error
if ohlcv is missing any of: date, open, high, low, close, volume    →  error

// fill missing eps
for each row in eps:
  if ttm_net_eps is empty:
    ttm_net_eps = stock_price / pe_ratio  // error if either is missing

// merge
rows = full outer join of ohlcv and eps on date, sorted by date
       each row: [date, open, high, low, close, volume, ttm_net_eps]

// shift eps up
for each row where ohlcv fields are all empty and ttm_net_eps is present:
  prev = last row above with ohlcv data
  if prev exists and prev.ttm_net_eps is empty:
    prev.ttm_net_eps = this row's ttm_net_eps
  drop this row

// interpolate eps
for each gap of empty ttm_net_eps values:
  if bounded on both sides by known values:
    fill linearly between them
  elif only a prior value exists (trailing gap):
    forward-fill from last known value
  else (leading gap — no prior value):
    leave empty

// pe ratio
for each row:
  if ttm_net_eps > 0 or < 0:  pe_ratio = close / ttm_net_eps
  if ttm_net_eps == 0:         pe_ratio = 0.00
  if ttm_net_eps is empty:     pe_ratio = empty

write combined.csv
```
