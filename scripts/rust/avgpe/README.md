# avgpe

Reads every `data/stocks/<TICKER>/combined.csv` and writes `avgpe_cut.csv`
containing only the last N years of rows. All tickers are processed in
parallel (10 at a time).

## Usage

```
cargo run -- <YEARS>
```

## Pipeline

```
read combined.csv

// validation
if combined is missing any of: date, pe_ratio  →  error

// cut
cutoff = today - YEARS (same month/day, N years prior)
keep rows where date >= cutoff

write avgpe_cut.csv
```
