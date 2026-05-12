# combined

Fills in missing `ttm_net_eps` values in an EPS CSV file.

For any row where `ttm_net_eps` is blank, it back-calculates EPS from the other two columns using `EPS = price / P/E ratio`. Rows that already have EPS are left untouched. The result is printed to stdout.

## Usage

```
cargo run -- <eps_file.csv>
```
