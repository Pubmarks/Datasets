mod cut;
mod validate;

use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

fn find_ticker_dir(ticker: &str) -> Option<PathBuf> {
    let mut dir = env::current_dir().ok()?;
    loop {
        let candidate = dir.join("data").join("stocks").join(ticker);
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let ticker = env::args().nth(1).ok_or("Usage: avgpe <TICKER> <YEARS>")?;
    let years: u32 = env::args()
        .nth(2)
        .ok_or("Usage: avgpe <TICKER> <YEARS>")?
        .parse()
        .map_err(|_| "YEARS must be a positive integer")?;

    let dir = find_ticker_dir(&ticker)
        .ok_or(format!("could not find data/stocks/{ticker}"))?;

    let combined = fs::read_to_string(dir.join("combined.csv"))?;
    validate::validate(&combined)?;

    let cut = cut::cut_to_last_n_years(&combined, years)?;
    let out_path = dir.join("avgpe_cut.csv");
    fs::write(&out_path, &cut)?;
    println!("wrote {}", out_path.display());

    Ok(())
}
