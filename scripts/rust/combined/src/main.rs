mod eps;

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
    let ticker = env::args().nth(1).ok_or("Usage: combined <TICKER>")?;
    let dir = find_ticker_dir(&ticker)
        .ok_or(format!("could not find data/stocks/{ticker}"))?;

    let eps_path = dir.join("eps.csv");
    let input = fs::read_to_string(&eps_path)?;
    let output = eps::fill_missing_eps(&input)?;

    let out_path = dir.join("eps_temp.csv");
    fs::write(&out_path, &output)?;
    println!("wrote {}", out_path.display());
    Ok(())
}
