mod combine;
mod eps;
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
    let ticker = env::args().nth(1).ok_or("Usage: combined <TICKER>")?;
    let dir = find_ticker_dir(&ticker)
        .ok_or(format!("could not find data/stocks/{ticker}"))?;

    let eps_input = fs::read_to_string(dir.join("eps.csv"))?;
    let ohlcv_input = fs::read_to_string(dir.join("ohlcv.csv"))?;

    validate::validate(&eps_input, &ohlcv_input)?;

    // fill missing eps
    let eps_output = eps::fill_missing_eps(&eps_input)?;
    // let eps_temp_path = dir.join("eps_temp.csv");
    // fs::write(&eps_temp_path, &eps_output)?;
    // println!("wrote {}", eps_temp_path.display());

    // merge ohlcv + eps, then forward-fill missing ohlcv rows
    let ohlcv = ohlcv_input;
    let combined = combine::combine_ohlcv_eps(&ohlcv, &eps_output)?;
    let combined = combine::forward_fill_ohlcv(&combined)?;
    let combined = combine::interpolate_eps(&combined)?;
    let combined = combine::add_pe_ratio(&combined)?;
    let combined_path = dir.join("combined.csv");
    fs::write(&combined_path, &combined)?;
    println!("wrote {}", combined_path.display());

    Ok(())
}
