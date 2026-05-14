mod cut;
mod validate;

use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use rayon::prelude::*;

fn find_stocks_dir() -> Option<PathBuf> {
    let mut dir = env::current_dir().ok()?;
    loop {
        let candidate = dir.join("data").join("stocks");
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

fn process_ticker(ticker_dir: &PathBuf, years: u32) -> Result<String, String> {
    let combined = fs::read_to_string(ticker_dir.join("combined.csv"))
        .map_err(|e| format!("{}: {e}", ticker_dir.display()))?;
    validate::validate(&combined)
        .map_err(|e| format!("{}: {e}", ticker_dir.display()))?;
    let cut = cut::cut_to_last_n_years(&combined, years)
        .map_err(|e| format!("{}: {e}", ticker_dir.display()))?;
    let out_path = ticker_dir.join("avgpe_cut.csv");
    fs::write(&out_path, &cut)
        .map_err(|e| format!("{}: {e}", ticker_dir.display()))?;
    Ok(format!("wrote {}", out_path.display()))
}

fn main() -> Result<(), Box<dyn Error>> {
    let years: u32 = env::args()
        .nth(1)
        .ok_or("Usage: avgpe <YEARS>")?
        .parse()
        .map_err(|_| "YEARS must be a positive integer")?;

    let stocks_dir = find_stocks_dir().ok_or("could not find data/stocks/")?;

    let mut ticker_dirs: Vec<PathBuf> = fs::read_dir(&stocks_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    ticker_dirs.sort();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(10)
        .build()?;

    let results: Vec<Result<String, String>> = pool.install(|| {
        ticker_dirs.par_iter().map(|d| process_ticker(d, years)).collect()
    });

    let mut errors = 0usize;
    for res in &results {
        match res {
            Ok(msg) => println!("{msg}"),
            Err(msg) => { eprintln!("error: {msg}"); errors += 1; }
        }
    }
    if errors > 0 {
        return Err(format!("{errors} ticker(s) failed").into());
    }
    Ok(())
}
