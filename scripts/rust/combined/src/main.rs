mod combine;
mod eps;
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

fn process_ticker(dir: &PathBuf) -> Result<String, String> {
    let ticker = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("UNKNOWN")
        .to_string();

    let eps_input = fs::read_to_string(dir.join("eps.csv")).map_err(|e| format!("{ticker}: {e}"))?;
    let ohlcv_input =
        fs::read_to_string(dir.join("ohlcv.csv")).map_err(|e| format!("{ticker}: {e}"))?;

    validate::validate(&eps_input, &ohlcv_input).map_err(|e| format!("{ticker}: {e}"))?;

    let eps_output = eps::fill_missing_eps(&eps_input).map_err(|e| format!("{ticker}: {e}"))?;
    let combined = combine::combine_ohlcv_eps(&ohlcv_input, &eps_output)
        .map_err(|e| format!("{ticker}: {e}"))?;
    let combined = combine::shift_above_eps(&combined).map_err(|e| format!("{ticker}: {e}"))?;
    let combined = combine::interpolate_eps(&combined).map_err(|e| format!("{ticker}: {e}"))?;
    let combined = combine::add_pe_ratio(&combined).map_err(|e| format!("{ticker}: {e}"))?;

    let combined_path = dir.join("combined.csv");
    fs::write(&combined_path, &combined).map_err(|e| format!("{ticker}: {e}"))?;
    Ok(format!("wrote {}", combined_path.display()))
}

fn main() -> Result<(), Box<dyn Error>> {
    let stocks_dir = find_stocks_dir().ok_or("could not find data/stocks/")?;

    let mut ticker_dirs: Vec<PathBuf> = fs::read_dir(&stocks_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    ticker_dirs.sort();

    let pool = rayon::ThreadPoolBuilder::new().num_threads(10).build()?;
    let results: Vec<Result<String, String>> = pool.install(|| {
        ticker_dirs.par_iter().map(process_ticker).collect()
    });

    let mut errors = 0usize;
    for res in &results {
        match res {
            Ok(msg) => println!("{msg}"),
            Err(msg) => {
                eprintln!("error: {msg}");
                errors += 1;
            }
        }
    }
    if errors > 0 {
        return Err(format!("{errors} ticker(s) failed").into());
    }
    Ok(())
}
