use std::collections::HashMap;
use std::error::Error;

use serde::Deserialize;

const FRED_BASE: &str = "https://api.stlouisfed.org/fred/series/observations";
const SERIES_CPI: &str = "CPIAUCSL";

#[derive(Deserialize)]
struct FredResponse {
    observations: Option<Vec<Observation>>,
    error_message: Option<String>,
}

#[derive(Deserialize)]
struct Observation {
    date: String,
    value: String,
}

/// Annual CPI values keyed by year, sourced from FRED (CPIAUCSL).
///
/// Use `adjustment_factor(year)` to get the multiplier that brings a dollar
/// amount from `year` to the latest year in this dataset, then multiply it
/// by the EPS value to get inflation-adjusted (real) EPS.
pub struct CpiData {
    by_year: HashMap<u32, f64>,
    latest_cpi: f64,
}

impl CpiData {
    /// Fetch annual CPI from FRED for the given year range (inclusive).
    ///
    /// Reads `FRED_API_KEY` from the environment — call `dotenvy::dotenv()`
    /// before this if your key lives in a `.env` file.
    pub fn fetch(start_year: u32, end_year: u32) -> Result<Self, Box<dyn Error>> {
        let api_key = std::env::var("FRED_API_KEY").map_err(|_| "FRED_API_KEY not set")?;

        let url = format!(
            "{FRED_BASE}?series_id={SERIES_CPI}\
             &observation_start={start_year}-01-01\
             &observation_end={end_year}-12-31\
             &frequency=a\
             &api_key={api_key}\
             &file_type=json"
        );

        let resp: FredResponse = reqwest::blocking::get(&url)?.json()?;

        if let Some(msg) = resp.error_message {
            return Err(format!("FRED error: {msg}").into());
        }

        let mut by_year: HashMap<u32, f64> = HashMap::new();
        for obs in resp.observations.as_deref().unwrap_or(&[]) {
            // FRED uses "." for missing values
            if obs.value == "." {
                continue;
            }
            if let (Ok(year), Ok(cpi)) = (obs.date[..4].parse::<u32>(), obs.value.parse::<f64>()) {
                by_year.insert(year, cpi);
            }
        }

        if by_year.is_empty() {
            return Err("FRED returned no usable CPI observations".into());
        }

        let latest_year = *by_year.keys().max().unwrap();
        let latest_cpi = by_year[&latest_year];

        Ok(Self {
            by_year,
            latest_cpi,
        })
    }

    /// Adjust a nominal EPS from `year` to the latest year's dollars.
    /// Falls back to the raw EPS unchanged when the year has no CPI entry
    /// (e.g. the current calendar year whose annual average isn't published yet).
    pub fn adjust_eps_or_nominal(&self, eps: f64, year: u32) -> f64 {
        match self.by_year.get(&year) {
            Some(&base) if base > 0.0 => eps * (self.latest_cpi / base),
            _ => eps,
        }
    }

    /// Empty instance with no CPI data — `adjust_eps_or_nominal` returns EPS unchanged.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            by_year: HashMap::new(),
            latest_cpi: 0.0,
        }
    }

    #[cfg(test)]
    pub fn from_map(by_year: HashMap<u32, f64>) -> Self {
        let latest_year = *by_year.keys().max().unwrap_or(&0);
        let latest_cpi = by_year.get(&latest_year).copied().unwrap_or(0.0);
        Self {
            by_year,
            latest_cpi,
        }
    }
}
