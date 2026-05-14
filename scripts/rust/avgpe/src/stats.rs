use std::error::Error;
use std::io::Cursor;

use serde::Serialize;

#[derive(Serialize)]
pub struct Stats {
    pub ticker:       String,
    pub start_date:   String,
    pub end_date:     String,
    pub p_e_min:      f64,
    pub p_e_min_date: String,
    pub p_e_max:      f64,
    pub p_e_max_date: String,
    pub p_e_last:     f64,
    pub price_last:   f64,
    pub eps_last:     f64,
}

pub fn compute_stats(cut: &str, ticker: &str) -> Result<Stats, Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(cut));
    let headers = reader.headers()?.clone();

    let i_date  = headers.iter().position(|c| c == "date") .ok_or("missing date")?;
    let i_close = headers.iter().position(|c| c == "close").ok_or("missing close")?;
    let i_eps   = headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;

    let mut start_date = String::new();
    let mut end_date   = String::new();
    let mut min_pe     = f64::MAX;
    let mut min_date   = String::new();
    let mut max_pe     = f64::MIN;
    let mut max_date   = String::new();
    let mut last_pe    = 0f64;
    let mut last_price = 0f64;
    let mut last_eps   = 0f64;

    for result in reader.records() {
        let record = result?;
        let date  = record.get(i_date) .unwrap_or("").to_string();
        let close = record.get(i_close).and_then(|v| v.parse::<f64>().ok());
        let eps   = record.get(i_eps)  .and_then(|v| v.parse::<f64>().ok());

        if start_date.is_empty() {
            start_date = date.clone();
        }
        end_date = date.clone();

        if let (Some(c), Some(e)) = (close, eps) {
            if e != 0.0 {
                let pe = c / e;
                // only consider positive pe (earnings positive)
                if pe > 0.0 {
                    if pe < min_pe { min_pe = pe; min_date = date.clone(); }
                    if pe > max_pe { max_pe = pe; max_date = date.clone(); }
                }
                last_pe    = pe;
                last_price = c;
                last_eps   = e;
            }
        }
    }

    if start_date.is_empty() {
        return Err("no rows in cut data".into());
    }
    if min_pe == f64::MAX {
        return Err("no rows with valid pe found".into());
    }

    Ok(Stats {
        ticker:       ticker.to_string(),
        start_date,
        end_date,
        p_e_min:      (min_pe * 10000.0).round() / 10000.0,
        p_e_min_date: min_date,
        p_e_max:      (max_pe * 10000.0).round() / 10000.0,
        p_e_max_date: max_date,
        p_e_last:     (last_pe    * 10000.0).round() / 10000.0,
        price_last:   last_price,
        eps_last:     last_eps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const CUT: &str = "\
date,open,high,low,close,volume,ttm_net_eps,pe_ratio
2021-01-04,100,110,90,200.00,1000,2.00,100.00
2022-06-15,105,115,95,105.00,2000,5.00,21.00
2023-03-20,90,100,80,180.00,3000,3.00,60.00
2024-12-31,120,130,110,250.00,4000,4.00,62.50";

    #[test]
    fn min_and_max_pe_are_correct() {
        let s = compute_stats(CUT, "TEST").unwrap();
        // pe values: 100.0, 21.0, 60.0, 62.5
        assert_eq!(s.p_e_min, 21.0);
        assert_eq!(s.p_e_min_date, "2022-06-15");
        assert_eq!(s.p_e_max, 100.0);
        assert_eq!(s.p_e_max_date, "2021-01-04");
    }

    #[test]
    fn last_row_fields_are_correct() {
        let s = compute_stats(CUT, "TEST").unwrap();
        assert_eq!(s.p_e_last,   62.5);
        assert_eq!(s.price_last, 250.0);
        assert_eq!(s.eps_last,   4.0);
    }

    #[test]
    fn start_and_end_dates_are_correct() {
        let s = compute_stats(CUT, "TEST").unwrap();
        assert_eq!(s.start_date, "2021-01-04");
        assert_eq!(s.end_date,   "2024-12-31");
    }

    #[test]
    fn rows_with_missing_eps_are_excluded_from_pe() {
        let cut = "\
date,open,high,low,close,volume,ttm_net_eps,pe_ratio
2021-01-04,100,110,90,200.00,1000,,
2022-06-15,105,115,95,105.00,2000,5.00,21.00";
        let s = compute_stats(cut, "TEST").unwrap();
        assert_eq!(s.p_e_min, 21.0);
        assert_eq!(s.p_e_max, 21.0);
    }

    #[test]
    fn zero_eps_rows_are_excluded_from_pe() {
        let cut = "\
date,open,high,low,close,volume,ttm_net_eps,pe_ratio
2021-01-04,100,110,90,200.00,1000,0.00,0.00
2022-06-15,105,115,95,105.00,2000,5.00,21.00";
        let s = compute_stats(cut, "TEST").unwrap();
        assert_eq!(s.p_e_min, 21.0);
        assert_eq!(s.p_e_max, 21.0);
    }

    #[test]
    fn negative_pe_rows_are_excluded_from_min_max() {
        let cut = "\
date,open,high,low,close,volume,ttm_net_eps,pe_ratio
2021-01-04,100,110,90,200.00,1000,-2.00,
2022-06-15,105,115,95,105.00,2000,5.00,21.00";
        let s = compute_stats(cut, "TEST").unwrap();
        assert_eq!(s.p_e_min, 21.0);
        assert_eq!(s.p_e_max, 21.0);
    }

    #[test]
    fn ticker_name_is_stored() {
        let s = compute_stats(CUT, "NVDA").unwrap();
        assert_eq!(s.ticker, "NVDA");
    }
}
