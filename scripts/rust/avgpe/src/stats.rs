use std::error::Error;
use std::io::Cursor;

use serde::Serialize;

#[derive(Serialize)]
pub struct Stats {
    pub ticker:            String,
    pub start_date:        String,
    pub end_date:          String,
    pub p_e_high_neg:      Option<f64>,
    pub p_e_high_neg_date: Option<String>,
    pub p_e_low_neg:       Option<f64>,
    pub p_e_low_neg_date:  Option<String>,
    pub p_e_low:           f64,
    pub p_e_low_date:      String,
    pub p_e_high:          f64,
    pub p_e_high_date:     String,
    pub p_e_last:          f64,
    pub price_last:        f64,
    pub eps_last:          f64,
}

pub fn compute_stats(cut: &str, ticker: &str) -> Result<Stats, Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(cut));
    let headers = reader.headers()?.clone();

    let i_date  = headers.iter().position(|c| c == "date") .ok_or("missing date")?;
    let i_close = headers.iter().position(|c| c == "close").ok_or("missing close")?;
    let i_eps   = headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;

    let mut start_date   = String::new();
    let mut end_date     = String::new();
    // positive band
    let mut low_pe       = f64::MAX;
    let mut low_date     = String::new();
    let mut high_pe      = f64::MIN;
    let mut high_date    = String::new();
    // negative band: high_neg = closest to zero (least negative), low_neg = most negative
    let mut high_neg_pe:   Option<f64>    = None;
    let mut high_neg_date: Option<String> = None;
    let mut low_neg_pe:    Option<f64>    = None;
    let mut low_neg_date:  Option<String> = None;
    let mut last_pe      = 0f64;
    let mut last_price   = 0f64;
    let mut last_eps     = 0f64;

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
                if pe > 0.0 {
                    if pe < low_pe  { low_pe  = pe; low_date  = date.clone(); }
                    if pe > high_pe { high_pe = pe; high_date = date.clone(); }
                } else {
                    // negative pe: high_neg is closest to zero, low_neg is most negative
                    if high_neg_pe.map_or(true, |h| pe > h) {
                        high_neg_pe   = Some(pe);
                        high_neg_date = Some(date.clone());
                    }
                    if low_neg_pe.map_or(true, |l| pe < l) {
                        low_neg_pe   = Some(pe);
                        low_neg_date = Some(date.clone());
                    }
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
    if low_pe == f64::MAX {
        return Err("no rows with valid pe found".into());
    }

    let round4 = |v: f64| (v * 10000.0).round() / 10000.0;

    Ok(Stats {
        ticker: ticker.to_string(),
        start_date,
        end_date,
        p_e_high_neg:      high_neg_pe  .map(round4),
        p_e_high_neg_date: high_neg_date,
        p_e_low_neg:       low_neg_pe   .map(round4),
        p_e_low_neg_date:  low_neg_date,
        p_e_low:           round4(low_pe),
        p_e_low_date:      low_date,
        p_e_high:          round4(high_pe),
        p_e_high_date:     high_date,
        p_e_last:          round4(last_pe),
        price_last:        last_price,
        eps_last:          last_eps,
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
    fn low_and_high_pe_are_correct() {
        let s = compute_stats(CUT, "TEST").unwrap();
        // pe values: 100.0, 21.0, 60.0, 62.5
        assert_eq!(s.p_e_low,       21.0);
        assert_eq!(s.p_e_low_date,  "2022-06-15");
        assert_eq!(s.p_e_high,      100.0);
        assert_eq!(s.p_e_high_date, "2021-01-04");
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
        assert_eq!(s.p_e_low,  21.0);
        assert_eq!(s.p_e_high, 21.0);
    }

    #[test]
    fn zero_eps_rows_are_excluded_from_pe() {
        let cut = "\
date,open,high,low,close,volume,ttm_net_eps,pe_ratio
2021-01-04,100,110,90,200.00,1000,0.00,0.00
2022-06-15,105,115,95,105.00,2000,5.00,21.00";
        let s = compute_stats(cut, "TEST").unwrap();
        assert_eq!(s.p_e_low,  21.0);
        assert_eq!(s.p_e_high, 21.0);
    }

    #[test]
    fn negative_pe_rows_populate_neg_band() {
        let cut = "\
date,open,high,low,close,volume,ttm_net_eps,pe_ratio
2021-01-04,100,110,90,200.00,1000,-2.00,
2021-06-01,100,110,90,100.00,1000,-10.00,
2022-06-15,105,115,95,105.00,2000,5.00,21.00";
        let s = compute_stats(cut, "TEST").unwrap();
        assert_eq!(s.p_e_low,  21.0);
        assert_eq!(s.p_e_high, 21.0);
        // high_neg = closest to zero = 100/-10 = -10.0
        assert_eq!(s.p_e_high_neg, Some(-10.0));
        assert_eq!(s.p_e_high_neg_date.as_deref(), Some("2021-06-01"));
        // low_neg = most negative = 200/-2 = -100.0
        assert_eq!(s.p_e_low_neg, Some(-100.0));
        assert_eq!(s.p_e_low_neg_date.as_deref(), Some("2021-01-04"));
    }

    #[test]
    fn no_negative_pe_rows_gives_none_neg_band() {
        let s = compute_stats(CUT, "TEST").unwrap();
        assert_eq!(s.p_e_high_neg, None);
        assert_eq!(s.p_e_low_neg,  None);
    }

    #[test]
    fn ticker_name_is_stored() {
        let s = compute_stats(CUT, "NVDA").unwrap();
        assert_eq!(s.ticker, "NVDA");
    }
}
