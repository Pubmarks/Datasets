use std::error::Error;
use std::io::Cursor;

// common split ratios: 2:1, 3:1, 4:1, 5:1 and their reverses (3:2 omitted — too close to large market moves)
const SPLIT_RATIOS: &[f64] = &[0.5, 1.0 / 3.0, 0.25, 0.2, 2.0, 3.0, 4.0, 5.0];
const SPLIT_TOLERANCE: f64 = 0.03;

fn check_no_zero_eps(eps: &str) -> Result<(), Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(eps));
    let headers = reader.headers()?.clone();
    let ie = headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;

    for result in reader.records() {
        let record = result?;
        if let Some(val) = record.get(ie) {
            if let Ok(v) = val.trim().parse::<f64>() {
                if v == 0.0 {
                    let date = record.get(0).unwrap_or("unknown");
                    return Err(format!("zero eps on {date}").into());
                }
            }
        }
    }
    Ok(())
}

fn check_no_splits(ohlcv: &str) -> Result<(), Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(ohlcv));
    let headers = reader.headers()?.clone();
    let ic = headers.iter().position(|c| c == "close").ok_or("missing close")?;

    let mut prev: Option<(String, f64)> = None;
    for result in reader.records() {
        let record = result?;
        let date = record.get(0).unwrap_or("").to_string();
        if let Some(close) = record.get(ic).and_then(|v| v.trim().parse::<f64>().ok()) {
            if let Some((prev_date, prev_close)) = &prev {
                let ratio = close / prev_close;
                if SPLIT_RATIOS.iter().any(|&r| (ratio - r).abs() < SPLIT_TOLERANCE) {
                    return Err(format!(
                        "split detected between {prev_date} ({prev_close}) and {date} ({close})"
                    ).into());
                }
            }
            prev = Some((date, close));
        }
    }
    Ok(())
}

pub fn validate(eps: &str, ohlcv: &str) -> Result<(), Box<dyn Error>> {
    check_no_zero_eps(eps)?;
    check_no_splits(ohlcv)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OHLCV: &str = "\
date,open,high,low,close,volume
2024-01-01,100,110,90,100.00,1000
2024-01-02,100,110,90,101.00,1000
2024-01-03,100,110,90,102.00,1000";

    const EPS: &str = "\
date,stock_price,ttm_net_eps,pe_ratio
2024-01-01,100,2.50,40.00
2024-01-02,101,2.51,40.24";

    #[test]
    fn valid_data_passes() {
        assert!(validate(EPS, OHLCV).is_ok());
    }

    #[test]
    fn zero_eps_fails() {
        let eps = "\
date,stock_price,ttm_net_eps,pe_ratio
2024-01-01,100,2.50,40.00
2024-01-02,101,0.00,0.00";
        assert!(validate(eps, OHLCV).is_err());
    }

    #[test]
    fn negative_eps_passes() {
        let eps = "\
date,stock_price,ttm_net_eps,pe_ratio
2024-01-01,100,-2.50,40.00
2024-01-02,101,-2.51,40.24";
        assert!(validate(eps, OHLCV).is_ok());
    }

    #[test]
    fn split_2_for_1_fails() {
        let ohlcv = "\
date,open,high,low,close,volume
2024-01-01,100,110,90,100.00,1000
2024-01-02,50,55,45,50.00,2000";
        assert!(validate(EPS, ohlcv).is_err());
    }

    #[test]
    fn reverse_split_fails() {
        let ohlcv = "\
date,open,high,low,close,volume
2024-01-01,100,110,90,100.00,1000
2024-01-02,200,220,180,200.00,500";
        assert!(validate(EPS, ohlcv).is_err());
    }

    #[test]
    fn large_but_not_split_move_passes() {
        let ohlcv = "\
date,open,high,low,close,volume
2024-01-01,100,110,90,100.00,1000
2024-01-02,130,140,120,130.00,1000";
        assert!(validate(EPS, ohlcv).is_ok());
    }
}
