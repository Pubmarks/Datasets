use std::error::Error;
use std::io::Cursor;

const EPS_COLUMNS:   &[&str] = &["date", "stock_price", "ttm_net_eps", "pe_ratio"];
const OHLCV_COLUMNS: &[&str] = &["date", "open", "high", "low", "close", "volume"];

fn check_columns(csv: &str, expected: &[&str], label: &str) -> Result<(), Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(csv));
    let headers = reader.headers()?.clone();
    for col in expected {
        if !headers.iter().any(|h| h == *col) {
            return Err(format!("{label}: missing column '{col}'").into());
        }
    }
    Ok(())
}

pub fn validate(eps: &str, ohlcv: &str) -> Result<(), Box<dyn Error>> {
    check_columns(eps,   EPS_COLUMNS,   "eps")?;
    check_columns(ohlcv, OHLCV_COLUMNS, "ohlcv")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OHLCV: &str = "\
date,open,high,low,close,volume
2024-01-01,100,110,90,100.00,1000
2024-01-02,100,110,90,101.00,1000";

    const EPS: &str = "\
date,stock_price,ttm_net_eps,pe_ratio
2024-01-01,100,2.50,40.00
2024-01-02,101,2.51,40.24";

    #[test]
    fn valid_data_passes() {
        assert!(validate(EPS, OHLCV).is_ok());
    }

    #[test]
    fn missing_eps_column_fails() {
        let bad = "date,stock_price,pe_ratio\n2024-01-01,100,40.00";
        assert!(validate(bad, OHLCV).is_err());
    }

    #[test]
    fn missing_ohlcv_column_fails() {
        let bad = "date,open,high,low,volume\n2024-01-01,100,110,90,1000";
        assert!(validate(EPS, bad).is_err());
    }
}
