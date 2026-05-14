use std::error::Error;
use std::io::Cursor;

pub fn validate(combined: &str) -> Result<(), Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(combined));
    let headers = reader.headers()?.clone();
    for col in ["date", "pe_ratio"] {
        if !headers.iter().any(|h| h == col) {
            return Err(format!("combined: missing column '{col}'").into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMBINED: &str = "\
date,open,high,low,close,volume,ttm_net_eps,pe_ratio
2024-01-01,100,110,90,200.00,1000,4.00,50.00
2024-01-02,100,110,90,150.00,1000,0.00,0.00";

    #[test]
    fn valid_data_passes() {
        assert!(validate(COMBINED).is_ok());
    }

    #[test]
    fn missing_date_column_fails() {
        let bad =
            "open,high,low,close,volume,ttm_net_eps,pe_ratio\n100,110,90,200.00,1000,4.00,50.00";
        assert!(validate(bad).is_err());
    }

    #[test]
    fn missing_pe_ratio_column_fails() {
        let bad =
            "date,open,high,low,close,volume,ttm_net_eps\n2024-01-01,100,110,90,200.00,1000,4.00";
        assert!(validate(bad).is_err());
    }
}
