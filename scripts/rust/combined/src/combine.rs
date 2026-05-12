use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::Cursor;

fn parse_ohlcv(ohlcv: &str) -> Result<(csv::StringRecord, HashMap<String, csv::StringRecord>), Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(ohlcv));
    let headers = reader.headers()?.clone();
    let mut by_date: HashMap<String, csv::StringRecord> = HashMap::new();
    for result in reader.records() {
        let record = result?;
        if let Some(date) = record.get(0) {
            by_date.insert(date.to_string(), record);
        }
    }
    Ok((headers, by_date))
}

fn parse_eps_lookup(eps: &str) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(eps));
    let headers = reader.headers()?.clone();
    let ie = headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;
    let mut by_date: HashMap<String, String> = HashMap::new();
    for result in reader.records() {
        let record = result?;
        if let (Some(date), Some(val)) = (record.get(0), record.get(ie)) {
            by_date.insert(date.to_string(), val.to_string());
        }
    }
    Ok(by_date)
}

pub fn combine_ohlcv_eps(ohlcv: &str, eps: &str) -> Result<String, Box<dyn Error>> {
    let (ohlcv_headers, ohlcv_by_date) = parse_ohlcv(ohlcv)?;
    let eps_by_date = parse_eps_lookup(eps)?;

    let all_dates: BTreeSet<String> = ohlcv_by_date.keys().chain(eps_by_date.keys()).cloned().collect();
    let ohlcv_col_count = ohlcv_headers.len();
    let empty_ohlcv: Vec<String> = (0..ohlcv_col_count).map(|_| String::new()).collect();

    let mut out = Vec::new();
    let mut writer = csv::Writer::from_writer(&mut out);

    let mut combined_headers: Vec<String> = ohlcv_headers.iter().map(str::to_string).collect();
    combined_headers.push("ttm_net_eps".to_string());
    writer.write_record(&combined_headers)?;

    for date in &all_dates {
        let eps_val = eps_by_date.get(date).map(String::as_str).unwrap_or("");
        let mut row: Vec<String> = match ohlcv_by_date.get(date) {
            Some(r) => r.iter().map(str::to_string).collect(),
            None => { let mut r = empty_ohlcv.clone(); r[0] = date.clone(); r }
        };
        row.push(eps_val.to_string());
        writer.write_record(&row)?;
    }

    writer.flush()?;
    drop(writer);
    Ok(String::from_utf8(out)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OHLCV: &str = "\
date,open,high,low,close,volume
2024-03-31,100,110,90,105,1000
2024-06-30,105,115,95,110,2000
2024-09-30,110,120,100,115,3000";

    const EPS: &str = "\
date,stock_price,ttm_net_eps,pe_ratio
2024-03-31,105,2.50,42.00
2024-06-30,110,2.75,40.00
2024-12-31,120,3.00,40.00";

    #[test]
    fn matching_dates_carry_eps() {
        let out = combine_ohlcv_eps(OHLCV, EPS).unwrap();
        let rows: Vec<&str> = out.trim().lines().collect();
        assert!(rows[1].ends_with(",2.50"), "mar row: {}", rows[1]);
        assert!(rows[2].ends_with(",2.75"), "jun row: {}", rows[2]);
    }

    #[test]
    fn ohlcv_only_date_has_empty_eps() {
        let out = combine_ohlcv_eps(OHLCV, EPS).unwrap();
        let sep_row = out.trim().lines().find(|l| l.starts_with("2024-09-30")).unwrap();
        assert!(sep_row.ends_with(","), "expected empty eps: {sep_row}");
    }

    #[test]
    fn eps_only_date_has_empty_ohlcv() {
        let out = combine_ohlcv_eps(OHLCV, EPS).unwrap();
        let dec_row = out.trim().lines().find(|l| l.starts_with("2024-12-31")).unwrap();
        assert_eq!(dec_row, "2024-12-31,,,,,,3.00");
    }

    #[test]
    fn output_is_sorted_by_date() {
        let out = combine_ohlcv_eps(OHLCV, EPS).unwrap();
        let dates: Vec<&str> = out.trim().lines().skip(1).map(|l| &l[..10]).collect();
        let mut sorted = dates.clone();
        sorted.sort();
        assert_eq!(dates, sorted);
    }

    #[test]
    fn errors_on_missing_ttm_net_eps_column() {
        let bad_eps = "date,stock_price,pe_ratio\n2024-03-31,100,40";
        assert!(combine_ohlcv_eps(OHLCV, bad_eps).is_err());
    }
}
