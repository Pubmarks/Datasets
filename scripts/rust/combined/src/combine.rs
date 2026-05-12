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

pub fn interpolate_eps(combined: &str) -> Result<String, Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(combined));
    let headers = reader.headers()?.clone();
    let ie = headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;

    let mut rows: Vec<Vec<String>> = reader.records()
        .map(|r| r.map(|rec| rec.iter().map(str::to_string).collect()))
        .collect::<Result<_, _>>()?;

    let mut i = 0;
    while i < rows.len() {
        if rows[i][ie].is_empty() {
            // find the last known value before this gap
            let prev = (0..i).rev().find(|&j| !rows[j][ie].is_empty());
            // find the next known value after this gap
            let next = (i + 1..rows.len()).find(|&j| !rows[j][ie].is_empty());

            match (prev, next) {
                (Some(p), Some(j)) => {
                    // interpolate between prev and next
                    let v0: f64 = rows[p][ie].parse()?;
                    let v1: f64 = rows[j][ie].parse()?;
                    let steps = (j - p) as f64;
                    for k in (p + 1)..j {
                        let t = (k - p) as f64 / steps;
                        rows[k][ie] = format!("{:.2}", v0 + t * (v1 - v0));
                    }
                    i = j;
                }
                (Some(p), None) => {
                    // trailing gap — forward-fill from last known
                    let last = rows[p][ie].clone();
                    for k in i..rows.len() {
                        rows[k][ie] = last.clone();
                    }
                    break;
                }
                // leading gap with no prior value — leave empty
                _ => { i += 1; }
            }
        } else {
            i += 1;
        }
    }

    let mut out = Vec::new();
    let mut writer = csv::Writer::from_writer(&mut out);
    writer.write_record(&headers)?;
    for row in &rows { writer.write_record(row)?; }
    writer.flush()?;
    drop(writer);
    Ok(String::from_utf8(out)?)
}

pub fn add_pe_ratio(combined: &str) -> Result<String, Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(combined));
    let headers = reader.headers()?.clone();

    let i_close = headers.iter().position(|c| c == "close").ok_or("missing close")?;
    let i_eps   = headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;

    let mut out = Vec::new();
    let mut writer = csv::Writer::from_writer(&mut out);

    let mut new_headers: Vec<String> = headers.iter().map(str::to_string).collect();
    new_headers.push("pe_ratio".to_string());
    writer.write_record(&new_headers)?;

    for result in reader.records() {
        let record = result?;
        let pe = match (
            record.get(i_close).and_then(|v| v.parse::<f64>().ok()),
            record.get(i_eps).and_then(|v| v.parse::<f64>().ok()),
        ) {
            (Some(close), Some(eps)) if eps != 0.0 => format!("{:.2}", close / eps),
            (_, Some(_)) => "0.00".to_string(),
            _ => String::new(),
        };
        let mut row: Vec<String> = record.iter().map(str::to_string).collect();
        row.push(pe);
        writer.write_record(&row)?;
    }

    writer.flush()?;
    drop(writer);
    Ok(String::from_utf8(out)?)
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

pub fn shift_above_eps(combined: &str) -> Result<String, Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(combined));
    let headers = reader.headers()?.clone();
    let ie = headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;

    let mut rows: Vec<Vec<String>> = reader.records()
        .map(|r| r.map(|rec| rec.iter().map(str::to_string).collect()))
        .collect::<Result<_, _>>()?;

    let ohlcv_missing = |row: &Vec<String>| row[1..row.len() - 1].iter().all(|v| v.is_empty());

    let mut i = 0;
    while i < rows.len() {
        if ohlcv_missing(&rows[i]) && !rows[i][ie].is_empty() {
            // shift eps to the nearest previous row that has ohlcv data
            if let Some(prev) = (0..i).rev().find(|&j| !ohlcv_missing(&rows[j])) {
                if rows[prev][ie].is_empty() {
                    let eps = rows[i][ie].clone();
                    rows[prev][ie] = eps;
                }
            }
            rows.remove(i);
        } else {
            i += 1;
        }
    }

    let mut out = Vec::new();
    let mut writer = csv::Writer::from_writer(&mut out);
    writer.write_record(&headers)?;
    for row in &rows { writer.write_record(row)?; }
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
    fn interpolate_fills_gap_linearly() {
        let combined = "\
date,open,high,low,close,volume,ttm_net_eps
2024-01-01,100,110,90,105,1000,3.00
2024-01-02,100,110,90,105,1000,
2024-01-03,100,110,90,105,1000,4.00";

        let out = interpolate_eps(combined).unwrap();
        let mid = out.trim().lines().find(|l| l.starts_with("2024-01-02")).unwrap();
        assert!(mid.ends_with(",3.50"), "expected 3.50, got: {mid}");
    }

    #[test]
    fn interpolate_handles_decreasing_eps() {
        let combined = "\
date,open,high,low,close,volume,ttm_net_eps
2024-01-01,100,110,90,105,1000,4.00
2024-01-02,100,110,90,105,1000,
2024-01-03,100,110,90,105,1000,2.00";

        let out = interpolate_eps(combined).unwrap();
        let mid = out.trim().lines().find(|l| l.starts_with("2024-01-02")).unwrap();
        assert!(mid.ends_with(",3.00"), "expected 3.00, got: {mid}");
    }

    #[test]
    fn interpolate_forward_fills_trailing_gap() {
        let combined = "\
date,open,high,low,close,volume,ttm_net_eps
2024-01-01,100,110,90,105,1000,3.00
2024-01-02,100,110,90,105,1000,
2024-01-03,100,110,90,105,1000,";

        let out = interpolate_eps(combined).unwrap();
        for line in out.trim().lines().skip(1) {
            assert!(line.ends_with(",3.00"), "expected 3.00, got: {line}");
        }
    }

    #[test]
    fn pe_ratio_is_calculated() {
        let combined = "\
date,open,high,low,close,volume,ttm_net_eps
2024-01-01,100,110,90,200.00,1000,4.00
2024-01-02,100,110,90,150.00,1000,0.00
2024-01-03,100,110,90,150.00,1000,";

        let out = add_pe_ratio(combined).unwrap();
        let rows: Vec<&str> = out.trim().lines().collect();
        assert!(rows[1].ends_with(",50.00"), "expected 50.00: {}", rows[1]);
        assert!(rows[2].ends_with(",0.00"), "expected 0.00 for zero eps: {}", rows[2]);
        assert!(rows[3].ends_with(","), "expected empty pe for missing eps: {}", rows[3]);
    }

    #[test]
    fn errors_on_missing_ttm_net_eps_column() {
        let bad_eps = "date,stock_price,pe_ratio\n2024-03-31,100,40";
        assert!(combine_ohlcv_eps(OHLCV, bad_eps).is_err());
    }

    #[test]
    fn shift_above_eps_moves_eps_to_prev_row_and_drops_eps_only_row() {
        let combined = "\
date,open,high,low,close,volume,ttm_net_eps
2024-06-28,105,115,95,110,2000,
2024-06-30,,,,,,3.00";

        let out = shift_above_eps(combined).unwrap();
        let rows: Vec<&str> = out.trim().lines().collect();
        assert_eq!(rows.len(), 2, "eps-only row should be dropped");
        assert_eq!(rows[1], "2024-06-28,105,115,95,110,2000,3.00");
    }

    #[test]
    fn shift_above_eps_does_not_overwrite_existing_eps() {
        let combined = "\
date,open,high,low,close,volume,ttm_net_eps
2024-06-28,105,115,95,110,2000,2.75
2024-06-30,,,,,,3.00";

        let out = shift_above_eps(combined).unwrap();
        let rows: Vec<&str> = out.trim().lines().collect();
        assert_eq!(rows.len(), 2, "eps-only row should be dropped");
        assert_eq!(rows[1], "2024-06-28,105,115,95,110,2000,2.75", "existing eps should not be overwritten");
    }

    #[test]
    fn shift_above_eps_drops_eps_only_row_with_no_prior_ohlcv() {
        let combined = "\
date,open,high,low,close,volume,ttm_net_eps
2024-01-01,,,,,,1.00
2024-03-31,100,110,90,105,1000,";

        let out = shift_above_eps(combined).unwrap();
        let rows: Vec<&str> = out.trim().lines().collect();
        assert_eq!(rows.len(), 2, "eps-only row with no prior ohlcv should be dropped");
        assert_eq!(rows[1], "2024-03-31,100,110,90,105,1000,");
    }
}
