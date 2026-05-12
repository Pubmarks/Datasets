use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::Cursor;

pub fn combine_ohlcv_eps(ohlcv: &str, eps: &str) -> Result<String, Box<dyn Error>> {
    // parse ohlcv
    let mut ohlcv_reader = csv::Reader::from_reader(Cursor::new(ohlcv));
    let ohlcv_headers = ohlcv_reader.headers()?.clone();
    let ohlcv_col_count = ohlcv_headers.len();

    let mut ohlcv_by_date: HashMap<String, csv::StringRecord> = HashMap::new();
    for result in ohlcv_reader.records() {
        let record = result?;
        if let Some(date) = record.get(0) {
            ohlcv_by_date.insert(date.to_string(), record);
        }
    }

    // parse eps -> date: ttm_net_eps
    let mut eps_reader = csv::Reader::from_reader(Cursor::new(eps));
    let eps_headers = eps_reader.headers()?.clone();
    let ie = eps_headers.iter().position(|c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;

    let mut eps_by_date: HashMap<String, String> = HashMap::new();
    for result in eps_reader.records() {
        let record = result?;
        if let (Some(date), Some(val)) = (record.get(0), record.get(ie)) {
            eps_by_date.insert(date.to_string(), val.to_string());
        }
    }

    // full outer join sorted by date
    let all_dates: BTreeSet<String> = ohlcv_by_date.keys().chain(eps_by_date.keys()).cloned().collect();

    let mut out = Vec::new();
    let mut writer = csv::Writer::from_writer(&mut out);

    let mut combined_headers: Vec<String> = ohlcv_headers.iter().map(str::to_string).collect();
    combined_headers.push("ttm_net_eps".to_string());
    writer.write_record(&combined_headers)?;

    let empty_ohlcv: Vec<String> = (0..ohlcv_col_count).map(|_| String::new()).collect();

    for date in &all_dates {
        let eps_val = eps_by_date.get(date).map(String::as_str).unwrap_or("");
        let ohlcv_row = ohlcv_by_date.get(date);

        let mut row: Vec<String> = match ohlcv_row {
            Some(r) => r.iter().map(str::to_string).collect(),
            None => {
                let mut r = empty_ohlcv.clone();
                r[0] = date.clone();
                r
            }
        };
        row.push(eps_val.to_string());
        writer.write_record(&row)?;
    }

    writer.flush()?;
    drop(writer);
    Ok(String::from_utf8(out)?)
}
