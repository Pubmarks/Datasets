use std::error::Error;

pub fn fill_missing_eps(input: &str) -> Result<String, Box<dyn Error>> {
    let mut lines = input.lines();

    let header = lines.next().ok_or("Empty file")?;
    let cols: Vec<&str> = header.split(',').collect();

    let ip  = cols.iter().position(|&c| c == "stock_price").ok_or("missing stock_price")?;
    let ie  = cols.iter().position(|&c| c == "ttm_net_eps").ok_or("missing ttm_net_eps")?;
    let ipe = cols.iter().position(|&c| c == "pe_ratio").ok_or("missing pe_ratio")?;

    let mut out = format!("{header}\n");

    for line in lines {
        if line.trim().is_empty() { continue; }
        let mut fields: Vec<String> = line.split(',').map(str::to_string).collect();

        if fields.get(ie).map(|v| v.trim().is_empty()).unwrap_or(true) {
            let price: f64 = fields.get(ip).and_then(|v| v.trim().parse().ok())
                .ok_or(format!("missing stock_price on row: {line}"))?;
            let pe: f64 = fields.get(ipe).and_then(|v| v.trim().parse().ok())
                .ok_or(format!("missing pe_ratio on row: {line}"))?;
            fields[ie] = format!("{:.2}", price / pe);
        }

        out.push_str(&fields.join(","));
        out.push('\n');
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_missing_eps() {
        let input = "\
date,stock_price,ttm_net_eps,pe_ratio
2024-03-31,100.00,2.50,40.00
2024-06-30,200.00,,25.00
2024-09-30,150.00,,0.00";

        let output = fill_missing_eps(input).unwrap();
        let rows: Vec<&str> = output.trim().lines().collect();

        assert_eq!(rows[1], "2024-03-31,100.00,2.50,40.00");
        assert_eq!(rows[2], "2024-06-30,200.00,8.00,25.00");
        assert!(rows[3].contains("inf") || rows[3].contains("NaN"));
    }

    #[test]
    fn errors_on_missing_column() {
        let input = "date,stock_price,pe_ratio\n2024-03-31,100.00,40.00";
        assert!(fill_missing_eps(input).is_err());
    }

    #[test]
    fn errors_on_missing_price_when_eps_absent() {
        let input = "\
date,stock_price,ttm_net_eps,pe_ratio
2024-03-31,,, 25.00";
        assert!(fill_missing_eps(input).is_err());
    }
}
