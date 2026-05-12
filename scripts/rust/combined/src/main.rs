use std::env;
use std::error::Error;
use std::fs;

fn fill_missing_eps(input: &str) -> Result<String, Box<dyn Error>> {
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

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args().nth(1).ok_or("Usage: combined <eps_file.csv>")?;
    let input = fs::read_to_string(&path)?;
    print!("{}", fill_missing_eps(&input)?);
    Ok(())
}
