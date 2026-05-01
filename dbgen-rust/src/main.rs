use clap::Parser;
use duckdb::Connection;
use std::path::PathBuf;

mod p01_ecommerce;
mod p02_fraud;
mod p03_iot;
mod p04_hr;
mod p05_logistics;
mod p06_saas;
mod p07_healthcare;
mod p08_adtech;
mod p09_gaming;
mod p10_energy;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Project to run (1-10)
    #[arg(short, long)]
    project: u32,

    /// Scale factor
    #[arg(short, long, default_value_t = 1.0)]
    sf: f64,

    /// Output database file
    #[arg(short, long, default_value = "data/warehouse.duckdb")]
    output: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Some(parent) = cli.output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut con = Connection::open(&cli.output)?;

    match cli.project {
        1 => p01_ecommerce::run(cli.sf, &mut con)?,
        2 => p02_fraud::run(cli.sf, &mut con)?,
        3 => p03_iot::run(cli.sf, &mut con)?,
        4 => p04_hr::run(cli.sf, &mut con)?,
        5 => p05_logistics::run(cli.sf, &mut con)?,
        6 => p06_saas::run(cli.sf, &mut con)?,
        7 => p07_healthcare::run(cli.sf, &mut con)?,
        8 => p08_adtech::run(cli.sf, &mut con)?,
        9 => p09_gaming::run(cli.sf, &mut con)?,
        10 => p10_energy::run(cli.sf, &mut con)?,
        _ => {
            eprintln!("Project p{:02} not implemented", cli.project);
            std::process::exit(1);
        }
    }

    Ok(())
}
