use clap::Parser;
use duckdb::Connection;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::Mutex;

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

pub struct SendAppender<'a>(pub duckdb::Appender<'a>);
unsafe impl<'a> Send for SendAppender<'a> {}

pub fn generate_table_parallel<T, F>(
    con: &Connection,
    table_name: &str,
    total_rows: usize,
    pb: &ProgressBar,
    msg: &str,
    generator: F,
) -> duckdb::Result<()>
where
    T: duckdb::AppenderParams + Send,
    F: Fn(usize) -> T + Sync + Send,
{
    const CHUNK_SIZE: usize = 1_000_000;
    pb.set_message(msg.to_string());
    let n_chunks = (total_rows + CHUNK_SIZE - 1) / CHUNK_SIZE;
    let appender = Mutex::new(SendAppender(con.appender(table_name)?));

    (0..n_chunks).into_par_iter().try_for_each(|chunk_idx| {
        let chunk_start = chunk_idx * CHUNK_SIZE + 1;
        let chunk_end = (chunk_start + CHUNK_SIZE).min(total_rows + 1);
        let rows: Vec<T> = (chunk_start..chunk_end)
            .into_par_iter()
            .map(&generator)
            .collect();

        let mut app = appender.lock().unwrap();
        app.0.append_rows(rows)?;
        Ok::<(), duckdb::Error>(())
    })?;

    pb.inc(1);
    Ok(())
}

pub fn generate_table_sequential<T, F>(
    con: &Connection,
    table_name: &str,
    total_rows: usize,
    pb: &ProgressBar,
    msg: &str,
    generator: F,
) -> duckdb::Result<()>
where
    T: duckdb::AppenderParams,
    F: Fn(usize) -> T,
{
    pb.set_message(msg.to_string());
    let mut appender = con.appender(table_name)?;
    let mut rows = Vec::with_capacity(total_rows);
    for i in 1..=total_rows {
        rows.push(generator(i));
    }
    appender.append_rows(rows)?;
    pb.inc(1);
    Ok(())
}

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
