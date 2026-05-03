use clap::Parser;
use duckdb::arrow::array::{ArrayRef, RecordBatch};
use duckdb::{params, Connection};
use indicatif::ProgressBar;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Mutex;
use tempfile::env::temp_dir;
use tempfile::{tempdir, tempfile};

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

pub fn generate_table<F>(
    con: &Connection,
    table_name: &str,
    total_rows: usize,
    pb: &ProgressBar,
    msg: &str,
    generator: F,
) -> duckdb::Result<()>
where
    F: Fn(usize, usize) -> Vec<ArrayRef> + Sync,
{
    const CHUNK_SIZE: usize = 1_000_000;
    pb.set_message(msg.to_string());
    let n_chunks = (total_rows + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // Create temp directory
    let tmp_dir = tempdir().unwrap();

    (0..n_chunks).into_par_iter().try_for_each(|chunk_idx| {
        let chunk_start = chunk_idx * CHUNK_SIZE + 1;
        let chunk_end = (chunk_start + CHUNK_SIZE).min(total_rows + 1);

        // `generator` should generate chunk_end - chunk_start rows, but in columnar form.
        let arrays: Vec<ArrayRef> = generator(chunk_start, chunk_end);

        let batch = RecordBatch::try_from_iter(
            arrays
                .into_iter()
                .enumerate()
                .map(|(i, a)| (format!("c{}", i), a)),
        )
        .unwrap();

        let file: File = File::create(
            tmp_dir
                .path()
                .join(format!("{}_{}.parquet", table_name, chunk_idx)),
        )
        .unwrap();

        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();
        Ok::<(), duckdb::Error>(())
    })?;

    // Connect to duckdb and `COPY` all Parquet files in the temporary directory into `table_name`
    con.execute(
        &format!(
            "COPY {} FROM '{}'",
            table_name,
            tmp_dir.path().to_str().unwrap()
        ),
        params![],
    )
    .unwrap();

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
