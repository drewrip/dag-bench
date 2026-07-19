use clap::Parser;
use duckdb::arrow::array::{ArrayRef, RecordBatch};
use duckdb::{Config, DuckdbConnectionManager};
use indicatif::ProgressBar;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use r2d2::Pool;
use rayon::prelude::*;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod arrow_gen;
mod common;
mod p01_iot;
mod p02_adtech;
mod p03_ecommerce;
mod p04_fraud;
mod p05_hr;
mod p06_logistics;
mod p07_saas;
mod p08_healthcare;
mod p09_gaming;
mod p10_energy;

pub fn generate_table_parallel<T, F>(
    pool: &Pool<DuckdbConnectionManager>,
    table_name: &str,
    total_rows: usize,
    pb: &ProgressBar,
    msg: &str,
    generator: F,
) -> duckdb::Result<()>
where
    T: crate::arrow_gen::RowBatch + Send,
    F: Fn(usize) -> T + Sync + Send,
{
    const CHUNK_SIZE: usize = 100_000;
    pb.set_message(msg.to_string());
    let n_chunks = (total_rows + CHUNK_SIZE - 1) / CHUNK_SIZE;
    let schema = Arc::new(T::schema());

    let tmp_dir = tempfile::tempdir().expect("couldn't create tmp dir");

    // Each chunk is generated in parallel and written to its own Parquet file, then all
    // chunks are read back and appended in parallel below.
    (0..n_chunks).into_par_iter().try_for_each(|chunk_idx| {
        let chunk_start = chunk_idx * CHUNK_SIZE + 1;
        let chunk_end = (chunk_start + CHUNK_SIZE).min(total_rows + 1);
        // Deliberately sequential within a chunk: parallelism already comes from the
        // outer `into_par_iter()` over chunks. Nesting a second `into_par_iter()` here
        // let rayon's work-stealing scheduler have many more chunks "open" (each holding
        // a fully materialized `Vec<T>`) than there are cores, since a thread stuck
        // helping with one chunk's inner split lets other threads start new outer chunks
        // instead of finishing existing ones - unbounded memory instead of a per-core cap.
        let rows: Vec<T> = (chunk_start..chunk_end).map(&generator).collect();

        let columns = T::to_columns(rows);
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .expect("row batch column/schema mismatch");

        let file = File::create(
            tmp_dir
                .path()
                .join(format!("{}_{}.parquet", table_name, chunk_idx)),
        )
        .expect("couldn't create parquet chunk file");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .expect("couldn't create parquet writer");
        writer.write(&batch).expect("writing batch");
        writer.close().expect("closing parquet writer");
        Ok::<(), duckdb::Error>(())
    })?;

    // Load each Parquet chunk back in parallel and append it through its own pooled
    // connection/appender, instead of one big `COPY ... FROM '*.parquet'` that would
    // otherwise pull every chunk into memory at once to satisfy a single statement.
    (0..n_chunks).into_par_iter().try_for_each(|chunk_idx| {
        let path = tmp_dir
            .path()
            .join(format!("{}_{}.parquet", table_name, chunk_idx));
        let file = File::open(&path).expect("couldn't open parquet chunk file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("couldn't create parquet reader")
            .build()
            .expect("couldn't build parquet reader");

        let con = pool.get().expect("couldn't get connection from pool");
        let mut app = con.appender(table_name)?;
        for batch in reader {
            let batch = batch.expect("couldn't read parquet chunk batch");
            app.append_record_batch(batch)?;
        }
        Ok::<(), duckdb::Error>(())
    })?;

    pb.inc(1);
    Ok(())
}

pub fn generate_table_arrow<F>(
    pool: &Pool<DuckdbConnectionManager>,
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

        let con = pool.get().expect("couldn't get connection from pool");

        let mut app = con.appender(table_name).unwrap();
        app.append_record_batch(batch)
            .expect("couldn't append record batch");
        Ok::<(), duckdb::Error>(())
    })?;

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

    /// Disable primary key / foreign key constraints on generated tables (faster loads)
    #[arg(long, default_value_t = false)]
    no_constraints: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Some(parent) = cli.output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut config = Config::default();
    config = config
        .with("preserve_insertion_order", "false")
        .expect("couldn't turn off preserve_insertion_order");
    let manager = DuckdbConnectionManager::file_with_flags(&cli.output, config)
        .expect("couldn't open connection pool");
    let mut pool = Pool::builder()
        .connection_timeout(Duration::from_hours(72))
        .max_size(32)
        .build(manager)
        .expect("couldn't build pool");

    match cli.project {
        1 => p01_iot::run(cli.sf, &mut pool, cli.no_constraints)?,
        2 => p02_adtech::run(cli.sf, &mut pool, cli.no_constraints)?,
        3 => p03_ecommerce::run(cli.sf, &mut pool, cli.no_constraints)?,
        4 => p04_fraud::run(cli.sf, &mut pool, cli.no_constraints)?,
        5 => p05_hr::run(cli.sf, &mut pool, cli.no_constraints)?,
        6 => p06_logistics::run(cli.sf, &mut pool, cli.no_constraints)?,
        7 => p07_saas::run(cli.sf, &mut pool, cli.no_constraints)?,
        8 => p08_healthcare::run(cli.sf, &mut pool, cli.no_constraints)?,
        9 => p09_gaming::run(cli.sf, &mut pool, cli.no_constraints)?,
        10 => p10_energy::run(cli.sf, &mut pool, cli.no_constraints)?,
        _ => {
            eprintln!("Project p{:02} not implemented", cli.project);
            std::process::exit(1);
        }
    }

    Ok(())
}
