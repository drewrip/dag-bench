use clap::Parser;
use duckdb::arrow::array::{ArrayRef, RecordBatch};
use duckdb::{params, Config, DuckdbConnectionManager};
use indicatif::ProgressBar;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use r2d2::Pool;
use rayon::prelude::*;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
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
    // NOTE: a write-Parquet-then-`COPY` variant was tried here (bigger chunks, no
    // cross-thread DB coordination while writing) and looked ~2.7x faster in an isolated
    // benchmark - but that benchmark didn't declare a PRIMARY KEY on the target table. Once
    // measured against our actual schemas (which all declare one upfront), the constraint
    // dominates either way and the Parquet path came out slightly slower end-to-end (~22.4s
    // vs ~20.4s on p03 at sf=1). Keep the direct Arrow-appender path below; don't switch
    // back without re-measuring against a PK-constrained table.
    const CHUNK_SIZE: usize = 1_000_000;
    pb.set_message(msg.to_string());
    let n_chunks = (total_rows + CHUNK_SIZE - 1) / CHUNK_SIZE;
    let schema = Arc::new(T::schema());

    // Each chunk grabs its own pooled connection/appender so appends run concurrently
    // instead of funneling through one shared, serialized Appender (DuckDB's row/vector
    // append path is cheap per-chunk but `Appender` itself is `!Sync`).
    (0..n_chunks).into_par_iter().try_for_each(|chunk_idx| {
        let chunk_start = chunk_idx * CHUNK_SIZE + 1;
        let chunk_end = (chunk_start + CHUNK_SIZE).min(total_rows + 1);
        let rows: Vec<T> = (chunk_start..chunk_end)
            .into_par_iter()
            .map(&generator)
            .collect();

        let columns = T::to_columns(rows);
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .expect("row batch column/schema mismatch");

        let con = pool.get().expect("couldn't get connection from pool");
        let mut app = con.appender(table_name)?;
        app.append_record_batch(batch)?;
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

pub fn generate_table<F>(
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
    const CHUNK_SIZE: usize = 10_000;
    pb.set_message(msg.to_string());
    let n_chunks = (total_rows + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // Create temp directory
    let tmp_dir = Path::new("tmp_data/");
    fs::create_dir(tmp_dir).expect("couldn't create tmp dir");

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

        let file: File =
            File::create(tmp_dir.join(format!("{}_{}.parquet", table_name, chunk_idx))).unwrap();

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
    let con = pool.get().expect("couldn't get connection from pool");
    con.execute(
        &format!(
            "COPY {} FROM '{}' (FORMAT parquet)",
            table_name,
            tmp_dir.join("*").to_str().unwrap()
        ),
        params![],
    )
    .unwrap();

    fs::remove_dir_all(tmp_dir).expect("couldn't delete tmp dir");
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
        1 => p01_iot::run(cli.sf, &mut pool)?,
        2 => p02_adtech::run(cli.sf, &mut pool)?,
        3 => p03_ecommerce::run(cli.sf, &mut pool)?,
        4 => p04_fraud::run(cli.sf, &mut pool)?,
        5 => p05_hr::run(cli.sf, &mut pool)?,
        6 => p06_logistics::run(cli.sf, &mut pool)?,
        7 => p07_saas::run(cli.sf, &mut pool)?,
        8 => p08_healthcare::run(cli.sf, &mut pool)?,
        9 => p09_gaming::run(cli.sf, &mut pool)?,
        10 => p10_energy::run(cli.sf, &mut pool)?,
        _ => {
            eprintln!("Project p{:02} not implemented", cli.project);
            std::process::exit(1);
        }
    }

    Ok(())
}
