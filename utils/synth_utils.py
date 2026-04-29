import pyarrow as pa
import duckdb
import os
from concurrent.futures import ProcessPoolExecutor

def batched_insert(con, table_name, columns, rows):
    if not rows:
        return
    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")

def run_parallel(executor, gen_func, total, *args):
    cpu_count = executor._max_workers
    chunk_size = max(1, total // cpu_count)
    futures = []
    for i in range(0, total, chunk_size):
        futures.append(executor.submit(gen_func, i + 1, min(i + chunk_size + 1, total + 1), *args))
    rows = []
    for f in futures:
        rows.extend(f.result())
    return rows
