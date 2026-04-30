import atexit
import os
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


_TEMP_BATCH_DIRS = set()


def _cleanup_temp_batch_dirs():
    while _TEMP_BATCH_DIRS:
        temp_dir = _TEMP_BATCH_DIRS.pop()
        shutil.rmtree(temp_dir, ignore_errors=True)


atexit.register(_cleanup_temp_batch_dirs)


class ParquetBackedRows:
    def __init__(self, parquet_paths, temp_dir):
        self.parquet_paths = parquet_paths
        self._temp_dir = temp_dir
        self._rows = None

    def __bool__(self):
        return bool(self.parquet_paths)

    def __len__(self):
        return len(self._materialize())

    def __iter__(self):
        return iter(self._materialize())

    def __getitem__(self, index):
        return self._materialize()[index]

    def _materialize(self):
        if self._rows is None:
            rows = []
            for parquet_path in self.parquet_paths:
                table = pq.read_table(parquet_path)
                column_names = table.column_names
                rows.extend(
                    tuple(record[name] for name in column_names)
                    for record in table.to_pylist()
                )
            self._rows = rows
        return self._rows


def _write_rows_to_parquet(rows, parquet_path):
    if not rows:
        return None

    columns = list(zip(*rows))
    arrays = [pa.array(column) for column in columns]
    names = [f"col_{idx}" for idx in range(len(arrays))]
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, parquet_path)
    return parquet_path


def _generate_parquet_chunk(gen_func, parquet_path, start, end, args):
    rows = gen_func(start, end, *args)
    return _write_rows_to_parquet(rows, parquet_path)


def _quote_sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def batched_insert(con, table_name, columns, rows):
    if not rows:
        return
    if isinstance(rows, ParquetBackedRows):
        parquet_glob = os.path.join(rows._temp_dir, "*.parquet")
        con.execute(
            f"COPY {table_name} ({', '.join(columns)}) "
            f"FROM {_quote_sql_string(parquet_glob)} (FORMAT parquet)"
        )
        return

    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")


def run_parallel(executor, gen_func, total, *args):
    cpu_count = executor._max_workers
    chunk_size = max(1, total // cpu_count)
    temp_dir = tempfile.mkdtemp(prefix="synth-parquet-")
    _TEMP_BATCH_DIRS.add(temp_dir)

    futures = []
    for i in range(0, total, chunk_size):
        start = i + 1
        end = min(i + chunk_size + 1, total + 1)
        parquet_path = os.path.join(temp_dir, f"chunk_{start}_{end - 1}.parquet")
        futures.append(executor.submit(_generate_parquet_chunk, gen_func, parquet_path, start, end, args))

    parquet_paths = [parquet_path for parquet_path in (future.result() for future in futures) if parquet_path]
    return ParquetBackedRows(parquet_paths, temp_dir)
