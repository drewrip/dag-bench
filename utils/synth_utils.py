import atexit
import os
import shutil
import tempfile
from concurrent.futures import as_completed
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq


_TEMP_BATCH_DIRS = set()
_DEFAULT_MAX_WORKERS = 32


class GenerationProgress:
    def __init__(self, project_name, total_steps):
        self.project_name = project_name
        self.total_steps = max(1, total_steps)
        self.current_step = 0

    def advance(self, step_name):
        self.current_step += 1
        filled = int(24 * self.current_step / self.total_steps)
        bar = "#" * filled + "-" * (24 - filled)
        print(
            f"[{self.project_name}] [{bar}] "
            f"{self.current_step}/{self.total_steps} {step_name}",
            flush=True,
        )


def print_generation_summary(project_name: str, sf: float, counts: Mapping[str, int]):
    count_summary = " | ".join(f"{name}={value}" for name, value in counts.items())
    print(f"[{project_name}] complete | sf={sf} | {count_summary}", flush=True)


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

    def iter_column(self, column_index):
        column_name = f"col_{column_index}"
        for parquet_path in self.parquet_paths:
            column = pq.read_table(parquet_path, columns=[column_name])[column_name].to_pylist()
            yield from column

    def iter_rows(self, columns=None):
        column_names = None if columns is None else [f"col_{column}" for column in columns]
        for parquet_path in self.parquet_paths:
            table = pq.read_table(parquet_path, columns=column_names)
            yield from (
                tuple(record[name] for name in table.column_names)
                for record in table.to_pylist()
            )


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


def get_worker_count():
    return max(1, min(os.cpu_count() or 1, _DEFAULT_MAX_WORKERS))


def run_parallel(executor, gen_func, total, *args, chunk_size_override=None):
    worker_count = executor._max_workers
    if chunk_size_override is not None:
        chunk_size = chunk_size_override
    else:
        chunk_size = max(1, total // worker_count)
    temp_dir = tempfile.mkdtemp(prefix="synth-parquet-")
    _TEMP_BATCH_DIRS.add(temp_dir)


    futures = []
    for i in range(0, total, chunk_size):
        start = i + 1
        end = min(i + chunk_size + 1, total + 1)
        parquet_path = os.path.join(temp_dir, f"chunk_{start}_{end - 1}.parquet")
        futures.append(executor.submit(_generate_parquet_chunk, gen_func, parquet_path, start, end, args))

    parquet_paths = []
    for future in as_completed(futures):
        parquet_path = future.result()
        if parquet_path:
            parquet_paths.append(parquet_path)
    parquet_paths.sort()
    return ParquetBackedRows(parquet_paths, temp_dir)
