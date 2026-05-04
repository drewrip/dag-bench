import os
import sys
import subprocess
import shutil
from pathlib import Path


def run_project(project_name: str) -> None:
    # 1. Extract project number from project_name (e.g. "p01_ecommerce" -> 1)
    try:
        project_num_str = project_name.split("_")[0][1:]
        project_num = int(project_num_str)
    except (IndexError, ValueError):
        print(f"Error: Could not extract project number from {project_name}")
        sys.exit(1)

    # 2. Get scale factor from sys.argv if provided to the calling script
    sf = 1.0
    if len(sys.argv) > 1:
        try:
            sf = float(sys.argv[1])
        except ValueError:
            pass  # Use default

    # 3. Check cache
    cache_dir = Path(__file__).parent / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    sf_str = str(sf).replace(".", "_")
    cache_filename = f"p{project_num}_sf{sf_str}.duckdb"
    cache_path = cache_dir / cache_filename
    output_path = "data/warehouse.duckdb"

    if cache_path.exists():
        print(f"Using cached duckdb file: {cache_path}")
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cache_path, output_path)
        return

    # 4. Get Rust binary path from environment
    dbgen_bin = os.environ.get("DBGEN")
    if not dbgen_bin:
        print(
            "Error: DBGEN environment variable not set. It should point to the compiled Rust binary."
        )
        sys.exit(1)

    # 5. Prepare arguments for the Rust binary
    # The output should be in 'data/warehouse.duckdb' relative to current working directory
    cmd = [dbgen_bin, "-p", str(project_num), "-s", str(sf), "-o", output_path]

    print(f"Running Rust dbgen: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        # 6. Cache the result
        shutil.copy2(output_path, cache_path)
        print(f"Cached generated duckdb file to: {cache_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error running dbgen: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: Rust binary '{dbgen_bin}' not found at specified path.")
        sys.exit(1)
