import os
import sys
import subprocess
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

    # 3. Get Rust binary path from environment
    dbgen_bin = os.environ.get("DBGEN")
    if not dbgen_bin:
        print(
            "Error: DBGEN environment variable not set. It should point to the compiled Rust binary."
        )
        sys.exit(1)

    # 4. Prepare arguments for the Rust binary
    # The output should be in 'data/warehouse.duckdb' relative to current working directory
    output_path = "data/warehouse.duckdb"

    cmd = [dbgen_bin, "-p", str(project_num), "-s", str(sf), "-o", output_path]

    print(f"Running Rust dbgen: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running dbgen: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: Rust binary '{dbgen_bin}' not found at specified path.")
        sys.exit(1)
