##!/usr/bin/env python3
"""
Compatibility test script that recursively tests all dbt projects in projects/
against Postgres to ensure they are dialect-compatible.
"""
import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from enum import Enum


def _log_error_to_file(project_dir: Path, error_msg: str) -> None:
    """Log an error message to a file with project name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_log_path = Path("errors") / f"compatibility_errors_{timestamp}.txt"
    error_log_path.parent.mkdir(parents=True, exist_ok=True)

    with open(error_log_path, "a") as f:
        f.write(f"\n" + "=" * 60 + "\n")
        f.write(f"Project: {project_dir}\n")
        f.write(f"Status:\n{error_msg}")


class Status(Enum):
    SUCCESS = "SUCCESS"
    DATA_GEN_FAILED = "Data Gen Failed"
    LOAD_PG_FAILED = "Load PG Failed"
    DBT_FAILED = "dbt Run Failed"
    SKIPPED = "Skipped"


@dataclass
class Result:
    project_dir: Path
    status: Status
    error: str = ""


def run_command(cmd: list[str], cwd: Path | None = None, timeout: int = 300) -> tuple[bool, str]:
    """Run a command and return (success, stdout_or_stderr)."""
    print(f"  $ {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        combined = (e.stderr or "") + (e.stdout or "")
        return False, combined
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"


def find_dbt_projects(projects_root: Path, exclude_names: list[str] | None = None) -> list[Path]:
    """Find all dbt_project.yml files recursively, excluding _shared and package dirs."""
    if exclude_names is None:
        exclude_names = []

    found = []
    for p in sorted(projects_root.glob("**/dbt_project.yml")):
        parent = p.parent
        
        # Skip any path containing 'dbt_utils' or 'dbt_packages'
        # These are installed dbt_package code, not actual dbt projects to test
        full_path_str = str(parent)
        if "dbt_utils" in full_path_str.lower() or "dbt_packages" in full_path_str.lower():
            continue
        
        # Skip _shared directory
        if "_shared" in parent.parts:
            continue
        
        # Check if project name matches any exclusion pattern
        include = True
        for exclude_name in exclude_names:
            dir_name = parent.name.lower()
            path_lower = str(parent).lower()
            if exclude_name.lower() in dir_name or exclude_name.lower() in path_lower:
                include = False
                break
        
        if include:
            found.append(parent)
    return found


def test_project(project_dir: Path) -> Result:
    """Test a single dbt project against Postgres."""
    print(f"\n{'='*60}")
    print(f"Testing: {project_dir}")
    print(f"{'='*60}")

    # 1. Generate data
    gen_data_script = project_dir / "generate_data.py"
    duckdb_files = list(project_dir.glob("*.duckdb"))
    if (project_dir / "data").exists():
        duckdb_files.extend((project_dir / "data").glob("*.duckdb"))

    if gen_data_script.exists() and not duckdb_files:
        print("  [1/3] Generating data...")
        success, output = run_command(["python3", "generate_data.py", "0.01"], cwd=project_dir)
        if not success:
            print(f"  ERROR: Data generation failed")
            _log_error_to_file(project_dir, f"[Data Gen Failed] {output}")
            return Result(project_dir, Status.DATA_GEN_FAILED, output)

    # 2. Load data into Postgres
    load_pg_script = project_dir / "load_postgres.py"
    if load_pg_script.exists():
        print("  [2/3] Loading data into Postgres...")
        success, output = run_command(["python3", "load_postgres.py"], cwd=project_dir)
        if not success:
            print(f"  ERROR: Postgres loading failed")
            _log_error_to_file(project_dir, f"[Load PG Failed] {output}")
            return Result(project_dir, Status.LOAD_PG_FAILED, output)
    else:
        print("  [2/3] No load_postgres.py found, skipping")

    # 3. Run dbt with postgres target
    print("  [3/3] Running dbt deps --target postgres...")
    success, output = run_command(["dbt", "deps", "--target", "postgres"], cwd=project_dir)
    if not success:
        error_text = f"[DBT Failed] {output}" if output else "[DBT Failed]"
        print(f"  FAIL: deps failed for {project_dir}")
        _log_error_to_file(project_dir, error_text)
        return Result(project_dir, Status.DBT_FAILED, output)

    print("  [4/4] Running dbt --target=postgres...")
    success, output = run_command(["dbt", "run", "--target", "postgres"], cwd=project_dir)
    if success:
        print(f"  PASS: {project_dir}")
        return Result(project_dir, Status.SUCCESS)
    else:
        error_text = f"[DBT Failed] {output}" if output else "[DBT Failed]"
        print(f"  FAIL: {project_dir}")
        _log_error_to_file(project_dir, error_text)
        return Result(project_dir, Status.DBT_FAILED, output)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Test dbt projects against Postgres for dialect compatibility."
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Project name(s) to exclude from testing (can be specified multiple times, matches project path or directory name)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    projects_root = Path("projects")
    
    if not projects_root.exists():
        print(f"Error: '{projects_root}' directory not found")
        _log_error_to_file(Path("."), f"[Script Error] '{projects_root}' directory not found")
        sys.exit(1)

    dbt_projects = find_dbt_projects(projects_root, args.exclude)
    if not dbt_projects:
        print("No dbt projects found in projects/")
        _log_error_to_file(Path("."), "[Script Error] No dbt projects found in projects/")
        sys.exit(1)

    print(f"Found {len(dbt_projects)} dbt project(s)\n")

    results: list[Result] = []
    for project_dir in dbt_projects:
        result = test_project(project_dir)
        results.append(result)

    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    passed = [r for r in results if r.status == Status.SUCCESS]
    failed = [r for r in results if r.status != Status.SUCCESS]
    skipped = [r for r in results if r.status == Status.SKIPPED]

    for r in results:
        icon = "PASS" if r.status == Status.SUCCESS else "FAIL"
        print(f"  [{icon}] {r.project_dir}")

    print(f"\nTotal: {len(results)} | Passed: {len(passed)} | Failed: {len(failed)} | Skipped: {len(skipped)}")

    if failed:
        print(f"\n{'='*60}")
        print("FAILURES")
        print(f"{'='*60}")
        for r in failed:
            print(f"\n--- {r.project_dir} ({r.status.value}) ---")
            if r.error:
                # Print first 2000 chars of error
                error_display = r.error[:2000]
                if len(r.error) > 2000:
                    error_display += "\n... (truncated)"
                print(error_display)

    # Exit with error code if any failed
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
