import os
import random
import subprocess
from pathlib import Path


def get_file_size(path):
    return os.path.getsize(path)


def run_generate(script_path, sf):
    script_path = Path(script_path).resolve()
    workdir = script_path.parent.parent
    db_path = Path(workdir) / "data" / "warehouse.duckdb"
    try:
        subprocess.run(
            ["python3", str(script_path), str(sf)],
            cwd=workdir,
            env=os.environ.copy()
            | {"PYTHONPATH": f"{os.environ.get('PYTHONPATH', '')}:{os.getcwd()}"},
            check=True,
        )
        return get_file_size(db_path)
    finally:
        db_path.unlink(missing_ok=True)


def print_results_table(results):
    headers = ("Project", "Correct SF")
    rows = [(project, "None" if sf is None else str(sf)) for project, sf in results]
    project_width = max(len(headers[0]), *(len(project) for project, _ in rows))
    sf_width = max(len(headers[1]), *(len(sf) for _, sf in rows))
    separator = f"+-{'-' * project_width}-+-{'-' * sf_width}-+"

    print()
    print(separator)
    print(f"| {headers[0]:<{project_width}} | {headers[1]:<{sf_width}} |")
    print(separator)
    for project, sf in rows:
        print(f"| {project:<{project_width}} | {sf:<{sf_width}} |")
    print(separator)


def find_correct_sf(script_path):
    sf = 1
    target_size = 1024 * 1024 * 1024  # 1GB
    tolerance = 0.1 * target_size
    rng = random.Random(str(script_path))
    seen_sfs = set()
    previous_sf = None
    best_sf = sf
    best_error = float("inf")
    max_attempts = 20

    for _ in range(max_attempts):
        try:
            size = run_generate(script_path, sf)
            error = abs(size - target_size)
            if error < best_error:
                best_error = error
                best_sf = sf

            if error <= tolerance:
                return sf

            # Simple linear adjustment based on current size
            # sf_new = sf * (target / current)
            next_sf = max(1, round(sf * (target_size / size)))

            # Break oscillation if the search stalls or repeats prior guesses.
            if next_sf == sf or next_sf == previous_sf or next_sf in seen_sfs:
                jitter = max(1, int(next_sf * rng.uniform(0.05, 0.2)))
                if size < target_size:
                    next_sf += jitter
                else:
                    next_sf = max(1, next_sf - jitter)

            seen_sfs.add(sf)
            previous_sf = sf
            sf = next_sf
        except Exception as e:
            print(f"Error running {script_path} with sf={sf}: {e}")
            return None

    print(f"Reached max attempts for {script_path}; returning best sf={best_sf}")
    return best_sf


def main():
    scripts = sorted(Path("dbgen").glob("*.py"))
    scripts = [str(script) for script in scripts if script.name not in {"__init__.py", "project_runner.py"}]

    results = []
    for script in scripts:
        project_name = Path(script).stem
        print(f"Processing {project_name}...")
        sf = find_correct_sf(script)
        results.append((project_name, sf))

    print_results_table(results)


if __name__ == "__main__":
    main()
