import os
import subprocess
import glob
from pathlib import Path


def get_file_size(path):
    return os.path.getsize(path)


def run_generate(script_path, sf):
    workdir = os.path.dirname(script_path)
    subprocess.run(
        ["python3", "generate_data.py", str(sf)],
        cwd=workdir,
        env=os.environ.copy()
        | {"PYTHONPATH": f"{os.environ.get('PYTHONPATH', '')}:{os.getcwd()}"},
        check=True,
    )
    db_path = os.path.join(workdir, "data/warehouse.duckdb")
    return get_file_size(db_path)


def find_correct_sf(script_path):
    sf = 1
    target_size = 1024 * 1024 * 1024  # 1GB
    tolerance = 0.1 * target_size

    while True:
        try:
            size = run_generate(script_path, sf)
            if abs(size - target_size) <= tolerance:
                return sf

            # Simple linear adjustment based on current size
            # sf_new = sf * (target / current)
            sf = round(sf * (target_size / size))
            if sf == 0:
                sf = 1
        except Exception as e:
            print(f"Error running {script_path} with sf={sf}: {e}")
            return None


def main():
    scripts = glob.glob("projects/synth/**/generate_data.py", recursive=True)
    scripts = [s for s in scripts if s != "projects/synth/generate_data.py"]

    results = []
    for script in scripts:
        project_name = Path(script).parent.name
        print(f"Processing {project_name}...")
        sf = find_correct_sf(script)
        results.append((project_name, sf))

    print("\n{:<<330} {:<<110}".format("Project", "Correct SF"))
    print("-" * 40)
    for project, sf in results:
        print("{:<<330} {:<<110}".format(project, sf))


if __name__ == "__main__":
    main()
