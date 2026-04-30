import argparse
import os
import subprocess
import yaml
import sys
import shutil

def run_command(command, cwd):
    """Executes a shell command in a specific directory."""
    print(f"Executing: {' '.join(command)} in {cwd}")
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Prepare dbt projects and generate data.")
    parser.add_argument("sf", type=str, help="Scale factor for data generation.")
    parser.add_argument("--db", type=str, choices=["duckdb", "postgres"], default="duckdb",
                        help="Target database (default: duckdb).")
    parser.add_argument("--skip-data-gen", action="store_true", help="Skip data generation step.")
    
    args = parser.parse_args()

    # Load projects from projects.yaml
    try:
        with open("projects.yaml", "r") as f:
            projects = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: projects.yaml not found.")
        sys.exit(1)

    for project_path in projects:
        project_path = project_path.strip()
        if not project_path:
            continue
        
        abs_project_path = os.path.abspath(project_path)
        if not os.path.exists(abs_project_path):
            print(f"Warning: Project path {project_path} does not exist. Skipping.")
            continue

        print(f"\n--- Preparing project: {project_path} ---")

        # 1. Data Generation
        if not args.skip_data_gen:
            gen_script = "generate_data.py"
            gen_script_path = os.path.join(abs_project_path, gen_script)
            cwd_for_gen = abs_project_path

            # If it's a multi-sink project, we generate data in the corresponding one-sink project and copy the duckdb file
            if "projects/synth/multi-sink/" in project_path:
                one_sink_path = project_path.replace("multi-sink", "one-sink")
                abs_one_sink_path = os.path.abspath(one_sink_path)
                src_db = os.path.join(abs_one_sink_path, "data", "warehouse.duckdb")
                dst_db = os.path.join(abs_project_path, "data", "warehouse.duckdb")
                if os.path.exists(src_db):
                    os.makedirs(os.path.dirname(dst_db), exist_ok=True)
                    print(f"Copying {src_db} to {dst_db}")
                    import shutil
                    shutil.copy2(src_db, dst_db)
                    # We've handled data gen for multi-sink by using one-sink, so we skip the default logic
                    # But we still need to proceed to dbt deps/compile.
                    # Using a flag to avoid running the default gen logic below.
                    gen_done = True
                else:
                    gen_done = False
            else:
                gen_done = False

            if not gen_done:
                if os.path.exists(gen_script_path):
                    run_command(["python3", gen_script, args.sf], cwd_for_gen)
                else:
                    print(f"Warning: {gen_script} not found for {project_path}. Skipping data generation.")
        else:
            print(f"Skipping data generation for {project_path} as requested.")

        # 2. dbt deps
        run_command(["dbt", "deps"], abs_project_path)

        # 3. dbt compile
        dbt_cmd = ["dbt", "compile"]
        if args.db == "postgres":
            dbt_cmd.extend(["--target", "postgres"])
        run_command(dbt_cmd, abs_project_path)

        # 4. python3 load_postgres.py (if db == postgres)
        if args.db == "postgres":
            load_script = "load_postgres.py"
            load_script_path = os.path.join(abs_project_path, load_script)
            if os.path.exists(load_script_path):
                run_command(["python3", load_script], abs_project_path)
            else:
                print(f"Warning: {load_script} not found for {project_path}. Skipping load.")

if __name__ == "__main__":
    main()
