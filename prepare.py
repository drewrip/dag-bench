import argparse
import os
import subprocess
import yaml
import sys

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

        # 1. python3 generate_data.py <sf>
        if not args.skip_data_gen:
            # Check if the script exists in the project directory.
            gen_script = "generate_data.py"
            gen_script_path = os.path.join(abs_project_path, gen_script)
            cwd_for_gen = abs_project_path
            
            # Heuristic for synth subprojects which share a generator in projects/synth/
            if not os.path.exists(gen_script_path) and "projects/synth/" in project_path:
                synth_root = os.path.abspath("projects/synth")
                potential_gen = os.path.join(synth_root, gen_script)
                if os.path.exists(potential_gen):
                    gen_script_path = potential_gen
                    cwd_for_gen = synth_root

            if os.path.exists(gen_script_path):
                # If we are using the shared synth generator, we might need to pass the subproject info
                # but the prompt simply says 'python3 generate_data.py <sf>'. 
                # We'll stick to the requested command.
                run_command(["python3", os.path.basename(gen_script_path), args.sf], cwd_for_gen)
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
