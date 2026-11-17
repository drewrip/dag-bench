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

def prepare_snowflake(abs_project_path, skip_data_gen):
    """Tiny input -> dbt deps -> budgeted load. No `dbt compile` here: compiling
    against Snowflake opens a session, so it belongs in the budgeted build."""
    repo = os.path.dirname(os.path.abspath(__file__))
    project = os.path.basename(abs_project_path.rstrip("/"))
    if not skip_data_gen:
        run_command([sys.executable, "-m", "utils.snowflake_tiny", "prepare", project], repo)
    run_command(["dbt", "deps"], abs_project_path)
    run_command([sys.executable, "-m", "utils.snowflake_budget", "run", "--estimate-min", "2",
                 "--label", f"load {project}", "--", sys.executable, "load_snowflake.py"],
                abs_project_path)


def main():
    parser = argparse.ArgumentParser(description="Prepare dbt projects and generate data.")
    parser.add_argument("sf", type=str, help="Scale factor for data generation.")
    parser.add_argument("--db", type=str, choices=["duckdb", "postgres", "snowflake"], default="duckdb",
                        help="Target database (default: duckdb). 'snowflake' ignores sf: it uses each "
                             "project's fixed tiny input (utils/snowflake_tiny.py) and loads it through "
                             "the budget wrapper.")
    parser.add_argument("--skip-data-gen", action="store_true", help="Skip data generation step.")
    parser.add_argument("--no-constraints", action="store_true",
                        help="Generate data without primary/foreign key constraints (faster).")
    
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

        if args.db == "snowflake":
            prepare_snowflake(abs_project_path, args.skip_data_gen)
            continue

        # 1. Data Generation
        if not args.skip_data_gen:
            gen_script = "generate_data.py"
            gen_script_path = os.path.join(abs_project_path, gen_script)
            cwd_for_gen = abs_project_path

            if os.path.exists(gen_script_path):
                gen_cmd = ["python3", gen_script, args.sf]
                if args.no_constraints:
                    gen_cmd.append("--no-constraints")
                run_command(gen_cmd, cwd_for_gen)
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
