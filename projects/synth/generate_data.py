#!/usr/bin/env python3
import os
import subprocess
import sys
import argparse
from concurrent.futures import ProcessPoolExecutor

def run_script(script_info):
    script_path, root, sf = script_info
    print(f"--- Starting {script_path} (sf={sf}) ---")
    try:
        subprocess.run([sys.executable, "generate_data.py", str(sf)], cwd=root, check=True, capture_output=True, text=True)
        return f"--- Finished {script_path} ---"
    except subprocess.CalledProcessError as e:
        return f"Error running {script_path}: {e.stderr}"

def main():
    parser = argparse.ArgumentParser(description="Recursively run generate_data.py scripts in parallel.")
    parser.add_argument("sf", type=float, nargs="?", default=1.0, help="Scale factor (default: 1.0)")
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_to_run = []
    
    for root, dirs, files in os.walk(base_dir):
        if "generate_data.py" in files:
            script_path = os.path.join(root, "generate_data.py")
            if os.path.abspath(script_path) == os.path.abspath(__file__):
                continue
            scripts_to_run.append((script_path, root, args.sf))

    print(f"Found {len(scripts_to_run)} scripts to run in parallel using {os.cpu_count()} cores.")
    
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(run_script, scripts_to_run))
        
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
