#!/usr/bin/env python3
import os
import subprocess
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="Recursively run generate_data.py scripts.")
    parser.add_argument("sf", type=float, nargs="?", default=1.0, help="Scale factor (default: 1.0)")
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    for root, dirs, files in os.walk(base_dir):
        if "generate_data.py" in files:
            script_path = os.path.join(root, "generate_data.py")
            
            # Avoid running this script itself
            if os.path.abspath(script_path) == os.path.abspath(__file__):
                continue
            
            print(f"--- Running {script_path} (sf={args.sf}) ---")
            # Change directory to the script's directory so it can find its 'data' folder locally if it uses relative paths
            try:
                subprocess.run([sys.executable, "generate_data.py", str(args.sf)], cwd=root, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error running {script_path}: {e}")

if __name__ == "__main__":
    main()
