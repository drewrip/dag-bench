import duckdb
import sys
import os

def generate_data(scale_factor):
    os.makedirs('data', exist_ok=True)
    db_file = os.path.join('data', 'tpch.duckdb')
    if os.path.exists(db_file):
        print(f"Removing existing {db_file}...")
        os.remove(db_file)
    con = duckdb.connect(db_file)
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={scale_factor});")
    print(f"TPC-H data generated in {db_file} with SF={scale_factor}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_data.py <scale_factor>")
        sys.exit(1)
    try:
        sf = float(sys.argv[1])
    except ValueError:
        print("Scale factor must be a number.")
        sys.exit(1)
    generate_data(sf)
