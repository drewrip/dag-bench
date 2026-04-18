import duckdb
import sys

def generate_data(scale_factor):
    con = duckdb.connect('tpch.duckdb')
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={scale_factor});")
    print(f"TPC-H data generated in tpch.duckdb with SF={scale_factor}")

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
