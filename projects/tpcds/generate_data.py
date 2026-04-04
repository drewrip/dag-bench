import duckdb
import argparse
import sys

def generate_tpcds(sf, db_path):
    """
    Generate TPC-DS data into a DuckDB file.
    """
    print(f"Connecting to {db_path}...")
    con = duckdb.connect(db_path)
    
    print(f"Installing and loading TPC-DS extension...")
    con.execute("INSTALL tpcds; LOAD tpcds;")
    
    print(f"Generating data for scale factor {sf}...")
    # sf=1 is approximately 1GB
    con.execute(f"CALL dsdgen(sf={sf});")
    
    tables = con.execute("SHOW TABLES;").fetchall()
    print(f"Generated {len(tables)} tables.")
    for table in tables:
        count = con.execute(f"SELECT count(*) FROM {table[0]};").fetchone()[0]
        print(f"  - {table[0]}: {count} rows")
        
    con.close()
    print("Data generation complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TPC-DS data for DuckDB")
    parser.add_argument("sf", type=float, help="Scale factor (SF=1 is ~1GB)")
    parser.add_argument("--path", type=str, default="tpcds.duckdb", help="Path to the DuckDB file")
    
    args = parser.parse_args()
    
    generate_tpcds(args.sf, args.path)
