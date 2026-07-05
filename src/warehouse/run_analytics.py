"""
Run the analytics SQL models against the DuckDB warehouse and print headline KPIs.
In production these become scheduled warehouse views / dbt models.
"""
import glob
import os

import duckdb

DB = os.environ.get("WAREHOUSE_DB", "data/warehouse.duckdb")
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "analytics")


def main():
    con = duckdb.connect(DB)
    for path in sorted(glob.glob(os.path.join(SQL_DIR, "*.sql"))):
        name = os.path.basename(path)
        with open(path) as fh:
            sql = fh.read()
        print(f"\n=== {name} ===")
        try:
            print(con.execute(sql).fetchdf().head(10).to_string(index=False))
        except Exception as exc:  # noqa: BLE001
            print(f"  skipped ({exc})")
    con.close()


if __name__ == "__main__":
    main()
