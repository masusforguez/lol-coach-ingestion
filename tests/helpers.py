import glob

import duckdb


def count_rows(lake, dataset, table) -> int:
    files = glob.glob(f"{lake}/{dataset}/{table}/*.parquet")
    if not files:
        return 0
    return duckdb.sql(f"select count(*) from read_parquet({files!r})").fetchone()[0]


def distinct(lake, dataset, table, col) -> int:
    files = glob.glob(f"{lake}/{dataset}/{table}/*.parquet")
    if not files:
        return 0
    return duckdb.sql(f"select count(distinct {col}) from read_parquet({files!r})").fetchone()[0]
