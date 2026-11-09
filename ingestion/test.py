import duckdb
import pathlib
import yaml

import duckdb
P = "data/bronze/places/release=2026-10-21.0/part-0.parquet"
D = "data/bronze/divisions/release=2026-10-21.0/part-0.parquet"

duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{P}')").show(max_rows=50)