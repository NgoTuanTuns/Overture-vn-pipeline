import duckdb

RELEASE = "2026-10-21.0"

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("INSTALL spatial; LOAD spatial;")
con.sql("SET s3_region='us-west-2';")

path = f"s3://overturemaps-us-west-2/release/{RELEASE}/theme=places/type=place/*"

print(con.sql(f"""
    SELECT count(*) AS n
    FROM read_parquet('{path}', hive_partitioning=1)
    WHERE bbox.xmin BETWEEN 102.1 AND 109.5
      AND bbox.ymin BETWEEN 8.4 AND 23.4
""").fetchall())

con.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}', hive_partitioning=1)").show(max_rows=50)