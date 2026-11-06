import duckdb

con = duckdb.connect()
con.sql("INSTALL spatial; LOAD spatial;")
f = "data/bronze/places/release=*/part-0.parquet"

print(con.sql(f"SELECT count(*) FROM read_parquet('{f}')"))
print(con.sql(f"""
    SELECT addresses[1].country AS country, count(*) AS n
    FROM read_parquet('{f}')
    GROUP BY 1 ORDER BY n DESC LIMIT 10
"""))
print(con.sql(f"""
    SELECT basic_category, count(*) AS n
    FROM read_parquet('{f}')
    GROUP BY 1 ORDER BY n DESC LIMIT 10
"""))