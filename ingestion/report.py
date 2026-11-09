"""Read-only reports over the bronze layer and the ingest log.

Usage:
    python ingestion/report.py bronze      # rows, size, country and category mix per places release
    python ingestion/report.py changelog   # change counts per release, most-changed columns
    python ingestion/report.py divisions   # admin levels present for Vietnam (latest release)
    python ingestion/report.py log         # latest ingest runs
"""
import argparse
import pathlib

import duckdb

from extract_overture import load_config
from ingest_log import DB_PATH


def bronze(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> None:
    for part in sorted((root / "places").glob("release=*/part-0.parquet")):
        release = part.parent.name.split("=", 1)[1]
        f = part.as_posix()
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{f}')").fetchall()[0][0]
        print(f"== release {release}: {rows:,} rows, {part.stat().st_size / 1024**2:,.1f} MiB ==")

        # Country comes from the first address; null means the place has no address
        con.sql(f"""
            SELECT coalesce(addresses[1].country, '(null)') AS country,
                   count(*) AS n,
                   round(100.0 * count(*) / {rows}, 2) AS pct
            FROM read_parquet('{f}')
            GROUP BY 1 ORDER BY n DESC LIMIT 10
        """).show()
        con.sql(f"""
            SELECT basic_category, count(*) AS n
            FROM read_parquet('{f}')
            GROUP BY 1 ORDER BY n DESC LIMIT 10
        """).show()


def changelog(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> None:
    glob = f"{(root / 'changelog' / 'places').as_posix()}/release=*/part-0.parquet"

    # Counts are for the coarse bbox, so they still include neighbouring countries
    con.sql(f"""
        SELECT _release,
               count(*) FILTER (WHERE change_type = 'added')        AS added,
               count(*) FILTER (WHERE change_type = 'removed')      AS removed,
               count(*) FILTER (WHERE change_type = 'data_changed') AS data_changed,
               count(*)                                             AS total
        FROM read_parquet('{glob}')
        GROUP BY _release ORDER BY _release
    """).show(max_rows=50)

    # Which columns change most in the latest release; separates real edits from noise
    latest = con.execute(f"SELECT max(_release) FROM read_parquet('{glob}')").fetchall()[0][0]
    print("latest release:", latest)
    con.sql(f"""
        SELECT unnest(columns_changed) AS column_changed, count(*) AS n
        FROM read_parquet('{glob}')
        WHERE _release = '{latest}' AND change_type = 'data_changed'
        GROUP BY 1 ORDER BY n DESC LIMIT 15
    """).show()


def divisions(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> None:
    latest = sorted((root / "divisions").glob("release=*"))[-1]
    print("release:", latest.name)
    # Admin levels present for Vietnam; compare against the 2025 two-tier reform
    con.sql(f"""
        SELECT subtype, class, count(*) AS n
        FROM read_parquet('{(latest / "part-0.parquet").as_posix()}')
        WHERE country = 'VN'
        GROUP BY ALL ORDER BY n DESC
    """).show()


def log(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> None:
    if not DB_PATH.exists():
        print("no ingest log yet")
        return
    duckdb.connect(str(DB_PATH), read_only=True).sql(
        "SELECT * FROM ingest_log ORDER BY run_at DESC LIMIT 40"
    ).show(max_width=200)


REPORTS = {"bronze": bronze, "changelog": changelog, "divisions": divisions, "log": log}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("report", choices=REPORTS)
    args = p.parse_args()

    root = pathlib.Path(load_config()["bronze_root"])
    REPORTS[args.report](duckdb.connect(), root)


if __name__ == "__main__":
    main()