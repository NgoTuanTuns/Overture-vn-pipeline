"""In this script I pull one Overture theme for Vietnam into the bronze layer.

I keep the data raw on purpose: I only add two metadata columns, so any cleaning
bug in a later layer can be fixed by re-running from bronze without re-downloading.
"""
import argparse
import os
import pathlib

import duckdb

# I use a rough bounding box here because bbox filtering is cheap on GeoParquet.
# The exact Vietnam boundary is applied later in the silver layer.
VN_BBOX = {"xmin": 102.1, "xmax": 109.5, "ymin": 8.4, "ymax": 23.4}

# Hive-style paths of the Overture themes I support so far
THEMES = {
    "places": "theme=places/type=place",
}


def extract(release: str, theme: str, out_root: str) -> pathlib.Path:
    """I extract one theme of one release and write it as a single bronze Parquet file."""
    # I use an in-memory DuckDB; nothing is persisted besides the output Parquet file
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL spatial; LOAD spatial;")
    # The Overture bucket lives in us-west-2 and is public, so no credentials are needed
    con.sql("SET s3_region='us-west-2';")

    src = f"s3://overturemaps-us-west-2/release/{release}/{THEMES[theme]}/*"

    # I partition the output by release so every release is an independent unit
    # that can be backfilled or re-run on its own
    out_dir = pathlib.Path(out_root) / theme / f"release={release}"
    out_dir.mkdir(parents=True, exist_ok=True)

    tmp = out_dir / "part-0.parquet.tmp"
    final = out_dir / "part-0.parquet"

    b = VN_BBOX
    # I add _release and _ingested_at so every row can be traced back to its source run
    con.sql(f"""
        COPY (
            SELECT *,
                   '{release}'         AS _release,
                   current_timestamp   AS _ingested_at
            FROM read_parquet('{src}', hive_partitioning=1)
            WHERE bbox.xmin >= {b['xmin']} AND bbox.xmax <= {b['xmax']}
              AND bbox.ymin >= {b['ymin']} AND bbox.ymax <= {b['ymax']}
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)

    # I write to a temp file and rename it afterwards, so a failed run never leaves
    # a half-written file behind and re-running the same release is idempotent
    tmp.replace(final)
    return final


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    # I fall back to the OVERTURE_RELEASE env var so the release can come from config
    p.add_argument(
        "--release",
        default=os.environ.get("OVERTURE_RELEASE"),
        help="Overture release, e.g. 2026-10-21.0 (defaults to $OVERTURE_RELEASE)",
    )
    p.add_argument("--theme", default="places", choices=THEMES.keys())
    p.add_argument("--out", default="data/bronze")
    args = p.parse_args()
    if not args.release:
        p.error("--release is required (or set OVERTURE_RELEASE)")
    print("Wrote", extract(args.release, args.theme, args.out))