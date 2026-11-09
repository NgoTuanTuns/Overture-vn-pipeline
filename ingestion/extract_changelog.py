"""Extract the Overture changelog for Vietnam into the bronze layer.

The changelog outlives the data itself (it goes back to mid-2024 while only three
data releases remain), so it is the only record of month-by-month history for
releases that no longer exist. It lists which feature ids were added, removed or
changed, but not their attributes.
"""
import argparse
import logging
import pathlib
import time

import duckdb

from extract_overture import load_config
from list_releases import list_releases
from ingest_log import log_run

BASE = "s3://overturemaps-us-west-2/changelog"


def extract_release(
    con: duckdb.DuckDBPyConnection, release: str, theme: str, cfg: dict
) -> pathlib.Path:
    """Write one changelog release, filtered to the Vietnam bbox, as bronze Parquet."""
    src = f"{BASE}/{release}/{cfg['changelog_themes'][theme]}/**/*.parquet"
    read = f"read_parquet('{src}', hive_partitioning=1, union_by_name=true)"

    # Older releases have a different schema, so columns are checked before selecting
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {read}").fetchall()}
    missing = {"id", "bbox", "change_type"} - cols
    if missing:
        raise ValueError(f"release {release} is missing required columns: {sorted(missing)}")
    # columns_changed was only added to the changelog in 2026
    changed = "columns_changed" if "columns_changed" in cols else "NULL::VARCHAR[] AS columns_changed"

    out_dir = pathlib.Path(cfg["bronze_root"]) / "changelog" / theme / f"release={release}"
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_dir / "part-0.parquet.tmp"
    final = out_dir / "part-0.parquet"

    b = cfg["bbox"]
    con.sql(f"""
        COPY (
            SELECT id, bbox, {changed}, change_type,
                   '{release}'       AS _release,
                   current_timestamp AS _ingested_at
            FROM {read}
            WHERE bbox.xmin >= {b['xmin']} AND bbox.xmax <= {b['xmax']}
              AND bbox.ymin >= {b['ymin']} AND bbox.ymax <= {b['ymax']}
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
    """)
    tmp.replace(final)
    return final


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()

    p = argparse.ArgumentParser()
    p.add_argument("--release", nargs="+", help="changelog releases (defaults to all on S3)")
    p.add_argument("--theme", nargs="+", default=list(cfg["changelog_themes"]))
    p.add_argument("--skip-existing", action="store_true", help="resume an interrupted run")
    args = p.parse_args()

    releases = args.release or list_releases("changelog/")

    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("SET s3_region='us-west-2';")

    failed: list[str] = []
    for release in releases:
        for theme in args.theme:
            final = pathlib.Path(cfg["bronze_root"]) / "changelog" / theme / f"release={release}" / "part-0.parquet"
            if args.skip_existing and final.exists():
                logging.info("release=%s theme=%s skipped (exists)", release, theme)
                continue
            started = time.perf_counter()
            try:
                path = extract_release(con, release, theme, cfg)
            except (duckdb.Error, ValueError) as exc:
                # One bad release should not stop the other 30
                logging.warning("release=%s theme=%s FAILED: %s", release, theme, exc)
                log_run("changelog", theme, release, "failed", time.perf_counter() - started, error=str(exc))
                failed.append(release)
                continue
            duration = time.perf_counter() - started
            log_run("changelog", theme, release, "ok", duration, path)
            logging.info(
                "release=%s theme=%s file=%s duration_s=%.1f", release, theme, path, duration
            )

    if failed:
        logging.warning("failed releases: %s", ", ".join(sorted(set(failed))))


if __name__ == "__main__":
    main()