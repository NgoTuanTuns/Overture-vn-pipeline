"""Extract Overture themes for Vietnam into the bronze layer.

Bronze stays raw: the only additions are lineage columns, so cleaning bugs in
later layers can be fixed by re-running from here without re-downloading.
"""
import argparse
import logging
import os
import pathlib
import time

import duckdb
import yaml

from ingest_log import log_run

CONFIG_PATH = pathlib.Path("config/settings.yaml")


def load_config(path: pathlib.Path = CONFIG_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def source_glob(release: str, theme: str, cfg: dict) -> str:
    """S3 glob for one theme of one Overture release."""
    return f"s3://overturemaps-us-west-2/release/{release}/{cfg['themes'][theme]}/*"


def bronze_path(release: str, theme: str, cfg: dict) -> pathlib.Path:
    """Final bronze file location for one theme of one release."""
    return (
        pathlib.Path(cfg["bronze_root"]) / theme / f"release={release}" / "part-0.parquet"
    )


def extract(release: str, theme: str, cfg: dict, src: str | None = None) -> pathlib.Path:
    """Write one theme of one release as a single bronze Parquet file.

    `src` overrides the S3 location; tests use it to read a small local file.
    """
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql("SET s3_region='us-west-2';")  # public bucket, no credentials needed

    src = src or source_glob(release, theme, cfg)

    final = bronze_path(release, theme, cfg)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_suffix(".parquet.tmp")

    b = cfg["bbox"]
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

    # Rename after a complete write so a failed run never leaves a partial file;
    # this also makes re-running the same release idempotent.
    tmp.replace(final)
    return final


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()

    p = argparse.ArgumentParser()
    p.add_argument(
        "--release", nargs="+",
        default=[os.environ["OVERTURE_RELEASE"]] if os.environ.get("OVERTURE_RELEASE") else cfg["releases"],
        help="one or more releases (defaults to $OVERTURE_RELEASE, then config)",
    )
    p.add_argument(
        "--theme", nargs="+", default=list(cfg["themes"]), choices=list(cfg["themes"]),
        help="themes to extract (defaults to all in config)",
    )
    args = p.parse_args()

    for release in args.release:
        for theme in args.theme:
            started = time.perf_counter()
            try:
                path = extract(release, theme, cfg)
            except Exception as exc:
                log_run("release", theme, release, "failed", time.perf_counter() - started, error=str(exc))
                raise
            duration = time.perf_counter() - started
            log_run("release", theme, release, "ok", duration, path)
            logging.info(
                "release=%s theme=%s file=%s duration_s=%.1f", release, theme, path, duration
            )


if __name__ == "__main__":
    main()