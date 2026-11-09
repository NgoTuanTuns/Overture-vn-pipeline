"""Append-only log of ingestion runs, kept in a small DuckDB file under data/meta."""
import pathlib

import duckdb

DB_PATH = pathlib.Path("data/meta/pipeline.duckdb")

_DDL = """
    CREATE TABLE IF NOT EXISTS ingest_log (
        run_at     TIMESTAMPTZ,
        source     VARCHAR,  -- 'release' or 'changelog'
        theme      VARCHAR,
        release    VARCHAR,
        row_count  BIGINT,
        size_bytes BIGINT,
        duration_s DOUBLE,
        status     VARCHAR,  -- 'ok' or 'failed'
        error      VARCHAR
    )
"""


def log_run(
    source: str,
    theme: str,
    release: str,
    status: str,
    duration_s: float,
    path: pathlib.Path | None = None,
    error: str | None = None,
) -> None:
    """Record one ingest attempt; row count and size are read back from the output file."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(_DDL)
        row_count = size_bytes = None
        if status == "ok" and path is not None:
            size_bytes = path.stat().st_size
            row_count = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [path.as_posix()]
            ).fetchall()[0][0]
        con.execute(
            "INSERT INTO ingest_log VALUES (now(), ?, ?, ?, ?, ?, ?, ?, ?)",
            [source, theme, release, row_count, size_bytes, duration_s, status, error],
        )
    finally:
        con.close()