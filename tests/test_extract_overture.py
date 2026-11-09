import pathlib

import duckdb

from extract_overture import bronze_path, extract, source_glob

CFG = {
    "bbox": {"xmin": 100.0, "xmax": 110.0, "ymin": 5.0, "ymax": 25.0},
    "themes": {"places": "theme=places/type=place"},
    "bronze_root": "data/bronze",
}


def make_source(tmp_path: pathlib.Path) -> str:
    """Tiny Overture-shaped file: two points inside the Vietnam bbox, one outside."""
    path = tmp_path / "source.parquet"
    duckdb.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('a', {{'xmin': 105.0, 'xmax': 105.0, 'ymin': 10.0, 'ymax': 10.0}}),
                ('b', {{'xmin': 106.0, 'xmax': 106.0, 'ymin': 11.0, 'ymax': 11.0}}),
                ('c', {{'xmin': -70.0, 'xmax': -70.0, 'ymin': 40.0, 'ymax': 40.0}})
            ) AS t(id, bbox)
        ) TO '{path.as_posix()}' (FORMAT parquet)
    """)
    return path.as_posix()


def test_source_glob():
    assert source_glob("2026-10-21.0", "places", CFG) == (
        "s3://overturemaps-us-west-2/release/2026-10-21.0/theme=places/type=place/*"
    )


def test_bronze_path():
    assert bronze_path("2026-10-21.0", "places", CFG) == pathlib.Path(
        "data/bronze/places/release=2026-10-21.0/part-0.parquet"
    )


def test_extract_keeps_only_bbox_rows_and_adds_lineage(tmp_path):
    cfg = {**CFG, "bronze_root": str(tmp_path / "bronze")}
    out = extract("2026-10-21.0", "places", cfg, src=make_source(tmp_path))

    rows = duckdb.execute(
        "SELECT id, _release FROM read_parquet(?) ORDER BY id", [out.as_posix()]
    ).fetchall()
    assert rows == [("a", "2026-10-21.0"), ("b", "2026-10-21.0")]


def test_extract_is_idempotent(tmp_path):
    cfg = {**CFG, "bronze_root": str(tmp_path / "bronze")}
    src = make_source(tmp_path)

    first = extract("2026-10-21.0", "places", cfg, src=src)
    second = extract("2026-10-21.0", "places", cfg, src=src)

    assert first == second
    # No temp file or duplicate part left behind after the second run
    assert list(first.parent.iterdir()) == [first]