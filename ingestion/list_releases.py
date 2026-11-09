"""List Overture releases published on S3 and verify the themes we ingest exist.

The S3 REST listing tells us which release folders exist; a glob per theme then
confirms each release actually contains data (a folder can exist while incomplete).
"""
import argparse
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import duckdb

BUCKET_URL = "https://overturemaps-us-west-2.s3.amazonaws.com/"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

# Theme/type pairs the pipeline needs, matching the paths used by ingestion
REQUIRED = {
    "places": "theme=places/type=place",
    "divisions": "theme=divisions/type=division_area",
}


def list_releases(prefix: str = "release/") -> list[str]:
    """Return release folder names (e.g. '2026-10-21.0') sorted oldest to newest."""
    releases: list[str] = []
    token = None
    while True:
        params = {"list-type": "2", "prefix": prefix, "delimiter": "/"}
        if token:
            params["continuation-token"] = token
        url = BUCKET_URL + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=30) as resp:
            root = ET.fromstring(resp.read())

        for p in root.findall("s3:CommonPrefixes/s3:Prefix", S3_NS):
            # Prefix looks like 'release/2026-10-21.0/'
            releases.append(p.text.split("/")[1])

        # Results are paged; keep going until S3 says the listing is complete
        if root.findtext("s3:IsTruncated", namespaces=S3_NS) != "true":
            break
        token = root.findtext("s3:NextContinuationToken", namespaces=S3_NS)

    return sorted(releases)


def has_theme(con: duckdb.DuckDBPyConnection, release: str, path: str) -> bool:
    """True when at least one Parquet file exists for the theme in this release."""
    pattern = f"s3://overturemaps-us-west-2/release/{release}/{path}/*"
    try:
        return len(con.execute("SELECT * FROM glob(?)", [pattern]).fetchall()) > 0
    except duckdb.Error:
        return False


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--check-themes", action="store_true", help="verify required themes exist")
    p.add_argument("--last", type=int, default=0, help="only show the N newest releases")
    args = p.parse_args()

    releases = list_releases()
    if args.last:
        releases = releases[-args.last:]

    con = duckdb.connect()
    if args.check_themes:
        con.sql("INSTALL httpfs; LOAD httpfs;")
        con.sql("SET s3_region='us-west-2';")

    for r in releases:
        if args.check_themes:
            status = {t: has_theme(con, r, path) for t, path in REQUIRED.items()}
            print(r, status)
        else:
            print(r)


if __name__ == "__main__":
    main()