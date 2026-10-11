"""Download NYC Yellow Taxi monthly parquet files and load them into DuckDB.

Library module: functions only, no entry point. It is called by
  - include/run_local.py   (local runs and debugging)
  - the Airflow DAG         (from Week 2; include/ is mounted into the container)

All functions are safe to re-run (idempotent).
"""
from pathlib import Path

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BASE_DIR = Path(__file__).resolve().parents[1]  # -> include/
RAW_DIR = BASE_DIR / "data" / "raw"
DB_PATH = str(BASE_DIR / "data" / "nyc_taxi.duckdb")

# The TLC CDN can drop connections from the default python-requests client,
# so send a browser-like User-Agent and retry transient failures.
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=2,  # waits 2s, 4s, 8s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s = requests.Session()
    s.headers.update(HEADERS)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def download_month(year: int, month: int) -> Path:
    """Download one month of Yellow Taxi data. Skips if the file already exists."""
    fname = f"yellow_tripdata_{year}-{month:02d}.parquet"
    path = RAW_DIR / fname
    if path.exists():
        print(f"{fname} already downloaded, skipping")
        return path

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    with _session().get(f"{BASE_URL}/{fname}", stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    tmp.rename(path)  # only a complete download gets the real name
    print(f"downloaded {fname}")
    return path


def load_month(path: Path, con: duckdb.DuckDBPyConnection) -> int:
    """Load one parquet file into raw_yellow_trips. Re-running replaces that month."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_yellow_trips AS
        SELECT *, ''::VARCHAR AS source_file
        FROM read_parquet(?) LIMIT 0
        """,
        [str(path)],
    )
    # delete-then-insert: running twice never duplicates data
    con.execute("DELETE FROM raw_yellow_trips WHERE source_file = ?", [path.name])
    con.execute(
        """
        INSERT INTO raw_yellow_trips BY NAME
        SELECT *, ? AS source_file FROM read_parquet(?)
        """,
        [path.name, str(path)],
    )
    return con.execute(
        "SELECT count(*) FROM raw_yellow_trips WHERE source_file = ?", [path.name]
    ).fetchone()[0]


def ingest_month(year: int, month: int) -> int:
    path = download_month(year, month)
    with duckdb.connect(DB_PATH) as con:
        rows = load_month(path, con)
    print(f"{rows:,} rows loaded from {path.name}")
    return rows

