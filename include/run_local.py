"""Run the pipeline locally for one month, outside Airflow.

Usage (from the project root):
    python -m include.run_local 2024 1
    python -m include.run_local --help
"""
import argparse

from include.ingestion.ingest import ingest_month


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the NYC taxi pipeline locally for one month")
    parser.add_argument("year", type=int, help="e.g. 2024")
    parser.add_argument("month", type=int, choices=range(1, 13), metavar="month", help="1-12")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Each step lives in its own module; this file only decides the order.
    ingest_month(args.year, args.month)
    # later: check_month(args.year, args.month)


if __name__ == "__main__":
    main()
