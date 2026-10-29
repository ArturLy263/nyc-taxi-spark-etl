"""Task logic for the Airflow DAG, kept free of Airflow imports so it can be
tested with plain pytest. Each function returns a small dict that the DAG
passes on through XCom.
"""

import os
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession

from src.main import run
from src.utils.spark_session import create_spark_session

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_TABLES = ["trips", "hourly_summary", "payment_summary", "top_revenue_locations", "peak_hours"]


@contextmanager
def spark_session(app_name: str):
    """A Spark session that is stopped afterwards only if this call created it.

    Each Airflow task runs in its own process, so there it is always created
    and stopped; in tests an existing session is reused and left running.
    """
    existing = SparkSession.getActiveSession()
    spark = existing or create_spark_session(app_name)
    try:
        yield spark
    finally:
        if existing is None:
            spark.stop()


def config() -> dict:
    """Input file and output base directory, from environment variables."""
    return {
        "input_path": os.environ.get(
            "NYC_TAXI_INPUT", str(REPO_ROOT / "examples" / "sample_trips.csv")
        ),
        "output_base": os.environ.get("NYC_TAXI_OUTPUT_DIR", str(REPO_ROOT / "output" / "airflow")),
    }


def output_dir_for(output_base: str, run_date: str) -> str:
    """One output folder per logical date, so a re-run replaces that date's results only."""
    return str(Path(output_base) / f"run_date={run_date}")


def check_input(input_path: str) -> dict:
    path = Path(input_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    size = path.stat().st_size
    if size == 0:
        raise ValueError(f"Input file is empty: {input_path}")
    return {"input_path": str(path), "size_bytes": size}


def run_etl(input_path: str, output_dir: str) -> dict:
    """Run the existing pipeline (src.main.run); its writes overwrite output_dir."""
    with spark_session("TaxiETL-airflow") as spark:
        run(spark, input_path, output_dir)
        trips = spark.read.parquet(f"{output_dir}/trips").count()
    return {"output_dir": output_dir, "trips": trips}


def validate_output(output_dir: str, min_trips: int = 1) -> dict:
    """Fail unless every output table exists and trips has at least min_trips rows."""
    missing = [t for t in OUTPUT_TABLES if not (Path(output_dir) / t / "_SUCCESS").exists()]
    if missing:
        raise ValueError(f"Missing or incomplete outputs in {output_dir}: {missing}")

    partitions = sorted(
        p.name.split("=", 1)[1] for p in (Path(output_dir) / "trips").glob("pickup_date=*")
    )
    with spark_session("TaxiETL-validate") as spark:
        trips = spark.read.parquet(f"{output_dir}/trips").count()
    if trips < min_trips:
        raise ValueError(f"Expected at least {min_trips} trips in {output_dir}, found {trips}")
    return {"trips": trips, "pickup_dates": partitions}
