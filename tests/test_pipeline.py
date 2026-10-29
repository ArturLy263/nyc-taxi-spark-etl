from datetime import datetime
from pathlib import Path

from src.main import run
from tests.test_transforms import COLUMNS, trip


def test_pipeline_end_to_end_on_small_parquet(spark, tmp_path):
    t = datetime
    rows = [
        trip(t(2019, 1, 1, 8, 0), t(2019, 1, 1, 8, 20), fare=10.0, loc=100),
        trip(t(2019, 1, 1, 8, 30), t(2019, 1, 1, 8, 50), fare=20.0, loc=100),
        trip(t(2019, 1, 1, 18, 0), t(2019, 1, 1, 18, 15), fare=30.0, payment=2, loc=200),
        trip(t(2019, 1, 2, 9, 0), t(2019, 1, 2, 9, 10), fare=40.0, loc=200),
        trip(t(2019, 1, 2, 10, 0), t(2019, 1, 2, 9, 0)),                 # dropoff before pickup
        trip(t(2019, 1, 2, 11, 0), t(2019, 1, 2, 11, 10), distance=0.0),  # zero distance
    ]
    raw = str(tmp_path / "raw.parquet")
    spark.createDataFrame(rows, COLUMNS).write.parquet(raw)
    out = tmp_path / "processed"

    run(spark, raw, str(out))

    trips = spark.read.parquet(str(out / "trips"))
    assert trips.count() == 4
    assert {p.name for p in (out / "trips").iterdir() if p.is_dir()} == {
        "pickup_date=2019-01-01", "pickup_date=2019-01-02",
    }

    hourly = {r["pickup_hour"]: r["total_trips"] for r in spark.read.parquet(str(out / "hourly_summary")).collect()}
    assert hourly == {8: 2, 18: 1, 9: 1}

    payments = {r["payment_type_label"]: r["total_trips"] for r in spark.read.parquet(str(out / "payment_summary")).collect()}
    assert payments == {"Credit Card": 3, "Cash": 1}

    revenue = {r["PULocationID"]: r["total_revenue"] for r in spark.read.parquet(str(out / "top_revenue_locations")).collect()}
    assert revenue == {100: 30.0, 200: 70.0}

    peaks = {(str(r["pickup_date"]), r["pickup_hour"]) for r in spark.read.parquet(str(out / "peak_hours")).collect()}
    assert peaks == {("2019-01-01", 8), ("2019-01-02", 9)}


def test_pipeline_on_the_committed_csv_sample(spark, tmp_path):
    # examples/sample_trips.csv: 18 synthetic trips, 6 of them invalid (dropoff before
    # pickup, no passengers, zero distance, fare over 500, 270-minute trip, no dropoff).
    sample = Path(__file__).resolve().parent.parent / "examples" / "sample_trips.csv"
    out = tmp_path / "processed"

    run(spark, str(sample), str(out))

    assert spark.read.parquet(str(out / "trips")).count() == 12
    peaks = {(str(r["pickup_date"]), r["pickup_hour"], r["count"]) for r in spark.read.parquet(str(out / "peak_hours")).collect()}
    assert peaks == {("2019-01-01", 8, 3), ("2019-01-02", 8, 2)}
