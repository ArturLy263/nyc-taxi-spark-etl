from datetime import datetime

import pytest

from src.analysis.peak_hours import calculate_peak_hours
from src.load.writer import write_parquet
from src.transform import cleaning, feature_engineering as fe

COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "tip_amount", "payment_type", "PULocationID",
]


def trip(pickup, dropoff, passengers=1, distance=3.0, fare=15.0, tip=3.0, payment=1, loc=100):
    return (pickup, dropoff, passengers, distance, fare, tip, payment, loc)


@pytest.fixture
def trips(spark):
    t = datetime
    rows = [
        trip(t(2019, 1, 1, 8, 0), t(2019, 1, 1, 8, 20)),                 # valid
        trip(t(2019, 1, 1, 9, 0), t(2019, 1, 1, 8, 0)),                  # dropoff before pickup
        trip(t(2019, 1, 1, 10, 0), t(2019, 1, 1, 10, 30), passengers=0), # no passengers
        trip(t(2019, 1, 1, 11, 0), t(2019, 1, 1, 11, 10), distance=0.0), # zero distance
        trip(t(2019, 1, 1, 12, 0), t(2019, 1, 1, 12, 10), fare=600.0),   # fare outlier
        trip(t(2019, 1, 1, 13, 0), t(2019, 1, 1, 17, 0)),                # 240-minute trip
    ]
    return spark.createDataFrame(rows, COLUMNS)


def test_cleaning_removes_invalid_trips(trips):
    df = trips
    for step in (
        cleaning.remove_invalid_distances,
        cleaning.remove_invalid_fares,
        cleaning.remove_invalid_passengers,
        cleaning.remove_invalid_datetimes,
        cleaning.remove_negative_trip_duration,
    ):
        df = step(df)
    df = fe.remove_unrealistic_trip_duration(fe.add_trip_duration(df))
    assert df.count() == 1


def test_trip_duration_speed_and_tip(trips):
    row = (
        fe.add_tip_percentage(fe.add_speed_column(fe.add_trip_duration(trips)))
        .orderBy("tpep_pickup_datetime")
        .first()
    )
    assert row["trip_duration_minutes"] == 20.0
    assert row["speed_mph"] == pytest.approx(9.0)
    assert row["tip_pct"] == 20.0


@pytest.mark.parametrize(
    "hour, expected",
    [(0, "night"), (5, "night"), (6, "morning"), (12, "afternoon"), (18, "evening"), (23, "evening")],
)
def test_time_of_day_boundaries(spark, hour, expected):
    pickup = datetime(2019, 1, 1, hour, 0)
    df = spark.createDataFrame([trip(pickup, pickup)], COLUMNS)
    assert fe.add_time_of_day(df).first()["time_of_day"] == expected


def test_unknown_payment_code_is_labelled_other(spark):
    t = datetime(2019, 1, 1, 8, 0)
    df = spark.createDataFrame([trip(t, t, payment=9)], COLUMNS)
    assert fe.add_payment_type_label(df).first()["payment_type_label"] == "Other"


def test_peak_hours_keeps_busiest_hour_per_day(spark):
    df = spark.createDataFrame(
        [("2019-01-01", 8), ("2019-01-01", 8), ("2019-01-01", 9), ("2019-01-02", 17)],
        ["pickup_date", "pickup_hour"],
    )
    result = {(r["pickup_date"], r["pickup_hour"]) for r in calculate_peak_hours(df).collect()}
    assert result == {("2019-01-01", 8), ("2019-01-02", 17)}


def test_write_parquet_partitions_output(spark, tmp_path):
    df = spark.createDataFrame([("2019-01-01", 1), ("2019-01-02", 2)], ["pickup_date", "trips"])
    out = str(tmp_path / "out")
    write_parquet(df, out, partition_cols=["pickup_date"])
    assert {p.name for p in (tmp_path / "out").iterdir() if p.is_dir()} == {
        "pickup_date=2019-01-01", "pickup_date=2019-01-02",
    }
    assert spark.read.parquet(out).count() == 2
