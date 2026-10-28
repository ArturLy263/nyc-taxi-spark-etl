from datetime import datetime

from pyspark.sql import functions as F

from src.extract.reader import read_taxi_data
from src.transform.feature_engineering import add_trip_duration


def test_reader_reads_parquet(spark, tmp_path):
    path = str(tmp_path / "trips.parquet")
    spark.createDataFrame([(1, 2.5)], ["VendorID", "trip_distance"]).write.parquet(path)
    assert read_taxi_data(spark, path).count() == 1


def test_reader_reads_csv_with_header(spark, tmp_path):
    path = tmp_path / "trips.csv"
    path.write_text("VendorID,trip_distance\n1,2.5\n2,4.0\n")
    df = read_taxi_data(spark, str(path))
    assert df.count() == 2
    assert dict(df.dtypes)["trip_distance"] == "double"


def test_trip_duration_handles_timestamp_ntz(spark):
    df = spark.createDataFrame(
        [(datetime(2019, 1, 1, 8, 0), datetime(2019, 1, 1, 8, 45))],
        ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    ).select(
        F.col("tpep_pickup_datetime").cast("timestamp_ntz").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp_ntz").alias("tpep_dropoff_datetime"),
    )
    assert dict(df.dtypes)["tpep_pickup_datetime"] == "timestamp_ntz"
    assert add_trip_duration(df).first()["trip_duration_minutes"] == 45.0
