import argparse

from src.utils.spark_session import create_spark_session
from src.extract.reader import read_taxi_data

from src.transform.cleaning import (
    remove_invalid_distances,
    remove_invalid_fares,
    remove_invalid_passengers,
    remove_invalid_datetimes,
    remove_negative_trip_duration,
)

from src.transform.feature_engineering import (
    add_trip_duration,
    remove_unrealistic_trip_duration,
    add_time_of_day,
    add_speed_column,
    add_tip_percentage,
    add_payment_type_label,
)

from src.analysis.revenue_analysis import (
    calculate_hourly_summary,
    calculate_payment_summary,
    calculate_top_revenue_locations,
)

from src.analysis.peak_hours import calculate_peak_hours
from src.load.writer import write_parquet

from pyspark.sql import functions as F


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi Spark ETL")
    parser.add_argument("--input", default="data/raw/yellow_tripdata_2019-01.parquet")
    parser.add_argument("--output", default="data/processed")
    return parser.parse_args(argv)


def run(spark, input_path, output_path):
    """Run the whole pipeline: read, clean, enrich, analyse and write Parquet."""
    df = read_taxi_data(spark, input_path)

    print("Initial row count:", df.count())

    df = remove_invalid_distances(df)
    df = remove_invalid_fares(df)
    df = remove_invalid_passengers(df)
    df = remove_invalid_datetimes(df)
    df = remove_negative_trip_duration(df)

    df = add_trip_duration(df)
    df = remove_unrealistic_trip_duration(df)
    df = add_time_of_day(df)
    df = add_speed_column(df)
    df = add_tip_percentage(df)
    df = add_payment_type_label(df)

    df = df.withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    )

    df = df.withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )

    hourly_summary = calculate_hourly_summary(df)
    payment_summary = calculate_payment_summary(df)
    top_locations = calculate_top_revenue_locations(df)
    peak_hours = calculate_peak_hours(df)

    print("\nHourly Summary:")
    hourly_summary.show()

    print("\nPayment Summary:")
    payment_summary.show()

    print("\nTop Revenue Locations:")
    top_locations.show()

    print("\nPeak Hours:")
    peak_hours.show()

    write_parquet(df, f"{output_path}/trips", partition_cols=["pickup_date"])
    write_parquet(hourly_summary, f"{output_path}/hourly_summary")
    write_parquet(payment_summary, f"{output_path}/payment_summary")
    write_parquet(top_locations, f"{output_path}/top_revenue_locations")
    write_parquet(peak_hours, f"{output_path}/peak_hours")
    print(f"\nParquet outputs written to {output_path}/")


def main(argv=None):
    args = parse_args(argv)
    spark = create_spark_session()
    run(spark, args.input, args.output)
    spark.stop()


if __name__ == "__main__":
    main()
