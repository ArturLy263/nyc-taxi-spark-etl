from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def add_trip_duration(df: DataFrame) -> DataFrame:
    # Casting to "timestamp" first also handles TIMESTAMP_NTZ columns,
    # which Spark may use for Parquet files and which cannot be cast to long.
    return df.withColumn(
        "trip_duration_minutes",
        (
            col("tpep_dropoff_datetime").cast("timestamp").cast("long") -
            col("tpep_pickup_datetime").cast("timestamp").cast("long")
        ) / 60
    )


def remove_unrealistic_trip_duration(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("trip_duration_minutes") > 0) &
        (col("trip_duration_minutes") <= 180)
    )


def add_time_of_day(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_of_day",
        F.when(
            (F.hour("tpep_pickup_datetime") >= 0) &
            (F.hour("tpep_pickup_datetime") <= 5),
            "night"
        )
        .when(
            (F.hour("tpep_pickup_datetime") >= 6) &
            (F.hour("tpep_pickup_datetime") <= 11),
            "morning"
        )
        .when(
            (F.hour("tpep_pickup_datetime") >= 12) &
            (F.hour("tpep_pickup_datetime") <= 17),
            "afternoon"
        )
        .when(
            (F.hour("tpep_pickup_datetime") >= 18) &
            (F.hour("tpep_pickup_datetime") <= 23),
            "evening"
        )
        .otherwise("unknown")
    )


def add_speed_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "speed_mph",
        (col("trip_distance") * 60) /
        col("trip_duration_minutes")
    )


def add_tip_percentage(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "tip_pct",
        F.when(
            col("fare_amount") > 0,
            F.round(
                (col("tip_amount") / col("fare_amount")) * 100,
                2
            )
        ).otherwise(0)
    )

def add_payment_type_label(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "payment_type_label",
        F.when(F.col("payment_type") == 1, "Credit Card")
        .when(F.col("payment_type") == 2, "Cash")
        .when(F.col("payment_type") == 3, "No Charge")
        .when(F.col("payment_type") == 4, "Dispute")
        .when(F.col("payment_type") == 5, "Unknown")
        .when(F.col("payment_type") == 6, "Voided Trip")
        .otherwise("Other"),
    )