from pyspark.sql import DataFrame, SparkSession


def read_taxi_data(spark: SparkSession, path: str) -> DataFrame:
    """Read trip records from Parquet (the TLC's current format) or from CSV."""
    if path.lower().endswith(".csv"):
        return spark.read.csv(path, header=True, inferSchema=True)
    return spark.read.parquet(path)
