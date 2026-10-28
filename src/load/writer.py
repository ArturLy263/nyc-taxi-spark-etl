from pyspark.sql import DataFrame


def write_parquet(
    df: DataFrame,
    path: str,
    partition_cols: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    """Write a DataFrame as Parquet, optionally partitioned by the given columns."""
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
