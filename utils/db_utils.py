from typing import Optional
from pyspark.sql import DataFrame, SparkSession


def write_table(df: DataFrame, path: str, format: str = "delta", mode: str = "overwrite") -> None:
    """Write DataFrame to a path in chosen format.

    Args:
        df: DataFrame to write
        path: destination path or table
        format: one of 'delta', 'parquet', 'csv'
        mode: write mode
    """
    fmt = format.lower()
    if fmt == "delta":
        df.write.format("delta").mode(mode).save(path)
    elif fmt == "parquet":
        df.write.mode(mode).parquet(path)
    elif fmt == "csv":
        df.write.mode(mode).option("header", True).csv(path)
    else:
        raise ValueError(f"Unsupported format: {format}")


def read_table(spark: SparkSession, path: str, format: str = "delta"):
    fmt = format.lower()
    if fmt == "delta":
        return spark.read.format("delta").load(path)
    elif fmt == "parquet":
        return spark.read.parquet(path)
    elif fmt == "csv":
        return spark.read.option("header", True).csv(path)
    else:
        raise ValueError(f"Unsupported format: {format}")
