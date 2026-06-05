import re
from typing import Optional
from pyspark.sql import DataFrame, SparkSession

_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$")


def _looks_like_table_name(path: str) -> bool:
    if not isinstance(path, str) or not path:
        return False
    if path.startswith("/") or path.startswith("dbfs:/") or path.startswith("file:") or path.startswith("s3://"):
        return False
    if "/" in path or "\\" in path:
        return False
    return bool(_TABLE_NAME_PATTERN.match(path))


def _ensure_schema_exists(df: DataFrame, table_name: str) -> None:
    spark = df.sparkSession
    schema_name = ".".join(table_name.split(".")[:-1])
    if not schema_name:
        return
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def write_table(df: DataFrame, path: str, format: str = "delta", mode: str = "overwrite") -> None:
    """Write DataFrame to a path or table in the chosen format.

    Args:
        df: DataFrame to write
        path: destination path or catalog table name
        format: one of 'delta', 'parquet', 'csv'
        mode: write mode
    """
    fmt = format.lower()
    if fmt == "delta":
        if _looks_like_table_name(path):
            _ensure_schema_exists(df, path)
            df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(path)
        else:
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
        if _looks_like_table_name(path):
            return spark.read.format("delta").table(path)
        return spark.read.format("delta").load(path)
    elif fmt == "parquet":
        return spark.read.parquet(path)
    elif fmt == "csv":
        return spark.read.option("header", True).csv(path)
    else:
        raise ValueError(f"Unsupported format: {format}")
