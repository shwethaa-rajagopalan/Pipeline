from typing import Optional, Dict

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "dbx_pipeline", configs: Optional[Dict[str, str]] = None) -> SparkSession:
    """Create and return a SparkSession with sensible defaults.

    Args:
        app_name: Spark app name
        configs: optional Spark configs to set

    Returns:
        SparkSession
    """
    builder = SparkSession.builder.appName(app_name)
    builder = builder.master("local[*]")
    if configs:
        for k, v in configs.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    return spark
