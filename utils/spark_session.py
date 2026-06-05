from typing import Optional, Dict
import os

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

    # Avoid configuring a local master when Spark Connect / remote is in use (Databricks Spark Connect).
    # If Spark Connect is configured via environment (SPARK_REMOTE) or via provided configs
    # (spark.remote), setting `master("local[*]")` will cause a startup validation error.
    spark_connect_env = os.environ.get("SPARK_REMOTE") or os.environ.get("SPARK_CONNECT")
    databricks_runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("DBR_RUNTIME_VERSION")
    configs_have_remote = configs and ("spark.remote" in configs)

    if not (spark_connect_env or databricks_runtime or configs_have_remote):
        builder = builder.master("local[*]")

    if configs:
        for k, v in configs.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    return spark
