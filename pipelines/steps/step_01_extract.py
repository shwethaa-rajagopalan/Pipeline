from pyspark.sql import SparkSession, DataFrame
from typing import Dict
import pyspark.sql.functions as F


def run(spark: SparkSession, config: Dict) -> DataFrame:
    """Extract step: read raw input or generate sample data when running locally.

    Returns a DataFrame with at least columns: `key`, `value`, `weight`.
    """
    raw_path = config.get("paths", {}).get("raw_input")
    if raw_path:
        try:
            df = spark.read.option("header", True).csv(raw_path)
            df = df.withColumn("value", F.col("value").cast("double")).withColumn("weight", F.col("weight").cast("double"))
            return df
        except Exception:
            pass

    # Fallback sample data for local/dev
    data = [("A", 10.0, 1.0), ("A", 20.0, 0.5), ("B", 5.0, 1.0), ("B", 15.0, 0.0)]
    df = spark.createDataFrame(data, schema=["key", "value", "weight"])
    return df
