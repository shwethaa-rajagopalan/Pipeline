from pyspark.sql import SparkSession, DataFrame
from typing import Dict
import pyspark.sql.functions as F


def run(spark: SparkSession, df: DataFrame, config: Dict) -> DataFrame:
    """Aggregate step: example of further aggregation on the metric.

    Produces a summary per key and a global summary.
    """
    summary = df.groupBy("key").agg(F.avg("complex_metric").alias("avg_metric"))
    global_summary = summary.agg(F.avg("avg_metric").alias("global_avg"))
    gval = global_summary.collect()[0]["global_avg"] if global_summary.count() > 0 else None
    if gval is not None:
        summary = summary.withColumn("global_avg", F.lit(gval))
    return summary
