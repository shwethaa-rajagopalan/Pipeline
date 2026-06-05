from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from calculations import compute_complex_metric


def run(spark: SparkSession, df: DataFrame, config: Dict) -> DataFrame:
    """Transform step: apply cleaning and complex calculations.

    Expects `df` with `key`, `value`, `weight`.
    """
    params = config.get("pipeline", {}).get("calculation_params", {})
    result = compute_complex_metric(df, params)
    return result
