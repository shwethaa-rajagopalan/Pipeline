from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from utils.db_utils import write_table
from calculations.core import apply_inflation_adjustment


def run(spark: SparkSession, config: Dict[str, Any], calc_df: DataFrame, config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 7: Inflation Adjustment Lookup.

    Loads inflation structures and computes an inflation adjustment factor.
    """
    inflation = apply_inflation_adjustment(calc_df, config_snapshot["inflation_adjustments"])

    write_table(inflation, config["paths"]["inflation_adjustment"], format=config.get("output_format", "delta"), mode="overwrite")
    return inflation
