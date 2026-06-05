from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from utils.db_utils import write_table
from calculations.core import compute_yield_delta


def run(spark: SparkSession, config: Dict[str, Any], proxy_df: DataFrame, inflation_df: DataFrame, config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 8: Yield Delta.

    Computes the forward yield delta using mappings and overrides.
    """
    yield_delta_df = compute_yield_delta(
        proxy_df, config_snapshot["yield_index_mapping"], config_snapshot["custom_yield_overrides"]
    ).join(
        inflation_df.select("run_id", "business_unit", "product", "inflation_adjustment_factor"),
        ["run_id", "business_unit", "product"],
        how="left",
    )

    write_table(yield_delta_df, config["paths"]["yield_delta"], format=config.get("output_format", "delta"), mode="overwrite")
    return yield_delta_df
