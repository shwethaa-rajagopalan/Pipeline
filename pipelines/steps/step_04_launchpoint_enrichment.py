from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from utils.db_utils import write_table


def run(
    spark: SparkSession,
    config: Dict[str, Any],
    actuals_df: DataFrame,
    forecast_df: DataFrame,
    config_snapshot: Dict[str, Any],
) -> DataFrame:
    """Step 4: Launchpoint Enrichment.

    Applies launchpoint windows and creates launchpoint baseline measures.
    """
    window_df = config_snapshot["launchpoint_windows"].select("run_id", "business_unit", "window_days")
    lp = (
        actuals_df.alias("a")
        .join(window_df.alias("w"), ["run_id", "business_unit"], how="inner")
        .groupBy("a.run_id", "a.business_unit", "a.business_segment_group", "a.cost_centre", "a.product", "a.currency")
        .agg(
            F.avg("actual_balance_adjusted").alias("launchpoint_balance"),
            F.avg("actual_income").alias("launchpoint_income"),
            F.first("window_days").alias("window_days"),
        )
        .withColumn("launchpoint_day_count", F.col("window_days") / F.lit(config.get("pipeline", {}).get("day_count_basis", 360)))
    )

    launchpoint = lp.join(
        forecast_df.select("run_id", "business_unit", "business_segment_group", "product", "currency", "forecast_month", "forecast_balance"),
        ["run_id", "business_unit", "business_segment_group", "product", "currency"],
        how="left",
    )

    write_table(launchpoint, config["paths"]["launchpoint_enriched"], format=config.get("output_format", "delta"), mode="overwrite")
    return launchpoint
