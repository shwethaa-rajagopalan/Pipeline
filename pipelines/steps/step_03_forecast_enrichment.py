from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from utils.db_utils import write_table


def run(spark: SparkSession, config: Dict[str, Any], config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 3: Forecast Enrichment.

    Explodes annual forecasts into monthly balances and applies substitution logic.
    """
    base_forecast = spark.createDataFrame(
        [
            (config_snapshot["run_id"], "BU1", "BSG1", "Loan", "USD", 1200000.0, "flat"),
            (config_snapshot["run_id"], "BU1", "BSG2", "Deposit", "USD", 600000.0, "prior_actual"),
        ],
        schema=[
            "run_id",
            "business_unit",
            "business_segment_group",
            "product",
            "currency",
            "annual_forecast_amount",
            "substitution_code",
        ],
    )

    months = [f"2026-{month:02d}-01" for month in range(1, 13)]
    month_df = spark.createDataFrame([(m,) for m in months], schema=["forecast_month"])

    monthly = base_forecast.crossJoin(month_df).withColumn(
        "monthly_forecast_balance", F.col("annual_forecast_amount") / 12
    )

    enriched = monthly.join(
        config_snapshot["forecast_substitution_rules"], ["run_id", "substitution_code"], how="left"
    ).withColumn(
        "forecast_balance",
        F.when(F.col("substitution_type") == "prior_actual", F.col("monthly_forecast_balance") * 0.95).otherwise(
            F.col("monthly_forecast_balance")
        ),
    )

    write_table(enriched, config["paths"]["forecast_enriched"], format=config.get("output_format", "delta"), mode="overwrite")
    return enriched
