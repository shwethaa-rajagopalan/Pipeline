from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_driver_layer(df: DataFrame) -> DataFrame:
    """Compute driver factors like currency mix, exposure, and base yield."""
    return (
        df.withColumn(
            "currency_mix",
            F.when(F.col("currency") == "USD", F.lit(1.0)).otherwise(F.lit(0.95)),
        )
        .withColumn("net_exposure_factor", F.lit(0.98))
        .withColumn("net_rate_factor", F.lit(1.02))
        .withColumn("base_yield", F.when(F.col("product") == "Loan", F.lit(0.055)).otherwise(F.lit(0.015)))
    )


def apply_inflation_adjustment(df: DataFrame, inflation_df: DataFrame) -> DataFrame:
    """Merge the inflation adjustment structure into the driver data."""
    return (
        df.join(inflation_df, ["run_id", "business_unit", "product"], how="left")
        .withColumn(
            "inflation_adjustment_factor",
            1
            + (
                F.coalesce(F.col("lag_0"), F.lit(0.0))
                + F.coalesce(F.col("lag_1"), F.lit(0.0))
                + F.coalesce(F.col("lag_2"), F.lit(0.0))
                + F.coalesce(F.col("lag_3"), F.lit(0.0))
            )
            / 100,
        )
    )


def compute_yield_delta(
    df: DataFrame,
    yield_mapping_df: DataFrame,
    custom_override_df: DataFrame,
) -> DataFrame:
    """Compute the yield delta using mappings, inflation adjustment, and overrides."""
    return (
        df.join(yield_mapping_df, ["run_id", "business_unit", "product", "currency"], how="left")
        .join(custom_override_df, ["run_id", "business_unit", "product", "currency"], how="left")
        .withColumn(
            "yield_delta",
            F.coalesce(F.col("base_yield"), F.lit(0.05))
            * F.coalesce(F.col("inflation_adjustment_factor"), F.lit(1.0))
            * F.coalesce(F.col("override_factor"), F.lit(1.0)),
        )
    )


def compute_nii_business_unit(df: DataFrame, day_count_basis: int = 360) -> DataFrame:
    """Compute BU-level NII using the core NII formula from the report."""
    return (
        df.withColumn(
            "delta_balance",
            F.coalesce(F.col("forecast_balance"), F.lit(0.0))
            - F.coalesce(F.col("launchpoint_balance"), F.lit(0.0)),
        )
        .withColumn("day_count_fraction", F.lit(1.0))
        .withColumn(
            "nii_business_unit",
            F.col("delta_balance")
            * F.col("currency_mix")
            * F.col("net_exposure_factor")
            * F.col("yield_delta")
            * F.col("day_count_fraction"),
        )
    )


def allocate_cost_centre_nii(df: DataFrame, ratios_df: DataFrame) -> DataFrame:
    """Allocate BU-level NII to individual cost centres."""
    window = Window.partitionBy("run_id", "business_unit")
    joined = df.select("run_id", "business_unit", "nii_business_unit").distinct().join(
        ratios_df, ["run_id", "business_unit"], how="inner"
    )
    return (
        joined.withColumn("allocation_denominator", F.sum(F.col("cc_ratio") * F.col("fe_weight")).over(window))
        .withColumn(
            "nii_cost_centre",
            F.col("nii_business_unit")
            * F.col("cc_ratio")
            * F.col("fe_weight")
            / F.col("allocation_denominator"),
        )
    )


def compute_complex_metric(df: DataFrame, params: Dict[str, float]) -> DataFrame:
    """Backwards-compatible representative complex metric function."""
    min_weight = params.get("min_weight", 0.0)
    window_size = params.get("window_size", 7)
    filtered = df.filter(F.col("weight") >= F.lit(min_weight))
    weighted = filtered.withColumn("weighted_value", F.col("value") * F.col("weight"))
    agg = weighted.groupBy("key").agg(
        F.sum("weighted_value").alias("weighted_sum"),
        F.sum("weight").alias("weight_sum"),
    )
    return agg.withColumn(
        "complex_metric",
        F.when(F.col("weight_sum") == 0, F.lit(None)).otherwise(F.col("weighted_sum") / F.col("weight_sum")),
    ).select("key", "complex_metric")
