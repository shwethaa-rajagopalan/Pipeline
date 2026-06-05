from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from utils.db_utils import write_table


def run(spark: SparkSession, config: Dict[str, Any], calc_df: DataFrame, config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 6: Proxy Enrichment.

    Applies proxy mappings for sparse segments and fills missing driver values.
    """
    proxy = calc_df.join(
        config_snapshot["proxy_source_mappings"],
        ["run_id", "business_unit", "product"],
        how="left",
    )

    enriched = proxy.withColumn(
        "proxy_adjusted_balance",
        F.when(F.col("proxy_business_unit").isNotNull(), F.col("launchpoint_balance") * F.col("scale_ratio") + F.col("fixed_adjustment"))
        .otherwise(F.col("launchpoint_balance")),
    ).withColumn("proxy_balance", F.coalesce(F.col("proxy_adjusted_balance"), F.col("launchpoint_balance")))

    write_table(enriched, config["paths"]["proxy_enriched"], format=config.get("output_format", "delta"), mode="overwrite")
    return enriched
