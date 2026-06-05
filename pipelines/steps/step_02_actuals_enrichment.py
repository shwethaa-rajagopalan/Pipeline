from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from utils.db_utils import write_table


def run(spark: SparkSession, config: Dict[str, Any], config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 2: Actuals Enrichment.

    Produces enriched actual balances and income at the business hierarchy level.
    """
    actuals = spark.createDataFrame(
        [
            (config_snapshot["run_id"], "BU1", "BSG1", "CC001", "Loan", "USD", 100000.0, 1200.0),
            (config_snapshot["run_id"], "BU1", "BSG2", "CC002", "Deposit", "USD", 50000.0, 200.0),
        ],
        schema=[
            "run_id",
            "business_unit",
            "business_segment_group",
            "cost_centre",
            "product",
            "currency",
            "actual_balance",
            "actual_income",
        ],
    )

    adjusted_actuals = actuals.withColumn("preallocation_factor", F.lit(1.0)).withColumn(
        "actual_balance_adjusted", F.col("actual_balance") * F.col("preallocation_factor")
    )

    write_table(adjusted_actuals, config["paths"]["actuals_enriched"], format=config.get("output_format", "delta"), mode="overwrite")
    return adjusted_actuals
