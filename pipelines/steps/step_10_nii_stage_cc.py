from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from utils.db_utils import write_table
from calculations.core import allocate_cost_centre_nii


def run(spark: SparkSession, config: Dict[str, Any], nii_df: DataFrame, config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 10: NII Stage CC.

    Allocates BU-level NII to cost centres using ratios and FE weights.
    """
    ratio_df = spark.createDataFrame(
        [
            (config_snapshot["run_id"], "BU1", "CC001", 0.6, 1.0),
            (config_snapshot["run_id"], "BU1", "CC002", 0.4, 1.0),
        ],
        schema=["run_id", "business_unit", "cost_centre", "cc_ratio", "fe_weight"],
    )

    cc_allocation = allocate_cost_centre_nii(nii_df, ratio_df)

    write_table(cc_allocation, config["paths"]["nii_stage_cc"], format=config.get("output_format", "delta"), mode="overwrite")
    return cc_allocation
