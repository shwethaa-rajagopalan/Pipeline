from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from utils.db_utils import write_table
from calculations.core import compute_nii_business_unit


def run(spark: SparkSession, config: Dict[str, Any], yield_df: DataFrame, config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 9: NII Stage.

    Computes the business unit level NII forecast using the report formula.
    """
    nii = compute_nii_business_unit(yield_df, config.get("pipeline", {}).get("day_count_basis", 360))

    write_table(nii, config["paths"]["nii_stage"], format=config.get("output_format", "delta"), mode="overwrite")
    return nii
