from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from utils.db_utils import write_table
from calculations.core import build_driver_layer


def run(spark: SparkSession, config: Dict[str, Any], launchpoint_df: DataFrame, config_snapshot: Dict[str, Any]) -> DataFrame:
    """Step 5: Calculation Lookup.

    Computes the driver-layer factors used in later NII computations.
    """
    drivers = build_driver_layer(launchpoint_df)

    write_table(drivers, config["paths"]["calculation_lookup"], format=config.get("output_format", "delta"), mode="overwrite")
    return drivers
