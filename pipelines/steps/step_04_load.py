from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from utils.db_utils import write_table


def run(spark: SparkSession, df: DataFrame, config: Dict) -> None:
    """Load step: write final DataFrame to target storage/table.

    Config should provide `paths.curated` and `format`.
    """
    target = config.get("paths", {}).get("curated", "data/curated/output")
    fmt = config.get("output_format", "delta")
    write_table(df, target, format=fmt, mode="overwrite")
