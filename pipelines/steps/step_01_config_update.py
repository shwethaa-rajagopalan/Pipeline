import uuid
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from utils.db_utils import write_table


def run(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Any]:
    """Step 1: Config Update.

    Materialises run-scoped configuration snapshots and governance tables.
    """
    run_id = f"{config['pipeline'].get('run_id_prefix', 'run')}_{uuid.uuid4().hex[:8]}"
    config_snapshot = {
        "run_id": run_id,
        "run_scope": spark.createDataFrame(
            [
                (run_id, "BU1", "BSG1", "CC001", "USD"),
                (run_id, "BU1", "BSG2", "CC002", "USD"),
            ],
            schema=["run_id", "business_unit", "business_segment_group", "cost_centre", "currency"],
        ),
        "yield_index_mapping": spark.createDataFrame(
            [
                (run_id, "BU1", "Loan", "USD", "SYC_CURVE"),
                (run_id, "BU1", "Deposit", "USD", "DEP_CURVE"),
            ],
            schema=["run_id", "business_unit", "product", "currency", "yield_index"],
        ),
        "launchpoint_windows": spark.createDataFrame(
            [
                (run_id, "BU1", "2026-01-01", "2026-01-31", 30),
            ],
            schema=["run_id", "business_unit", "window_start", "window_end", "window_days"],
        ),
        "proxy_source_mappings": spark.createDataFrame(
            [
                (run_id, "BU1", "Loan", "BU1", "Loan", 1.0, 1.0),
            ],
            schema=[
                "run_id",
                "business_unit",
                "product",
                "proxy_business_unit",
                "proxy_product",
                "scale_ratio",
                "fixed_adjustment",
            ],
        ),
        "inflation_adjustments": spark.createDataFrame(
            [
                (run_id, "BU1", "Loan", 0.5, 0.3, 0.2, 0.1),
                (run_id, "BU1", "Deposit", 0.2, 0.1, 0.05, 0.02),
            ],
            schema=["run_id", "business_unit", "product", "lag_0", "lag_1", "lag_2", "lag_3"],
        ),
        "custom_yield_overrides": spark.createDataFrame(
            [
                (run_id, "BU1", "Loan", "USD", 1.0),
                (run_id, "BU1", "Deposit", "USD", 1.0),
            ],
            schema=["run_id", "business_unit", "product", "currency", "override_factor"],
        ),
        "forecast_substitution_rules": spark.createDataFrame(
            [
                (run_id, "flat", "flat"),
                (run_id, "prior_actual", "prior_actual"),
            ],
            schema=["run_id", "substitution_code", "substitution_type"],
        ),
        "launchpoint_period_yield_overrides": spark.createDataFrame(
            [
                (run_id, "BU1", "Loan", "USD", 1.0),
                (run_id, "BU1", "Deposit", "USD", 1.0),
            ],
            schema=["run_id", "business_unit", "product", "currency", "period_override"],
        ),
    }

    write_table(config_snapshot["run_scope"], config["paths"]["config_snapshot"], format=config.get("output_format", "delta"), mode="overwrite")
    return config_snapshot
