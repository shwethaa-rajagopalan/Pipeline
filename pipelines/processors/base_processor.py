from typing import Any, Dict, Optional

from pyspark.sql import SparkSession


class BaseProcessor:
    def __init__(self, task_context: Dict[str, Any]):
        self.task_context = task_context
        self.system_params = task_context["system_params"]
        self.business_params = task_context["business_params"]
        self.config_versions = task_context["configuration_versions"]
        self.config = task_context["config"]
        self.task_definition = task_context["task_definition"]

        self.job_name = self.system_params.get("job_name")
        self.run_date = self.system_params.get("run_date")
        self.environment = self.system_params.get("environment")
        self.business_unit = self.business_params.get("business_unit")
        self.product = self.business_params.get("product")
        self.currency = self.business_params.get("currency")

    def load_config_row(self, spark: SparkSession, config_key: str) -> Optional[Dict[str, Any]]:
        if not spark:
            return None

        parameter_store_path = self.config.get("tables", {}).get("config", {}).get("parameter_store")
        if not parameter_store_path:
            return None

        df = spark.read.format("delta").load(parameter_store_path)
        rows = (
            df.filter(
                (df.config_key == config_key) & (df.config_version == self.config_versions.get(config_key))
            )
            .limit(1)
            .collect()
        )
        if not rows:
            return None
        return rows[0].asDict()

    def resolve_output_path(self, table_key: str) -> str:
        return self.config["paths"].get(table_key)
