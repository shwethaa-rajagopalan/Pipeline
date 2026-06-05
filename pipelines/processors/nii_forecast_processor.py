from typing import Any, Dict

from pyspark.sql import SparkSession

from pipelines.processors.base_processor import BaseProcessor
from pipelines.steps import step_01_config_update as config_update
from pipelines.steps import step_02_actuals_enrichment as actuals_enrichment
from pipelines.steps import step_03_forecast_enrichment as forecast_enrichment
from pipelines.steps import step_04_launchpoint_enrichment as launchpoint_enrichment
from pipelines.steps import step_05_calculation_lookup as calculation_lookup
from pipelines.steps import step_06_proxy_enrichment as proxy_enrichment
from pipelines.steps import step_07_inflation_adjustment_lookup as inflation_adjustment_lookup
from pipelines.steps import step_08_yield_delta as yield_delta
from pipelines.steps import step_09_nii_stage as nii_stage
from pipelines.steps import step_10_nii_stage_cc as nii_stage_cc


class NiiForecastProcessor(BaseProcessor):
    def __init__(self, task_context: Dict[str, Any]):
        super().__init__(task_context)

    def execute(self, spark: SparkSession) -> None:
        config_snapshot = config_update.run(spark, self.config)
        actuals = actuals_enrichment.run(spark, self.config, config_snapshot)
        forecast = forecast_enrichment.run(spark, self.config, config_snapshot)
        launchpoint = launchpoint_enrichment.run(spark, self.config, actuals, forecast, config_snapshot)
        drivers = calculation_lookup.run(spark, self.config, launchpoint, config_snapshot)
        proxy = proxy_enrichment.run(spark, self.config, drivers, config_snapshot)
        inflation = inflation_adjustment_lookup.run(spark, self.config, proxy, config_snapshot)
        yield_delta_df = yield_delta.run(spark, self.config, proxy, inflation, config_snapshot)
        nii = nii_stage.run(spark, self.config, yield_delta_df, config_snapshot)
        nii_stage_cc.run(spark, self.config, nii, config_snapshot)
