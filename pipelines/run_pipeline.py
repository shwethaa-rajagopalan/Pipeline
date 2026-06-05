import yaml
from typing import Dict
from utils.spark_session import create_spark_session

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


def load_config(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main(config_path: str = "conf/sample_config.yaml") -> None:
    config = load_config(config_path)
    spark = create_spark_session("nii_pipeline")

    config_snapshot = config_update.run(spark, config)
    actuals = actuals_enrichment.run(spark, config, config_snapshot)
    forecast = forecast_enrichment.run(spark, config, config_snapshot)
    launchpoint = launchpoint_enrichment.run(spark, config, actuals, forecast, config_snapshot)
    drivers = calculation_lookup.run(spark, config, launchpoint, config_snapshot)
    proxy = proxy_enrichment.run(spark, config, drivers, config_snapshot)
    inflation = inflation_adjustment_lookup.run(spark, config, proxy, config_snapshot)
    yield_delta_df = yield_delta.run(spark, config, proxy, inflation, config_snapshot)
    nii = nii_stage.run(spark, config, yield_delta_df, config_snapshot)
    nii_stage_cc.run(spark, config, nii, config_snapshot)


if __name__ == "__main__":
    main()
