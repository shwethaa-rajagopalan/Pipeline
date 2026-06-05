import argparse
import os
from pathlib import Path
from typing import Dict

from utils.spark_session import create_spark_session
from pipelines.task_runner import TaskRunner


def get_repo_root() -> Path:
    """Get repo root, handling both local and Databricks notebook contexts."""
    try:
        return Path(__file__).resolve().parent.parent
    except NameError:
        # In Databricks notebooks, __file__ is not defined
        # Use current working directory or environment variable
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            return Path.cwd()
        return Path.cwd()


REPO_ROOT = get_repo_root()


def main(config_path: str = "conf/sample_config.yaml", task_definition: str = "conf/tasks/nii_forecast_task.yml") -> None:
    config_path = Path(config_path)
    task_definition_path = Path(task_definition)

    if not config_path.is_absolute():
        config_path = REPO_ROOT / config_path
    if not task_definition_path.is_absolute():
        task_definition_path = REPO_ROOT / task_definition_path

    runner = TaskRunner(task_definition_path, config_path)
    spark = create_spark_session("nii_pipeline")

    task_input = {
        "system_params": runner.config.get("default_task_input", {}).get("system_params", {}),
        "business_params": runner.config.get("default_task_input", {}).get("business_params", {}),
        "configuration_versions": runner.config.get("default_task_input", {}).get("configuration_versions", {}),
    }

    runner.run(spark, task_input)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the NII pipeline task runner.")
    parser.add_argument(
        "--config",
        default="conf/sample_config.yaml",
        help="Path to the pipeline configuration YAML.",
    )
    parser.add_argument(
        "--task-definition",
        default="conf/tasks/nii_forecast_task.yml",
        help="Path to the YAML task definition.",
    )
    args = parser.parse_args()
    main(config_path=args.config, task_definition=args.task_definition)
