import argparse
import os
from pathlib import Path
from typing import Dict

from utils.spark_session import create_spark_session
from pipelines.task_runner import TaskRunner


def get_repo_root() -> Path:
    """Resolve repository root robustly.

    Strategy:
    - Prefer __file__ when available (normal script execution).
    - Otherwise, walk upward from current working directory to find a folder
      that contains a `conf` directory with `sample_config.yaml`.
    - Fallback to current working directory.
    """
    # 1) Try __file__ (standard script run)
    try:
        p = Path(__file__).resolve()
        candidate = p.parent.parent
        # If this candidate looks like repo root (contains conf), accept it
        if (candidate / "conf" / "sample_config.yaml").exists() or (candidate / "conf").exists():
            return candidate
        # Otherwise fallthrough to search parents
    except Exception:
        pass

    # 2) Walk up from cwd to find conf/sample_config.yaml
    cwd = Path.cwd()
    for ancestor in [cwd] + list(cwd.parents):
        if (ancestor / "conf" / "sample_config.yaml").exists() or (ancestor / "conf").exists():
            return ancestor

    # 3) Last resort: return cwd
    return cwd


REPO_ROOT = get_repo_root()


def main(config_path: str = "conf/sample_config.yaml", task_definition: str = "conf/tasks/nii_forecast_task.yml") -> None:
    config_path = Path(config_path)
    task_definition_path = Path(task_definition)

    def _resolve_relative(p: Path) -> Path:
        if p.is_absolute():
            return p
        candidates = [REPO_ROOT, REPO_ROOT.parent, Path.cwd(), Path.cwd().parent]
        for root in candidates:
            candidate = root / p
            if candidate.exists():
                return candidate
        # fallback to REPO_ROOT / p even if missing
        return REPO_ROOT / p

    config_path = _resolve_relative(config_path)
    task_definition_path = _resolve_relative(task_definition_path)

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
