import json
from pathlib import Path
from typing import Any, Dict

import yaml
from pipelines.processors.factory import ProcessorFactory


class TaskRunner:
    def __init__(self, task_definition_path: Path, config_path: Path):
        self.task_definition_path = Path(task_definition_path)
        self.config_path = Path(config_path)
        self.task_definition = self._load_task_definition()
        self.config = self._load_config()

    def _load_task_definition(self) -> Dict[str, Any]:
        with open(self.task_definition_path, "r") as f:
            return yaml.safe_load(f)

    def _load_config(self) -> Dict[str, Any]:
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)

    def _validate_required_params(self, task_context: Dict[str, Any]) -> None:
        required = self.task_definition.get("required_params", [])
        missing = []
        for param in required:
            if param not in task_context["system_params"] and param not in task_context["business_params"]:
                missing.append(param)
        if missing:
            raise ValueError(f"Missing required parameters: {missing}")

    def _enrich_context(self, task_context: Dict[str, Any]) -> Dict[str, Any]:
        enriched = {
            "system_params": {
                **self.config.get("default_task_input", {}).get("system_params", {}),
                **task_context.get("system_params", {}),
            },
            "business_params": {
                **self.config.get("default_task_input", {}).get("business_params", {}),
                **task_context.get("business_params", {}),
            },
            "configuration_versions": {
                **self.config.get("default_task_input", {}).get("configuration_versions", {}),
                **task_context.get("configuration_versions", {}),
            },
            "config": self.config,
            "task_definition": self.task_definition,
        }
        return enriched

    def run(self, spark: Any, task_context: Dict[str, Any]) -> None:
        enriched_context = self._enrich_context(task_context)
        self._validate_required_params(enriched_context)

        processor_class_name = self.task_definition["processing_class"]
        processor = ProcessorFactory.create(processor_class_name, enriched_context)
        processor.execute(spark)
