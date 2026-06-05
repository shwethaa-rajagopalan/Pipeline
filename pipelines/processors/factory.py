from typing import Dict, Type

from pipelines.processors.base_processor import BaseProcessor
from pipelines.processors.nii_forecast_processor import NiiForecastProcessor


class ProcessorFactory:
    _registry: Dict[str, Type[BaseProcessor]] = {
        "NiiForecastProcessor": NiiForecastProcessor,
    }

    @classmethod
    def resolve(cls, class_name: str) -> Type[BaseProcessor]:
        if class_name not in cls._registry:
            raise ValueError(f"Unsupported processor class: {class_name}")
        return cls._registry[class_name]

    @classmethod
    def create(cls, class_name: str, task_context: Dict[str, object]) -> BaseProcessor:
        processor_class = cls.resolve(class_name)
        return processor_class(task_context)
