"""Bounded Phase 2 batch-ingestion components.

The package deliberately has no Airflow dependency. Airflow imports the public
workflow functions, while unit tests and local commands call the same code
directly.
"""

from .models import PipelineError, RunPlan

__all__ = ["PipelineError", "RunPlan"]
