"""Calculation helpers encapsulating complex logic from the attached report (2024TM93028.pdf).

Expose simple, testable functions used by pipeline steps.
"""

from .core import compute_complex_metric

__all__ = ["compute_complex_metric"]
