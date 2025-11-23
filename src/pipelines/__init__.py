"""
Pipelines de procesamiento de datos con Spark.

Cada pipeline representa una tarea de análisis que genera
resultados en formato JSON para el dashboard.
"""

from .executive_summary_pipeline import ExecutiveSummaryPipeline
from .analytics_pipeline import AnalyticsPipeline
from .clustering_pipeline import ClusteringPipeline
from .recommendations_pipeline import RecommendationsPipeline

__all__ = [
    "ExecutiveSummaryPipeline",
    "AnalyticsPipeline",
    "ClusteringPipeline",
    "RecommendationsPipeline",
]
