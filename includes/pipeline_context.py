from contextlib import contextmanager
from typing import Iterator

from config.spark_config import stop_spark_session
from src.pipeline import SalesAnalyticsPipeline


@contextmanager
def pipeline_context(app_name: str) -> Iterator[SalesAnalyticsPipeline]:
    """
    Context manager que inicializa componentes del pipeline y garantiza
    el cierre correcto de la sesión Spark.
    """
    pipeline = SalesAnalyticsPipeline(app_name=app_name)
    pipeline.initialize()
    try:
        yield pipeline
    finally:
        if pipeline.spark:
            stop_spark_session(pipeline.spark)

