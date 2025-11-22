import logging
from contextlib import contextmanager
from typing import Iterator

from config.spark_config import stop_spark_session
from src.pipeline import SalesAnalyticsPipeline

logger = logging.getLogger(__name__)


@contextmanager
def pipeline_context(app_name: str) -> Iterator[SalesAnalyticsPipeline]:
    """
    Context manager que inicializa componentes del pipeline y garantiza
    el cierre correcto de la sesión Spark.
    """
    pipeline = None
    try:
        logger.info(f"Inicializando pipeline: {app_name}")
        pipeline = SalesAnalyticsPipeline(app_name=app_name)
        pipeline.initialize()
        logger.info(f"Pipeline {app_name} inicializado correctamente")
        yield pipeline
    except Exception as e:
        logger.error(f"Error durante la ejecución del pipeline {app_name}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        if pipeline and pipeline.spark:
            try:
                logger.info(f"Cerrando sesión Spark para {app_name}")
                stop_spark_session(pipeline.spark)
            except Exception as e:
                logger.warning(f"Error al cerrar sesión Spark: {str(e)}")

