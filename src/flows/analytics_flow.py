"""
Flow de Prefect para el Pipeline de Análisis Temporal.

Análisis de patrones temporales y correlaciones.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect import flow, task
from flows.notifications import get_notification_service
from pipelines.analytics_pipeline import AnalyticsPipeline


@task(name="Generar Análisis Temporal", retries=1, retry_delay_seconds=30)
def generate_analytics_task():
    """
    Task que ejecuta el pipeline de análisis temporal.

    Returns:
        Diccionario con resultado de la ejecución
    """
    notifier = get_notification_service()
    notifier.log_task_start("Generar Análisis Temporal")

    try:
        # Ejecutar el pipeline existente
        pipeline = AnalyticsPipeline()
        pipeline.run()

        notifier.log_task_success(
            "Generar Análisis Temporal", "Pipeline ejecutado correctamente"
        )

        return {"status": "success", "pipeline": "analytics"}

    except Exception as e:
        notifier.log_task_failure("Generar Análisis Temporal", e)
        raise


@flow(
    name="Pipeline de Análisis Temporal",
    description="Análisis de series temporales, patrones y correlaciones",
    log_prints=True,
)
def analytics_flow():
    """
    Flow principal del pipeline de análisis temporal.

    Returns:
        Resultado de la ejecución del pipeline
    """
    notifier = get_notification_service()
    start_time = datetime.now()

    notifier.log_flow_start("analytics_flow")

    try:
        # Ejecutar el pipeline
        result = generate_analytics_task()

        # Calcular duración
        duration = (datetime.now() - start_time).total_seconds()
        notifier.log_flow_success("analytics_flow", duration)

        return result

    except Exception as e:
        notifier.log_flow_failure("analytics_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el flow localmente
    analytics_flow()

