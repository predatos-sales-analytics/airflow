"""
Flow de Prefect para el Pipeline de Resumen Ejecutivo.

Genera métricas clave para el dashboard ejecutivo.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect import flow, task
from flows.notifications import get_notification_service
from pipelines.executive_summary_pipeline import ExecutiveSummaryPipeline


@task(name="Generar Resumen Ejecutivo", retries=1, retry_delay_seconds=30)
def generate_executive_summary_task():
    """
    Task que ejecuta el pipeline de resumen ejecutivo.

    Returns:
        Diccionario con resultado de la ejecución
    """
    notifier = get_notification_service()
    notifier.log_task_start("Generar Resumen Ejecutivo")

    try:
        # Ejecutar el pipeline existente
        pipeline = ExecutiveSummaryPipeline()
        pipeline.run()

        notifier.log_task_success(
            "Generar Resumen Ejecutivo", "Pipeline ejecutado correctamente"
        )

        return {"status": "success", "pipeline": "executive_summary"}

    except Exception as e:
        notifier.log_task_failure("Generar Resumen Ejecutivo", e)
        raise


@flow(
    name="Pipeline de Resumen Ejecutivo",
    description="Genera métricas ejecutivas: ventas, top productos, top clientes",
    log_prints=True,
)
def executive_summary_flow():
    """
    Flow principal del pipeline de resumen ejecutivo.

    Returns:
        Resultado de la ejecución del pipeline
    """
    notifier = get_notification_service()
    start_time = datetime.now()

    notifier.log_flow_start("executive_summary_flow")

    try:
        # Ejecutar el pipeline
        result = generate_executive_summary_task()

        # Calcular duración
        duration = (datetime.now() - start_time).total_seconds()
        notifier.log_flow_success("executive_summary_flow", duration)

        return result

    except Exception as e:
        notifier.log_flow_failure("executive_summary_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el flow localmente
    executive_summary_flow()

