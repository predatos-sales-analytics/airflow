"""
Flow de Prefect para el Pipeline de Recomendaciones.

Sistema de recomendaciones basado en reglas de asociación (FP-Growth).
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect import flow, task
from flows.notifications import get_notification_service
from pipelines.recommendations_pipeline import RecommendationsPipeline


@task(name="Generar Recomendaciones de Productos", retries=1, retry_delay_seconds=30)
def generate_recommendations_task(min_support: float = 0.005, min_confidence: float = 0.2):
    """
    Task que ejecuta el pipeline de recomendaciones.

    Args:
        min_support: Soporte mínimo para reglas de asociación
        min_confidence: Confianza mínima para reglas de asociación

    Returns:
        Diccionario con resultado de la ejecución
    """
    notifier = get_notification_service()
    notifier.log_task_start(
        f"Generar Recomendaciones (support={min_support}, confidence={min_confidence})"
    )

    try:
        # Ejecutar el pipeline existente
        pipeline = RecommendationsPipeline(
            min_support=min_support, min_confidence=min_confidence
        )
        pipeline.run()

        notifier.log_task_success(
            "Generar Recomendaciones de Productos", "Pipeline ejecutado correctamente"
        )

        return {
            "status": "success",
            "pipeline": "recommendations",
            "min_support": min_support,
            "min_confidence": min_confidence,
        }

    except Exception as e:
        notifier.log_task_failure("Generar Recomendaciones de Productos", e)
        raise


@flow(
    name="Pipeline de Recomendaciones",
    description="Sistema de recomendaciones basado en reglas de asociación",
    log_prints=True,
)
def recommendations_flow(min_support: float = 0.005, min_confidence: float = 0.2):
    """
    Flow principal del pipeline de recomendaciones.

    Args:
        min_support: Soporte mínimo para reglas (default: 0.005)
        min_confidence: Confianza mínima para reglas (default: 0.2)

    Returns:
        Resultado de la ejecución del pipeline
    """
    notifier = get_notification_service()
    start_time = datetime.now()

    notifier.log_flow_start(
        "recommendations_flow",
        {"min_support": min_support, "min_confidence": min_confidence},
    )

    try:
        # Ejecutar el pipeline
        result = generate_recommendations_task(
            min_support=min_support, min_confidence=min_confidence
        )

        # Calcular duración
        duration = (datetime.now() - start_time).total_seconds()
        notifier.log_flow_success("recommendations_flow", duration)

        return result

    except Exception as e:
        notifier.log_flow_failure("recommendations_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el flow localmente
    recommendations_flow()

