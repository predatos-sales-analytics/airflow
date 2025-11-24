"""
Flow de Prefect para el Pipeline de Clustering.

Segmentación de clientes usando K-Means.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect import flow, task
from flows.notifications import get_notification_service
from pipelines.clustering_pipeline import ClusteringPipeline


@task(name="Generar Clustering de Clientes", retries=1, retry_delay_seconds=30)
def generate_clustering_task(n_clusters: int = 4):
    """
    Task que ejecuta el pipeline de clustering.

    Args:
        n_clusters: Número de clusters a generar

    Returns:
        Diccionario con resultado de la ejecución
    """
    notifier = get_notification_service()
    notifier.log_task_start(f"Generar Clustering de Clientes (k={n_clusters})")

    try:
        # Ejecutar el pipeline existente
        pipeline = ClusteringPipeline(n_clusters=n_clusters)
        pipeline.run()

        notifier.log_task_success(
            "Generar Clustering de Clientes", "Pipeline ejecutado correctamente"
        )

        return {"status": "success", "pipeline": "clustering", "n_clusters": n_clusters}

    except Exception as e:
        notifier.log_task_failure("Generar Clustering de Clientes", e)
        raise


@flow(
    name="Pipeline de Clustering",
    description="Segmentación de clientes usando K-Means",
    log_prints=True,
)
def clustering_flow(n_clusters: int = 4):
    """
    Flow principal del pipeline de clustering.

    Args:
        n_clusters: Número de clusters a generar (default: 4)

    Returns:
        Resultado de la ejecución del pipeline
    """
    notifier = get_notification_service()
    start_time = datetime.now()

    notifier.log_flow_start("clustering_flow", {"n_clusters": n_clusters})

    try:
        # Ejecutar el pipeline
        result = generate_clustering_task(n_clusters=n_clusters)

        # Calcular duración
        duration = (datetime.now() - start_time).total_seconds()
        notifier.log_flow_success("clustering_flow", duration)

        return result

    except Exception as e:
        notifier.log_flow_failure("clustering_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el flow localmente
    clustering_flow()

