"""
Flow Maestro de Prefect que orquesta todos los pipelines.

Ejecuta todos los pipelines de análisis en secuencia y sincroniza
los resultados al frontend.
"""

import os
import sys
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect import flow, task
from flows.notifications import get_notification_service

# Importar flows individuales
from flows.executive_summary_flow import executive_summary_flow
from flows.analytics_flow import analytics_flow
from flows.clustering_flow import clustering_flow
from flows.recommendations_flow import recommendations_flow
from flows.output_sync_flow import output_sync_flow


@task(name="Validar datos en PostgreSQL", retries=2, retry_delay_seconds=10)
def validate_postgres_data():
    """
    Valida que existan datos en PostgreSQL antes de ejecutar pipelines.

    Raises:
        Exception: Si no hay datos suficientes en la base de datos
    """
    notifier = get_notification_service()
    notifier.log_task_start("Validar datos en PostgreSQL")

    try:
        import psycopg2
        from prefect_config import get_postgres_config

        pg_config = get_postgres_config()

        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"],
        )
        cursor = conn.cursor()

        # Verificar tablas y contar registros
        cursor.execute("SELECT COUNT(*) FROM categories;")
        categories_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM product_categories;")
        product_categories_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM transactions;")
        transactions_count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        # Validar que haya datos
        if categories_count == 0:
            raise Exception("No hay categorías en la base de datos")

        if product_categories_count == 0:
            raise Exception("No hay productos-categorías en la base de datos")

        if transactions_count == 0:
            raise Exception("No hay transacciones en la base de datos")

        notifier.log_info(f"      ✓ Categorías: {categories_count}")
        notifier.log_info(f"      ✓ Productos-Categorías: {product_categories_count}")
        notifier.log_info(f"      ✓ Transacciones: {transactions_count}")

        notifier.log_task_success(
            "Validar datos en PostgreSQL",
            f"{transactions_count} transacciones disponibles",
        )

        return {
            "categories": categories_count,
            "product_categories": product_categories_count,
            "transactions": transactions_count,
        }

    except Exception as e:
        notifier.log_task_failure("Validar datos en PostgreSQL", e)
        raise


@flow(
    name="Pipeline Maestro - Análisis Completo",
    description="Ejecuta todos los pipelines de análisis y sincroniza resultados al frontend",
    log_prints=True,
)
def master_flow(
    n_clusters: int = 4,
    min_support: float = 0.005,
    min_confidence: float = 0.2,
    sync_to_frontend: bool = True,
):
    """
    Flow maestro que orquesta todos los pipelines de análisis.

    Args:
        n_clusters: Número de clusters para el pipeline de clustering (default: 4)
        min_support: Soporte mínimo para recomendaciones (default: 0.005)
        min_confidence: Confianza mínima para recomendaciones (default: 0.2)
        sync_to_frontend: Si True, sincroniza outputs al frontend (default: True)

    Returns:
        Diccionario con resultados de todas las ejecuciones
    """
    notifier = get_notification_service()
    start_time = datetime.now()

    notifier.log_info("\n" + "=" * 70)
    notifier.log_info("🚀 FLOW MAESTRO - ANÁLISIS COMPLETO DE VENTAS")
    notifier.log_info("=" * 70)

    notifier.log_flow_start(
        "master_flow",
        {
            "n_clusters": n_clusters,
            "min_support": min_support,
            "min_confidence": min_confidence,
            "sync_to_frontend": sync_to_frontend,
        },
    )

    results = {}
    failed_pipelines = []

    try:
        # Paso 0: Validar que haya datos en PostgreSQL
        notifier.log_info("\n📊 Paso 0: Validando datos en PostgreSQL...")
        data_validation = validate_postgres_data()
        results["data_validation"] = data_validation

        # Paso 1: Resumen Ejecutivo
        notifier.log_info("\n📈 Paso 1: Ejecutando Pipeline de Resumen Ejecutivo...")
        try:
            result_summary = executive_summary_flow()
            results["executive_summary"] = result_summary
            notifier.log_info("   ✅ Resumen Ejecutivo completado")
        except Exception as e:
            failed_pipelines.append("executive_summary")
            results["executive_summary"] = {"status": "failed", "error": str(e)}
            notifier.log_error(f"   ❌ Error en Resumen Ejecutivo: {str(e)}")

        # Paso 2: Análisis Temporal
        notifier.log_info("\n📉 Paso 2: Ejecutando Pipeline de Análisis Temporal...")
        try:
            result_analytics = analytics_flow()
            results["analytics"] = result_analytics
            notifier.log_info("   ✅ Análisis Temporal completado")
        except Exception as e:
            failed_pipelines.append("analytics")
            results["analytics"] = {"status": "failed", "error": str(e)}
            notifier.log_error(f"   ❌ Error en Análisis Temporal: {str(e)}")

        # Paso 3: Clustering de Clientes
        notifier.log_info(
            f"\n🎯 Paso 3: Ejecutando Pipeline de Clustering (k={n_clusters})..."
        )
        try:
            result_clustering = clustering_flow(n_clusters=n_clusters)
            results["clustering"] = result_clustering
            notifier.log_info("   ✅ Clustering completado")
        except Exception as e:
            failed_pipelines.append("clustering")
            results["clustering"] = {"status": "failed", "error": str(e)}
            notifier.log_error(f"   ❌ Error en Clustering: {str(e)}")

        # Paso 4: Recomendaciones
        notifier.log_info(
            f"\n💡 Paso 4: Ejecutando Pipeline de Recomendaciones (support={min_support}, confidence={min_confidence})..."
        )
        try:
            result_recommendations = recommendations_flow(
                min_support=min_support, min_confidence=min_confidence
            )
            results["recommendations"] = result_recommendations
            notifier.log_info("   ✅ Recomendaciones completado")
        except Exception as e:
            failed_pipelines.append("recommendations")
            results["recommendations"] = {"status": "failed", "error": str(e)}
            notifier.log_error(f"   ❌ Error en Recomendaciones: {str(e)}")

        # Paso 5: Sincronizar al Frontend
        if sync_to_frontend:
            notifier.log_info("\n🔄 Paso 5: Sincronizando outputs al frontend...")
            try:
                result_sync = output_sync_flow()
                results["output_sync"] = result_sync
                notifier.log_info("   ✅ Sincronización completada")
            except Exception as e:
                failed_pipelines.append("output_sync")
                results["output_sync"] = {"status": "failed", "error": str(e)}
                notifier.log_error(f"   ❌ Error en sincronización: {str(e)}")
        else:
            notifier.log_info("\n⏭️  Paso 5: Sincronización al frontend omitida")
            results["output_sync"] = {"status": "skipped"}

        # Resumen final
        duration = (datetime.now() - start_time).total_seconds()
        duration_minutes = duration / 60

        notifier.log_info("\n" + "=" * 70)
        notifier.log_info("📊 RESUMEN DE EJECUCIÓN DEL FLOW MAESTRO")
        notifier.log_info("=" * 70)
        notifier.log_info(f"Duración total:      {duration_minutes:.2f} minutos")
        notifier.log_info(f"Pipelines exitosos:  {4 - len(failed_pipelines)}/4")

        if failed_pipelines:
            notifier.log_warning(f"Pipelines fallidos:  {len(failed_pipelines)}/4")
            notifier.log_warning(f"   - {', '.join(failed_pipelines)}")
            notifier.log_warning(
                "\n⚠️  Algunos pipelines fallaron, pero el flow continuó"
            )
        else:
            notifier.log_info("\n✅ Todos los pipelines completados exitosamente")

        notifier.log_info("=" * 70)

        results["summary"] = {
            "duration_seconds": duration,
            "total_pipelines": 4,
            "successful_pipelines": 4 - len(failed_pipelines),
            "failed_pipelines": failed_pipelines,
        }

        if failed_pipelines:
            notifier.log_flow_success("master_flow", duration)
            # No lanzamos excepción para permitir éxito parcial
        else:
            notifier.log_flow_success("master_flow", duration)

        return results

    except Exception as e:
        notifier.log_flow_failure("master_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el flow maestro localmente
    master_flow()

