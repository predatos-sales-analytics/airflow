"""
Deployment de Prefect para el monitor de datos.

Este script crea un deployment que ejecuta el monitor de datos
automáticamente cada 2 minutos.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta
from flows.data_monitor_flow import data_monitor_flow


def create_monitor_deployment():
    """
    Crea el deployment del monitor de datos.
    
    El deployment se ejecuta cada 2 minutos y solo notifica cuando
    hay datos nuevos, sin disparar el master flow automáticamente.
    """
    print("=" * 70)
    print("CREANDO DEPLOYMENT DEL MONITOR DE DATOS")
    print("=" * 70)
    print()
    
    deployment = Deployment.build_from_flow(
        flow=data_monitor_flow,
        name="data-monitor-scheduled",
        description="Monitor automático que detecta nuevos datos cada 2 minutos",
        schedule=IntervalSchedule(interval=timedelta(minutes=2)),
        work_pool_name="default-pool",
        parameters={
            "auto_trigger_master": False,  # Solo notifica, no ejecuta pipelines
            "save_state": True,  # Guarda estado para próximas verificaciones
        },
        tags=["monitor", "automated", "data-check"],
    )
    
    print("📋 Configuración del deployment:")
    print(f"   - Nombre: {deployment.name}")
    print(f"   - Intervalo: 2 minutos")
    print(f"   - Auto-trigger master: No")
    print(f"   - Work pool: default-pool")
    print()
    
    # Aplicar el deployment
    try:
        deployment_id = deployment.apply()
        print("✅ Deployment creado exitosamente!")
        print(f"   ID: {deployment_id}")
        print()
        print("El monitor se ejecutará automáticamente cada 2 minutos.")
        print("Puedes pausar/reanudar el deployment desde la UI de Prefect:")
        print("   → http://localhost:4200/deployments")
        print()
        print("=" * 70)
        return deployment_id
    except Exception as e:
        print(f"❌ Error al crear el deployment: {str(e)}")
        print()
        print("Esto puede ocurrir si:")
        print("  - El work pool 'default-pool' no existe")
        print("  - Prefect Server no está disponible")
        print("  - Ya existe un deployment con el mismo nombre")
        print()
        print("Intenta ejecutar el script de setup nuevamente.")
        print("=" * 70)
        raise


if __name__ == "__main__":
    create_monitor_deployment()

