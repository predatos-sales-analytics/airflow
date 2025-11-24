"""
Flow de Prefect para monitorear nuevos datos en PostgreSQL.

Este flow verifica periódicamente si hay nuevos datos en la base de datos
comparando el estado actual con el último estado conocido.
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect import flow, task
from flows.notifications import get_notification_service


STATE_FILE_PATH = os.path.join(
    os.getenv("OUTPUT_PATH", "/opt/prefect/work-dir/output"),
    "metadata",
    "data_monitor_state.json"
)


@task(name="Obtener estado actual de la base de datos", retries=3, retry_delay_seconds=10)
def get_current_db_state() -> Dict[str, Any]:
    """
    Obtiene el estado actual de la base de datos PostgreSQL.

    Returns:
        Diccionario con el estado actual:
        - transactions_count: Número total de transacciones
        - last_transaction_date: Fecha de la última transacción
        - categories_count: Número de categorías
        - products_count: Número de productos
        - check_timestamp: Timestamp de la verificación
    """
    notifier = get_notification_service()
    notifier.log_task_start("Obtener estado actual de la base de datos")

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

        # Obtener conteo de transacciones
        cursor.execute("SELECT COUNT(*) FROM transactions;")
        transactions_count = cursor.fetchone()[0]

        # Obtener fecha de la última transacción
        cursor.execute(
            "SELECT MAX(transaction_date) FROM transactions;"
        )
        last_transaction_date = cursor.fetchone()[0]
        
        # Convertir a string si no es None
        if last_transaction_date:
            last_transaction_date = str(last_transaction_date)

        # Obtener conteo de categorías
        cursor.execute("SELECT COUNT(*) FROM categories;")
        categories_count = cursor.fetchone()[0]

        # Obtener conteo de productos
        cursor.execute("SELECT COUNT(*) FROM product_categories;")
        products_count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        state = {
            "transactions_count": transactions_count,
            "last_transaction_date": last_transaction_date,
            "categories_count": categories_count,
            "products_count": products_count,
            "check_timestamp": datetime.now().isoformat(),
        }

        notifier.log_info(f"      ✓ Transacciones: {transactions_count}")
        notifier.log_info(f"      ✓ Última transacción: {last_transaction_date or 'N/A'}")
        notifier.log_info(f"      ✓ Categorías: {categories_count}")
        notifier.log_info(f"      ✓ Productos: {products_count}")

        notifier.log_task_success(
            "Obtener estado actual de la base de datos",
            f"{transactions_count} transacciones"
        )

        return state

    except Exception as e:
        notifier.log_task_failure("Obtener estado actual de la base de datos", e)
        raise


@task(name="Cargar último estado conocido")
def load_last_known_state() -> Optional[Dict[str, Any]]:
    """
    Carga el último estado conocido desde el archivo de estado.

    Returns:
        Diccionario con el último estado conocido o None si no existe
    """
    notifier = get_notification_service()
    notifier.log_task_start("Cargar último estado conocido")

    try:
        state_file = Path(STATE_FILE_PATH)
        
        if not state_file.exists():
            notifier.log_warning("      ⚠️  No existe archivo de estado previo")
            notifier.log_task_success("Cargar último estado conocido", "Primera ejecución")
            return None

        with open(state_file, "r", encoding="utf-8") as f:
            last_state = json.load(f)

        notifier.log_info(
            f"      ✓ Último estado: {last_state.get('transactions_count', 0)} transacciones"
        )
        notifier.log_info(
            f"      ✓ Última verificación: {last_state.get('check_timestamp', 'N/A')}"
        )

        notifier.log_task_success("Cargar último estado conocido", "Estado cargado")
        return last_state

    except Exception as e:
        notifier.log_task_failure("Cargar último estado conocido", e)
        # No lanzamos excepción, devolvemos None para tratar como primera ejecución
        return None


@task(name="Guardar estado actual")
def save_current_state(state: Dict[str, Any]) -> None:
    """
    Guarda el estado actual en el archivo de estado.

    Args:
        state: Diccionario con el estado actual a guardar
    """
    notifier = get_notification_service()
    notifier.log_task_start("Guardar estado actual")

    try:
        state_file = Path(STATE_FILE_PATH)
        state_file.parent.mkdir(parents=True, exist_ok=True)

        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)

        notifier.log_task_success(
            "Guardar estado actual",
            f"Estado guardado en {state_file}"
        )

    except Exception as e:
        notifier.log_task_failure("Guardar estado actual", e)
        # No lanzamos excepción, solo advertimos
        notifier.log_warning("      ⚠️  No se pudo guardar el estado")


@task(name="Comparar estados y detectar cambios")
def compare_states(
    current_state: Dict[str, Any],
    last_state: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compara el estado actual con el último estado conocido.

    Args:
        current_state: Estado actual de la base de datos
        last_state: Último estado conocido (o None si es la primera ejecución)

    Returns:
        Diccionario con información sobre los cambios detectados:
        - has_new_data: True si hay datos nuevos
        - new_transactions: Número de transacciones nuevas
        - new_categories: Número de categorías nuevas
        - new_products: Número de productos nuevos
        - date_changed: True si cambió la fecha de la última transacción
    """
    notifier = get_notification_service()
    notifier.log_task_start("Comparar estados y detectar cambios")

    # Si no hay estado previo, consideramos que hay datos "nuevos"
    if last_state is None:
        notifier.log_info("      ℹ️  Primera ejecución - No hay estado previo para comparar")
        
        changes = {
            "has_new_data": True,
            "is_first_run": True,
            "new_transactions": current_state["transactions_count"],
            "new_categories": current_state["categories_count"],
            "new_products": current_state["products_count"],
            "date_changed": False,
        }
        
        notifier.log_task_success("Comparar estados", "Primera ejecución detectada")
        return changes

    # Comparar conteos
    new_transactions = current_state["transactions_count"] - last_state.get("transactions_count", 0)
    new_categories = current_state["categories_count"] - last_state.get("categories_count", 0)
    new_products = current_state["products_count"] - last_state.get("products_count", 0)
    
    # Comparar fechas
    date_changed = (
        current_state["last_transaction_date"] != last_state.get("last_transaction_date")
    )

    # Determinar si hay datos nuevos
    has_new_data = (new_transactions > 0 or new_categories > 0 or new_products > 0 or date_changed)

    changes = {
        "has_new_data": has_new_data,
        "is_first_run": False,
        "new_transactions": new_transactions,
        "new_categories": new_categories,
        "new_products": new_products,
        "date_changed": date_changed,
        "current_total_transactions": current_state["transactions_count"],
        "previous_total_transactions": last_state.get("transactions_count", 0),
    }

    # Logging de cambios
    if has_new_data:
        notifier.log_info("      🔔 ¡Cambios detectados!")
        if new_transactions > 0:
            notifier.log_info(f"         → Nuevas transacciones: +{new_transactions}")
        if new_categories > 0:
            notifier.log_info(f"         → Nuevas categorías: +{new_categories}")
        if new_products > 0:
            notifier.log_info(f"         → Nuevos productos: +{new_products}")
        if date_changed:
            notifier.log_info(f"         → Fecha actualizada: {current_state['last_transaction_date']}")
    else:
        notifier.log_info("      ℹ️  No hay cambios desde la última verificación")

    notifier.log_task_success(
        "Comparar estados",
        "Cambios detectados" if has_new_data else "Sin cambios"
    )

    return changes


@task(name="Notificar cambios detectados")
def notify_changes(changes: Dict[str, Any]) -> None:
    """
    Notifica sobre los cambios detectados.

    Args:
        changes: Diccionario con información sobre los cambios
    """
    notifier = get_notification_service()
    
    if changes["is_first_run"]:
        notifier.log_info("\n" + "=" * 70)
        notifier.log_info("📊 PRIMERA EJECUCIÓN DEL MONITOR")
        notifier.log_info("=" * 70)
        notifier.log_info(f"Transacciones detectadas: {changes['new_transactions']}")
        notifier.log_info(f"Categorías detectadas: {changes['new_categories']}")
        notifier.log_info(f"Productos detectados: {changes['new_products']}")
        notifier.log_info("=" * 70)
        return

    if not changes["has_new_data"]:
        notifier.log_info("\n✅ Base de datos sin cambios desde la última verificación")
        return

    notifier.log_info("\n" + "=" * 70)
    notifier.log_info("🔔 NUEVOS DATOS DETECTADOS EN LA BASE DE DATOS")
    notifier.log_info("=" * 70)
    
    if changes["new_transactions"] > 0:
        notifier.log_info(
            f"📈 Nuevas transacciones: +{changes['new_transactions']} "
            f"(Total: {changes['current_total_transactions']})"
        )
    
    if changes["new_categories"] > 0:
        notifier.log_info(f"🏷️  Nuevas categorías: +{changes['new_categories']}")
    
    if changes["new_products"] > 0:
        notifier.log_info(f"📦 Nuevos productos: +{changes['new_products']}")
    
    notifier.log_info("=" * 70)
    notifier.log_info("💡 Sugerencia: Ejecuta el master flow para actualizar los análisis")
    notifier.log_info("   Comando: ./scripts/[windows|linux]/run_prefect_flow.[bat|sh] master")
    notifier.log_info("=" * 70)


@flow(
    name="Monitor de Nuevos Datos",
    description="Monitorea la base de datos PostgreSQL para detectar nuevos datos",
    log_prints=True,
)
def data_monitor_flow(
    auto_trigger_master: bool = False,
    save_state: bool = True
) -> Dict[str, Any]:
    """
    Flow que monitorea la base de datos para detectar nuevos datos.

    Args:
        auto_trigger_master: Si True, dispara automáticamente el master flow cuando hay datos nuevos
        save_state: Si True, guarda el estado actual para la próxima verificación

    Returns:
        Diccionario con información sobre la ejecución:
        - current_state: Estado actual de la base de datos
        - changes: Cambios detectados
        - master_triggered: True si se disparó el master flow
    """
    notifier = get_notification_service()
    start_time = datetime.now()

    notifier.log_info("\n" + "=" * 70)
    notifier.log_info("🔍 MONITOR DE NUEVOS DATOS EN BASE DE DATOS")
    notifier.log_info("=" * 70)

    notifier.log_flow_start(
        "data_monitor_flow",
        {
            "auto_trigger_master": auto_trigger_master,
            "save_state": save_state,
        }
    )

    try:
        # Paso 1: Obtener estado actual de la BD
        notifier.log_info("\n📊 Paso 1: Consultando estado actual de PostgreSQL...")
        current_state = get_current_db_state()

        # Paso 2: Cargar último estado conocido
        notifier.log_info("\n📂 Paso 2: Cargando último estado conocido...")
        last_state = load_last_known_state()

        # Paso 3: Comparar estados
        notifier.log_info("\n🔍 Paso 3: Comparando estados...")
        changes = compare_states(current_state, last_state)

        # Paso 4: Notificar cambios
        notifier.log_info("\n📢 Paso 4: Notificando cambios...")
        notify_changes(changes)

        # Paso 5: Guardar estado actual (si está habilitado)
        if save_state:
            notifier.log_info("\n💾 Paso 5: Guardando estado actual...")
            save_current_state(current_state)
        else:
            notifier.log_info("\n⏭️  Paso 5: Guardado de estado deshabilitado")

        # Paso 6: Disparar master flow si está configurado y hay datos nuevos
        master_triggered = False
        if auto_trigger_master and changes["has_new_data"] and not changes["is_first_run"]:
            notifier.log_info("\n🚀 Paso 6: Disparando master flow automáticamente...")
            try:
                from flows.master_flow import master_flow
                
                notifier.log_info("   Iniciando ejecución del master flow...")
                master_flow()
                master_triggered = True
                notifier.log_info("   ✅ Master flow completado exitosamente")
            except Exception as e:
                notifier.log_error(f"   ❌ Error al ejecutar master flow: {str(e)}")
                # No lanzamos la excepción para no fallar el monitor
        elif auto_trigger_master and changes["is_first_run"]:
            notifier.log_info("\n⏭️  Paso 6: Master flow no disparado (primera ejecución)")
        else:
            notifier.log_info("\n⏭️  Paso 6: Master flow no configurado para ejecución automática")

        # Resumen final
        duration = (datetime.now() - start_time).total_seconds()

        notifier.log_info("\n" + "=" * 70)
        notifier.log_info("✅ MONITOR COMPLETADO")
        notifier.log_info("=" * 70)
        notifier.log_info(f"Duración: {duration:.2f} segundos")
        notifier.log_info(f"Datos nuevos: {'Sí' if changes['has_new_data'] else 'No'}")
        if master_triggered:
            notifier.log_info("Master flow: Ejecutado automáticamente")
        notifier.log_info("=" * 70)

        result = {
            "current_state": current_state,
            "changes": changes,
            "master_triggered": master_triggered,
            "duration_seconds": duration,
        }

        notifier.log_flow_success("data_monitor_flow", duration)

        return result

    except Exception as e:
        notifier.log_flow_failure("data_monitor_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el monitor localmente
    # Por defecto no dispara el master flow automáticamente
    data_monitor_flow(auto_trigger_master=False, save_state=True)

