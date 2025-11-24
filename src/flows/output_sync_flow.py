"""
Flow de Prefect para sincronizar outputs JSON al frontend.

Copia los archivos JSON generados por los pipelines al directorio
público del frontend para que puedan ser consumidos por la aplicación React.
"""

import os
import sys
import shutil
from pathlib import Path
from typing import List, Dict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prefect import flow, task
from flows.notifications import get_notification_service
from prefect_config import get_paths_config


@task(name="Validar archivos JSON de output", retries=0)
def validate_output_files() -> Dict[str, List[str]]:
    """
    Valida que existan los archivos JSON en el directorio de output.

    Returns:
        Diccionario con rutas de archivos encontrados por categoría

    Raises:
        FileNotFoundError: Si no se encuentran archivos JSON
    """
    notifier = get_notification_service()
    notifier.log_task_start("Validar archivos JSON de output")

    paths_config = get_paths_config()
    output_path = Path(paths_config["output_path"])

    if not output_path.exists():
        error = FileNotFoundError(f"No existe el directorio de output: {output_path}")
        notifier.log_task_failure("Validar archivos JSON de output", error)
        raise error

    # Buscar archivos JSON en subdirectorios
    file_map = {
        "summary": [],
        "analytics": [],
        "advanced": [],
        "recommendations": [],
    }

    # Summary files
    summary_dir = output_path / "summary"
    if summary_dir.exists():
        file_map["summary"] = [
            str(f) for f in summary_dir.glob("*.json") if f.is_file()
        ]

    # Analytics files
    analytics_dir = output_path / "analytics"
    if analytics_dir.exists():
        file_map["analytics"] = [
            str(f) for f in analytics_dir.glob("*.json") if f.is_file()
        ]

    # Advanced files (clustering)
    advanced_dir = output_path / "advanced"
    if advanced_dir.exists():
        for subdir in advanced_dir.iterdir():
            if subdir.is_dir():
                file_map["advanced"].extend(
                    [str(f) for f in subdir.glob("*.json") if f.is_file()]
                )

    # Recommendations files
    recommendations_dir = output_path / "recommendations"
    if recommendations_dir.exists():
        file_map["recommendations"] = [
            str(f) for f in recommendations_dir.glob("*.json") if f.is_file()
        ]

    # Contar total de archivos
    total_files = sum(len(files) for files in file_map.values())

    if total_files == 0:
        error = FileNotFoundError(
            f"No se encontraron archivos JSON en {output_path}"
        )
        notifier.log_task_failure("Validar archivos JSON de output", error)
        raise error

    notifier.log_task_success(
        "Validar archivos JSON de output", f"{total_files} archivos encontrados"
    )

    return file_map


@task(name="Sincronizar archivos al frontend", retries=2, retry_delay_seconds=5)
def sync_files_to_frontend(file_map: Dict[str, List[str]]) -> Dict[str, int]:
    """
    Copia los archivos JSON al directorio público del frontend.

    Args:
        file_map: Diccionario con rutas de archivos por categoría

    Returns:
        Diccionario con estadísticas de sincronización
    """
    notifier = get_notification_service()
    notifier.log_task_start("Sincronizar archivos al frontend")

    paths_config = get_paths_config()
    frontend_path = Path(paths_config["frontend_path"])

    # Crear directorio si no existe
    frontend_path.mkdir(parents=True, exist_ok=True)

    copied_files = 0
    failed_files = 0

    try:
        for category, files in file_map.items():
            if not files:
                continue

            # Crear subdirectorio en frontend
            category_dir = frontend_path / category
            category_dir.mkdir(parents=True, exist_ok=True)

            for file_path in files:
                try:
                    source = Path(file_path)
                    
                    # Determinar destino
                    if category == "advanced":
                        # Para advanced, mantener la estructura de subdirectorios
                        # e.g., advanced/clustering/file.json
                        relative_path = source.relative_to(
                            Path(paths_config["output_path"]) / "advanced"
                        )
                        dest = category_dir / relative_path
                        dest.parent.mkdir(parents=True, exist_ok=True)
                    else:
                        dest = category_dir / source.name

                    # Copiar archivo
                    shutil.copy2(source, dest)
                    copied_files += 1

                    notifier.log_info(f"      ✓ Copiado: {source.name} -> {category}/")

                except Exception as e:
                    failed_files += 1
                    notifier.log_warning(f"      ✗ Error al copiar {source.name}: {e}")

        result = {
            "copied_files": copied_files,
            "failed_files": failed_files,
        }

        if copied_files > 0:
            notifier.log_task_success(
                "Sincronizar archivos al frontend",
                f"{copied_files} archivos copiados a {frontend_path}",
            )
        else:
            error = Exception("No se copiaron archivos")
            notifier.log_task_failure("Sincronizar archivos al frontend", error)
            raise error

        return result

    except Exception as e:
        notifier.log_task_failure("Sincronizar archivos al frontend", e)
        raise


@flow(
    name="Sincronización de Outputs al Frontend",
    description="Copia archivos JSON de output al directorio público del frontend",
    log_prints=True,
)
def output_sync_flow():
    """
    Flow principal para sincronizar outputs al frontend.

    Returns:
        Diccionario con estadísticas de sincronización
    """
    notifier = get_notification_service()
    notifier.log_flow_start("output_sync_flow")

    try:
        # 1. Validar que existan archivos JSON
        file_map = validate_output_files()

        # 2. Copiar archivos al frontend
        sync_stats = sync_files_to_frontend(file_map)

        # Resumen
        notifier.log_info("\n" + "=" * 70)
        notifier.log_info("RESUMEN DE SINCRONIZACIÓN")
        notifier.log_info("=" * 70)
        notifier.log_info(f"Archivos copiados:   {sync_stats['copied_files']}")
        if sync_stats["failed_files"] > 0:
            notifier.log_warning(f"Archivos fallidos:   {sync_stats['failed_files']}")
        notifier.log_info("=" * 70)

        notifier.log_flow_success("output_sync_flow")

        return sync_stats

    except Exception as e:
        notifier.log_flow_failure("output_sync_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el flow localmente
    output_sync_flow()

