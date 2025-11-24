"""
Configuración de Prefect para el proyecto.

Este módulo contiene utilidades para configurar la conexión a Prefect Server,
work pools, y deployment de flows.
"""

import os
from typing import Optional


def get_prefect_api_url() -> str:
    """
    Obtiene la URL de la API de Prefect.

    Returns:
        URL de la API de Prefect
    """
    return os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")


def get_work_pool_name() -> str:
    """
    Obtiene el nombre del work pool por defecto.

    Returns:
        Nombre del work pool
    """
    return os.getenv("PREFECT_WORK_POOL", "default-pool")


def get_postgres_config() -> dict:
    """
    Obtiene la configuración de PostgreSQL desde variables de entorno.

    Returns:
        Diccionario con configuración de PostgreSQL
    """
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "sales"),
        "user": os.getenv("POSTGRES_USER", "sales"),
        "password": os.getenv("POSTGRES_PASSWORD", "sales"),
    }


def get_spark_config() -> dict:
    """
    Obtiene la configuración de Spark desde variables de entorno.

    Returns:
        Diccionario con configuración de Spark
    """
    return {
        "master_url": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "pyspark_python": os.getenv("PYSPARK_PYTHON", "/usr/bin/python3"),
    }


def get_paths_config() -> dict:
    """
    Obtiene las rutas de directorios del proyecto.

    Returns:
        Diccionario con rutas de directorios
    """
    return {
        "data_path": os.getenv("DATA_PATH", "/opt/prefect/work-dir/data"),
        "output_path": os.getenv("OUTPUT_PATH", "/opt/prefect/work-dir/output"),
        "frontend_path": os.getenv(
            "FRONTEND_PATH", "/opt/prefect/work-dir/frontend"
        ),
        "logs_path": os.getenv("LOGS_PATH", "/opt/prefect/work-dir/logs"),
    }


def print_config():
    """Imprime la configuración actual del proyecto."""
    print("=" * 70)
    print("CONFIGURACIÓN DE PREFECT")
    print("=" * 70)
    print(f"API URL: {get_prefect_api_url()}")
    print(f"Work Pool: {get_work_pool_name()}")
    print()
    print("PostgreSQL:")
    pg_config = get_postgres_config()
    print(f"  Host: {pg_config['host']}:{pg_config['port']}")
    print(f"  Database: {pg_config['database']}")
    print(f"  User: {pg_config['user']}")
    print()
    print("Spark:")
    spark_config = get_spark_config()
    print(f"  Master URL: {spark_config['master_url']}")
    print()
    print("Rutas:")
    paths = get_paths_config()
    for key, value in paths.items():
        print(f"  {key}: {value}")
    print("=" * 70)


if __name__ == "__main__":
    print_config()

