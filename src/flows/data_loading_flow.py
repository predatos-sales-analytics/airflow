"""
Flow de Prefect para cargar datos desde CSV a PostgreSQL.

Este flow automatiza el proceso de carga de datos que anteriormente se hacía
mediante scripts bash/bat manuales.
"""

import os
from pathlib import Path
from typing import List, Optional
import psycopg2
from psycopg2 import sql
from prefect import flow, task
from prefect.runtime import flow_run

import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flows.notifications import get_notification_service
from prefect_config import get_postgres_config, get_paths_config


@task(name="Verificar archivos CSV", retries=0)
def verify_csv_files() -> dict:
    """
    Verifica que los archivos CSV necesarios existan.

    Returns:
        Diccionario con rutas de archivos encontrados

    Raises:
        FileNotFoundError: Si faltan archivos necesarios
    """
    notifier = get_notification_service()
    notifier.log_task_start("Verificar archivos CSV")

    paths_config = get_paths_config()
    data_path = Path(paths_config["data_path"])

    # Archivos de referencia
    categories_file = data_path / "products" / "Categories.csv"
    product_categories_file = data_path / "products" / "ProductCategory.csv"

    # Archivos de transacciones conocidos
    transactions_dir = data_path / "transactions"
    transaction_files = [
        "102_Tran.csv",
        "103_Tran.csv",
        "107_Tran.csv",
        "110_Tran.csv",
    ]

    # Verificar archivos de referencia
    if not categories_file.exists():
        notifier.log_task_failure(
            "Verificar archivos CSV", FileNotFoundError(f"No existe: {categories_file}")
        )
        raise FileNotFoundError(f"No se encontró {categories_file}")

    if not product_categories_file.exists():
        notifier.log_task_failure(
            "Verificar archivos CSV",
            FileNotFoundError(f"No existe: {product_categories_file}"),
        )
        raise FileNotFoundError(f"No se encontró {product_categories_file}")

    # Verificar archivos de transacciones (al menos uno debe existir)
    existing_transaction_files = []
    for filename in transaction_files:
        file_path = transactions_dir / filename
        if file_path.exists():
            existing_transaction_files.append(str(file_path))

    if not existing_transaction_files:
        notifier.log_task_failure(
            "Verificar archivos CSV",
            FileNotFoundError("No se encontraron archivos de transacciones"),
        )
        raise FileNotFoundError(
            f"No se encontraron archivos de transacciones en {transactions_dir}"
        )

    result = {
        "categories_file": str(categories_file),
        "product_categories_file": str(product_categories_file),
        "transaction_files": existing_transaction_files,
    }

    notifier.log_task_success(
        "Verificar archivos CSV",
        f"Archivos encontrados: {len(existing_transaction_files)} transacciones",
    )

    return result


@task(name="Crear tablas en PostgreSQL", retries=2, retry_delay_seconds=5)
def create_tables():
    """Crea las tablas necesarias en PostgreSQL si no existen."""
    notifier = get_notification_service()
    notifier.log_task_start("Crear tablas en PostgreSQL")

    pg_config = get_postgres_config()

    try:
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"],
        )
        cursor = conn.cursor()

        # Crear tabla de categorías
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                category_id INT PRIMARY KEY,
                category_name TEXT
            );
        """
        )

        # Crear tabla de productos-categorías
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS product_categories (
                product_id INT,
                category_id INT
            );
        """
        )

        # Crear tabla de transacciones
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_date DATE,
                store_id INT,
                customer_id INT,
                products TEXT
            );
        """
        )

        conn.commit()
        cursor.close()
        conn.close()

        notifier.log_task_success("Crear tablas en PostgreSQL", "Tablas creadas/verificadas")

    except Exception as e:
        notifier.log_task_failure("Crear tablas en PostgreSQL", e)
        raise


@task(name="Limpiar tablas existentes", retries=2, retry_delay_seconds=5)
def truncate_tables():
    """Limpia todas las tablas antes de cargar nuevos datos."""
    notifier = get_notification_service()
    notifier.log_task_start("Limpiar tablas existentes")

    pg_config = get_postgres_config()

    try:
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"],
        )
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE categories;")
        cursor.execute("TRUNCATE TABLE product_categories;")
        cursor.execute("TRUNCATE TABLE transactions;")

        conn.commit()
        cursor.close()
        conn.close()

        notifier.log_task_success("Limpiar tablas existentes", "Tablas limpiadas")

    except Exception as e:
        notifier.log_task_failure("Limpiar tablas existentes", e)
        raise


@task(name="Cargar categorías", retries=2, retry_delay_seconds=5)
def load_categories(categories_file: str) -> int:
    """
    Carga el archivo de categorías a PostgreSQL.

    Args:
        categories_file: Ruta al archivo Categories.csv

    Returns:
        Número de registros cargados
    """
    notifier = get_notification_service()
    notifier.log_task_start("Cargar categorías")

    pg_config = get_postgres_config()

    try:
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"],
        )
        cursor = conn.cursor()

        # Cargar usando COPY
        with open(categories_file, "r", encoding="utf-8") as f:
            cursor.copy_expert(
                """
                COPY categories(category_id, category_name)
                FROM STDIN WITH (FORMAT csv, DELIMITER '|')
                """,
                f,
            )

        conn.commit()

        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM categories;")
        count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        notifier.log_task_success("Cargar categorías", f"{count} registros cargados")

        return count

    except Exception as e:
        notifier.log_task_failure("Cargar categorías", e)
        raise


@task(name="Cargar productos-categorías", retries=2, retry_delay_seconds=5)
def load_product_categories(product_categories_file: str) -> int:
    """
    Carga el archivo de productos-categorías a PostgreSQL.

    Args:
        product_categories_file: Ruta al archivo ProductCategory.csv

    Returns:
        Número de registros cargados
    """
    notifier = get_notification_service()
    notifier.log_task_start("Cargar productos-categorías")

    pg_config = get_postgres_config()

    try:
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"],
        )
        cursor = conn.cursor()

        # Cargar usando COPY (skipea el header)
        with open(product_categories_file, "r", encoding="utf-8") as f:
            # Saltar la primera línea (header)
            next(f)
            cursor.copy_expert(
                """
                COPY product_categories(product_id, category_id)
                FROM STDIN WITH (FORMAT csv, DELIMITER '|')
                """,
                f,
            )

        conn.commit()

        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM product_categories;")
        count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        notifier.log_task_success(
            "Cargar productos-categorías", f"{count} registros cargados"
        )

        return count

    except Exception as e:
        notifier.log_task_failure("Cargar productos-categorías", e)
        raise


@task(name="Cargar transacciones", retries=2, retry_delay_seconds=5)
def load_transactions(transaction_files: List[str]) -> dict:
    """
    Carga archivos de transacciones a PostgreSQL.

    Args:
        transaction_files: Lista de rutas a archivos de transacciones

    Returns:
        Diccionario con estadísticas de carga
    """
    notifier = get_notification_service()
    notifier.log_task_start("Cargar transacciones")

    pg_config = get_postgres_config()

    loaded_files = 0
    failed_files = 0
    failed_file_names = []

    try:
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"],
        )

        for file_path in transaction_files:
            file_name = Path(file_path).name
            try:
                cursor = conn.cursor()

                with open(file_path, "r", encoding="utf-8") as f:
                    cursor.copy_expert(
                        """
                        COPY transactions(transaction_date, store_id, customer_id, products)
                        FROM STDIN WITH (FORMAT csv, DELIMITER '|')
                        """,
                        f,
                    )

                conn.commit()
                cursor.close()

                loaded_files += 1
                notifier.log_info(f"      ✓ {file_name} cargado")

            except Exception as e:
                failed_files += 1
                failed_file_names.append(file_name)
                notifier.log_warning(f"      ✗ Error al cargar {file_name}: {str(e)}")

        # Contar total de transacciones
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM transactions;")
        total_transactions = cursor.fetchone()[0]
        cursor.close()

        conn.close()

        result = {
            "loaded_files": loaded_files,
            "failed_files": failed_files,
            "failed_file_names": failed_file_names,
            "total_transactions": total_transactions,
        }

        if loaded_files > 0:
            notifier.log_task_success(
                "Cargar transacciones",
                f"{loaded_files} archivos, {total_transactions} registros",
            )
        else:
            notifier.log_task_failure(
                "Cargar transacciones", Exception("No se cargaron archivos")
            )
            raise Exception("No se pudieron cargar archivos de transacciones")

        return result

    except Exception as e:
        notifier.log_task_failure("Cargar transacciones", e)
        raise


@flow(
    name="Carga de Datos CSV a PostgreSQL",
    description="Carga datos desde archivos CSV a la base de datos PostgreSQL",
    log_prints=True,
)
def data_loading_flow(truncate: bool = True):
    """
    Flow principal para cargar datos desde CSV a PostgreSQL.

    Args:
        truncate: Si True, limpia las tablas antes de cargar (default: True)

    Returns:
        Diccionario con estadísticas de carga
    """
    notifier = get_notification_service()
    notifier.log_flow_start("data_loading_flow", {"truncate": truncate})

    try:
        # 1. Verificar que existen los archivos CSV
        file_paths = verify_csv_files()

        # 2. Crear tablas si no existen
        create_tables()

        # 3. Limpiar tablas existentes si se solicita
        if truncate:
            truncate_tables()

        # 4. Cargar datos de referencia
        categories_count = load_categories(file_paths["categories_file"])
        product_categories_count = load_product_categories(
            file_paths["product_categories_file"]
        )

        # 5. Cargar transacciones
        transactions_stats = load_transactions(file_paths["transaction_files"])

        # Resumen final
        result = {
            "categories": categories_count,
            "product_categories": product_categories_count,
            "transactions": transactions_stats["total_transactions"],
            "transaction_files_loaded": transactions_stats["loaded_files"],
            "transaction_files_failed": transactions_stats["failed_files"],
        }

        notifier.log_info("\n" + "=" * 70)
        notifier.log_info("RESUMEN DE CARGA DE DATOS")
        notifier.log_info("=" * 70)
        notifier.log_info(f"Categorías:           {result['categories']}")
        notifier.log_info(f"Productos-Categorías: {result['product_categories']}")
        notifier.log_info(f"Transacciones:        {result['transactions']}")
        notifier.log_info(
            f"Archivos cargados:    {result['transaction_files_loaded']}"
        )
        if result["transaction_files_failed"] > 0:
            notifier.log_warning(
                f"Archivos fallidos:    {result['transaction_files_failed']}"
            )
        notifier.log_info("=" * 70)

        notifier.log_flow_success("data_loading_flow")

        return result

    except Exception as e:
        notifier.log_flow_failure("data_loading_flow", e)
        raise


if __name__ == "__main__":
    # Ejecutar el flow localmente
    data_loading_flow()

