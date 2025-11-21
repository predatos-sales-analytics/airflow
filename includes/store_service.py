from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_store_ids(conn_id: str = "sales_postgres") -> List[str]:
    """
    Obtiene la lista de tiendas distintas desde Postgres para habilitar
    el mapeo dinámico de tareas en Airflow.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    records = hook.get_records("SELECT DISTINCT store_id FROM transactions ORDER BY store_id")
    stores = [str(row[0]) for row in records]

    if not stores:
        raise ValueError("No se encontraron tiendas en la base de datos Postgres.")

    return stores

