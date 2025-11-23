"""
Módulo para carga de datos desde PostgreSQL.

Este módulo carga datos directamente desde la base de datos PostgreSQL
usando JDBC de Spark para procesamiento distribuido.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, trim
import os
from typing import List, Optional


class DataLoader:
    """Clase para cargar datos desde PostgreSQL usando Spark JDBC."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el cargador de datos.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

        # Configuración de PostgreSQL desde variables de entorno
        postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        postgres_db = os.getenv("POSTGRES_DB", "sales")
        postgres_user = os.getenv("POSTGRES_USER", "sales")
        postgres_password = os.getenv("POSTGRES_PASSWORD", "sales")

        self.jdbc_url = (
            f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
        )
        self.jdbc_properties = {
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver",
        }

        print(f"📡 Configuración JDBC: {postgres_host}:{postgres_port}/{postgres_db}")

    def load_categories(self) -> DataFrame:
        """
        Carga el catálogo de categorías desde PostgreSQL.

        Returns:
            DataFrame con categorías (category_id: int, category_name: string)
        """
        print("   → Cargando tabla 'categories' desde PostgreSQL...")

        df = self.spark.read.jdbc(
            url=self.jdbc_url, table="categories", properties=self.jdbc_properties
        )

        # Asegurar tipos correctos
        df = df.withColumn("category_id", col("category_id").cast("int"))

        return df

    def load_product_categories(self) -> DataFrame:
        """
        Carga la relación productos-categorías desde PostgreSQL.

        Returns:
            DataFrame con relación (product_id: int, category_id: int)
        """
        print("   → Cargando tabla 'product_categories' desde PostgreSQL...")

        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table="product_categories",
            properties=self.jdbc_properties,
        )

        # Asegurar tipos correctos
        df = df.withColumn("product_id", col("product_id").cast("int"))
        df = df.withColumn("category_id", col("category_id").cast("int"))

        return df

    def load_transactions(self, store_ids: Optional[List[str]] = None) -> DataFrame:
        """
        Carga transacciones desde PostgreSQL.

        Args:
            store_ids: Lista de IDs de tiendas a cargar (None = todas)

        Returns:
            DataFrame con transacciones
        """
        print("   → Cargando tabla 'transactions' desde PostgreSQL...")

        if store_ids is None:
            # Cargar todas las transacciones
            df = self.spark.read.jdbc(
                url=self.jdbc_url, table="transactions", properties=self.jdbc_properties
            )
        else:
            # Filtrar por tiendas específicas usando pushdown predicate
            store_ids_str = ",".join(str(sid) for sid in store_ids)
            query = f"(SELECT * FROM transactions WHERE store_id IN ({store_ids_str})) as filtered"

            df = self.spark.read.jdbc(
                url=self.jdbc_url, table=query, properties=self.jdbc_properties
            )

        # Asegurar tipos correctos
        df = df.withColumn("store_id", col("store_id").cast("int"))
        df = df.withColumn("customer_id", col("customer_id").cast("int"))

        return df

    def explode_transactions(self, df: DataFrame) -> DataFrame:
        """
        Explota las transacciones para tener un producto por fila.

        La columna 'products' contiene IDs de productos separados por espacio.
        Este método crea una fila por cada producto en cada transacción.

        Args:
            df: DataFrame de transacciones

        Returns:
            DataFrame con productos explodidos (una fila por producto)
        """
        print("   → Explodiendo productos (una fila por producto)...")

        # Separar la columna de productos en array
        df_with_array = df.withColumn(
            "product_array", split(trim(col("products")), " ")
        )

        # Explotar el array para tener un producto por fila
        df_exploded = df_with_array.withColumn(
            "product_id", explode(col("product_array"))
        ).select("transaction_date", "store_id", "customer_id", "product_id")

        # Convertir product_id a entero
        df_exploded = df_exploded.withColumn(
            "product_id", col("product_id").cast("int")
        )

        return df_exploded

    def get_available_stores(self) -> List[str]:
        """
        Obtiene la lista de tiendas disponibles en la base de datos.

        Returns:
            Lista de IDs de tiendas
        """
        print("   → Obteniendo lista de tiendas desde PostgreSQL...")

        query = (
            "(SELECT DISTINCT store_id FROM transactions ORDER BY store_id) as stores"
        )

        df = self.spark.read.jdbc(
            url=self.jdbc_url, table=query, properties=self.jdbc_properties
        )

        store_ids = [str(row.store_id) for row in df.collect()]

        print(f"   ✅ {len(store_ids)} tiendas encontradas: {', '.join(store_ids)}")

        return store_ids
