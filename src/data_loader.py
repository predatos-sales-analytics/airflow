"""
Módulo para carga y preparación de datos.

Este módulo proporciona funcionalidades para cargar y transformar
datasets desde archivos CSV utilizando Apache Spark.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from typing import List, Optional


class DataLoader:
    """Clase para cargar y preparar datasets"""

    def __init__(self, spark: SparkSession, data_path: Optional[str] = None):
        """
        Inicializa el cargador de datos

        Args:
            spark: Sesión de Spark
            data_path: Ruta base de los datos
        """
        self.spark = spark
        self.data_path = data_path or os.getenv("DATA_PATH", "data")
        self.data_source = os.getenv("DATA_SOURCE", "files").lower()
        self.use_postgres = self.data_source == "postgres"
        self.postgres_schema = os.getenv("POSTGRES_SCHEMA", "public")
        postgres_db = os.getenv("POSTGRES_DB", "sales")
        postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        self.jdbc_url = os.getenv(
            "POSTGRES_JDBC_URL", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
        )
        self.jdbc_properties = {
            "user": os.getenv("POSTGRES_USER", "sales"),
            "password": os.getenv("POSTGRES_PASSWORD", "sales"),
            "driver": os.getenv("POSTGRES_JDBC_DRIVER", "org.postgresql.Driver"),
        }

    def load_categories(self) -> DataFrame:
        """
        Carga el catálogo de categorías

        Returns:
            DataFrame con categorías
        """
        if self.use_postgres:
            return self._read_postgres_table("categories")

        categories_path = os.path.join(self.data_path, "products", "Categories.csv")

        schema = StructType(
            [
                StructField("category_id", IntegerType(), True),
                StructField("category_name", StringType(), True),
            ]
        )

        return self.spark.read.csv(categories_path, sep="|", header=False, schema=schema)

    def load_product_categories(self) -> DataFrame:
        """
        Carga la relación productos-categorías

        Returns:
            DataFrame con relación productos-categorías
        """
        if self.use_postgres:
            return self._read_postgres_table("product_categories")

        product_cat_path = os.path.join(self.data_path, "products", "ProductCategory.csv")

        df = self.spark.read.csv(product_cat_path, sep="|", header=True, inferSchema=True)

        return df.withColumnRenamed("v.Code_pr", "product_id").withColumnRenamed("v.code", "category_id")

    def load_transactions(self, store_ids: Optional[List[str]] = None) -> DataFrame:
        """
        Carga archivos de transacciones

        Args:
            store_ids: Lista de IDs de tiendas a cargar. Si es None, carga todas.

        Returns:
            DataFrame con transacciones
        """
        if self.use_postgres:
            return self._load_transactions_from_postgres(store_ids)

        transactions_path = os.path.join(self.data_path, "transactions")

        schema = StructType(
            [
                StructField("transaction_date", StringType(), True),
                StructField("store_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("products", StringType(), True),
            ]
        )

        if store_ids is None:
            available_stores = self.get_available_stores()
            files_to_load = [os.path.join(transactions_path, f"{store_id}_Tran.csv") for store_id in available_stores]
        else:
            files_to_load = [os.path.join(transactions_path, f"{store_id}_Tran.csv") for store_id in store_ids]

        dataframes = []
        for file_path in files_to_load:
            if os.path.exists(file_path):
                df_temp = self.spark.read.csv(file_path, sep="|", header=False, schema=schema)
                dataframes.append(df_temp)

        if not dataframes:
            raise FileNotFoundError("No se encontraron archivos de transacciones")

        df = dataframes[0]
        for df_temp in dataframes[1:]:
            df = df.union(df_temp)

        return df

    def explode_transactions(self, df: DataFrame) -> DataFrame:
        """
        Explode las transacciones para tener un producto por fila

        Args:
            df: DataFrame de transacciones

        Returns:
            DataFrame con productos explodidos
        """
        # Separar la columna de productos en array
        df_exploded = df.withColumn("product_array", split(trim(col("products")), " "))

        # Explotar el array para tener un producto por fila
        df_exploded = df_exploded.withColumn(
            "product_id", explode(col("product_array"))
        ).drop("products", "product_array")

        # Convertir product_id a integer
        df_exploded = df_exploded.withColumn(
            "product_id", col("product_id").cast(IntegerType())
        )

        return df_exploded

    def get_available_stores(self) -> List[str]:
        """
        Obtiene la lista de tiendas disponibles en los datos

        Returns:
            Lista de IDs de tiendas
        """
        if self.use_postgres:
            query = f"(SELECT DISTINCT store_id FROM {self.postgres_schema}.transactions) stores"
            df = self._read_postgres_query(query)
            return sorted([str(row.store_id) for row in df.collect()])

        transactions_path = os.path.join(self.data_path, "transactions")
        files = os.listdir(transactions_path)

        store_ids = []
        for file in files:
            if file.endswith("_Tran.csv"):
                store_id = file.replace("_Tran.csv", "")
                store_ids.append(store_id)

        return sorted(store_ids)

    def _read_postgres_table(self, table_name: str) -> DataFrame:
        table = f"{self.postgres_schema}.{table_name}"
        return self.spark.read.jdbc(url=self.jdbc_url, table=table, properties=self.jdbc_properties)

    def _read_postgres_query(self, query: str) -> DataFrame:
        return self.spark.read.jdbc(url=self.jdbc_url, table=f"({query}) as q", properties=self.jdbc_properties)

    def _load_transactions_from_postgres(self, store_ids: Optional[List[str]]) -> DataFrame:
        base_query = (
            f"SELECT transaction_date::text as transaction_date, store_id, customer_id, products "
            f"FROM {self.postgres_schema}.transactions"
        )

        if store_ids:
            sanitized_ids = ",".join([str(int(store_id)) for store_id in store_ids])
            base_query += f" WHERE store_id IN ({sanitized_ids})"

        wrapped_query = f"({base_query}) AS transactions"
        return self.spark.read.jdbc(url=self.jdbc_url, table=wrapped_query, properties=self.jdbc_properties)

