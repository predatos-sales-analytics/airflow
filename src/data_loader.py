"""
Módulo para carga de datos desde PostgreSQL.

Este módulo carga datos directamente desde la base de datos PostgreSQL
usando JDBC de Spark para procesamiento distribuido.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, trim
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os
from typing import List, Optional


class DataLoader:
    """Clase para cargar datos desde PostgreSQL o S3 usando Spark."""

    def __init__(
        self,
        spark: SparkSession,
        source_type: Optional[str] = None,
        s3_base_path: Optional[str] = None,
    ):
        """
        Inicializa el cargador de datos.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark
        self.source_type = (source_type or os.getenv("DATA_SOURCE", "postgres")).lower()

        if self.source_type not in {"postgres", "s3"}:
            raise ValueError(
                "DATA_SOURCE debe ser 'postgres' o 's3'. Valor recibido: "
                f"{self.source_type}"
            )

        if self.source_type == "postgres":
            self._setup_postgres()
        else:
            self._setup_s3_paths(s3_base_path)

    def _setup_postgres(self):
        """Lee configuración JDBC desde variables de entorno."""
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

    def _setup_s3_paths(self, base_path: Optional[str]):
        """Configura rutas de entrada cuando la fuente es S3."""
        self.s3_base_path = (base_path or os.getenv("S3_DATA_BASE_URI", "")).rstrip("/")
        if not self.s3_base_path:
            raise ValueError(
                "Debes definir S3_DATA_BASE_URI cuando DATA_SOURCE='s3'. "
                "Ejemplo: s3://exec-summary-frank-bullfrog/raw-data"
            )

        def _default(path_suffix: str, env_var: str) -> str:
            return os.getenv(env_var, f"{self.s3_base_path}/{path_suffix}").rstrip("/")

        self.s3_transactions_path = _default("transactions", "S3_TRANSACTIONS_PATH")
        self.s3_categories_path = os.getenv(
            "S3_CATEGORIES_PATH", f"{self.s3_base_path}/products/Categories.csv"
        )
        self.s3_product_categories_path = os.getenv(
            "S3_PRODUCT_CATEGORIES_PATH",
            f"{self.s3_base_path}/products/ProductCategory.csv",
        )

        self.csv_separator = os.getenv("S3_FILE_SEPARATOR", "|")

        print("📦 Configuración S3:")
        print(f"   • Base path: {self.s3_base_path}")
        print(f"   • Transacciones: {self.s3_transactions_path}")
        print(f"   • Categorías: {self.s3_categories_path}")
        print(f"   • Producto-Categoría: {self.s3_product_categories_path}")

    def _read_csv(
        self,
        path: str,
        schema: StructType,
        header: bool = False,
    ) -> DataFrame:
        """Encapsula la lectura de archivos CSV delimitados."""
        return (
            self.spark.read.option("sep", self.csv_separator)
            .option("header", "true" if header else "false")
            .schema(schema)
            .csv(path)
        )

    def _load_categories_from_s3(self) -> DataFrame:
        schema = StructType(
            [
                StructField("category_id", IntegerType(), True),
                StructField("category_name", StringType(), True),
            ]
        )

        return self._read_csv(self.s3_categories_path, schema)

    def _load_product_categories_from_s3(self) -> DataFrame:
        schema = StructType(
            [
                StructField("product_id", IntegerType(), True),
                StructField("category_id", IntegerType(), True),
            ]
        )

        return self._read_csv(
            self.s3_product_categories_path,
            schema,
            header=True,
        )

    def _load_transactions_from_s3(self) -> DataFrame:
        schema = StructType(
            [
                StructField("transaction_date", StringType(), True),
                StructField("store_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("products", StringType(), True),
            ]
        )

        path = f"{self.s3_transactions_path}/*.csv"
        return self._read_csv(path, schema)

    def load_categories(self) -> DataFrame:
        """
        Carga el catálogo de categorías desde PostgreSQL.

        Returns:
            DataFrame con categorías (category_id: int, category_name: string)
        """
        print("   → Cargando tabla 'categories' desde PostgreSQL...")

        if self.source_type == "postgres":
            df = self.spark.read.jdbc(
                url=self.jdbc_url, table="categories", properties=self.jdbc_properties
            )
        else:
            df = self._load_categories_from_s3()

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

        if self.source_type == "postgres":
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="product_categories",
                properties=self.jdbc_properties,
            )
        else:
            df = self._load_product_categories_from_s3()

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

        if self.source_type == "postgres":
            if store_ids is None:
                # Cargar todas las transacciones
                df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="transactions",
                    properties=self.jdbc_properties,
                )
            else:
                # Filtrar por tiendas específicas usando pushdown predicate
                store_ids_str = ",".join(str(sid) for sid in store_ids)
                query = f"(SELECT * FROM transactions WHERE store_id IN ({store_ids_str})) as filtered"

                df = self.spark.read.jdbc(
                    url=self.jdbc_url, table=query, properties=self.jdbc_properties
                )
        else:
            df = self._load_transactions_from_s3()
            if store_ids:
                store_ids_int = [int(sid) for sid in store_ids]
                df = df.filter(col("store_id").isin(store_ids_int))

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

        if self.source_type == "postgres":
            query = (
                "(SELECT DISTINCT store_id FROM transactions ORDER BY store_id) as stores"
            )

            df = self.spark.read.jdbc(
                url=self.jdbc_url, table=query, properties=self.jdbc_properties
            )
        else:
            df = self._load_transactions_from_s3().select("store_id").distinct()

        store_ids = [str(row.store_id) for row in df.collect()]

        print(f"   ✅ {len(store_ids)} tiendas encontradas: {', '.join(store_ids)}")

        return store_ids
