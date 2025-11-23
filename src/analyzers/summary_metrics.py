"""
Módulo para calcular métricas del Resumen Ejecutivo.

Implementa las métricas solicitadas en el enunciado:
- Total de ventas (unidades vendidas)
- Número de transacciones
- Top 10 productos
- Top 10 clientes  
- Días pico de compra
- Categorías más rentables
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    desc,
    size,
    split,
    trim,
    to_date,
    date_format,
    explode as spark_explode,
    coalesce,
    lit,
)
from typing import Dict, Any


class SummaryMetrics:
    """Clase para calcular métricas del resumen ejecutivo."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el calculador de métricas.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

    def calculate_basic_metrics(self, df_transactions: DataFrame) -> Dict[str, int]:
        """
        Calcula métricas básicas: total ventas y número transacciones.

        Args:
            df_transactions: DataFrame de transacciones

        Returns:
            Diccionario con métricas básicas
        """
        print("\n📊 Calculando métricas básicas...")
        print("   [1/2] Contando transacciones y calculando ventas...")

        # Calcular ambas métricas en una sola pasada
        result = df_transactions.select(
            count("*").alias("total_transactions"),
            spark_sum(size(split(trim(col("products")), " "))).alias("total_sales"),
        ).first()

        total_transactions = result["total_transactions"]
        total_sales = result["total_sales"]

        metrics = {
            "total_transactions": int(total_transactions),
            "total_sales_units": int(total_sales),
        }

        print(f"   ✅ Total transacciones: {total_transactions:,}")
        print(f"   ✅ Total unidades vendidas: {total_sales:,}")

        return metrics

    def get_top_products(
        self,
        df_transactions_exploded: DataFrame,
        df_product_categories: DataFrame,
        df_categories: DataFrame,
        top_n: int = 10,
    ) -> DataFrame:
        """
        Obtiene los productos más vendidos con su categoría.

        Args:
            df_transactions_exploded: DataFrame de transacciones explodidas
            df_product_categories: DataFrame de relación producto-categoría
            df_categories: DataFrame de categorías
            top_n: Número de productos a retornar

        Returns:
            DataFrame con top productos incluyendo categoría
        """
        print(f"\n🏆 Calculando Top {top_n} productos...")

        # Calcular cantidad vendida por producto
        df_product_counts = df_transactions_exploded.groupBy("product_id").agg(
            count("*").alias("total_sold")
        )

        # Hacer join con categorías para obtener información de categoría
        df_products_with_category = (
            df_product_counts.join(df_product_categories, "product_id", "left")
            .join(df_categories, "category_id", "left")
            .select(
                "product_id",
                col("category_id").cast("int").alias("category_id"),
                col("category_name"),
                "total_sold",
            )
        )

        # Obtener top N productos ordenados por cantidad vendida
        df_top_products = df_products_with_category.orderBy(desc("total_sold")).limit(
            top_n
        )

        df_top_products.show(truncate=False)

        return df_top_products

    def get_top_customers(
        self, df_transactions: DataFrame, top_n: int = 10
    ) -> DataFrame:
        """
        Obtiene los clientes con más compras.

        Args:
            df_transactions: DataFrame de transacciones
            top_n: Número de clientes a retornar

        Returns:
            DataFrame con top clientes
        """
        print(f"\n👥 Calculando Top {top_n} clientes...")

        # Calcular total de compras por cliente
        df_top_customers = (
            df_transactions.groupBy("customer_id")
            .agg(count("*").alias("total_purchases"))
            .orderBy(desc("total_purchases"))
            .limit(top_n)
        )

        df_top_customers.show(truncate=False)

        return df_top_customers

    def get_peak_days(self, df_transactions: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Identifica los días con más transacciones.

        Args:
            df_transactions: DataFrame de transacciones
            top_n: Número de días a retornar

        Returns:
            DataFrame con días pico
        """
        print(f"\n📅 Calculando {top_n} días pico de compras...")

        # Calcular días pico de compras
        df_peak_days_temp = (
            df_transactions.withColumn("date", to_date(col("transaction_date")))
            .groupBy("date")
            .agg(count("*").alias("num_transactions"))
            .orderBy(desc("num_transactions"))
            .limit(top_n)
        )

        # Convertir fecha a string para JSON (después del groupBy)
        df_peak_days = df_peak_days_temp.withColumn(
            "date", date_format(col("date"), "yyyy-MM-dd")
        )

        df_peak_days.show(truncate=False)

        return df_peak_days

    def get_peak_days_by_products(
        self, df_transactions: DataFrame, top_n: int = 10
    ) -> DataFrame:
        """
        Identifica los días con más productos vendidos (unidades), no transacciones.

        Args:
            df_transactions: DataFrame de transacciones
            top_n: Número de días a retornar

        Returns:
            DataFrame con días con más productos vendidos
        """
        print(f"\n📆 Calculando {top_n} días con más productos vendidos...")

        # Calcular días con más productos vendidos
        df_peak_days_by_products_temp = (
            df_transactions.withColumn("date", to_date(col("transaction_date")))
            .groupBy("date")
            .agg(
                spark_sum(size(split(trim(col("products")), " "))).alias(
                    "total_products_sold"
                )
            )
            .orderBy(desc("total_products_sold"))
            .limit(top_n)
        )

        # Convertir fecha a string para JSON (después del groupBy)
        df_peak_days_by_products = df_peak_days_by_products_temp.withColumn(
            "date", date_format(col("date"), "yyyy-MM-dd")
        )

        df_peak_days_by_products.show(truncate=False)

        return df_peak_days_by_products

    def get_top_categories(
        self,
        df_transactions_exploded: DataFrame,
        df_product_categories: DataFrame,
        df_categories: DataFrame,
        top_n: int = 10,
    ) -> DataFrame:
        """
        Identifica las categorías más rentables (por volumen de unidades vendidas).
        Incluye productos sin categoría (NULL) porque es información importante.

        Args:
            df_transactions_exploded: DataFrame de transacciones explodidas
            df_product_categories: DataFrame de relación producto-categoría
            df_categories: DataFrame de categorías
            top_n: Número de categorías a retornar

        Returns:
            DataFrame con top categorías por volumen (incluyendo NULL si está en top)
        """
        print(f"\n📦 Calculando Top {top_n} categorías más rentables (por volumen)...")

        # Join para obtener categorías de productos
        df_with_category = (
            df_transactions_exploded.join(df_product_categories, "product_id", "left")
            .join(df_categories, "category_id", "left")
            .withColumn("category_id", col("category_id").cast("int"))
        )

        # Calcular volumen de ventas por categoría
        df_top_categories = (
            df_with_category.groupBy("category_id", "category_name")
            .agg(count("*").alias("total_volume"))
            .orderBy(desc("total_volume"))
            .limit(top_n)
        )

        df_top_categories.show(truncate=False)

        return df_top_categories

    def get_top_categories_by_product_count(
        self,
        df_transactions_exploded: DataFrame,
        df_product_categories: DataFrame,
        df_categories: DataFrame,
        top_n: int = 10,
    ) -> DataFrame:
        """
        Identifica las categorías con mayor número de productos en el catálogo.
        Cuenta el total de productos únicos que tiene cada categoría.

        Args:
            df_transactions_exploded: DataFrame de transacciones explodidas (no se usa, mantenido por compatibilidad)
            df_product_categories: DataFrame de relación producto-categoría
            df_categories: DataFrame de categorías
            top_n: Número de categorías a retornar

        Returns:
            DataFrame con top categorías por número total de productos en catálogo
        """
        print(f"\n🏷️ Calculando Top {top_n} categorías con más productos en catálogo...")

        # Calcular número de productos por categoría
        df_top_categories_by_products = (
            df_product_categories.join(df_categories, "category_id", "inner")
            .groupBy("category_id", "category_name")
            .agg(countDistinct("product_id").alias("num_products"))
            .orderBy(desc("num_products"))
            .limit(top_n)
        )

        df_top_categories_by_products.show(truncate=False)

        return df_top_categories_by_products

    def generate_executive_summary(
        self,
        df_transactions: DataFrame,
        df_transactions_exploded: DataFrame,
        df_product_categories: DataFrame,
        df_categories: DataFrame,
    ) -> Dict[str, Any]:
        """
        Genera el resumen ejecutivo completo.

        Args:
            df_transactions: DataFrame de transacciones
            df_transactions_exploded: DataFrame de transacciones explodidas
            df_product_categories: DataFrame de producto-categoría
            df_categories: DataFrame de categorías

        Returns:
            Diccionario con todos los resultados
        """
        print("\n" + "=" * 70)
        print("📊 GENERANDO RESUMEN EJECUTIVO")
        print("=" * 70)

        # Calcular métricas básicas
        basic_metrics = self.calculate_basic_metrics(df_transactions)

        # Top 10 productos
        top_products = self.get_top_products(
            df_transactions_exploded, df_product_categories, df_categories, top_n=10
        )

        # Top 10 clientes
        top_customers = self.get_top_customers(df_transactions, top_n=10)

        # Días pico
        peak_days = self.get_peak_days(df_transactions, top_n=10)

        # Días con más productos vendidos (unidades)
        peak_days_by_products = self.get_peak_days_by_products(
            df_transactions, top_n=10
        )

        # Top categorías por volumen
        top_categories = self.get_top_categories(
            df_transactions_exploded, df_product_categories, df_categories, top_n=10
        )

        # Top categorías por cantidad de productos
        top_categories_by_products = self.get_top_categories_by_product_count(
            df_transactions_exploded, df_product_categories, df_categories, top_n=10
        )

        print("\n" + "=" * 70)
        print("✅ Resumen ejecutivo generado exitosamente")
        print("=" * 70)

        return {
            "basic_metrics": basic_metrics,
            "top_products": top_products,
            "top_customers": top_customers,
            "peak_days": peak_days,
            "top_categories": top_categories,
            "top_categories_by_products": top_categories_by_products,
            "peak_days_by_products": peak_days_by_products,
        }
