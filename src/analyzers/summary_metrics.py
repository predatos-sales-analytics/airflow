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
    sum as spark_sum,
    desc,
    size,
    split,
    trim,
    to_date,
    explode as spark_explode,
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
        self, df_transactions_exploded: DataFrame, top_n: int = 10
    ) -> DataFrame:
        """
        Obtiene los productos más vendidos.

        Args:
            df_transactions_exploded: DataFrame de transacciones explodidas
            top_n: Número de productos a retornar

        Returns:
            DataFrame con top productos
        """
        print(f"\n🏆 Calculando Top {top_n} productos...")

        df_top_products = (
            df_transactions_exploded.groupBy("product_id")
            .agg(count("*").alias("total_sold"))
            .orderBy(desc("total_sold"))
            .limit(top_n)
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

        df_peak_days = (
            df_transactions.withColumn("date", to_date(col("transaction_date")))
            .groupBy("date")
            .agg(count("*").alias("num_transactions"))
            .orderBy(desc("num_transactions"))
            .limit(top_n)
        )

        df_peak_days.show(truncate=False)

        return df_peak_days

    def get_top_categories(
        self,
        df_transactions_exploded: DataFrame,
        df_product_categories: DataFrame,
        df_categories: DataFrame,
        top_n: int = 10,
    ) -> DataFrame:
        """
        Identifica las categorías más rentables (por volumen).

        Args:
            df_transactions_exploded: DataFrame de transacciones explodidas
            df_product_categories: DataFrame de relación producto-categoría
            df_categories: DataFrame de categorías
            top_n: Número de categorías a retornar

        Returns:
            DataFrame con top categorías
        """
        print(f"\n📦 Calculando Top {top_n} categorías más rentables...")

        # Join: transactions -> products -> categories
        df_with_category = df_transactions_exploded.join(
            df_product_categories, "product_id", "left"
        ).join(df_categories, "category_id", "left")

        df_top_categories = (
            df_with_category.groupBy("category_id", "category_name")
            .agg(count("*").alias("total_volume"))
            .orderBy(desc("total_volume"))
            .limit(top_n)
        )

        df_top_categories.show(truncate=False)

        return df_top_categories

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
        top_products = self.get_top_products(df_transactions_exploded, top_n=10)

        # Top 10 clientes
        top_customers = self.get_top_customers(df_transactions, top_n=10)

        # Días pico
        peak_days = self.get_peak_days(df_transactions, top_n=10)

        # Top categorías
        top_categories = self.get_top_categories(
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
        }
