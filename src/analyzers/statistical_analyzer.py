"""
Módulo para análisis estadísticos: distribuciones (boxplot) y correlaciones (heatmap).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    size,
    split,
    trim,
    to_date,
    explode as spark_explode,
    stddev,
    percentile_approx,
    expr,
)
from typing import Dict, Any, List
import pandas as pd
import numpy as np


class StatisticalAnalyzer:
    """Clase para realizar análisis estadísticos: distribuciones y correlaciones."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el analizador estadístico.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

    def analyze_category_products_by_store(
        self,
        df_transactions: DataFrame,
        df_product_categories: DataFrame,
        df_categories: DataFrame,
    ) -> DataFrame:
        """
        Calcula total_products_sold por categoría y por tienda (para boxplot).

        Cada categoría tendrá 4 puntos de datos (uno por cada tienda).
        Esto permite crear un boxplot donde cada caja representa una categoría
        y contiene los 4 valores de las 4 tiendas.

        Args:
            df_transactions: DataFrame de transacciones
            df_product_categories: DataFrame de relación producto-categoría
            df_categories: DataFrame de categorías

        Returns:
            DataFrame con columnas: category_id, category_name, store_id, total_products_sold
        """
        print(
            "\n📦 Analizando total_products_sold por categoría y tienda (para boxplot)..."
        )
        print("-" * 60)

        # Explotar productos
        df_exploded = (
            df_transactions.withColumn(
                "product_array", split(trim(col("products")), " ")
            )
            .withColumn("product_id", spark_explode(col("product_array")))
            .withColumn("product_id", col("product_id").cast("int"))
        )

        # Join con categorías
        df_with_categories = (
            df_exploded.join(df_product_categories, "product_id", "left")
            .join(df_categories, "category_id", "left")
            .withColumn("category_id", col("category_id").cast("int"))
            .withColumn("store_id", col("store_id").cast("int"))
        )

        # Filtrar categorías NULL
        df_with_categories = df_with_categories.filter(col("category_id").isNotNull())

        # Calcular total_products_sold por categoría y tienda
        df_category_store_stats = df_with_categories.groupBy(
            "category_id", "category_name", "store_id"
        ).agg(count("*").alias("total_products_sold"))

        # Asegurar tipos correctos
        df_category_store_stats = df_category_store_stats.select(
            col("category_id").cast("int"),
            col("category_name"),
            col("store_id").cast("int"),
            col("total_products_sold").cast("double"),
        ).orderBy("category_id", "store_id")

        print(f"📋 Datos de total_products_sold por categoría y tienda:")
        df_category_store_stats.show(30, truncate=False)

        # Estadísticas por categoría
        stats_by_category = (
            df_category_store_stats.groupBy("category_id", "category_name")
            .agg(
                avg("total_products_sold").alias("promedio"),
                spark_min("total_products_sold").alias("minimo"),
                spark_max("total_products_sold").alias("maximo"),
                count("store_id").alias("num_tiendas"),
            )
            .orderBy("category_id")
        )

        print(f"\n📊 Estadísticas por categoría (promedio de las 4 tiendas):")
        stats_by_category.show(20, truncate=False)

        return df_category_store_stats

    def calculate_correlation_matrix(
        self,
        df_transactions: DataFrame,
        df_product_categories: DataFrame = None,
    ) -> Dict[str, Any]:
        """
        Calcula matriz de correlación entre variables numéricas.

        Variables calculadas:
        - Frecuencia: número de transacciones por cliente
        - Cantidad promedio: promedio de productos por transacción
        - Diversidad: número de productos únicos por cliente
        - Total productos: total de productos comprados por cliente

        Args:
            df_transactions: DataFrame de transacciones
            df_product_categories: DataFrame de relación producto-categoría (opcional)

        Returns:
            Diccionario con matriz de correlación y nombres de variables
        """
        print("\n🔥 Calculando matriz de correlación...")
        print("-" * 60)

        # Explotar productos
        df_exploded = (
            df_transactions.withColumn(
                "product_array", split(trim(col("products")), " ")
            )
            .withColumn("product_id", spark_explode(col("product_array")))
            .withColumn("product_id", col("product_id").cast("int"))
        )

        # Crear ID único de transacción
        df_exploded = df_exploded.withColumn(
            "transaction_key",
            col("transaction_date").cast("string")
            + "_"
            + col("store_id").cast("string")
            + "_"
            + col("customer_id").cast("string"),
        )

        # Calcular métricas numéricas por cliente
        df_customer_metrics = df_exploded.groupBy("customer_id").agg(
            countDistinct("transaction_key").alias("frequency"),
            count("*").alias("total_products"),
            countDistinct("product_id").alias("diversity"),
            countDistinct("store_id").alias("unique_stores"),
        )

        # Calcular cantidad promedio por transacción
        df_transaction_sizes = df_transactions.withColumn(
            "transaction_size", size(split(trim(col("products")), " "))
        )

        df_avg_per_transaction = df_transaction_sizes.groupBy("customer_id").agg(
            avg("transaction_size").alias("avg_quantity")
        )

        # Combinar métricas
        df_combined = df_customer_metrics.join(
            df_avg_per_transaction, "customer_id", "inner"
        )

        # Convertir a pandas para calcular correlaciones (datos pequeños después de agregación)
        print("   → Convirtiendo a pandas para cálculo de correlaciones...")
        pdf = df_combined.select(
            "frequency", "total_products", "diversity", "avg_quantity", "unique_stores"
        ).toPandas()

        # Calcular matriz de correlación
        correlation_matrix = pdf.corr().to_dict()

        # Preparar datos para exportación
        variables = [
            "frequency",
            "total_products",
            "diversity",
            "avg_quantity",
            "unique_stores",
        ]
        variable_names = {
            "frequency": "Frecuencia (nº transacciones)",
            "total_products": "Total Productos",
            "diversity": "Diversidad (productos únicos)",
            "avg_quantity": "Cantidad Promedio",
            "unique_stores": "Tiendas Únicas",
        }

        # Crear estructura para JSON
        correlation_data = {
            "variables": variables,
            "variable_names": variable_names,
            "matrix": {},
        }

        # Convertir matriz a formato serializable
        for var1 in variables:
            correlation_data["matrix"][var1] = {}
            for var2 in variables:
                corr_value = correlation_matrix.get(var1, {}).get(var2, 0.0)
                # Convertir NaN a None
                if pd.isna(corr_value):
                    corr_value = None
                else:
                    corr_value = float(corr_value)
                correlation_data["matrix"][var1][var2] = corr_value

        print(f"✅ Matriz de correlación calculada ({len(variables)}x{len(variables)})")
        print("\n📊 Valores de correlación:")
        for var1 in variables[:3]:  # Mostrar solo primeras 3
            for var2 in variables[:3]:
                if var1 != var2:
                    corr_val = correlation_data["matrix"][var1][var2]
                    if corr_val is not None:
                        print(
                            f"   {variable_names[var1]} ↔ {variable_names[var2]}: {corr_val:.3f}"
                        )

        return correlation_data

    def analyze_customer_purchase_distribution(
        self,
        df_transactions: DataFrame,
    ) -> DataFrame:
        """
        Calcula la distribución de compras por cliente para boxplot interactivo.
        
        Retorna TODOS los clientes ordenados por volumen de compra.
        El frontend se encargará de filtrar/paginar los datos.
        
        Args:
            df_transactions: DataFrame de transacciones
            
        Returns:
            DataFrame con columnas: customer_id, total_products_purchased
        """
        print(f"\n👥 Analizando distribución de compras por cliente...")
        print("-" * 60)
        
        # Calcular total de productos por cliente
        df_customer_purchases = df_transactions.groupBy("customer_id").agg(
            spark_sum(size(split(trim(col("products")), " "))).alias("total_products_purchased")
        )
        
        # Calcular estadísticas descriptivas de TODOS los clientes
        stats = df_customer_purchases.select(
            avg("total_products_purchased").alias("promedio"),
            spark_min("total_products_purchased").alias("minimo"),
            spark_max("total_products_purchased").alias("maximo"),
            stddev("total_products_purchased").alias("desviacion_std"),
            percentile_approx("total_products_purchased", 0.25).alias("q1"),
            percentile_approx("total_products_purchased", 0.50).alias("mediana"),
            percentile_approx("total_products_purchased", 0.75).alias("q3"),
            count("*").alias("num_clientes"),
        ).collect()[0]
        
        print(f"📊 Estadísticas generales (todos los clientes):")
        print(f"   Total clientes: {stats['num_clientes']:,}")
        print(f"   Promedio: {stats['promedio']:.2f} productos")
        print(f"   Mediana: {stats['mediana']:.2f} productos")
        print(f"   Mínimo: {stats['minimo']:.0f} productos")
        print(f"   Máximo: {stats['maximo']:.0f} productos")
        print(f"   Q1 (25%): {stats['q1']:.2f} productos")
        print(f"   Q3 (75%): {stats['q3']:.2f} productos")
        print(f"   Desviación estándar: {stats['desviacion_std']:.2f}")
        
        # Ordenar todos los clientes por volumen de compra (descendente)
        df_all_customers = df_customer_purchases.orderBy(
            col("total_products_purchased").desc()
        )
        
        # Asegurar tipos correctos
        df_all_customers = df_all_customers.select(
            col("customer_id").cast("int"),
            col("total_products_purchased").cast("double"),
        )
        
        print(f"\n📋 Exportando {stats['num_clientes']:,} clientes al JSON...")
        print(f"   (El frontend filtrará los datos para visualización interactiva)")
        
        return df_all_customers