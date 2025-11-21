"""
Módulo para análisis de clientes.
Incluye frecuencia de compra, tiempo entre compras y segmentación RFM.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    datediff,
    to_date,
    current_date,
    lag,
    desc,
    row_number,
    ntile,
    when,
    lit,
)
from pyspark.sql.window import Window
from typing import Dict, Any
from datetime import timedelta


class CustomerAnalyzer:
    """Clase para realizar análisis de clientes."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el analizador de clientes.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

    def analyze_purchase_frequency(self, df: DataFrame) -> DataFrame:
        """
        Analiza la frecuencia de compra por cliente.

        Args:
            df: DataFrame con transacciones

        Returns:
            DataFrame con frecuencias de compra
        """
        print("\n🛒 Análisis de Frecuencia de Compra")
        print("-" * 60)

        df_frequency = (
            df.groupBy("customer_id")
            .agg(
                count("*").alias("num_compras"),
                spark_min("transaction_date").alias("primera_compra"),
                spark_max("transaction_date").alias("ultima_compra"),
            )
            .orderBy(desc("num_compras"))
        )

        # Estadísticas generales
        stats = df_frequency.select(
            avg("num_compras").alias("promedio_compras"),
            spark_min("num_compras").alias("min_compras"),
            spark_max("num_compras").alias("max_compras"),
        ).collect()[0]

        print(f"📊 Promedio de compras por cliente: {stats['promedio_compras']:.2f}")
        print(f"📉 Mínimo de compras: {stats['min_compras']}")
        print(f"📈 Máximo de compras: {stats['max_compras']}")

        # Distribución de frecuencias
        print(f"\n📋 Distribución de frecuencia de compra:")
        df_frequency.groupBy("num_compras").agg(
            count("*").alias("num_clientes")
        ).orderBy("num_compras").show(20, truncate=False)

        # Top 10 clientes más frecuentes
        print(f"\n👥 Top 10 clientes con más compras:")
        df_frequency.show(10, truncate=False)

        return df_frequency

    def analyze_time_between_purchases(self, df: DataFrame) -> DataFrame:
        """
        Analiza el tiempo promedio entre compras por cliente.

        Args:
            df: DataFrame con transacciones

        Returns:
            DataFrame con tiempos entre compras
        """
        print("\n⏱️ Análisis de Tiempo Entre Compras")
        print("-" * 60)

        # Preparar datos con fechas ordenadas por cliente
        df_with_date = df.withColumn("date", to_date(col("transaction_date")))

        # Ventana particionada por cliente, ordenada por fecha
        window_spec = Window.partitionBy("customer_id").orderBy("date")

        # Calcular diferencia entre compras consecutivas
        df_time_diff = (
            df_with_date.withColumn("prev_date", lag("date").over(window_spec))
            .withColumn("days_between", datediff(col("date"), col("prev_date")))
            .filter(col("days_between").isNotNull())
        )

        # Calcular tiempo promedio por cliente
        df_avg_time = (
            df_time_diff.groupBy("customer_id")
            .agg(
                avg("days_between").alias("promedio_dias_entre_compras"),
                spark_min("days_between").alias("min_dias"),
                spark_max("days_between").alias("max_dias"),
                count("*").alias("num_intervalos"),
            )
            .filter(col("num_intervalos") >= 1)
            .orderBy("promedio_dias_entre_compras")
        )

        # Estadísticas generales
        stats = df_avg_time.select(
            avg("promedio_dias_entre_compras").alias("promedio_general"),
            spark_min("promedio_dias_entre_compras").alias("min_promedio"),
            spark_max("promedio_dias_entre_compras").alias("max_promedio"),
        ).collect()[0]

        print(
            f"📊 Tiempo promedio entre compras (global): {stats['promedio_general']:.2f} días"
        )
        print(f"📉 Mínimo tiempo promedio: {stats['min_promedio']:.2f} días")
        print(f"📈 Máximo tiempo promedio: {stats['max_promedio']:.2f} días")

        print(f"\n📋 Distribución de tiempos entre compras:")
        df_avg_time.show(20, truncate=False)

        # Categorizar clientes por frecuencia
        df_categorized = df_avg_time.withColumn(
            "categoria_frecuencia",
            when(col("promedio_dias_entre_compras") <= 7, "Muy Frecuente (≤7 días)")
            .when(col("promedio_dias_entre_compras") <= 15, "Frecuente (8-15 días)")
            .when(col("promedio_dias_entre_compras") <= 30, "Regular (16-30 días)")
            .when(col("promedio_dias_entre_compras") <= 60, "Ocasional (31-60 días)")
            .otherwise("Esporádico (>60 días)"),
        )

        print(f"\n📊 Categorización de clientes por frecuencia:")
        df_categorized.groupBy("categoria_frecuencia").agg(
            count("*").alias("num_clientes")
        ).orderBy("num_clientes", ascending=False).show(truncate=False)

        return df_avg_time

    def rfm_segmentation(self, df: DataFrame, reference_date: str = None) -> DataFrame:
        """
        Realiza segmentación RFM (Recency, Frequency, Monetary).

        Nota: Como no tenemos valor monetario, usaremos RF + cantidad de productos.

        Args:
            df: DataFrame con transacciones
            reference_date: Fecha de referencia para calcular recency (default: última fecha en datos)

        Returns:
            DataFrame con segmentación RFM
        """
        print("\n📊 Segmentación de Clientes (RFM)")
        print("-" * 60)

        # Preparar datos
        df_with_date = df.withColumn("date", to_date(col("transaction_date")))

        # Si no hay fecha de referencia, usar la última fecha + 1 día
        if reference_date is None:
            max_date = df_with_date.select(spark_max("date")).collect()[0][0]
            reference_date = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"📅 Fecha de referencia para Recency: {reference_date}")

        # Calcular RFM por cliente (Recency = días desde última compra hasta fecha de referencia)
        df_rfm = (
            df_with_date.groupBy("customer_id")
            .agg(
                spark_max("date").alias("ultima_compra"), count("*").alias("frequency")
            )
            .withColumn("recency", datediff(lit(reference_date), col("ultima_compra")))
            .drop("ultima_compra")
        )

        # Crear cuartiles para cada métrica (1 = mejor, 4 = peor)
        # Para Recency: menor es mejor (más reciente)
        # Para Frequency: mayor es mejor (más compras)

        df_rfm_scored = df_rfm.withColumn(
            "R_score", ntile(4).over(Window.orderBy(col("recency")))
        ).withColumn(  # Menor recency = mejor score
            "F_score", ntile(4).over(Window.orderBy(desc("frequency")))
        )  # Mayor frequency = mejor score

        # Invertir R_score para que 4 sea mejor (más reciente)
        df_rfm_scored = df_rfm_scored.withColumn("R_score", 5 - col("R_score"))

        # Crear segmentos basados en scores
        df_rfm_scored = df_rfm_scored.withColumn(
            "RFM_segment",
            when((col("R_score") >= 4) & (col("F_score") >= 4), "Champions")
            .when((col("R_score") >= 3) & (col("F_score") >= 3), "Loyal Customers")
            .when((col("R_score") >= 4) & (col("F_score") <= 2), "Promising")
            .when((col("R_score") <= 2) & (col("F_score") >= 4), "At Risk")
            .when((col("R_score") <= 2) & (col("F_score") <= 2), "Lost")
            .when((col("R_score") >= 3) & (col("F_score") <= 2), "Potential Loyalists")
            .otherwise("Regular"),
        )

        print(f"\n📊 Distribución de segmentos RFM:")
        df_rfm_scored.groupBy("RFM_segment").agg(
            count("*").alias("num_clientes"),
            avg("recency").alias("avg_recency"),
            avg("frequency").alias("avg_frequency"),
        ).orderBy(desc("num_clientes")).show(truncate=False)

        print(f"\n📋 Ejemplos de clientes por segmento:")
        df_rfm_scored.orderBy(desc("R_score"), desc("F_score")).show(20, truncate=False)

        return df_rfm_scored

    def generate_customer_summary(self, df: DataFrame) -> Dict[str, Any]:
        """
        Genera un resumen completo del análisis de clientes.

        Args:
            df: DataFrame con transacciones

        Returns:
            Diccionario con resumen
        """
        print("\n" + "=" * 60)
        print("👥 RESUMEN DE ANÁLISIS DE CLIENTES")
        print("=" * 60)

        # Análisis de frecuencia
        df_frequency = self.analyze_purchase_frequency(df)

        # Análisis de tiempo entre compras
        df_time_between = self.analyze_time_between_purchases(df)

        # Segmentación RFM
        df_rfm = self.rfm_segmentation(df)

        print("=" * 60)

        return {
            "frequency": df_frequency,
            "time_between": df_time_between,
            "rfm": df_rfm,
        }

