"""
Módulo para análisis temporal de ventas.

Incluye análisis diarios, semanales, mensuales y picos.
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
    date_format,
    dayofweek,
    weekofyear,
    month,
    year,
    to_date,
    datediff,
    lead,
    lag,
    row_number,
)
from pyspark.sql.window import Window
from typing import Dict, Any


class TemporalAnalyzer:
    """Clase para realizar análisis temporal de ventas."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el analizador temporal.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

    def analyze_daily_sales(
        self, df: DataFrame, date_column: str = "transaction_date"
    ) -> DataFrame:
        """
        Analiza ventas diarias.

        Args:
            df: DataFrame con transacciones
            date_column: Nombre de la columna de fecha

        Returns:
            DataFrame con estadísticas diarias
        """
        print("\n📅 Análisis de Ventas Diarias")
        print("-" * 60)

        df_daily = (
            df.withColumn("date", to_date(col(date_column)))
            .groupBy("date")
            .agg(
                count("*").alias("num_transacciones"),
                countDistinct("customer_id").alias("clientes_unicos"),
            )
            .withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
            .orderBy("date")
        )

        # Estadísticas generales
        stats = df_daily.select(
            avg("num_transacciones").alias("promedio_diario"),
            spark_min("num_transacciones").alias("min_diario"),
            spark_max("num_transacciones").alias("max_diario"),
        ).collect()[0]

        print(f"📊 Promedio de transacciones diarias: {stats['promedio_diario']:.2f}")
        print(f"📉 Mínimo de transacciones en un día: {stats['min_diario']}")
        print(f"📈 Máximo de transacciones en un día: {stats['max_diario']}")

        print(f"\n📋 Primeros 10 días:")
        df_daily.show(10, truncate=False)

        return df_daily

    def analyze_weekly_sales(
        self, df: DataFrame, date_column: str = "transaction_date"
    ) -> DataFrame:
        """
        Analiza ventas semanales.

        Args:
            df: DataFrame con transacciones
            date_column: Nombre de la columna de fecha

        Returns:
            DataFrame con estadísticas semanales
        """
        print("\n📅 Análisis de Ventas Semanales")
        print("-" * 60)

        df_weekly = (
            df.withColumn("date", to_date(col(date_column)))
            .withColumn("year", year(col("date")))
            .withColumn("week", weekofyear(col("date")))
            .groupBy("year", "week")
            .agg(
                count("*").alias("num_transacciones"),
                spark_min("date").alias("inicio_semana"),
                spark_max("date").alias("fin_semana"),
            )
            .withColumn(
                "inicio_semana", date_format(col("inicio_semana"), "yyyy-MM-dd")
            )
            .withColumn("fin_semana", date_format(col("fin_semana"), "yyyy-MM-dd"))
            .orderBy("year", "week")
        )

        # Estadísticas generales
        stats = df_weekly.select(
            avg("num_transacciones").alias("promedio_semanal"),
            spark_min("num_transacciones").alias("min_semanal"),
            spark_max("num_transacciones").alias("max_semanal"),
        ).collect()[0]

        print(
            f"📊 Promedio de transacciones semanales: {stats['promedio_semanal']:.2f}"
        )
        print(f"📉 Mínimo de transacciones en una semana: {stats['min_semanal']}")
        print(f"📈 Máximo de transacciones en una semana: {stats['max_semanal']}")

        print(f"\n📋 Primeras 10 semanas:")
        df_weekly.show(10, truncate=False)

        return df_weekly

    def analyze_monthly_sales(
        self, df: DataFrame, date_column: str = "transaction_date"
    ) -> DataFrame:
        """
        Analiza ventas mensuales.

        Args:
            df: DataFrame con transacciones
            date_column: Nombre de la columna de fecha

        Returns:
            DataFrame con estadísticas mensuales
        """
        print("\n📅 Análisis de Ventas Mensuales")
        print("-" * 60)

        df_monthly = (
            df.withColumn("date", to_date(col(date_column)))
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("month_name", date_format(col("date"), "MMMM"))
            .groupBy("year", "month", "month_name")
            .agg(count("*").alias("num_transacciones"))
            .orderBy("year", "month")
        )

        # Estadísticas generales
        stats = df_monthly.select(
            avg("num_transacciones").alias("promedio_mensual"),
            spark_min("num_transacciones").alias("min_mensual"),
            spark_max("num_transacciones").alias("max_mensual"),
        ).collect()[0]

        print(
            f"📊 Promedio de transacciones mensuales: {stats['promedio_mensual']:.2f}"
        )
        print(f"📉 Mínimo de transacciones en un mes: {stats['min_mensual']}")
        print(f"📈 Máximo de transacciones en un mes: {stats['max_mensual']}")

        print(f"\n📋 Ventas por mes:")
        df_monthly.show(20, truncate=False)

        return df_monthly

    def analyze_day_of_week_patterns(
        self, df: DataFrame, date_column: str = "transaction_date"
    ) -> DataFrame:
        """
        Analiza patrones de ventas por día de la semana.

        Args:
            df: DataFrame con transacciones
            date_column: Nombre de la columna de fecha

        Returns:
            DataFrame con estadísticas por día de semana
        """
        print("\n📅 Análisis de Picos por Día de la Semana")
        print("-" * 60)

        df_dow = (
            df.withColumn("date", to_date(col(date_column)))
            .withColumn("day_of_week", dayofweek(col("date")))
            .withColumn("day_name", date_format(col("date"), "EEEE"))
            .groupBy("day_of_week", "day_name")
            .agg(count("*").alias("num_transacciones"))
            .orderBy("day_of_week")
        )

        print(f"📊 Distribución de transacciones por día de la semana:")
        df_dow.show(10, truncate=False)

        # Identificar el día con más y menos ventas
        dow_list = df_dow.collect()
        max_day = max(dow_list, key=lambda x: x["num_transacciones"])
        min_day = min(dow_list, key=lambda x: x["num_transacciones"])

        print(
            f"\n📈 Día con MÁS ventas: {max_day['day_name']} ({max_day['num_transacciones']:,} transacciones)"
        )
        print(
            f"📉 Día con MENOS ventas: {min_day['day_name']} ({min_day['num_transacciones']:,} transacciones)"
        )

        return df_dow

    def generate_temporal_summary(
        self, df: DataFrame, date_column: str = "transaction_date"
    ) -> Dict[str, Any]:
        """
        Genera un resumen completo del análisis temporal.

        Args:
            df: DataFrame con transacciones
            date_column: Nombre de la columna de fecha

        Returns:
            Diccionario con resumen
        """
        print("\n" + "=" * 60)
        print("📊 RESUMEN DE ANÁLISIS TEMPORAL")
        print("=" * 60)

        # Análisis diario
        df_daily = self.analyze_daily_sales(df, date_column)

        # Análisis semanal
        df_weekly = self.analyze_weekly_sales(df, date_column)

        # Análisis mensual
        df_monthly = self.analyze_monthly_sales(df, date_column)

        # Patrones por día de la semana
        df_dow = self.analyze_day_of_week_patterns(df, date_column)

        print("=" * 60)

        return {
            "daily": df_daily,
            "weekly": df_weekly,
            "monthly": df_monthly,
            "day_of_week": df_dow,
        }
