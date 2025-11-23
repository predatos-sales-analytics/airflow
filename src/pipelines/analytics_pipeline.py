"""
Pipeline 2: Análisis Analítico (Semanal)

Genera visualizaciones analíticas:
- Series temporales (diarias, semanales, mensuales)
- Distribuciones de ventas
- Análisis de tendencias
- Correlaciones entre variables

Output: output/analytics/*.json
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config.spark_config import create_spark_session, stop_spark_session
from src.data_loader import DataLoader
from src.analyzers.temporal_analyzer import TemporalAnalyzer
from src.analyzers.statistical_analyzer import StatisticalAnalyzer
from src.json_exporter import JSONExporter
from pyspark.sql.functions import col, concat_ws


class AnalyticsPipeline:
    """Pipeline para generar análisis analítico."""

    def __init__(self):
        """Inicializa el pipeline."""
        self.spark = None
        self.data_loader = None
        self.temporal_analyzer = None
        self.statistical_analyzer = None
        self.exporter = None

    def run(self):
        """Ejecuta el pipeline completo."""
        try:
            print("=" * 70)
            print("🚀 PIPELINE 2: ANÁLISIS ANALÍTICO")
            print("=" * 70)

            # Inicializar Spark
            print("\n📡 Inicializando Spark...")
            self.spark = create_spark_session("AnalyticsPipeline")

            # Inicializar componentes
            self.data_loader = DataLoader(self.spark)
            self.temporal_analyzer = TemporalAnalyzer(self.spark)
            self.statistical_analyzer = StatisticalAnalyzer(self.spark)
            self.exporter = JSONExporter()

            # Cargar datos
            print("\n📂 Cargando datos desde PostgreSQL...")

            df_transactions = self.data_loader.load_transactions()
            df_product_categories = self.data_loader.load_product_categories()
            df_categories = self.data_loader.load_categories()

            # ============================================================
            # VISUALIZACIÓN 1: SERIE DE TIEMPO
            # ============================================================
            print("\n" + "=" * 70)
            print("📈 VISUALIZACIÓN 1: SERIE DE TIEMPO")
            print("=" * 70)

            # Análisis temporal diario
            print("\n📅 Generando serie temporal diaria...")
            df_daily = self.temporal_analyzer.analyze_daily_sales(
                df_transactions, date_column="transaction_date"
            )
            self.exporter.export_time_series(
                df_daily,
                series_name="daily_sales",
                date_col="date",
                value_col="num_transacciones",
            )

            # Análisis temporal semanal
            print("\n📅 Generando serie temporal semanal...")
            df_weekly = self.temporal_analyzer.analyze_weekly_sales(
                df_transactions, date_column="transaction_date"
            )
            self.exporter.export_time_series(
                df_weekly,
                series_name="weekly_sales",
                date_col="inicio_semana",
                value_col="num_transacciones",
            )

            # Análisis temporal mensual
            print("\n📅 Generando serie temporal mensual...")
            df_monthly = self.temporal_analyzer.analyze_monthly_sales(
                df_transactions, date_column="transaction_date"
            )
            # Crear columna de fecha para exportación (año-mes)
            from pyspark.sql.functions import concat_ws, lit

            df_monthly_export = df_monthly.withColumn(
                "date", concat_ws("-", col("year"), col("month"))
            )
            self.exporter.export_time_series(
                df_monthly_export,
                series_name="monthly_sales",
                date_col="date",
                value_col="num_transacciones",
            )

            # Análisis por día de la semana
            print("\n📅 Generando análisis por día de la semana...")
            df_day_of_week = self.temporal_analyzer.analyze_day_of_week_patterns(
                df_transactions, date_column="transaction_date"
            )
            # Exportar como distribución (no serie temporal)
            self.exporter.export_distribution(
                df_day_of_week,
                distribution_name="day_of_week_patterns",
            )

            # ============================================================
            # VISUALIZACIÓN 2: BOXPLOT - TOTAL PRODUCTS SOLD POR CATEGORÍA
            # ============================================================
            print("\n" + "=" * 70)
            print("📦 VISUALIZACIÓN 2: BOXPLOT - TOTAL PRODUCTS SOLD POR CATEGORÍA")
            print("=" * 70)

            df_category_store_boxplot = (
                self.statistical_analyzer.analyze_category_products_by_store(
                    df_transactions, df_product_categories, df_categories
                )
            )
            self.exporter.export_distribution(
                df_category_store_boxplot,
                distribution_name="category_products_by_store",
            )

            # ============================================================
            # VISUALIZACIÓN 3: HEATMAP - MATRIZ DE CORRELACIÓN
            # ============================================================
            print("\n" + "=" * 70)
            print("🔥 VISUALIZACIÓN 3: HEATMAP - MATRIZ DE CORRELACIÓN")
            print("=" * 70)

            correlation_data = self.statistical_analyzer.calculate_correlation_matrix(
                df_transactions, df_product_categories
            )
            self.exporter.export_correlation_matrix(
                correlation_data, matrix_name="variable_correlation"
            )

            # Metadata de ejecución
            self.exporter.export_execution_metadata(
                dag_id="analytics_pipeline",
                task_id="generate_analytics",
                execution_info={
                    "status": "success",
                    "files_generated": [
                        "daily_sales.json",
                        "weekly_sales.json",
                        "monthly_sales.json",
                        "day_of_week_patterns_distribution.json",
                        "category_products_by_store_distribution.json",
                        "variable_correlation.json",
                    ],
                    "visualizations": [
                        "Serie de tiempo (diaria, semanal, mensual)",
                        "Patrones por día de la semana",
                        "Boxplot - Total productos vendidos por categoría (4 tiendas por categoría)",
                        "Heatmap - Matriz de correlación",
                    ],
                },
            )

            print("\n" + "=" * 70)
            print("✅ ANÁLISIS ANALÍTICO GENERADO EXITOSAMENTE")
            print("=" * 70)

        except Exception as e:
            print(f"\n❌ Error en Análisis Analítico: {str(e)}")
            import traceback

            traceback.print_exc()
            raise
        finally:
            if self.spark:
                stop_spark_session(self.spark)


if __name__ == "__main__":
    pipeline = AnalyticsPipeline()
    pipeline.run()
