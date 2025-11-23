"""
Pipeline 1: Resumen Ejecutivo (Diario)

Genera métricas clave para el dashboard:
- Total de ventas (unidades)
- Número de transacciones
- Top 10 productos
- Top 10 clientes
- Días pico de compra
- Categorías más rentables

Output: output/summary/*.json
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config.spark_config import create_spark_session, stop_spark_session
from src.data_loader import DataLoader
from src.analyzers.summary_metrics import SummaryMetrics
from src.json_exporter import JSONExporter


class ExecutiveSummaryPipeline:
    """Pipeline para generar el resumen ejecutivo."""

    def __init__(self):
        """Inicializa el pipeline."""
        self.spark = None
        self.data_loader = None
        self.metrics_calculator = None
        self.exporter = None

    def run(self):
        """Ejecuta el pipeline completo."""
        try:
            print("=" * 70)
            print("🚀 PIPELINE 1: RESUMEN EJECUTIVO")
            print("=" * 70)

            # Inicializar Spark
            print("\n📡 Inicializando Spark...")
            self.spark = create_spark_session("ExecutiveSummaryPipeline")

            # Inicializar componentes
            self.data_loader = DataLoader(self.spark)
            self.metrics_calculator = SummaryMetrics(self.spark)
            self.exporter = JSONExporter()

            # Cargar datos
            print("\n📂 Cargando datos desde PostgreSQL...")
            print("   [1/5] Cargando transacciones...")
            df_transactions = self.data_loader.load_transactions()
            print(f"   ✅ Transacciones cargadas")

            print("   [2/5] Explodiendo transacciones...")
            df_transactions_exploded = self.data_loader.explode_transactions(
                df_transactions
            )
            print("   ✅ Transacciones explodidas")
            df_transactions_exploded.cache()

            print("   [3/5] Cargando productos-categorías...")
            df_product_categories = self.data_loader.load_product_categories()
            print("   ✅ Productos-categorías cargadas")

            print("   [4/5] Cargando categorías...")
            df_categories = self.data_loader.load_categories()
            print("   ✅ Categorías cargadas")

            print("   [5/5] Todos los datos cargados correctamente")

            # Generar resumen ejecutivo
            results = self.metrics_calculator.generate_executive_summary(
                df_transactions,
                df_transactions_exploded,
                df_product_categories,
                df_categories,
            )

            # Exportar resultados a JSON
            print("\n💾 Exportando resultados a JSON...")

            # Métricas básicas
            self.exporter.export_summary_metrics(
                results["basic_metrics"], filename="basic_metrics.json"
            )

            # Top 10 productos
            self.exporter.export_top_items(
                results["top_products"], item_type="products", top_n=10
            )

            # Top 10 clientes
            self.exporter.export_top_items(
                results["top_customers"], item_type="customers", top_n=10
            )

            # Días pico
            self.exporter.export_top_items(
                results["peak_days"], item_type="peak_days", top_n=10
            )

            # Top categorías
            self.exporter.export_top_items(
                results["top_categories"], item_type="categories", top_n=10
            )

            # Metadata de ejecución
            self.exporter.export_execution_metadata(
                dag_id="executive_summary_pipeline",
                task_id="generate_executive_summary",
                execution_info={
                    "status": "success",
                    "files_generated": [
                        "basic_metrics.json",
                        "top_10_products.json",
                        "top_10_customers.json",
                        "top_10_peak_days.json",
                        "top_10_categories.json",
                    ],
                },
            )

            # Liberar caché
            df_transactions_exploded.unpersist()

            print("\n" + "=" * 70)
            print("✅ RESUMEN EJECUTIVO GENERADO EXITOSAMENTE")
            print("=" * 70)

        except Exception as e:
            print(f"\n❌ Error en Resumen Ejecutivo: {str(e)}")
            import traceback

            traceback.print_exc()
            raise
        finally:
            if self.spark:
                stop_spark_session(self.spark)


if __name__ == "__main__":
    pipeline = ExecutiveSummaryPipeline()
    pipeline.run()
