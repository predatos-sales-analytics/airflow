"""
Pipeline 3: Clustering de Clientes (Mensual)

Genera segmentación de clientes usando K-Means:
- Asignación de clusters a clientes
- Perfiles de cada cluster
- Recomendaciones de negocio por cluster

Output: output/advanced/clustering/*.json
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config.spark_config import create_spark_session, stop_spark_session
from src.data_loader import DataLoader
from src.analyzers.customer_analyzer import CustomerAnalyzer
from src.json_exporter import JSONExporter


class ClusteringPipeline:
    """Pipeline para generar clustering de clientes."""

    def __init__(self, n_clusters: int = 4):
        """
        Inicializa el pipeline.

        Args:
            n_clusters: Número de clusters para K-Means (default: 4)
        """
        self.spark = None
        self.data_loader = None
        self.customer_analyzer = None
        self.exporter = None
        self.n_clusters = n_clusters

    def run(self):
        """Ejecuta el pipeline completo."""
        try:
            print("=" * 70)
            print("🚀 PIPELINE 3: CLUSTERING DE CLIENTES")
            print("=" * 70)

            # Inicializar Spark
            print("\n📡 Inicializando Spark...")
            self.spark = create_spark_session("ClusteringPipeline")

            # Inicializar componentes
            self.data_loader = DataLoader(self.spark)
            self.customer_analyzer = CustomerAnalyzer(self.spark)
            self.exporter = JSONExporter()

            # Cargar datos
            print("\n📂 Cargando datos desde PostgreSQL...")
            df_transactions = self.data_loader.load_transactions()
            print("   ✅ Transacciones cargadas")

            print("   [2/3] Explodiendo transacciones...")
            df_transactions_exploded = self.data_loader.explode_transactions(
                df_transactions
            )
            print("   ✅ Transacciones explodidas")
            df_transactions_exploded.cache()

            print("   [3/3] Cargando productos-categorías...")
            df_product_categories = self.data_loader.load_product_categories()
            print("   ✅ Productos-categorías cargadas")
            df_product_categories.cache()

            # Generar segmentación de clientes
            print(f"\n🔍 Generando segmentación con {self.n_clusters} clusters...")
            results = self.customer_analyzer.generate_customer_segmentation(
                df_transactions,
                df_transactions_exploded,
                df_product_categories=df_product_categories,
                n_clusters=self.n_clusters,
            )

            # Liberar caché
            df_product_categories.unpersist()

            # Exportar resultados
            print("\n💾 Exportando resultados a JSON...")
            self.exporter.export_clustering_results(
                clusters_df=results["clusters_df"],
                cluster_summary=results["cluster_summary"],
                n_clusters=self.n_clusters,
            )

            # Liberar caché
            df_transactions_exploded.unpersist()

            # Metadata de ejecución
            self.exporter.export_execution_metadata(
                dag_id="clustering_pipeline",
                task_id="generate_clustering",
                execution_info={
                    "status": "success",
                    "n_clusters": self.n_clusters,
                    "files_generated": [
                        "customer_clusters.json",
                        "cluster_summary.json",
                        "clustering_visualization.json",
                    ],
                    "deliverables": [
                        "Visualización del clustering",
                        "Descripción de cada grupo",
                        "Recomendaciones de negocio por cluster",
                    ],
                },
            )

            print("\n" + "=" * 70)
            print("✅ CLUSTERING GENERADO EXITOSAMENTE")
            print("=" * 70)

        except Exception as e:
            print(f"\n❌ Error en Clustering: {str(e)}")
            import traceback

            traceback.print_exc()
            raise
        finally:
            if self.spark:
                stop_spark_session(self.spark)


if __name__ == "__main__":
    pipeline = ClusteringPipeline(n_clusters=4)
    pipeline.run()
