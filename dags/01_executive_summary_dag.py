"""
DAG 1: Resumen Ejecutivo

Genera métricas clave para el dashboard:
- Total de ventas (unidades)
- Número de transacciones
- Top 10 productos
- Top 10 clientes
- Días pico de compra
- Categorías más rentables

Frecuencia sugerida: Diaria
Output: output/summary/*.json
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from config.spark_config import create_spark_session, stop_spark_session
from src.data_loader import DataLoader
from src.summary_metrics import SummaryMetrics
from src.json_exporter import JSONExporter


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="01_executive_summary",
    description="Genera métricas del resumen ejecutivo para el dashboard",
    default_args=default_args,
    schedule=None,  # Cambiar a "@daily" en producción
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "summary", "dashboard"],
)
def executive_summary_dag():
    """DAG para generar el resumen ejecutivo."""

    @task(task_id="generate_executive_summary")
    def generate_summary():
        """Genera todas las métricas del resumen ejecutivo."""
        spark = None
        try:
            # Inicializar Spark
            print("🚀 Inicializando Spark para Resumen Ejecutivo...")
            spark = create_spark_session("ExecutiveSummary")
            
            # Inicializar componentes
            data_loader = DataLoader(spark)
            metrics_calculator = SummaryMetrics(spark)
            exporter = JSONExporter()
            
            # Cargar datos
            print("\n📂 Cargando datos desde PostgreSQL...")
            print("   [1/5] Cargando transacciones...")
            df_transactions = data_loader.load_transactions()
            print(f"   ✅ Transacciones cargadas (schema: {df_transactions.columns})")
            df_transactions.cache()
            print("   → DataFrame cacheado")
            
            print("   [2/5] Explodiendo transacciones...")
            df_transactions_exploded = data_loader.explode_transactions(df_transactions)
            print("   ✅ Transacciones explodidas")
            df_transactions_exploded.cache()
            print("   → DataFrame cacheado")
            
            print("   [3/5] Cargando productos-categorías...")
            df_product_categories = data_loader.load_product_categories()
            print(f"   ✅ Productos-categorías cargadas")
            df_product_categories.cache()
            print("   → DataFrame cacheado")

            print("   [4/5] Cargando categorías...")
            df_categories = data_loader.load_categories()
            print(f"   ✅ Categorías cargadas")
            df_categories.cache()
            print("   → DataFrame cacheado")
            
            print("   [5/5] Todos los datos cargados correctamente")
            
            # Generar resumen ejecutivo
            results = metrics_calculator.generate_executive_summary(
                df_transactions,
                df_transactions_exploded,
                df_product_categories,
                df_categories
            )
            
            # Exportar resultados a JSON
            print("\n💾 Exportando resultados a JSON...")
            
            # Métricas básicas
            exporter.export_summary_metrics(
                results["basic_metrics"],
                filename="basic_metrics.json"
            )
            
            # Top 10 productos
            exporter.export_top_items(
                results["top_products"],
                item_type="products",
                top_n=10
            )
            
            # Top 10 clientes
            exporter.export_top_items(
                results["top_customers"],
                item_type="customers",
                top_n=10
            )
            
            # Días pico
            exporter.export_top_items(
                results["peak_days"],
                item_type="peak_days",
                top_n=10
            )
            
            # Top categorías
            exporter.export_top_items(
                results["top_categories"],
                item_type="categories",
                top_n=10
            )
            
            # Metadata de ejecución
            exporter.export_execution_metadata(
                dag_id="01_executive_summary",
                task_id="generate_executive_summary",
                execution_info={
                    "total_transactions": results["basic_metrics"]["total_transactions"],
                    "total_sales_units": results["basic_metrics"]["total_sales_units"],
                    "status": "success"
                }
            )
            
            # Liberar caché
            df_transactions.unpersist()
            df_transactions_exploded.unpersist()
            df_product_categories.unpersist()
            df_categories.unpersist()
            
            print("\n✅ Resumen ejecutivo generado y exportado exitosamente")
            
        except Exception as e:
            print(f"\n❌ Error en Resumen Ejecutivo: {str(e)}")
            raise
        finally:
            if spark:
                stop_spark_session(spark)

    # Definir flujo
    generate_summary()


# Instanciar el DAG
executive_summary_dag()
