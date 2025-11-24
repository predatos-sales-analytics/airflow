"""
Pipeline 4: Recomendaciones (Mensual)

Genera recomendaciones usando FP-Growth para cubrir dos escenarios:
- Dado un producto → sugerir productos que se compran juntos (complementarios).
- Dado un cliente → sugerir nuevos productos basados en su historial.

Outputs JSON compatibles con el frontend:
- output/advanced/recommendations/product_associations.json
- output/advanced/recommendations/product_recommendations.json
- output/advanced/recommendations/customer_recommendations.json
"""

import os
import sys
from typing import List, Dict, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql.functions import desc

from src.config.spark_config import create_spark_session, stop_spark_session
from src.data_loader import DataLoader
from src.analyzers.product_analyzer import ProductAnalyzer
from src.json_exporter import JSONExporter


class RecommendationsPipeline:
    """Pipeline para generar recomendaciones basadas en Market Basket Analysis."""

    def __init__(
        self,
        min_support: float = 0.01,
        min_confidence: float = 0.5,
        top_product_recs: int = 5,
        top_customer_recs: int = 5,
        max_products: int = 200,
        max_customers: int = 200,
    ):
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.top_product_recs = top_product_recs
        self.top_customer_recs = top_customer_recs
        self.max_products = max_products
        self.max_customers = max_customers

        self.spark = None
        self.data_loader = None
        self.product_analyzer = None
        self.exporter = None

    def run(self):
        df_transactions = None
        try:
            print("=" * 70)
            print("🚀 PIPELINE 4: RECOMENDACIONES DE PRODUCTOS")
            print("=" * 70)

            self.spark = create_spark_session("RecommendationsPipeline")
            self.data_loader = DataLoader(self.spark)
            self.product_analyzer = ProductAnalyzer(self.spark)
            self.exporter = JSONExporter()

            print("\n📂 Cargando transacciones desde PostgreSQL...")
            df_transactions = self.data_loader.load_transactions()
            df_transactions.cache()
            print("   ✅ Transacciones cargadas")

            print("\n🧠 Ejecutando FP-Growth...")
            analysis_results = self.product_analyzer.market_basket_analysis(
                df_transactions,
                min_support=self.min_support,
                min_confidence=self.min_confidence,
                top_rules=max(self.top_product_recs * 5, 50),
            )

            rules_df = analysis_results.get("association_rules")
            itemsets_df = analysis_results.get("frequent_itemsets")

            if rules_df is None or rules_df.rdd.isEmpty():
                print("\n⚠️ No se generaron reglas de asociación.")
                self._export_empty_outputs()
                return

            print("\n📊 Generando recomendaciones por producto...")
            product_recommendations = (
                self.product_analyzer.build_product_recommendations(
                    rules_df,
                    top_n=self.top_product_recs,
                    max_products=self.max_products,
                )
            )

            print("\n👤 Generando recomendaciones personalizadas por cliente...")
            customer_recommendations = (
                self.product_analyzer.build_customer_recommendations(
                    df_transactions,
                    rules_df,
                    top_recommendations=self.top_customer_recs,
                    max_customers=self.max_customers,
                )
            )

            print("\n💾 Preparando payloads para exportación...")
            association_payload = self._build_association_payload(rules_df, itemsets_df)

            product_payload = {
                "summary": {
                    "total_products": len(product_recommendations),
                    "max_recommendations_per_product": self.top_product_recs,
                },
                "recommendations": product_recommendations,
            }

            customer_payload = {
                "summary": {
                    "total_customers": len(customer_recommendations),
                    "max_recommendations_per_customer": self.top_customer_recs,
                },
                "recommendations": customer_recommendations,
            }

            print("\n📝 Exportando archivos JSON...")
            files_generated = [
                self.exporter.export_recommendations(
                    association_payload, rec_type="product_associations"
                ),
                self.exporter.export_recommendations(
                    product_payload, rec_type="product_recommendations"
                ),
                self.exporter.export_recommendations(
                    customer_payload, rec_type="customer_recommendations"
                ),
            ]

            self.exporter.export_execution_metadata(
                dag_id="recommendations_pipeline",
                task_id="generate_recommendations",
                execution_info={
                    "status": "success",
                    "files_generated": files_generated,
                    "params": {
                        "min_support": self.min_support,
                        "min_confidence": self.min_confidence,
                        "top_product_recs": self.top_product_recs,
                        "top_customer_recs": self.top_customer_recs,
                    },
                },
            )

            print("\n" + "=" * 70)
            print("✅ RECOMENDACIONES GENERADAS EXITOSAMENTE")
            print("=" * 70)

        except Exception as exc:
            print(f"\n❌ Error en RecommendationsPipeline: {str(exc)}")
            import traceback

            traceback.print_exc()
            raise
        finally:
            if "df_transactions" in locals() and df_transactions is not None:
                try:
                    df_transactions.unpersist()
                except Exception:
                    pass

            if self.spark:
                stop_spark_session(self.spark)

    def _build_association_payload(self, rules_df, itemsets_df) -> Dict[str, Any]:
        total_rules = rules_df.count() if rules_df else 0
        total_itemsets = itemsets_df.count() if itemsets_df else 0

        top_rules = (
            self.exporter._spark_to_dict_list(
                rules_df.orderBy(desc("lift"), desc("confidence")).limit(100)
            )
            if rules_df
            else []
        )

        top_itemsets = (
            self.exporter._spark_to_dict_list(
                itemsets_df.orderBy(desc("freq")).limit(100)
            )
            if itemsets_df
            else []
        )

        return {
            "summary": {
                "total_rules": total_rules,
                "total_itemsets": total_itemsets,
                "min_support": self.min_support,
                "min_confidence": self.min_confidence,
            },
            "top_rules": top_rules,
            "top_itemsets": top_itemsets,
        }

    def _export_empty_outputs(self):
        empty_payload = {
            "summary": {
                "total_rules": 0,
                "total_itemsets": 0,
                "min_support": self.min_support,
                "min_confidence": self.min_confidence,
            },
            "top_rules": [],
            "top_itemsets": [],
        }

        self.exporter.export_recommendations(
            empty_payload, rec_type="product_associations"
        )
        self.exporter.export_recommendations(
            {"summary": {"total_products": 0}, "recommendations": []},
            rec_type="product_recommendations",
        )
        self.exporter.export_recommendations(
            {"summary": {"total_customers": 0}, "recommendations": []},
            rec_type="customer_recommendations",
        )


if __name__ == "__main__":
    RecommendationsPipeline().run()
