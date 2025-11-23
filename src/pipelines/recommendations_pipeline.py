"""
Pipeline 4: Recomendaciones (Mensual)

Genera recomendaciones básicas usando reglas de asociación (FP-Growth)
para dos vistas:
- Productos: sugerencias de productos complementarios dado un SKU base.
- Clientes: sugerencias personalizadas basadas en su historial de compra.

Outputs: output/recommendations/product_recs.json y customer_recs.json
"""

import os
import sys
import json
from datetime import datetime
from typing import List, Dict, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    size,
    row_number,
    desc,
    collect_set,
    explode,
    array_contains,
)
from pyspark.sql.window import Window

from src.config.spark_config import create_spark_session, stop_spark_session
from src.data_loader import DataLoader
from src.analyzers.product_analyzer import ProductAnalyzer
from src.json_exporter import JSONExporter


class RecommendationsPipeline:
    """Pipeline para generar recomendaciones basadas en reglas de asociación."""

    def __init__(
        self,
        min_support: float = 0.01,
        min_confidence: float = 0.5,
        top_product_recs: int = 5,
        top_customer_recs: int = 5,
        max_customers: int = 500,
    ):
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.top_product_recs = top_product_recs
        self.top_customer_recs = top_customer_recs
        self.max_customers = max_customers

        self.spark = None
        self.data_loader = None
        self.product_analyzer = None
        self.exporter = None

    def run(self):
        """Ejecuta el pipeline completo."""
        try:
            print("=" * 70)
            print("🚀 PIPELINE 4: RECOMENDACIONES")
            print("=" * 70)

            print("\n📡 Inicializando Spark...")
            self.spark = create_spark_session("RecommendationsPipeline")

            print("\n📦 Inicializando componentes...")
            self.data_loader = DataLoader(self.spark)
            self.product_analyzer = ProductAnalyzer(self.spark)
            self.exporter = JSONExporter()

            df_transactions = None
            df_transactions_exploded = None

            print("\n📂 Cargando transacciones desde PostgreSQL...")
            df_transactions = self.data_loader.load_transactions()
            df_transactions.cache()
            print("   ✅ Transacciones cargadas")

            print("   → Generando vista explotada de productos...")
            df_transactions_exploded = self.data_loader.explode_transactions(
                df_transactions
            )
            df_transactions_exploded.cache()
            print("   ✅ Vista explotada lista")

            print("\n🧠 Ejecutando FP-Growth para obtener reglas de asociación...")
            analysis_results = self.product_analyzer.market_basket_analysis(
                df_transactions,
                min_support=self.min_support,
                min_confidence=self.min_confidence,
                top_rules=max(self.top_product_recs * 5, 20),
            )

            rules_df = analysis_results.get("association_rules")

            if rules_df is None or rules_df.rdd.isEmpty():
                print("\n⚠️ No se generaron reglas de asociación. Finalizando.")
                self._export_empty_outputs()
                return

            print("\n📊 Construyendo recomendaciones por producto...")
            product_recommendations = self._build_product_recommendations(rules_df)

            print("\n👤 Construyendo recomendaciones por cliente...")
            customer_recommendations = self._build_customer_recommendations(
                df_transactions_exploded, rules_df
            )

            print("\n💾 Exportando resultados...")
            files_generated = self._export_outputs(
                product_recommendations, customer_recommendations
            )

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
            try:
                if df_transactions_exploded is not None:
                    df_transactions_exploded.unpersist()
                if df_transactions is not None:
                    df_transactions.unpersist()
            except Exception:
                pass

            if self.spark:
                stop_spark_session(self.spark)

    def _prepare_simple_rules(self, rules_df: DataFrame) -> DataFrame:
        """
        Filtra reglas a pares (1 antecedente → 1 consecuente) para facilitar
        la generación de recomendaciones.
        """
        if rules_df is None or rules_df.rdd.isEmpty():
            return None

        simple_rules = (
            rules_df.filter(
                (size(col("antecedent")) == 1) & (size(col("consequent")) == 1)
            )
            .select(
                col("antecedent")[0].cast("int").alias("source_product_id"),
                col("consequent")[0].cast("int").alias("recommended_product_id"),
                col("confidence"),
                col("lift"),
                col("support"),
            )
            .cache()
        )

        if simple_rules.rdd.isEmpty():
            return None

        return simple_rules

    def _build_product_recommendations(self, rules_df: DataFrame) -> List[Dict[str, Any]]:
        simple_rules = self._prepare_simple_rules(rules_df)
        if simple_rules is None:
            print("   ⚠️ Sin reglas simples para productos.")
            return []

        window_rank = Window.partitionBy("source_product_id").orderBy(
            desc("confidence"), desc("lift")
        )

        ranked_rules = (
            simple_rules.withColumn("rank", row_number().over(window_rank))
            .filter(col("rank") <= self.top_product_recs)
            .select(
                col("source_product_id").alias("product_id"),
                col("recommended_product_id"),
                col("confidence"),
                col("lift"),
                col("support"),
                col("rank"),
            )
            .orderBy("product_id", "rank")
        )

        rec_map: Dict[int, List[Dict[str, Any]]] = {}
        for row in ranked_rules.collect():
            product_id = int(row.product_id) if row.product_id is not None else None
            recommended_id = (
                int(row.recommended_product_id)
                if row.recommended_product_id is not None
                else None
            )

            if product_id is None or recommended_id is None:
                continue

            rec_map.setdefault(product_id, []).append(
                {
                    "product_id": recommended_id,
                    "confidence": float(row.confidence) if row.confidence is not None else None,
                    "lift": float(row.lift) if row.lift is not None else None,
                    "support": float(row.support) if row.support is not None else None,
                }
            )

        return [
            {"product_id": product_id, "recommendations": recs}
            for product_id, recs in rec_map.items()
        ]

    def _build_customer_recommendations(
        self, df_transactions_exploded: DataFrame, rules_df: DataFrame
    ) -> List[Dict[str, Any]]:
        simple_rules = self._prepare_simple_rules(rules_df)
        if simple_rules is None:
            print("   ⚠️ Sin reglas simples para clientes.")
            return []

        customer_products = (
            df_transactions_exploded.groupBy("customer_id")
            .agg(collect_set("product_id").alias("purchased_products"))
            .withColumn("source_product_id", explode("purchased_products"))
        )

        joined = (
            customer_products.join(simple_rules, on="source_product_id", how="inner")
            .filter(
                ~array_contains(
                    col("purchased_products"), col("recommended_product_id")
                )
            )
            .select(
                col("customer_id"),
                col("source_product_id"),
                col("recommended_product_id"),
                col("confidence"),
                col("lift"),
                col("support"),
            )
        )

        if joined.rdd.isEmpty():
            print("   ⚠️ No se pudieron derivar recomendaciones de clientes.")
            return []

        window_duplicate = Window.partitionBy(
            "customer_id", "recommended_product_id"
        ).orderBy(desc("confidence"), desc("lift"))

        dedup = joined.withColumn("dup_rank", row_number().over(window_duplicate)).filter(
            col("dup_rank") == 1
        )

        window_customer = Window.partitionBy("customer_id").orderBy(
            desc("confidence"), desc("lift")
        )

        ranked = (
            dedup.withColumn("rank", row_number().over(window_customer))
            .filter(col("rank") <= self.top_customer_recs)
            .orderBy("customer_id", "rank")
        )

        rec_map: Dict[int, List[Dict[str, Any]]] = {}
        for row in ranked.collect():
            customer_id = int(row.customer_id) if row.customer_id is not None else None
            if customer_id is None:
                continue

            rec_map.setdefault(customer_id, []).append(
                {
                    "product_id": int(row.recommended_product_id),
                    "confidence": float(row.confidence) if row.confidence is not None else None,
                    "lift": float(row.lift) if row.lift is not None else None,
                    "support": float(row.support) if row.support is not None else None,
                    "based_on_product": int(row.source_product_id),
                }
            )

        # Limitar número de clientes para evitar archivos demasiado grandes
        limited_results: List[Dict[str, Any]] = []
        for customer_id in sorted(rec_map.keys()):
            limited_results.append(
                {"customer_id": customer_id, "recommendations": rec_map[customer_id]}
            )
            if len(limited_results) >= self.max_customers:
                break

        return limited_results

    def _export_outputs(
        self,
        product_recommendations: List[Dict[str, Any]],
        customer_recommendations: List[Dict[str, Any]],
    ) -> List[str]:
        timestamp = datetime.now().isoformat()
        recommendations_dir = os.path.join(self.exporter.output_base, "recommendations")
        os.makedirs(recommendations_dir, exist_ok=True)

        product_payload = {
            "generated_at": timestamp,
            "recommendation_type": "product_to_product",
            "total_products": len(product_recommendations),
            "min_support": self.min_support,
            "min_confidence": self.min_confidence,
            "data": product_recommendations,
        }

        customer_payload = {
            "generated_at": timestamp,
            "recommendation_type": "customer_personalized",
            "total_customers": len(customer_recommendations),
            "max_customers": self.max_customers,
            "min_support": self.min_support,
            "min_confidence": self.min_confidence,
            "data": customer_recommendations,
        }

        product_path = os.path.join(recommendations_dir, "product_recs.json")
        customer_path = os.path.join(recommendations_dir, "customer_recs.json")

        with open(product_path, "w", encoding="utf-8") as f:
            json.dump(product_payload, f, indent=2, ensure_ascii=False)
        print(f"   ✅ Product recs: {product_path}")

        with open(customer_path, "w", encoding="utf-8") as f:
            json.dump(customer_payload, f, indent=2, ensure_ascii=False)
        print(f"   ✅ Customer recs: {customer_path}")

        return [
            os.path.relpath(product_path, self.exporter.output_base),
            os.path.relpath(customer_path, self.exporter.output_base),
        ]

    def _export_empty_outputs(self):
        """Genera archivos vacíos para mantener consistencia con el frontend."""
        self._export_outputs([], [])


if __name__ == "__main__":
    pipeline = RecommendationsPipeline()
    pipeline.run()

