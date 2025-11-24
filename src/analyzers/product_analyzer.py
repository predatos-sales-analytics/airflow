"""
Módulo para análisis de productos.
Incluye análisis de productos más vendidos y reglas de asociación (Market Basket Analysis).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    desc,
    collect_list,
    collect_set,
    array_distinct,
    size,
    explode,
    split,
    trim,
    row_number,
    concat_ws,
    slice as array_slice,
    udf,
)
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.window import Window
from typing import Dict, Any, List, Tuple, Union
import os
import json
from datetime import datetime


class ProductAnalyzer:
    """Clase para realizar análisis de productos y reglas de asociación."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el analizador de productos.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

    def _compute_basket_stats(self, df_baskets: DataFrame) -> Dict[str, Any]:
        """
        Calcula las estadísticas de las canastas. Es decir, el número de transacciones, el tamaño promedio de la canasta, el tamaño mínimo de la canasta y el tamaño máximo de la canasta. Estas estadísticas se usan para determinar si se debe recalcular el FP-Growth o se pueden reutilizar los resultados previos.

        Args:
            df_baskets: DataFrame con las canastas

        Returns:
            Diccionario con las estadísticas de las canastas
        """
        df_basket_stats = df_baskets.withColumn("basket_size", size(col("items")))

        stats = df_basket_stats.select(
            count("*").alias("num_transacciones"),
            avg("basket_size").alias("avg_basket_size"),
            spark_min("basket_size").alias("min_basket_size"),
            spark_max("basket_size").alias("max_basket_size"),
        ).collect()[0]

        return {
            "total_transactions": int(stats["num_transacciones"]),
            "average_basket_size": (
                round(float(stats["avg_basket_size"]), 2)
                if stats["avg_basket_size"] is not None
                else None
            ),
            "min_basket_size": (
                int(stats["min_basket_size"])
                if stats["min_basket_size"] is not None
                else None
            ),
            "max_basket_size": (
                int(stats["max_basket_size"])
                if stats["max_basket_size"] is not None
                else None
            ),
        }

    def prepare_baskets(
        self, df: DataFrame, return_stats: bool = False
    ) -> Union[DataFrame, Tuple[DataFrame, Dict[str, Any]]]:
        """
        Prepara los datos en formato de "canastas" para Market Basket Analysis.

        Cada fila representa una transacción (canasta) con la lista de productos.

        Args:
            df: DataFrame con transacciones

        Returns:
            DataFrame con formato de canastas
        """
        print("\n🛒 Preparando datos para Market Basket Analysis")
        print("-" * 60)

        # Convertir string de productos a array
        df_baskets = df.withColumn("items", split(trim(col("products")), " ")).select(
            col("customer_id"), col("transaction_date"), col("store_id"), col("items")
        )

        stats_dict = self._compute_basket_stats(df_baskets)

        print(f"📊 Estadísticas de las canastas:")
        print(f"   Total de transacciones: {stats_dict['total_transactions']:,}")
        print(
            f"   Tamaño promedio de canasta: {stats_dict['average_basket_size']:.2f} productos"
            if stats_dict["average_basket_size"] is not None
            else "   Tamaño promedio de canasta: N/A"
        )
        print(f"   Canasta más pequeña: {stats_dict['min_basket_size']} producto(s)")
        print(f"   Canasta más grande: {stats_dict['max_basket_size']} productos")

        if return_stats:
            return df_baskets, stats_dict

        self.last_basket_stats = stats_dict
        return df_baskets

    def market_basket_analysis(
        self,
        df: DataFrame,
        min_support: float = 0.01,
        min_confidence: float = 0.3,
        top_rules: int = 20,
    ) -> Dict[str, DataFrame]:
        """
        Realiza Market Basket Analysis usando FP-Growth.

        Encuentra patrones frecuentes y reglas de asociación entre productos.

        Args:
            df: DataFrame con transacciones
            min_support: Soporte mínimo (default: 0.01 = 1%)
            min_confidence: Confianza mínima para reglas (default: 0.3 = 30%)
            top_rules: Número de mejores reglas a mostrar

        Returns:
            Diccionario con itemsets frecuentes y reglas de asociación
        """
        print("\n🔍 Market Basket Analysis (FP-Growth)")
        print("-" * 60)
        print(f"⚙️ Parámetros:")
        print(f"   Soporte mínimo: {min_support*100:.1f}%")
        print(f"   Confianza mínima: {min_confidence*100:.1f}%")

        # Preparar canastas y obtener estadísticas
        input_is_baskets = "items" in df.columns and "products" not in df.columns
        if input_is_baskets:
            df_baskets = df
            stats_dict = self._compute_basket_stats(df_baskets)
        else:
            df_baskets, stats_dict = self.prepare_baskets(df, return_stats=True)
        self.last_basket_stats = stats_dict

        # Definir rutas de archivos
        base_output = os.getenv("OUTPUT_PATH", "/opt/airflow/output")
        output_dir = os.path.join(base_output, "data")
        freq_itemsets_path = os.path.join(output_dir, "fp_growth_freq_itemsets")
        association_rules_path = os.path.join(output_dir, "fp_growth_association_rules")
        stats_path = os.path.join(output_dir, "fp_growth_dataset_stats.json")

        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)
        print(f"💾 Directorio de salida: {os.path.abspath(output_dir)}")

        # Verificar si los archivos ya existen (puede ser directorio de Spark o archivo CSV individual)
        def check_csv_files_exist(path):
            """Verifica si existen archivos CSV en el directorio o como archivo individual."""
            # Verificar si es un archivo CSV individual
            if os.path.exists(path) and os.path.isfile(path) and path.endswith(".csv"):
                return True
            # Verificar si es un directorio con archivos CSV
            if os.path.exists(path) and os.path.isdir(path):
                files = os.listdir(path)
                # Spark puede crear archivos .csv o archivos sin extensión que empiezan con 'part-'
                csv_files = [
                    f for f in files if f.endswith(".csv") or f.startswith("part-")
                ]
                return len(csv_files) > 0
            return False

        # También verificar archivos CSV individuales (método alternativo con pandas)
        freq_itemsets_csv_path = os.path.join(output_dir, "fp_growth_freq_itemsets.csv")
        rules_csv_path = os.path.join(output_dir, "fp_growth_association_rules.csv")

        freq_itemsets_exists = check_csv_files_exist(
            freq_itemsets_path
        ) or os.path.exists(freq_itemsets_csv_path)
        association_rules_exists = check_csv_files_exist(
            association_rules_path
        ) or os.path.exists(rules_csv_path)

        previous_stats = None
        stats_changed = True
        if os.path.exists(stats_path):
            try:
                with open(stats_path, "r", encoding="utf-8") as f:
                    previous_stats = json.load(f)
                    if isinstance(previous_stats, dict):
                        previous_stats = {
                            k: v
                            for k, v in previous_stats.items()
                            if k != "generated_at"
                        }
            except Exception:
                previous_stats = None

        if previous_stats == stats_dict:
            stats_changed = False

        if stats_changed:
            print("🆕 Estadísticas de dataset cambiaron, se recalculará FP-Growth.")
        else:
            print(
                "♻️ Estadísticas sin cambios, se intentará reutilizar resultados previos."
            )

        use_cached = (
            freq_itemsets_exists and association_rules_exists and not stats_changed
        )

        if use_cached:
            print(f"\n📂 Cargando resultados de FP-Growth desde archivos existentes...")
            print(f"   Ruta: {output_dir}/")

            # Determinar qué archivo usar (directorio de Spark o CSV individual)
            freq_path_to_load = (
                freq_itemsets_csv_path
                if os.path.exists(freq_itemsets_csv_path)
                else freq_itemsets_path
            )
            rules_path_to_load = (
                rules_csv_path
                if os.path.exists(rules_csv_path)
                else association_rules_path
            )

            # Cargar itemsets frecuentes
            df_freq_itemsets = self.spark.read.csv(
                freq_path_to_load, header=True, inferSchema=True
            )
            # Convertir la columna items de string a array
            df_freq_itemsets = df_freq_itemsets.withColumn(
                "items", split(trim(col("items")), ",")
            )

            # Cargar reglas de asociación
            df_rules = self.spark.read.csv(
                rules_path_to_load, header=True, inferSchema=True
            )
            # Convertir las columnas antecedent y consequent de string a array
            df_rules = df_rules.withColumn(
                "antecedent", split(trim(col("antecedent")), ",")
            ).withColumn("consequent", split(trim(col("consequent")), ","))

            total_rules = df_rules.count()
            print(f"✅ Resultados cargados exitosamente")
            print(f"📊 Itemsets Frecuentes encontrados: {df_freq_itemsets.count():,}")

            print(f"\n📋 Top itemsets más frecuentes:")
            df_freq_itemsets.orderBy(desc("freq")).show(20, truncate=False)

            print(f"📊 Reglas de Asociación encontradas: {total_rules:,}")

            # Crear un modelo dummy para mantener compatibilidad
            model = None
        else:
            # Aplicar FP-Growth
            print(f"\n🔄 Ejecutando algoritmo FP-Growth...")
            fpGrowth = FPGrowth(
                itemsCol="items", minSupport=min_support, minConfidence=min_confidence
            )

            model = fpGrowth.fit(df_baskets)

            # Obtener itemsets frecuentes
            df_freq_itemsets = model.freqItemsets
            print(f"\n📊 Itemsets Frecuentes encontrados: {df_freq_itemsets.count():,}")

            print(f"\n📋 Top itemsets más frecuentes:")
            df_freq_itemsets.orderBy(desc("freq")).show(20, truncate=False)

            # Obtener reglas de asociación
            df_rules = model.associationRules
            total_rules = df_rules.count()
            print(f"\n📊 Reglas de Asociación encontradas: {total_rules:,}")

            # Guardar resultados en CSV
            print(f"\n💾 Guardando resultados de FP-Growth en {output_dir}/...")

            # Convertir arrays a strings para guardar en CSV
            df_freq_itemsets_to_save = df_freq_itemsets.withColumn(
                "items", concat_ws(",", col("items"))
            )

            df_rules_to_save = df_rules.withColumn(
                "antecedent", concat_ws(",", col("antecedent"))
            ).withColumn("consequent", concat_ws(",", col("consequent")))

            # Intentar guardar usando Spark CSV, si falla usar pandas (compatible con Windows)
            try:
                # Guardar itemsets frecuentes
                df_freq_itemsets_to_save.coalesce(1).write.mode("overwrite").option(
                    "header", "true"
                ).csv(freq_itemsets_path)

                # Guardar reglas de asociación
                df_rules_to_save.coalesce(1).write.mode("overwrite").option(
                    "header", "true"
                ).csv(association_rules_path)

                print(f"✅ Resultados guardados exitosamente en {output_dir}/")
            except Exception as e:
                # Si falla con Spark (problema común en Windows), usar pandas como respaldo
                print(
                    f"⚠️ Error al guardar con Spark CSV, usando método alternativo (pandas)..."
                )
                import pandas as pd

                # Convertir a pandas y guardar directamente
                freq_itemsets_csv_path = os.path.join(
                    output_dir, "fp_growth_freq_itemsets.csv"
                )
                rules_csv_path = os.path.join(
                    output_dir, "fp_growth_association_rules.csv"
                )

                # Guardar itemsets frecuentes
                pdf_freq = df_freq_itemsets_to_save.toPandas()
                pdf_freq.to_csv(freq_itemsets_csv_path, index=False, encoding="utf-8")

                # Guardar reglas de asociación
                pdf_rules = df_rules_to_save.toPandas()
                pdf_rules.to_csv(rules_csv_path, index=False, encoding="utf-8")

                print(
                    f"✅ Resultados guardados exitosamente usando método alternativo en {output_dir}/"
                )

            # Guardar estadísticas de dataset
            with open(stats_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        **stats_dict,
                        "generated_at": datetime.now().isoformat(),
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )

        if total_rules > 0:
            # Ordenar por lift (mejor métrica que confidence sola)
            df_rules_sorted = df_rules.orderBy(desc("lift"), desc("confidence"))

            print(f"\n🏆 Top {top_rules} Reglas de Asociación (ordenadas por Lift):")
            df_rules_sorted.show(top_rules, truncate=False)

            # Explicar las métricas
            print(f"\n📖 Interpretación de métricas:")
            print(f"   • Antecedent (SI): Producto(s) en la canasta")
            print(
                f"   • Consequent (ENTONCES): Producto(s) frecuentemente comprados juntos"
            )
            print(
                f"   • Confidence: Probabilidad de comprar 'consequent' dado 'antecedent'"
            )
            print(
                f"   • Lift > 1: Indica asociación positiva (compran juntos más de lo esperado)"
            )
            print(f"   • Lift = 1: Independientes")
            print(f"   • Lift < 1: Asociación negativa")

            # Filtrar mejores reglas (lift > 1.5 y confidence > 0.5)
            df_best_rules = df_rules_sorted.filter(
                (col("lift") > 1.5) & (col("confidence") > 0.5)
            )

            best_count = df_best_rules.count()
            if best_count > 0:
                print(
                    f"\n⭐ Reglas FUERTES (Lift > 1.5 y Confidence > 50%): {best_count}"
                )
                df_best_rules.show(20, truncate=False)
            else:
                print(
                    f"\n⚠️ No se encontraron reglas fuertes con los criterios especificados"
                )
                print(f"   Considera ajustar min_support o min_confidence")

        else:
            print(f"\n⚠️ No se encontraron reglas de asociación")
            print(f"   Considera reducir min_support o min_confidence")

        return {
            "frequent_itemsets": df_freq_itemsets,
            "association_rules": df_rules,
            "model": model,
        }

    def analyze_product_combinations(self, df: DataFrame, top_n: int = 20) -> DataFrame:
        """
        Analiza las combinaciones de productos más frecuentes en transacciones.

        Args:
            df: DataFrame con transacciones
            top_n: Número de combinaciones top a mostrar

        Returns:
            DataFrame con combinaciones más frecuentes
        """
        print("\n🔗 Análisis de Combinaciones de Productos")
        print("-" * 60)

        # Preparar datos
        df_combinations = (
            df.withColumn("items", split(trim(col("products")), " "))
            .withColumn("num_items", size(col("items")))
            .filter(col("num_items") >= 2)  # Solo combinaciones de 2+ productos
        )

        print(f"\n📊 Transacciones con múltiples productos:")
        print(f"   Total: {df_combinations.count():,}")

        # Agrupar por combinación exacta
        df_combo_freq = (
            df_combinations.groupBy("items")
            .agg(count("*").alias("frequency"))
            .orderBy(desc("frequency"))
        )

        print(f"\n🏆 Top {top_n} combinaciones más frecuentes:")
        df_combo_freq.show(top_n, truncate=False)

        return df_combo_freq

    def generate_product_summary(
        self,
        df: DataFrame,
        df_exploded: DataFrame,
        run_market_basket: bool = True,
        min_support: float = 0.01,
        min_confidence: float = 0.3,
    ) -> Dict[str, Any]:
        """
        Genera un resumen completo del análisis de productos.

        Args:
            df: DataFrame con transacciones (sin explotar)
            df_exploded: DataFrame con productos explotados
            run_market_basket: Si ejecutar Market Basket Analysis
            min_support: Soporte mínimo para FP-Growth
            min_confidence: Confianza mínima para reglas

        Returns:
            Diccionario con resumen
        """
        print("\n" + "=" * 60)
        print("🛍️ RESUMEN DE ANÁLISIS DE PRODUCTOS")
        print("=" * 60)

        # Análisis de combinaciones
        df_combinations = self.analyze_product_combinations(df, top_n=20)

        # Market Basket Analysis
        market_basket_results = None
        if run_market_basket:
            try:
                market_basket_results = self.market_basket_analysis(
                    df,
                    min_support=min_support,
                    min_confidence=min_confidence,
                    top_rules=20,
                )
            except Exception as e:
                print(f"\n⚠️ Error en Market Basket Analysis: {str(e)}")
                print(f"   Continuando sin este análisis...")

        print("=" * 60)

        return {
            "top_products": df_top_products,
            "store_products": df_store_products,
            "combinations": df_combinations,
            "market_basket": market_basket_results,
        }

    @staticmethod
    def _safe_int(value):
        try:
            return int(value)
        except Exception:
            return None

    def build_product_recommendations(
        self,
        rules_df: DataFrame,
        top_n: int = 5,
        max_products: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Construye recomendaciones por producto a partir de las reglas FP-Growth.

        Args:
            rules_df: DataFrame con reglas de asociación
            top_n: Número máximo de recomendaciones por producto
            max_products: Número máximo de productos a exportar (para frontend)
        """
        if rules_df is None or rules_df.rdd.isEmpty():
            return []

        rules = [
            {
                "antecedent": [self._safe_int(p) for p in row["antecedent"]],
                "consequent": [self._safe_int(p) for p in row["consequent"]],
                "confidence": float(row["confidence"]),
                "lift": float(row["lift"]),
                "support": float(row["support"]),
            }
            for row in rules_df.collect()
        ]

        from collections import defaultdict

        product_map = defaultdict(dict)

        for rule in rules:
            antecedent = [p for p in rule["antecedent"] if p is not None]
            consequent = [p for p in rule["consequent"] if p is not None]
            if not antecedent or not consequent:
                continue

            score = rule["lift"] * rule["confidence"]
            for source_product in antecedent:
                for recommended_product in consequent:
                    if source_product == recommended_product:
                        continue

                    existing = product_map[source_product].get(recommended_product)
                    if existing is None or score > existing["score"]:
                        product_map[source_product][recommended_product] = {
                            "product_id": int(recommended_product),
                            "confidence": round(rule["confidence"], 4),
                            "lift": round(rule["lift"], 4),
                            "support": round(rule["support"], 4),
                            "score": score,
                        }

        results = []
        for product_id, recs in product_map.items():
            sorted_recs = sorted(
                recs.values(), key=lambda r: (r["lift"], r["confidence"]), reverse=True
            )
            for rec in sorted_recs:
                rec.pop("score", None)
            results.append(
                {
                    "product_id": int(product_id),
                    "recommendations": sorted_recs[:top_n],
                }
            )

        results.sort(key=lambda r: len(r["recommendations"]), reverse=True)
        return results[:max_products]

    def build_customer_recommendations(
        self,
        df_transactions: DataFrame,
        rules_df: DataFrame,
        top_recommendations: int = 5,
        max_customers: int = 200,
        max_rules: int = 300,
    ) -> List[Dict[str, Any]]:
        """
        Construye recomendaciones por cliente usando las reglas FP-Growth.

        Args:
            df_transactions: DataFrame con transacciones originales
            rules_df: DataFrame con reglas de asociación
            top_recommendations: Número máximo de productos recomendados por cliente
            max_customers: Número máximo de clientes a exportar
            max_rules: Número máximo de reglas a considerar (para performance)
        """
        if (
            rules_df is None
            or rules_df.rdd.isEmpty()
            or df_transactions is None
            or df_transactions.rdd.isEmpty()
        ):
            return []

        rules = [
            {
                "antecedent": [self._safe_int(p) for p in row["antecedent"]],
                "consequent": [self._safe_int(p) for p in row["consequent"]],
                "confidence": float(row["confidence"]),
                "lift": float(row["lift"]),
                "support": float(row["support"]),
            }
            for row in rules_df.orderBy(desc("lift"), desc("confidence"))
            .limit(max_rules)
            .collect()
        ]

        if not rules:
            return []

        # Productos por cliente
        df_customer_products = (
            df_transactions.withColumn(
                "product_array", split(trim(col("products")), " ")
            )
            .withColumn("product_id", explode(col("product_array")))
            .withColumn("product_id", col("product_id").cast("int"))
            .groupBy("customer_id")
            .agg(collect_set(col("product_id")).alias("products"))
        )

        customer_rows = [
            {
                "customer_id": int(row["customer_id"]),
                "products": [
                    self._safe_int(p) for p in row["products"] if p is not None
                ],
            }
            for row in df_customer_products.collect()
        ]

        results = []
        for customer in customer_rows:
            purchased = set(filter(lambda x: x is not None, customer["products"]))
            if not purchased:
                continue

            scored = {}
            for rule in rules:
                antecedent = [p for p in rule["antecedent"] if p is not None]
                if not antecedent:
                    continue

                if set(antecedent).issubset(purchased):
                    for product in rule["consequent"]:
                        if product is None or product in purchased:
                            continue
                        score = rule["lift"] * rule["confidence"]
                        existing = scored.get(product)
                        if existing is None or score > existing["score"]:
                            scored[product] = {
                                "product_id": int(product),
                                "confidence": round(rule["confidence"], 4),
                                "lift": round(rule["lift"], 4),
                                "support": round(rule["support"], 4),
                                "based_on": [int(p) for p in antecedent],
                                "score": score,
                            }

            if not scored:
                continue

            recommendations = sorted(
                scored.values(),
                key=lambda r: (r["lift"], r["confidence"]),
                reverse=True,
            )
            for rec in recommendations:
                rec.pop("score", None)

            results.append(
                {
                    "customer_id": customer["customer_id"],
                    "products_owned": sorted(purchased),
                    "recommendations": recommendations[:top_recommendations],
                }
            )

        results.sort(key=lambda r: len(r["recommendations"]), reverse=True)
        return results[:max_customers]
