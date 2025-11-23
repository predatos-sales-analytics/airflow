"""
Módulo para análisis y segmentación de clientes usando K-Means.

Implementa clustering para identificar grupos de clientes según su
comportamiento de compra.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    countDistinct,
    size,
    split,
    trim,
    desc,
    to_date,
    lit,
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from typing import Dict, Any, Tuple
import os


class CustomerAnalyzer:
    """Clase para realizar análisis y segmentación de clientes con K-Means."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el analizador de clientes.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

    def prepare_customer_features(
        self,
        df_transactions: DataFrame,
        df_transactions_exploded: DataFrame,
        df_product_categories: DataFrame = None,
    ) -> DataFrame:
        """
        Prepara features para clustering de clientes.

        Features requeridas según enunciado:
        - Frecuencia: número de transacciones
        - Número de productos distintos: diversidad de productos
        - Volumen total: total de productos comprados
        - Diversidad de categorías: número de categorías distintas compradas

        Args:
            df_transactions: DataFrame de transacciones sin explotar
            df_transactions_exploded: DataFrame de transacciones explotadas
            df_product_categories: DataFrame de relación producto-categoría (opcional)

        Returns:
            DataFrame con features por cliente
        """
        print("\n🔧 Preparando features para segmentación de clientes...")
        print("-" * 70)

        # Feature 1: Frecuencia (número de transacciones)
        df_frequency = df_transactions.groupBy("customer_id").agg(
            count("*").alias("frequency")
        )

        # Feature 2: Número de productos distintos (diversidad de productos)
        df_unique_products = df_transactions_exploded.groupBy("customer_id").agg(
            countDistinct("product_id").alias("unique_products")
        )

        # Feature 3: Volumen total (total de productos comprados)
        df_volume = df_transactions.groupBy("customer_id").agg(
            spark_sum(size(split(trim(col("products")), " "))).alias("total_volume")
        )

        # Feature 4: Diversidad de categorías
        if df_product_categories is not None:
            # Join con categorías de productos
            df_exploded_with_cats = df_transactions_exploded.join(
                df_product_categories, "product_id", "left"
            )
            df_unique_categories = df_exploded_with_cats.groupBy("customer_id").agg(
                countDistinct("category_id").alias("unique_categories")
            )
        else:
            # Si no hay categorías, crear columna con 0
            print(
                "   ⚠️ No se proporcionó df_product_categories, usando 0 para diversidad de categorías"
            )
            df_unique_categories = (
                df_transactions.groupBy("customer_id")
                .agg(count("*").alias("temp"))
                .select("customer_id", lit(0).cast("int").alias("unique_categories"))
            )

        # Combinar todas las features
        df_features = (
            df_frequency.join(df_unique_products, "customer_id", "inner")
            .join(df_volume, "customer_id", "inner")
            .join(df_unique_categories, "customer_id", "inner")
        )

        # Convertir customer_id a int
        df_features = df_features.withColumn(
            "customer_id", col("customer_id").cast("int")
        )

        # Mostrar estadísticas
        stats = df_features.select(
            count("*").alias("total_customers"),
            avg("frequency").alias("avg_frequency"),
            avg("total_volume").alias("avg_volume"),
            avg("unique_products").alias("avg_unique_products"),
            avg("unique_categories").alias("avg_unique_categories"),
        ).collect()[0]

        print(f"📊 Total de clientes: {stats['total_customers']:,}")
        print(f"📊 Frecuencia promedio: {stats['avg_frequency']:.2f} transacciones")
        print(f"📊 Volumen total promedio: {stats['avg_volume']:.2f} productos")
        print(f"📊 Productos distintos promedio: {stats['avg_unique_products']:.2f}")
        print(f"📊 Categorías distintas promedio: {stats['avg_unique_categories']:.2f}")

        return df_features

    def perform_kmeans_clustering(
        self, df_features: DataFrame, n_clusters: int = 4
    ) -> Tuple[DataFrame, Any]:
        """
        Ejecuta clustering K-Means en los clientes.

        Args:
            df_features: DataFrame con features de clientes
            n_clusters: Número de clusters a crear

        Returns:
            Tuple (DataFrame con asignaciones de clusters, modelo K-Means)
        """
        print(f"\n🎯 Ejecutando K-Means con {n_clusters} clusters...")
        print("-" * 70)

        # Seleccionar features numéricas para clustering (según enunciado)
        feature_cols = [
            "frequency",  # Frecuencia
            "unique_products",  # Número de productos distintos
            "total_volume",  # Volumen total
            "unique_categories",  # Diversidad de categorías
        ]

        # Ensamblar features en vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        df_assembled = assembler.transform(df_features)

        # Escalar features (importante para K-Means)
        scaler = StandardScaler(
            inputCol="features_raw", outputCol="features", withStd=True, withMean=True
        )
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)

        # Entrenar K-Means
        kmeans = KMeans(
            k=n_clusters,
            featuresCol="features",
            predictionCol="cluster",
            seed=42,
            maxIter=20,
        )

        model = kmeans.fit(df_scaled)
        df_clustered = model.transform(df_scaled)

        # Evaluar clustering
        evaluator = ClusteringEvaluator(
            featuresCol="features", predictionCol="cluster", metricName="silhouette"
        )
        silhouette = evaluator.evaluate(df_clustered)

        print(f"✅ Clustering completado")
        print(f"📊 Silhouette Score: {silhouette:.4f}")
        print(f"📊 Centros de clusters: {n_clusters}")

        # Seleccionar columnas relevantes
        df_result = df_clustered.select(
            "customer_id",
            "frequency",
            "unique_products",
            "total_volume",
            "unique_categories",
            "cluster",
        )

        return df_result, model

    def analyze_clusters(
        self, df_clustered: DataFrame, n_clusters: int
    ) -> Dict[str, Any]:
        """
        Analiza y describe los clusters encontrados.

        Args:
            df_clustered: DataFrame con clientes y sus clusters
            n_clusters: Número de clusters

        Returns:
            Diccionario con descripción de cada cluster
        """
        print(f"\n📊 Analizando características de cada cluster...")
        print("-" * 70)

        cluster_profiles = {}
        total_customers = df_clustered.count()

        for cluster_id in range(n_clusters):
            print(f"\n{'='*70}")
            print(f"🏷️  CLUSTER {cluster_id}")
            print(f"{'='*70}")

            df_cluster = df_clustered.filter(col("cluster") == cluster_id)
            n_customers = df_cluster.count()

            stats = df_cluster.select(
                avg("frequency").alias("avg_frequency"),
                avg("unique_products").alias("avg_unique_products"),
                avg("total_volume").alias("avg_volume"),
                avg("unique_categories").alias("avg_unique_categories"),
                spark_min("frequency").alias("min_frequency"),
                spark_max("frequency").alias("max_frequency"),
                spark_min("total_volume").alias("min_volume"),
                spark_max("total_volume").alias("max_volume"),
            ).collect()[0]

            print(f"👥 Número de clientes: {n_customers:,}")
            print(f"📊 Frecuencia promedio: {stats['avg_frequency']:.2f} transacciones")
            print(
                f"📊 Productos distintos promedio: {stats['avg_unique_products']:.2f}"
            )
            print(f"📊 Volumen total promedio: {stats['avg_volume']:.2f} productos")
            print(
                f"📊 Categorías distintas promedio: {stats['avg_unique_categories']:.2f}"
            )

            # Clasificar el cluster y generar descripción detallada
            avg_freq = stats["avg_frequency"]
            avg_vol = stats["avg_volume"]
            avg_prod_dist = stats["avg_unique_products"]
            avg_cat_dist = stats["avg_unique_categories"]

            # Determinar tipo de cluster y generar descripción
            if avg_freq > 15 and avg_vol > 50 and avg_cat_dist > 10:
                cluster_label = "VIP / Compradores Premium"
                description = (
                    f"Clientes con alta frecuencia de compra ({avg_freq:.1f} transacciones en promedio), "
                    f"alto volumen de productos ({avg_vol:.0f} productos), amplia diversidad de productos "
                    f"({avg_prod_dist:.0f} productos distintos) y exploran múltiples categorías "
                    f"({avg_cat_dist:.1f} categorías). Son los clientes más valiosos."
                )
                recommendations = [
                    "Programa de fidelización premium con beneficios exclusivos",
                    "Acceso anticipado a nuevos productos y ofertas especiales",
                    "Asesor personalizado o atención prioritaria",
                    "Descuentos por volumen y puntos de recompensa",
                ]
            elif avg_freq > 10:
                cluster_label = "Clientes Frecuentes"
                description = (
                    f"Clientes que compran regularmente ({avg_freq:.1f} transacciones) con buen volumen "
                    f"({avg_vol:.0f} productos). Suelen explorar {avg_prod_dist:.0f} productos distintos "
                    f"y {avg_cat_dist:.1f} categorías. Tienen potencial de crecimiento."
                )
                recommendations = [
                    "Cross-selling y up-selling de productos relacionados",
                    "Recomendaciones personalizadas basadas en historial",
                    "Programas de lealtad con recompensas progresivas",
                    "Ofertas en categorías que aún no han explorado",
                ]
            elif avg_prod_dist > 20 or avg_cat_dist > 8:
                cluster_label = "Exploradores / Diversificadores"
                description = (
                    f"Clientes que buscan variedad: {avg_prod_dist:.0f} productos distintos y "
                    f"{avg_cat_dist:.1f} categorías diferentes, aunque con frecuencia moderada "
                    f"({avg_freq:.1f} transacciones). Valoran la diversidad y experimentación."
                )
                recommendations = [
                    "Mostrar productos nuevos y tendencias del mercado",
                    "Destacar diversidad de catálogo y categorías especiales",
                    "Campañas de descubrimiento con muestras o pruebas",
                    "Recomendaciones de productos similares a los que les gustan",
                ]
            elif avg_freq < 5 and avg_vol < 20:
                cluster_label = "Clientes Ocasionales / Nuevos"
                description = (
                    f"Clientes con bajo compromiso: {avg_freq:.1f} transacciones y {avg_vol:.0f} productos. "
                    f"Compran productos limitados ({avg_prod_dist:.0f} distintos) en pocas categorías "
                    f"({avg_cat_dist:.1f}). Pueden estar empezando o ser clientes ocasionales."
                )
                recommendations = [
                    "Campañas de bienvenida para nuevos clientes",
                    "Ofertas de reactivación con descuentos especiales",
                    "Programas de referencia para atraer amigos",
                    "Comunicación educativa sobre beneficios y productos",
                ]
            else:
                cluster_label = "Clientes Regulares"
                description = (
                    f"Clientes con comportamiento promedio: {avg_freq:.1f} transacciones, "
                    f"{avg_vol:.0f} productos, {avg_prod_dist:.0f} productos distintos y "
                    f"{avg_cat_dist:.1f} categorías. Representan la base de clientes."
                )
                recommendations = [
                    "Ofertas regulares para mantener el interés",
                    "Recordatorios de productos frecuentemente comprados",
                    "Programas de puntos o cashback",
                    "Comunicación sobre nuevas categorías y productos",
                ]

            print(f"🏷️  Etiqueta: {cluster_label}")
            print(f"📝 Descripción: {description}")
            print(f"💡 Recomendaciones:")
            for i, rec in enumerate(recommendations, 1):
                print(f"   {i}. {rec}")

            cluster_profiles[f"cluster_{cluster_id}"] = {
                "cluster_id": cluster_id,
                "label": cluster_label,
                "description": description,
                "n_customers": n_customers,
                "percentage": (
                    (n_customers / total_customers) * 100
                    if total_customers > 0
                    else 0.0
                ),
                "metrics": {
                    "avg_frequency": float(avg_freq),
                    "avg_unique_products": float(avg_prod_dist),
                    "avg_total_volume": float(avg_vol),
                    "avg_unique_categories": float(avg_cat_dist),
                    "min_frequency": int(stats["min_frequency"]),
                    "max_frequency": int(stats["max_frequency"]),
                    "min_volume": int(stats["min_volume"]),
                    "max_volume": int(stats["max_volume"]),
                },
                "business_recommendations": recommendations,
            }

        return cluster_profiles

    def generate_customer_segmentation(
        self,
        df_transactions: DataFrame,
        df_transactions_exploded: DataFrame,
        df_product_categories: DataFrame = None,
        n_clusters: int = 4,
    ) -> Dict[str, Any]:
        """
        Genera segmentación completa de clientes usando K-Means.

        Args:
            df_transactions: DataFrame de transacciones
            df_transactions_exploded: DataFrame de transacciones explotadas
            df_product_categories: DataFrame de relación producto-categoría (opcional)
            n_clusters: Número de clusters deseados

        Returns:
            Diccionario con resultados de segmentación
        """
        print("\n" + "=" * 70)
        print("👥 SEGMENTACIÓN DE CLIENTES CON K-MEANS")
        print("=" * 70)

        # 1. Preparar features (incluyendo diversidad de categorías)
        df_features = self.prepare_customer_features(
            df_transactions, df_transactions_exploded, df_product_categories
        )

        # 2. Ejecutar clustering
        df_clustered, model = self.perform_kmeans_clustering(df_features, n_clusters)

        # 3. Analizar clusters
        cluster_profiles = self.analyze_clusters(df_clustered, n_clusters)

        # 4. Mostrar distribución de clusters
        print(f"\n📊 Distribución de clientes por cluster:")
        df_clustered.groupBy("cluster").agg(count("*").alias("n_customers")).orderBy(
            "cluster"
        ).show()

        print("=" * 70)
        print("✅ Segmentación completada exitosamente")

        return {
            "clusters_df": df_clustered,
            "cluster_summary": cluster_profiles,
            "n_clusters": n_clusters,
            "model": model,
        }
