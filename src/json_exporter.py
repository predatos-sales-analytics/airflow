"""
Módulo para exportar resultados de análisis en formato JSON.

Diseñado para que un frontend React consuma los datos y genere
visualizaciones interactivas.
"""

import json
import os
from datetime import datetime, date
from typing import Dict, Any, List
import numpy as np
from pyspark.sql import DataFrame
import pandas as pd


class JSONExporter:
    """Clase para exportar datos de análisis a formato JSON."""

    def __init__(self, output_base: str = None):
        """
        Inicializa el exportador.

        Args:
            output_base: Ruta base de salida
        """
        if output_base is None:
            output_base = os.getenv("OUTPUT_PATH", "/opt/airflow/output")

        self.output_base = output_base
        self.summary_path = os.path.join(output_base, "summary")
        self.analytics_path = os.path.join(output_base, "analytics")
        self.advanced_path = os.path.join(output_base, "advanced")
        self.metadata_path = os.path.join(output_base, "metadata")

        # Crear directorios
        for path in [
            self.summary_path,
            self.analytics_path,
            self.advanced_path,
            self.metadata_path,
        ]:
            os.makedirs(path, exist_ok=True)

        print(f"📁 Directorio de salida JSON: {os.path.abspath(output_base)}")

    def _spark_to_dict_list(self, df: DataFrame, limit: int = None) -> List[Dict]:
        """
        Convierte un DataFrame de Spark a lista de diccionarios.

        Args:
            df: DataFrame de Spark
            limit: Límite de registros (None = todos)

        Returns:
            Lista de diccionarios
        """
        if limit:
            df = df.limit(limit)

        # Convertir a pandas y luego a dict
        pdf = df.toPandas()

        # Manejar tipos de datos especiales y NULLs
        for col_name in pdf.columns:
            col_data = pdf[col_name]

            # Detectar columnas de fecha/datetime
            if "datetime" in str(col_data.dtype) or col_data.dtype.name == "date64":
                # Convertir fechas a string ISO format
                pdf[col_name] = col_data.apply(
                    lambda x: (
                        x.isoformat()
                        if pd.notna(x) and isinstance(x, (date, datetime, pd.Timestamp))
                        else (str(x) if pd.notna(x) else None)
                    )
                )
            elif col_data.dtype == "object":
                # Para columnas object, verificar si contiene fechas
                try:
                    # Si la columna tiene objetos date/datetime, convertirlos
                    pdf[col_name] = col_data.apply(
                        lambda x: (
                            x.isoformat()
                            if isinstance(x, (date, datetime))
                            else (None if pd.isna(x) else x)
                        )
                    )
                except:
                    # Si falla, solo manejar NaN
                    pdf[col_name] = col_data.where(col_data.notna(), None)
            elif col_data.isna().any():
                # Para columnas numéricas, convertir NaN a None
                pdf[col_name] = col_data.where(col_data.notna(), None)

        # Convertir a dict
        records = pdf.to_dict("records")

        def normalize_value(value):
            if isinstance(value, np.ndarray):
                value = value.tolist()
            if isinstance(value, (list, tuple)):
                return [normalize_value(item) for item in value]
            if value is None:
                return None
            if isinstance(value, (pd.Timestamp, date, datetime)):
                try:
                    return value.isoformat()
                except Exception:
                    return str(value)
            if isinstance(value, (np.floating, float)):
                if pd.isna(value):
                    return None
                return int(value) if float(value).is_integer() else float(value)
            if isinstance(value, (np.integer, int)):
                return int(value)
            if isinstance(value, (np.bool_, bool)):
                return bool(value)
            if isinstance(value, str):
                return value
            try:
                if pd.isna(value):
                    return None
            except Exception:
                pass
            return str(value)

        # Limpieza final: asegurar que todos los valores sean serializables
        for record in records:
            for key, value in record.items():
                record[key] = normalize_value(value)

        return records

    def export_summary_metrics(
        self, metrics: Dict[str, Any], filename: str = "metrics.json"
    ):
        """
        Exporta métricas del resumen ejecutivo.

        Args:
            metrics: Diccionario con métricas (ej: {'total_sales': 1000000, ...})
            filename: Nombre del archivo JSON
        """
        filepath = os.path.join(self.summary_path, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)

        print(f"✅ Métricas exportadas: {filepath}")
        return filepath

    def export_top_items(self, df: DataFrame, item_type: str, top_n: int = 10):
        """
        Exporta top N items (productos, clientes, etc).

        Args:
            df: DataFrame con items ordenados
            item_type: Tipo de item ('products', 'customers', 'categories')
            top_n: Número de items a exportar
        """
        filepath = os.path.join(self.summary_path, f"top_{top_n}_{item_type}.json")

        data = self._spark_to_dict_list(df, limit=top_n)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "type": item_type,
                    "top_n": top_n,
                    "data": data,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"✅ Top {top_n} {item_type} exportado: {filepath}")
        return filepath

    def export_time_series(
        self,
        df: DataFrame,
        series_name: str,
        date_col: str = "date",
        value_col: str = "count",
    ):
        """
        Exporta serie de tiempo para gráficos con estadísticas descriptivas.

        Args:
            df: DataFrame con serie temporal
            series_name: Nombre de la serie ('daily_sales', 'weekly_sales', etc)
            date_col: Nombre de la columna de fecha
            value_col: Nombre de la columna de valores
        """
        from pyspark.sql.functions import avg, min as spark_min, max as spark_max

        filepath = os.path.join(self.analytics_path, f"{series_name}.json")

        # Calcular estadísticas descriptivas
        stats_row = df.select(
            avg(value_col).alias("promedio"),
            spark_min(value_col).alias("minimo"),
            spark_max(value_col).alias("maximo"),
        ).collect()[0]

        stats = {
            "promedio": (
                float(stats_row["promedio"])
                if stats_row["promedio"] is not None
                else None
            ),
            "minimo": (
                float(stats_row["minimo"]) if stats_row["minimo"] is not None else None
            ),
            "maximo": (
                float(stats_row["maximo"]) if stats_row["maximo"] is not None else None
            ),
        }

        # Convertir datos a lista de diccionarios
        data = self._spark_to_dict_list(df)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "series_name": series_name,
                    "date_column": date_col,
                    "value_column": value_col,
                    "statistics": stats,
                    "data": data,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"✅ Serie temporal exportada: {filepath}")
        return filepath

    def export_distribution(self, df: DataFrame, distribution_name: str):
        """
        Exporta datos de distribución (para boxplot, histograma, etc).

        Args:
            df: DataFrame con datos de distribución
            distribution_name: Nombre de la distribución
        """
        filepath = os.path.join(
            self.analytics_path, f"{distribution_name}_distribution.json"
        )

        data = self._spark_to_dict_list(df)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "distribution_name": distribution_name,
                    "data": data,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"✅ Distribución exportada: {filepath}")
        return filepath

    def export_correlation_matrix(
        self, correlation_data: Dict[str, Any], matrix_name: str = "correlation_matrix"
    ):
        """
        Exporta matriz de correlación para heatmap.

        Args:
            correlation_data: Diccionario con matriz de correlación
            matrix_name: Nombre de la matriz
        """
        filepath = os.path.join(self.analytics_path, f"{matrix_name}.json")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "matrix_name": matrix_name,
                    "correlation_data": correlation_data,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"✅ Matriz de correlación exportada: {filepath}")
        return filepath

    def export_heatmap_data(
        self, heatmap_data: Dict[str, Any], heatmap_name: str = "heatmap"
    ):
        """
        Exporta datos normalizados para heatmap.

        Args:
            heatmap_data: Diccionario con datos normalizados para heatmap
            heatmap_name: Nombre del heatmap
        """
        filepath = os.path.join(self.analytics_path, f"{heatmap_name}.json")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "heatmap_name": heatmap_name,
                    "data": heatmap_data,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"✅ Datos de heatmap exportados: {filepath}")
        return filepath

    def export_clustering_results(
        self, clusters_df: DataFrame, cluster_summary: Dict[str, Any], n_clusters: int
    ):
        """
        Exporta resultados de clustering K-Means.

        Args:
            clusters_df: DataFrame con clientes y sus clusters
            cluster_summary: Resumen de cada cluster
            n_clusters: Número de clusters
        """
        clustering_dir = os.path.join(self.advanced_path, "clustering")
        os.makedirs(clustering_dir, exist_ok=True)

        # Exportar asignaciones de clusters
        clusters_path = os.path.join(clustering_dir, "customer_clusters.json")
        clusters_data = self._spark_to_dict_list(
            clusters_df, limit=1000
        )  # Limitar para performance

        with open(clusters_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "n_clusters": n_clusters,
                    "total_customers": clusters_df.count(),
                    "sample_assignments": clusters_data,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        # Exportar resumen de clusters
        summary_path = os.path.join(clustering_dir, "cluster_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "n_clusters": n_clusters,
                    "cluster_profiles": cluster_summary,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        # Exportar datos para visualización del clustering
        visualization_path = os.path.join(
            clustering_dir, "clustering_visualization.json"
        )
        visualization_data = self._prepare_clustering_visualization_data(
            clusters_df, cluster_summary
        )

        with open(visualization_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "n_clusters": n_clusters,
                    "visualization_data": visualization_data,
                    "cluster_summary": cluster_summary,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"✅ Clustering exportado:")
        print(f"   - Asignaciones: {clusters_path}")
        print(f"   - Resumen y recomendaciones: {summary_path}")
        print(f"   - Datos para visualización: {visualization_path}")

        return clusters_path, summary_path, visualization_path

    def _prepare_clustering_visualization_data(
        self, clusters_df: DataFrame, cluster_summary: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepara datos agregados para visualización del clustering.

        Args:
            clusters_df: DataFrame con clientes y clusters
            cluster_summary: Resumen de clusters

        Returns:
            Diccionario con datos para visualización
        """
        from pyspark.sql.functions import avg, count

        # Agregar estadísticas por cluster para visualización
        cluster_stats = (
            clusters_df.groupBy("cluster")
            .agg(
                count("*").alias("n_customers"),
                avg("frequency").alias("avg_frequency"),
                avg("unique_products").alias("avg_unique_products"),
                avg("total_volume").alias("avg_total_volume"),
                avg("unique_categories").alias("avg_unique_categories"),
            )
            .orderBy("cluster")
        )

        stats_list = self._spark_to_dict_list(cluster_stats)

        # Preparar datos para gráficos
        visualization_data = {
            "cluster_statistics": stats_list,
            "cluster_labels": {
                str(cluster_id): cluster_summary[f"cluster_{cluster_id}"]["label"]
                for cluster_id in range(len(cluster_summary))
            },
            "distribution": {
                str(cluster_id): cluster_summary[f"cluster_{cluster_id}"]["n_customers"]
                for cluster_id in range(len(cluster_summary))
            },
        }

        return visualization_data

    def export_recommendations(
        self,
        recommendations_data: Dict[str, Any],
        rec_type: str = "product_associations",
    ):
        """
        Exporta resultados del recomendador.

        Args:
            recommendations_data: Datos de recomendaciones
            rec_type: Tipo de recomendación ('product_associations', 'customer_recommendations')
        """
        rec_dir = os.path.join(self.advanced_path, "recommendations")
        os.makedirs(rec_dir, exist_ok=True)

        filepath = os.path.join(rec_dir, f"{rec_type}.json")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "recommendation_type": rec_type,
                    "data": recommendations_data,
                    "generated_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"✅ Recomendaciones exportadas: {filepath}")
        return filepath

    def export_execution_metadata(
        self, dag_id: str, task_id: str, execution_info: Dict[str, Any]
    ):
        """
        Exporta metadata de ejecución del DAG.

        Args:
            dag_id: ID del DAG
            task_id: ID de la tarea
            execution_info: Información de ejecución
        """
        filepath = os.path.join(self.metadata_path, f"{dag_id}_{task_id}.json")

        metadata = {
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_timestamp": datetime.now().isoformat(),
            **execution_info,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        print(f"✅ Metadata exportada: {filepath}")
        return filepath
