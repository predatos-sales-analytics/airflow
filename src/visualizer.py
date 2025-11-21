"""
Módulo para generación de visualizaciones del análisis exploratorio.

Este módulo crea gráficas para variables categóricas, frecuencias
y distribuciones, facilitando la interpretación de resultados.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc, concat_ws
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
import os
from typing import Optional


class DataVisualizer:
    """Clase para generar visualizaciones del análisis EDA."""

    def __init__(self, output_path: str = "output/plots"):
        """
        Inicializa el visualizador.

        Args:
            output_path: Ruta donde guardar las gráficas
        """
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)

        # Configurar estilo de gráficas
        sns.set_style("whitegrid")
        plt.rcParams["figure.figsize"] = (12, 6)
        plt.rcParams["font.size"] = 10

    def plot_categorical_frequency(
        self,
        df: DataFrame,
        column: str,
        dataset_name: str,
        top_n: int = 15,
        title: Optional[str] = None,
    ):
        """
        Genera gráfico de barras para frecuencias de variable categórica.

        Args:
            df: DataFrame de Spark
            column: Nombre de la columna categórica
            dataset_name: Nombre del dataset (para nombre de archivo)
            top_n: Número de categorías más frecuentes a mostrar
            title: Título personalizado del gráfico
        """
        # Calcular frecuencias
        freq_data = (
            df.groupBy(column)
            .agg(count("*").alias("frequency"))
            .orderBy(desc("frequency"))
            .limit(top_n)
            .toPandas()
        )

        if freq_data.empty:
            print(f"ADVERTENCIA: No hay datos para graficar {column}")
            return

        # Crear gráfico
        plt.figure(figsize=(12, 6))
        bars = plt.bar(range(len(freq_data)), freq_data["frequency"], color="steelblue")

        # Personalizar
        plt.xlabel(column.replace("_", " ").title())
        plt.ylabel("Frecuencia")
        plot_title = title or f"Top {top_n} - {column.replace('_', ' ').title()}"
        plt.title(plot_title)
        plt.xticks(
            range(len(freq_data)),
            freq_data[column].astype(str),
            rotation=45,
            ha="right",
        )

        # Agregar valores sobre las barras
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{int(height):,}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_{column}_frequency.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_top_items(
        self,
        df: DataFrame,
        group_column: str,
        dataset_name: str,
        top_n: int = 15,
        title: Optional[str] = None,
        ylabel: str = "Frecuencia",
    ):
        """
        Genera gráfico de barras horizontales para top items.

        Args:
            df: DataFrame de Spark
            group_column: Columna para agrupar
            dataset_name: Nombre del dataset
            top_n: Número de items a mostrar
            title: Título del gráfico
            ylabel: Etiqueta del eje Y
        """
        # Calcular top items
        top_data = (
            df.groupBy(group_column)
            .agg(count("*").alias("count"))
            .orderBy(desc("count"))
            .limit(top_n)
            .toPandas()
        )

        if top_data.empty:
            print(f"ADVERTENCIA: No hay datos para graficar {group_column}")
            return

        # Crear gráfico horizontal
        plt.figure(figsize=(10, 8))
        bars = plt.barh(
            range(len(top_data)), top_data["count"], color="forestgreen", alpha=0.7
        )

        # Personalizar
        plt.xlabel("Cantidad")
        plt.ylabel(ylabel)
        plot_title = title or f"Top {top_n} - {group_column.replace('_', ' ').title()}"
        plt.title(plot_title)
        plt.yticks(range(len(top_data)), top_data[group_column].astype(str), fontsize=9)

        # Agregar valores a la derecha de las barras
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(
                width,
                bar.get_y() + bar.get_height() / 2.0,
                f" {int(width):,}",
                ha="left",
                va="center",
                fontsize=9,
            )

        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_top_{group_column}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_category_distribution(
        self, df: DataFrame, dataset_name: str, top_n: int = 10
    ):
        """
        Genera gráfico de pastel para distribución de categorías.

        Args:
            df: DataFrame de Spark
            dataset_name: Nombre del dataset
            top_n: Número de categorías principales (resto se agrupa en "Otros")
        """
        # Calcular distribución
        dist_data = (
            df.groupBy("category_name")
            .agg(count("*").alias("count"))
            .orderBy(desc("count"))
            .toPandas()
        )

        if dist_data.empty:
            print("ADVERTENCIA: No hay datos para graficar distribución de categorías")
            return

        # Agrupar categorías pequeñas en "Otros"
        if len(dist_data) > top_n:
            top_categories = dist_data.head(top_n)
            others_sum = dist_data.iloc[top_n:]["count"].sum()
            others_row = {"category_name": "Otros", "count": others_sum}
            dist_data = pd.concat(
                [top_categories, pd.DataFrame([others_row])], ignore_index=True
            )
        else:
            dist_data = dist_data.head(top_n)

        # Crear gráfico de pastel
        plt.figure(figsize=(10, 8))
        colors = sns.color_palette("Set3", len(dist_data))

        wedges, texts, autotexts = plt.pie(
            dist_data["count"],
            labels=dist_data["category_name"],
            autopct="%1.1f%%",
            colors=colors,
            startangle=90,
        )

        # Mejorar legibilidad
        for text in texts:
            text.set_fontsize(9)
        for autotext in autotexts:
            autotext.set_color("white")
            autotext.set_fontsize(9)
            autotext.set_weight("bold")

        plt.title(f"Distribución de Categorías - Top {top_n}")
        plt.axis("equal")
        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_category_distribution.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_temporal_trend(
        self,
        df: DataFrame,
        date_column: str,
        dataset_name: str,
        sample_dates: int = None,
    ):
        """
        Genera gráfico de línea para tendencia temporal.

        Args:
            df: DataFrame de Spark
            date_column: Columna de fecha
            dataset_name: Nombre del dataset
            sample_dates: Número de fechas a mostrar
        """
        # Calcular transacciones por fecha
        temporal_query = (
            df.groupBy(date_column).agg(count("*").alias("count")).orderBy(date_column)
        )

        if sample_dates:
            temporal_query = temporal_query.limit(sample_dates)

        temporal_data = temporal_query.toPandas()

        if temporal_data.empty:
            print("ADVERTENCIA: No hay datos temporales para graficar")
            return

        # Convertir columna de fecha a datetime para mejor manejo
        temporal_data[date_column] = pd.to_datetime(temporal_data[date_column])

        # Crear gráfico
        plt.figure(figsize=(16, 6))
        plt.plot(
            temporal_data[date_column],
            temporal_data["count"],
            marker="o",
            linewidth=2,
            markersize=3,
            color="darkblue",
            alpha=0.7,
        )

        plt.xlabel("Fecha", fontsize=11)
        plt.ylabel("Número de Transacciones", fontsize=11)
        plt.title(
            f"Tendencia Temporal - {dataset_name.replace('_', ' ').title()}",
            fontsize=13,
            fontweight="bold",
        )

        # Configurar el eje X para mostrar días 1, 15 y 30 de cada mes (quincenas de pago)
        ax = plt.gca()
        ax.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 15, 30]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

        plt.xticks(rotation=45, ha="right", fontsize=9)
        plt.grid(True, alpha=0.3, linestyle="--")
        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_temporal_trend.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_weekly_sales(self, df: DataFrame, dataset_name: str = "weekly_sales"):
        """
        Genera gráfico de ventas semanales.

        Args:
            df: DataFrame con datos semanales (debe tener columnas: year, week, num_transacciones)
            dataset_name: Nombre del dataset
        """
        df_pandas = df.toPandas()

        if df_pandas.empty:
            print("ADVERTENCIA: No hay datos semanales para graficar")
            return

        # Crear etiqueta de semana
        df_pandas["week_label"] = (
            df_pandas["year"].astype(str)
            + "-W"
            + df_pandas["week"].astype(str).str.zfill(2)
        )

        plt.figure(figsize=(16, 6))
        plt.plot(
            range(len(df_pandas)),
            df_pandas["num_transacciones"],
            marker="o",
            linewidth=2,
            markersize=4,
            color="darkgreen",
        )

        plt.xlabel("Semana", fontsize=11)
        plt.ylabel("Número de Transacciones", fontsize=11)
        plt.title("Ventas Semanales", fontsize=13, fontweight="bold")

        # Mostrar cada 4 semanas en el eje X
        step = max(1, len(df_pandas) // 10)
        plt.xticks(
            range(0, len(df_pandas), step),
            df_pandas["week_label"].iloc[::step],
            rotation=45,
            ha="right",
            fontsize=9,
        )

        plt.grid(True, alpha=0.3, linestyle="--")
        plt.tight_layout()

        filename = f"{dataset_name}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_monthly_sales(self, df: DataFrame, dataset_name: str = "monthly_sales"):
        """
        Genera gráfico de ventas mensuales.

        Args:
            df: DataFrame con datos mensuales
            dataset_name: Nombre del dataset
        """
        df_pandas = df.toPandas()

        if df_pandas.empty:
            print("ADVERTENCIA: No hay datos mensuales para graficar")
            return

        # Crear etiqueta de mes
        df_pandas["month_label"] = (
            df_pandas["year"].astype(str)
            + "-"
            + df_pandas["month"].astype(str).str.zfill(2)
        )

        plt.figure(figsize=(14, 6))

        # Gráfico de barras
        bars = plt.bar(
            range(len(df_pandas)),
            df_pandas["num_transacciones"],
            color="steelblue",
            alpha=0.7,
        )

        # Agregar línea de tendencia
        plt.plot(
            range(len(df_pandas)),
            df_pandas["num_transacciones"],
            color="darkred",
            linewidth=2,
            marker="o",
            markersize=6,
            label="Tendencia",
        )

        plt.xlabel("Mes", fontsize=11)
        plt.ylabel("Número de Transacciones", fontsize=11)
        plt.title("Ventas Mensuales", fontsize=13, fontweight="bold")
        plt.xticks(
            range(len(df_pandas)),
            df_pandas["month_label"],
            rotation=45,
            ha="right",
            fontsize=9,
        )
        plt.legend()
        plt.grid(True, alpha=0.3, linestyle="--", axis="y")
        plt.tight_layout()

        filename = f"{dataset_name}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_day_of_week(self, df: DataFrame, dataset_name: str = "day_of_week"):
        """
        Genera gráfico de ventas por día de la semana.

        Args:
            df: DataFrame con datos por día de semana
            dataset_name: Nombre del dataset
        """
        df_pandas = df.toPandas()

        if df_pandas.empty:
            print("ADVERTENCIA: No hay datos por día de semana para graficar")
            return

        plt.figure(figsize=(12, 6))

        bars = plt.bar(
            df_pandas["day_name"],
            df_pandas["num_transacciones"],
            color=[
                "#FF6B6B",
                "#4ECDC4",
                "#45B7D1",
                "#FFA07A",
                "#98D8C8",
                "#F7DC6F",
                "#BB8FCE",
            ],
            alpha=0.8,
        )

        plt.xlabel("Día de la Semana", fontsize=11)
        plt.ylabel("Número de Transacciones", fontsize=11)
        plt.title(
            "Distribución de Ventas por Día de la Semana",
            fontsize=13,
            fontweight="bold",
        )
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, alpha=0.3, linestyle="--", axis="y")
        plt.tight_layout()

        filename = f"{dataset_name}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_customer_frequency_distribution(
        self, df: DataFrame, dataset_name: str = "customer_frequency"
    ):
        """
        Genera gráfico de distribución de frecuencia de compra.

        Args:
            df: DataFrame con frecuencias por cliente
            dataset_name: Nombre del dataset
        """
        # Agrupar por número de compras
        df_dist = (
            df.groupBy("num_compras")
            .agg(count("*").alias("num_clientes"))
            .orderBy("num_compras")
            .limit(20)  # Limitar a primeras 20 frecuencias
        )

        df_pandas = df_dist.toPandas()

        if df_pandas.empty:
            print("ADVERTENCIA: No hay datos de frecuencia para graficar")
            return

        plt.figure(figsize=(14, 6))

        plt.bar(
            df_pandas["num_compras"].astype(str),
            df_pandas["num_clientes"],
            color="purple",
            alpha=0.7,
        )

        plt.xlabel("Número de Compras", fontsize=11)
        plt.ylabel("Número de Clientes", fontsize=11)
        plt.title(
            "Distribución de Frecuencia de Compra", fontsize=13, fontweight="bold"
        )
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, alpha=0.3, linestyle="--", axis="y")
        plt.tight_layout()

        filename = f"{dataset_name}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_rfm_segments(self, df: DataFrame, dataset_name: str = "rfm_segments"):
        """
        Genera gráfico de distribución de segmentos RFM.

        Args:
            df: DataFrame con segmentación RFM
            dataset_name: Nombre del dataset
        """
        df_segments = (
            df.groupBy("RFM_segment")
            .agg(count("*").alias("num_clientes"))
            .orderBy(col("num_clientes").desc())
        )

        df_pandas = df_segments.toPandas()

        if df_pandas.empty:
            print("ADVERTENCIA: No hay datos de segmentos RFM para graficar")
            return

        plt.figure(figsize=(12, 8))

        # Gráfico de barras horizontales
        colors = plt.cm.Set3(range(len(df_pandas)))
        plt.barh(
            df_pandas["RFM_segment"], df_pandas["num_clientes"], color=colors, alpha=0.8
        )

        plt.xlabel("Número de Clientes", fontsize=11)
        plt.ylabel("Segmento RFM", fontsize=11)
        plt.title("Distribución de Segmentos RFM", fontsize=13, fontweight="bold")
        plt.grid(True, alpha=0.3, linestyle="--", axis="x")
        plt.tight_layout()

        filename = f"{dataset_name}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_association_rules(
        self, df: DataFrame, dataset_name: str = "association_rules", top_n: int = 15
    ):
        """
        Genera gráfico de reglas de asociación.

        Args:
            df: DataFrame con reglas de asociación
            dataset_name: Nombre del dataset
            top_n: Número de reglas top a graficar
        """
        # Convertir arrays a strings para mejor visualización
        df_viz = (
            df.withColumn("antecedent_str", concat_ws(", ", col("antecedent")))
            .withColumn("consequent_str", concat_ws(", ", col("consequent")))
            .orderBy(desc("lift"))
            .limit(top_n)
        )

        df_pandas = df_viz.toPandas()

        if df_pandas.empty:
            print("ADVERTENCIA: No hay reglas de asociación para graficar")
            return

        # Crear etiquetas de reglas
        df_pandas["rule"] = (
            df_pandas["antecedent_str"] + " → " + df_pandas["consequent_str"]
        )
        df_pandas["rule"] = df_pandas["rule"].str[:50]  # Truncar

        plt.figure(figsize=(14, 10))

        # Scatter plot: Confidence vs Lift, tamaño por support
        scatter = plt.scatter(
            df_pandas["confidence"],
            df_pandas["lift"],
            s=df_pandas["support"] * 10000,
            alpha=0.6,
            c=range(len(df_pandas)),
            cmap="viridis",
        )

        # Agregar etiquetas
        for idx, row in df_pandas.iterrows():
            plt.annotate(
                f"#{idx+1}", (row["confidence"], row["lift"]), fontsize=8, ha="center"
            )

        plt.xlabel("Confidence (Confianza)", fontsize=11)
        plt.ylabel("Lift", fontsize=11)
        plt.title(
            f"Top {top_n} Reglas de Asociación\n(Tamaño = Support)",
            fontsize=13,
            fontweight="bold",
        )
        plt.axhline(
            y=1, color="r", linestyle="--", alpha=0.5, label="Lift = 1 (independencia)"
        )
        plt.legend()
        plt.grid(True, alpha=0.3, linestyle="--")
        plt.tight_layout()

        filename = f"{dataset_name}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def create_summary_report(self):
        """Muestra un resumen de gráficas generadas en consola."""
        if not os.path.exists(self.output_path):
            print("   No se generaron gráficas")
            return

        files = [f for f in os.listdir(self.output_path) if f.endswith(".png")]

        if not files:
            print("   No se generaron gráficas")
            return

        print(f"   Total de gráficas generadas: {len(files)}")
        print(f"   Ubicación: {os.path.abspath(self.output_path)}")
        print("\n   Archivos generados:")
        for file in sorted(files):
            print(f"      • {file}")

