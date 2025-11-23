#!/usr/bin/env python3
"""
Script principal para ejecutar pipelines de procesamiento.

Uso:
    python run_pipeline.py <pipeline_name> [options]

Pipeline names:
    - executive_summary  : Resumen ejecutivo (diario)
    - analytics          : Analisis analitico (semanal)
    - clustering         : Clustering de clientes (mensual)
    - recommendations    : Recomendaciones de productos (mensual)
    - all                : Ejecutar todos los pipelines

Ejemplos:
    python run_pipeline.py executive_summary
    python run_pipeline.py clustering --n-clusters 5
    python run_pipeline.py recommendations --min-support 0.02
    python run_pipeline.py all
"""

import sys
import os
import argparse

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)
sys.path.insert(0, current_dir)

from src.pipelines.executive_summary_pipeline import ExecutiveSummaryPipeline
from src.pipelines.analytics_pipeline import AnalyticsPipeline
from src.pipelines.clustering_pipeline import ClusteringPipeline
from src.pipelines.recommendations_pipeline import RecommendationsPipeline


def run_executive_summary():
    """Ejecuta el pipeline de resumen ejecutivo."""
    pipeline = ExecutiveSummaryPipeline()
    pipeline.run()


def run_analytics():
    """Ejecuta el pipeline de analisis analitico."""
    pipeline = AnalyticsPipeline()
    pipeline.run()


def run_clustering(n_clusters: int = 4):
    """Ejecuta el pipeline de clustering."""
    pipeline = ClusteringPipeline(n_clusters=n_clusters)
    pipeline.run()


def run_recommendations(min_support: float = 0.01, min_confidence: float = 0.5):
    """Ejecuta el pipeline de recomendaciones."""
    pipeline = RecommendationsPipeline(
        min_support=min_support, min_confidence=min_confidence
    )
    pipeline.run()


def run_all():
    """Ejecuta todos los pipelines en orden."""
    print("=" * 70)
    print("EJECUTANDO TODOS LOS PIPELINES")
    print("=" * 70)

    pipelines = [
        ("Resumen Ejecutivo", run_executive_summary),
        ("Analisis Analitico", run_analytics),
        ("Clustering", lambda: run_clustering(n_clusters=4)),
        ("Recomendaciones", lambda: run_recommendations()),
    ]

    for name, pipeline_func in pipelines:
        try:
            print(f"\n{'='*70}")
            print(f"> Ejecutando: {name}")
            print("=" * 70)
            pipeline_func()
            print(f"[OK] {name} completado exitosamente")
        except Exception as e:
            print(f"[ERROR] Error en {name}: {str(e)}")
            print("Continuando con el siguiente pipeline...")

    print("\n" + "=" * 70)
    print("[OK] TODOS LOS PIPELINES COMPLETADOS")
    print("=" * 70)


def main():
    """Funcion principal del script."""
    parser = argparse.ArgumentParser(
        description="Ejecuta pipelines de procesamiento de datos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "pipeline",
        choices=[
            "executive_summary",
            "analytics",
            "clustering",
            "recommendations",
            "all",
        ],
        help="Nombre del pipeline a ejecutar",
    )

    parser.add_argument(
        "--n-clusters",
        type=int,
        default=4,
        help="Numero de clusters para clustering (default: 4)",
    )

    parser.add_argument(
        "--min-support",
        type=float,
        default=0.01,
        help="Soporte minimo para reglas de asociacion (default: 0.01)",
    )

    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.5,
        help="Confianza minima para reglas de asociacion (default: 0.5)",
    )

    args = parser.parse_args()

    try:
        if args.pipeline == "executive_summary":
            run_executive_summary()
        elif args.pipeline == "analytics":
            run_analytics()
        elif args.pipeline == "clustering":
            run_clustering(n_clusters=args.n_clusters)
        elif args.pipeline == "recommendations":
            run_recommendations(
                min_support=args.min_support, min_confidence=args.min_confidence
            )
        elif args.pipeline == "all":
            run_all()
    except KeyboardInterrupt:
        print("\n[WARNING] Ejecucion interrumpida por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Error fatal: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
