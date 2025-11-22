import logging
import traceback
from typing import List, Optional

from pyspark.sql import DataFrame

from includes.pipeline_context import pipeline_context

# Usar el logger de Airflow si está disponible, si no, usar el logger estándar
try:
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)


def _load_transactions(pipeline, store_ids: Optional[List[str]] = None, sample_size: Optional[int] = None) -> DataFrame:
    df_transactions = pipeline.data_loader.load_transactions(store_ids=store_ids)
    if sample_size:
        df_transactions = df_transactions.limit(sample_size)
    df_transactions.cache()
    return df_transactions


def run_categories_analysis_task() -> None:
    print("=" * 80)
    print("Iniciando análisis de categorías...")
    print("=" * 80)
    logger.info("Iniciando análisis de categorías...")
    try:
        with pipeline_context("Airflow-Categories") as pipeline:
            pipeline.run_categories_analysis()
        print("Análisis de categorías completado exitosamente")
        logger.info("Análisis de categorías completado exitosamente")
    except Exception as e:
        error_msg = f"Error en análisis de categorías: {str(e)}"
        print(f"ERROR: {error_msg}")
        print(traceback.format_exc())
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise


def run_product_categories_analysis_task() -> None:
    logger.info("Iniciando análisis de productos-categorías...")
    try:
        with pipeline_context("Airflow-ProductCategories") as pipeline:
            pipeline.run_product_categories_analysis()
        logger.info("Análisis de productos-categorías completado exitosamente")
    except Exception as e:
        logger.error(f"Error en análisis de productos-categorías: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def run_transactions_analysis_task(sample_size: Optional[int] = None) -> None:
    logger.info(f"Iniciando análisis de transacciones (sample_size={sample_size})...")
    try:
        with pipeline_context("Airflow-Transactions") as pipeline:
            pipeline.run_transactions_analysis(sample_size=sample_size)
        logger.info("Análisis de transacciones completado exitosamente")
    except Exception as e:
        logger.error(f"Error en análisis de transacciones: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def run_transactions_exploded_analysis_task(sample_size: Optional[int] = None) -> None:
    logger.info(f"Iniciando análisis de transacciones explodidas (sample_size={sample_size})...")
    try:
        with pipeline_context("Airflow-TransactionsExploded") as pipeline:
            pipeline.run_transactions_exploded_analysis(sample_size=sample_size)
        logger.info("Análisis de transacciones explodidas completado exitosamente")
    except Exception as e:
        logger.error(f"Error en análisis de transacciones explodidas: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def run_temporal_analysis_task(sample_size: Optional[int] = None) -> None:
    logger.info(f"Iniciando análisis temporal (sample_size={sample_size})...")
    try:
        with pipeline_context("Airflow-TemporalAnalysis") as pipeline:
            df_transactions = _load_transactions(pipeline, sample_size=sample_size)
            pipeline.run_temporal_analysis(df_transactions)
            df_transactions.unpersist()
        logger.info("Análisis temporal completado exitosamente")
    except Exception as e:
        logger.error(f"Error en análisis temporal: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def run_customer_analysis_task(sample_size: Optional[int] = None) -> None:
    logger.info(f"Iniciando análisis de clientes (sample_size={sample_size})...")
    try:
        with pipeline_context("Airflow-CustomerAnalysis") as pipeline:
            df_transactions = _load_transactions(pipeline, sample_size=sample_size)
            pipeline.run_customer_analysis(df_transactions)
            df_transactions.unpersist()
        logger.info("Análisis de clientes completado exitosamente")
    except Exception as e:
        logger.error(f"Error en análisis de clientes: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def run_product_analysis_task(
    store_ids: Optional[List[str]] = None,
    sample_size: Optional[int] = None,
) -> None:
    logger.info(f"Iniciando análisis de productos (store_ids={store_ids}, sample_size={sample_size})...")
    try:
        with pipeline_context("Airflow-ProductAnalysis") as pipeline:
            df_transactions = _load_transactions(pipeline, store_ids=store_ids, sample_size=sample_size)
            df_transactions_exploded = pipeline.data_loader.explode_transactions(df_transactions)
            df_transactions_exploded.cache()

            pipeline.product_analyzer.generate_product_summary(
                df_transactions,
                df_transactions_exploded,
                run_market_basket=False,
            )

            df_transactions_exploded.unpersist()
            df_transactions.unpersist()
        logger.info("Análisis de productos completado exitosamente")
    except Exception as e:
        logger.error(f"Error en análisis de productos: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def train_fp_growth_distributed_task(
    min_support: float = 0.05,
    min_confidence: float = 0.4,
) -> None:
    logger.info(f"Iniciando entrenamiento FP-Growth (min_support={min_support}, min_confidence={min_confidence})...")
    try:
        with pipeline_context("Airflow-FPGrowth") as pipeline:
            df_transactions = _load_transactions(pipeline)
            results = pipeline.product_analyzer.market_basket_analysis(
                df_transactions,
                min_support=min_support,
                min_confidence=min_confidence,
            )
            if results["model"]:
                logger.info("Modelo FP-Growth entrenado en modo distribuido.")
            df_transactions.unpersist()
        logger.info("Entrenamiento FP-Growth completado exitosamente")
    except Exception as e:
        logger.error(f"Error en entrenamiento FP-Growth: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def run_store_product_analysis_task(store_id: str) -> None:
    logger.info(f"Iniciando análisis de productos para tienda {store_id}...")
    try:
        with pipeline_context(f"Airflow-Store-{store_id}") as pipeline:
            df_transactions = _load_transactions(pipeline, store_ids=[store_id])
            df_transactions_exploded = pipeline.data_loader.explode_transactions(df_transactions)
            df_transactions_exploded.cache()

            summary = pipeline.product_analyzer.generate_product_summary(
                df_transactions,
                df_transactions_exploded,
                run_market_basket=False,
            )

            # Persistir resultados minimales como CSV específicos por tienda
            output_dir = f"output/stores/{store_id}"
            for key, df in summary.items():
                if isinstance(df, DataFrame):
                    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/{key}")

            df_transactions_exploded.unpersist()
            df_transactions.unpersist()
        logger.info(f"Análisis de productos para tienda {store_id} completado exitosamente")
    except Exception as e:
        logger.error(f"Error en análisis de productos para tienda {store_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

