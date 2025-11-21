from typing import List, Optional

from pyspark.sql import DataFrame

from airflow.includes.pipeline_context import pipeline_context


def _load_transactions(pipeline, store_ids: Optional[List[str]] = None, sample_size: Optional[int] = None) -> DataFrame:
    df_transactions = pipeline.data_loader.load_transactions(store_ids=store_ids)
    if sample_size:
        df_transactions = df_transactions.limit(sample_size)
    df_transactions.cache()
    return df_transactions


def run_categories_analysis_task() -> None:
    with pipeline_context("Airflow-Categories") as pipeline:
        pipeline.run_categories_analysis()


def run_product_categories_analysis_task() -> None:
    with pipeline_context("Airflow-ProductCategories") as pipeline:
        pipeline.run_product_categories_analysis()


def run_transactions_analysis_task(sample_size: Optional[int] = None) -> None:
    with pipeline_context("Airflow-Transactions") as pipeline:
        pipeline.run_transactions_analysis(sample_size=sample_size)


def run_transactions_exploded_analysis_task(sample_size: Optional[int] = None) -> None:
    with pipeline_context("Airflow-TransactionsExploded") as pipeline:
        pipeline.run_transactions_exploded_analysis(sample_size=sample_size)


def run_temporal_analysis_task(sample_size: Optional[int] = None) -> None:
    with pipeline_context("Airflow-TemporalAnalysis") as pipeline:
        df_transactions = _load_transactions(pipeline, sample_size=sample_size)
        pipeline.run_temporal_analysis(df_transactions)
        df_transactions.unpersist()


def run_customer_analysis_task(sample_size: Optional[int] = None) -> None:
    with pipeline_context("Airflow-CustomerAnalysis") as pipeline:
        df_transactions = _load_transactions(pipeline, sample_size=sample_size)
        pipeline.run_customer_analysis(df_transactions)
        df_transactions.unpersist()


def run_product_analysis_task(
    store_ids: Optional[List[str]] = None,
    sample_size: Optional[int] = None,
) -> None:
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


def train_fp_growth_distributed_task(
    min_support: float = 0.05,
    min_confidence: float = 0.4,
) -> None:
    with pipeline_context("Airflow-FPGrowth") as pipeline:
        df_transactions = _load_transactions(pipeline)
        results = pipeline.product_analyzer.market_basket_analysis(
            df_transactions,
            min_support=min_support,
            min_confidence=min_confidence,
        )
        if results["model"]:
            print("Modelo FP-Growth entrenado en modo distribuido.")
        df_transactions.unpersist()


def run_store_product_analysis_task(store_id: str) -> None:
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

