from datetime import datetime
from typing import List

from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.includes.bootstrap import bootstrap_project

bootstrap_project()

from airflow.includes import tasks as pipeline_tasks  # noqa: E402
from airflow.includes.store_service import fetch_store_ids  # noqa: E402


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id="advanced_sales_analytics",
    description=(
        "Análisis temporal, clientes y productos, con procesamiento paralelo por tienda "
        "y entrenamiento distribuido de FP-Growth."
    ),
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sales", "advanced", "spark"],
)
def advanced_sales_analytics():
    sample_size_value = Variable.get("sales_transactions_sample_size", default_var=None)
    sample_size = int(sample_size_value) if sample_size_value else None
    min_support = float(Variable.get("sales_fp_growth_min_support", default_var="0.05"))
    min_confidence = float(Variable.get("sales_fp_growth_min_confidence", default_var="0.4"))

    @task(task_id="temporal_analysis")
    def temporal_analysis():
        pipeline_tasks.run_temporal_analysis_task(sample_size=sample_size)

    @task(task_id="customer_analysis")
    def customer_analysis():
        pipeline_tasks.run_customer_analysis_task(sample_size=sample_size)

    @task(task_id="global_product_analysis")
    def global_product_analysis():
        pipeline_tasks.run_product_analysis_task(sample_size=sample_size)

    @task(task_id="fetch_store_ids")
    def fetch_stores() -> List[str]:
        return fetch_store_ids()

    @task(task_id="analyze_store", multiple_outputs=False)
    def analyze_store(store_id: str):
        pipeline_tasks.run_store_product_analysis_task(store_id)

    @task(task_id="train_fp_growth")
    def train_fp_growth():
        pipeline_tasks.train_fp_growth_distributed_task(
            min_support=min_support,
            min_confidence=min_confidence,
        )

    temporal = temporal_analysis()
    customers = customer_analysis()
    products = global_product_analysis()

    store_ids = fetch_stores()
    per_store_results = analyze_store.expand(store_id=store_ids)

    [temporal, customers, products] >> per_store_results >> train_fp_growth()


advanced_sales_analytics()

