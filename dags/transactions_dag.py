from datetime import datetime

from airflow.sdk import dag, task

from includes import tasks as pipeline_tasks  # noqa: E402


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id="transactions_quality_pipeline",
    description="Ejecuta análisis de calidad y datasets explodidos de transacciones.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sales", "transactions"],
)
def transactions_quality_pipeline():
    @task(task_id="analyze_transactions")
    def analyze_transactions():
        pipeline_tasks.run_transactions_analysis_task()

    @task(task_id="analyze_transactions_exploded")
    def analyze_transactions_exploded():
        pipeline_tasks.run_transactions_exploded_analysis_task()

    analyze_transactions() >> analyze_transactions_exploded()


transactions_quality_pipeline()

