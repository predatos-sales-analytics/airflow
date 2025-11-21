from datetime import datetime

from airflow.decorators import dag, task

from includes import tasks as pipeline_tasks  # noqa: E402


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id="categories_reference_pipeline",
    description="Analiza datasets de categorías y productos-categorías.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sales", "reference"],
)
def categories_reference_pipeline():
    @task(task_id="analyze_categories")
    def analyze_categories():
        pipeline_tasks.run_categories_analysis_task()

    @task(task_id="analyze_product_categories")
    def analyze_product_categories():
        pipeline_tasks.run_product_categories_analysis_task()

    analyze_categories() >> analyze_product_categories()


categories_reference_pipeline()

