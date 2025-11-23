"""
DAG 1: Resumen Ejecutivo

Ahora el procesamiento se delega a EMR Serverless. Airflow únicamente
orquesta la ejecución y, opcionalmente, sincroniza los resultados a disco
para que el frontend los consuma.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.exceptions import AirflowFailException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

REQUIRED_CONFIG_KEYS = {
    "application_id",
    "execution_role_arn",
    "entry_point",
    "data_base_uri",
    "output_prefix",
}


def parse_s3_uri(uri: str):
    if not uri or not uri.startswith("s3://"):
        raise AirflowFailException(f"URI S3 inválida: {uri}")
    without_scheme = uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket:
        raise AirflowFailException(f"No se pudo obtener el bucket de {uri}")
    return bucket, key.rstrip("/")


def load_emr_config():
    config = Variable.get("EXEC_SUMMARY_EMR_CONFIG", deserialize_json=True)
    missing = REQUIRED_CONFIG_KEYS - set(config.keys())
    if missing:
        raise AirflowFailException(
            f"EXEC_SUMMARY_EMR_CONFIG carece de las llaves requeridas: {missing}"
        )
    return config


def build_entry_point_args(config: dict) -> list[str]:
    args = [
        "--output-prefix",
        config["output_prefix"],
        "--data-base-uri",
        config["data_base_uri"],
    ]

    optional_map = {
        "transactions_path": "--transactions-path",
        "categories_path": "--categories-path",
        "product_categories_path": "--product-categories-path",
    }
    for key, flag in optional_map.items():
        if config.get(key):
            args.extend([flag, config[key]])

    top_n = config.get("top_n", 10)
    args.extend(["--top-n", str(top_n)])
    args.extend(["--dag-id", config.get("dag_id", "01_executive_summary")])
    args.extend(["--task-id", config.get("task_id", "generate_executive_summary")])
    return args


def build_spark_submit_parameters(config: dict) -> str | None:
    params = []
    if config.get("py_files"):
        params += ["--py-files", config["py_files"]]
    params += config.get("extra_submit_params", [])
    return " ".join(params) if params else None


def build_configuration_overrides(config: dict) -> dict | None:
    log_uri = config.get("log_uri")
    if not log_uri and config.get("output_prefix"):
        bucket, _ = parse_s3_uri(config["output_prefix"])
        log_uri = f"s3://{bucket}/logs/"

    if not log_uri:
        return None

    return {
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": log_uri,
            }
        }
    }


def download_results_from_s3(
    bucket: str,
    prefix: str,
    local_dir: str,
    aws_conn_id: str,
    **_,
):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    os.makedirs(local_dir, exist_ok=True)
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []

    if not keys:
        print(f"⚠️ No se encontraron archivos en s3://{bucket}/{prefix}")
        return

    for key in keys:
        if key.endswith("/"):
            continue
        filename = os.path.basename(key)
        destination = os.path.join(local_dir, filename)
        hook.download_file(
            key=key,
            bucket_name=bucket,
            local_path=destination,
            preserve_file_name=True,
        )
        print(f"   ↳ Descargado {key} -> {destination}")


@dag(
    dag_id="01_executive_summary",
    description="Genera métricas del resumen ejecutivo usando EMR Serverless",
    default_args=default_args,
    schedule=None,  # Cambiar a "@daily" en producción
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "summary", "dashboard"],
)
def executive_summary_dag():
    """DAG para generar el resumen ejecutivo en EMR Serverless."""

    config = load_emr_config()
    aws_conn_id = config.get("aws_conn_id", "aws_default")

    job_driver = {
        "sparkSubmit": {
            "entryPoint": config["entry_point"],
            "entryPointArguments": build_entry_point_args(config),
        }
    }
    spark_submit_parameters = build_spark_submit_parameters(config)
    if spark_submit_parameters:
        job_driver["sparkSubmit"]["sparkSubmitParameters"] = spark_submit_parameters

    configuration_overrides = build_configuration_overrides(config)

    run_emr_job = EmrServerlessStartJobOperator(
        task_id="run_exec_summary_emr",
        application_id=config["application_id"],
        execution_role_arn=config["execution_role_arn"],
        job_driver=job_driver,
        configuration_overrides=configuration_overrides,
        aws_conn_id="aws_default",
        wait_for_completion=True
    )

    bucket, output_key = parse_s3_uri(config["output_prefix"])
    summary_prefix = f"{output_key}/summary"
    local_output = config.get("local_download_path", "/opt/airflow/output/summary")

    sync_results = PythonOperator(
        task_id="sync_summary_results",
        python_callable=download_results_from_s3,
        op_kwargs={
            "bucket": bucket,
            "prefix": summary_prefix,
            "local_dir": local_output,
            "aws_conn_id": aws_conn_id,
        },
    )

    run_emr_job >> sync_results


executive_summary_dag()
