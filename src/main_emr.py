"""
Entry point para ejecutar el resumen ejecutivo en EMR Serverless.

El script replica lo que hace el DAG local pero leyendo datos desde S3 y
escribiendo los resultados nuevamente en S3 para que el frontend/Airflow
puedan sincronizarlos.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Dict, Any, Tuple, List

import boto3
from pyspark.sql import SparkSession, DataFrame

from src.data_loader import DataLoader
from src.summary_metrics import SummaryMetrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el resumen ejecutivo en EMR Serverless usando datos en S3."
    )
    parser.add_argument(
        "--data-base-uri",
        default=os.getenv("S3_DATA_BASE_URI"),
        help="Prefijo base en S3 donde viven los datos crudos (ej. s3://bucket/raw-data).",
    )
    parser.add_argument(
        "--transactions-path",
        help="Ruta completa en S3 (sin comodines) para la carpeta de transacciones.",
    )
    parser.add_argument(
        "--categories-path",
        help="Ruta al archivo Categories.csv en S3.",
    )
    parser.add_argument(
        "--product-categories-path",
        help="Ruta al archivo ProductCategory.csv en S3.",
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="Prefijo en S3 donde se escribirán los JSON (ej. s3://bucket/results/summary).",
    )
    parser.add_argument(
        "--job-name",
        default="ExecutiveSummaryEMR",
        help="Nombre amigable para la sesión de Spark.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Cantidad de elementos para los rankings.",
    )
    parser.add_argument(
        "--dag-id",
        default="01_executive_summary",
        help="Se usa en la metadata final.",
    )
    parser.add_argument(
        "--task-id",
        default="generate_executive_summary",
        help="Se usa en la metadata final.",
    )
    return parser.parse_args()


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri or not uri.startswith("s3://"):
        raise ValueError(f"URI S3 inválida: {uri}")

    without_scheme = uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket:
        raise ValueError(f"No se encontró bucket en la URI {uri}")
    return bucket, key.rstrip("/")


def df_to_json_records(df: DataFrame, limit: int | None = None) -> List[Dict[str, Any]]:
    dataset = df if limit is None else df.limit(limit)
    return [json.loads(row) for row in dataset.toJSON().collect()]


def upload_json(s3_client, bucket: str, key: str, payload: Dict[str, Any]):
    body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    print(f"   ↳ S3://{bucket}/{key}")


def write_results_to_s3(
    results: Dict[str, Any],
    output_prefix: str,
    top_n: int,
    dag_id: str,
    task_id: str,
):
    bucket, base_key = parse_s3_uri(output_prefix)
    summary_prefix = f"{base_key}/summary".rstrip("/")
    metadata_prefix = f"{base_key}/metadata".rstrip("/")

    s3_client = boto3.client("s3")

    print("\n💾 Subiendo resultados a S3...")
    upload_json(
        s3_client,
        bucket,
        f"{summary_prefix}/basic_metrics.json",
        results["basic_metrics"],
    )
    upload_json(
        s3_client,
        bucket,
        f"{summary_prefix}/top_{top_n}_products.json",
        {
            "type": "products",
            "top_n": top_n,
            "data": df_to_json_records(results["top_products"], top_n),
            "generated_at": datetime.utcnow().isoformat(),
        },
    )
    upload_json(
        s3_client,
        bucket,
        f"{summary_prefix}/top_{top_n}_customers.json",
        {
            "type": "customers",
            "top_n": top_n,
            "data": df_to_json_records(results["top_customers"], top_n),
            "generated_at": datetime.utcnow().isoformat(),
        },
    )
    upload_json(
        s3_client,
        bucket,
        f"{summary_prefix}/top_{top_n}_peak_days.json",
        {
            "type": "peak_days",
            "top_n": top_n,
            "data": df_to_json_records(results["peak_days"], top_n),
            "generated_at": datetime.utcnow().isoformat(),
        },
    )
    upload_json(
        s3_client,
        bucket,
        f"{summary_prefix}/top_{top_n}_categories.json",
        {
            "type": "categories",
            "top_n": top_n,
            "data": df_to_json_records(results["top_categories"], top_n),
            "generated_at": datetime.utcnow().isoformat(),
        },
    )

    metadata = {
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_timestamp": datetime.utcnow().isoformat(),
        "total_transactions": results["basic_metrics"]["total_transactions"],
        "total_sales_units": results["basic_metrics"]["total_sales_units"],
        "status": "success",
    }
    upload_json(
        s3_client,
        bucket,
        f"{metadata_prefix}/{dag_id}_{task_id}.json",
        metadata,
    )


def main():
    args = parse_args()
    if not args.data_base_uri:
        raise ValueError(
            "Debe proporcionar --data-base-uri o definir S3_DATA_BASE_URI en el entorno."
        )

    # Permitir overrides de rutas específicas
    if args.transactions_path:
        os.environ["S3_TRANSACTIONS_PATH"] = args.transactions_path.rstrip("/")
    if args.categories_path:
        os.environ["S3_CATEGORIES_PATH"] = args.categories_path
    if args.product_categories_path:
        os.environ["S3_PRODUCT_CATEGORIES_PATH"] = args.product_categories_path

    spark = (
        SparkSession.builder.appName(args.job_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        data_loader = DataLoader(
            spark,
            source_type="s3",
            s3_base_path=args.data_base_uri,
        )
        metrics_calculator = SummaryMetrics(spark)

        print("\n📂 Cargando datos desde S3...")
        df_transactions = data_loader.load_transactions()
        df_transactions.cache()

        df_transactions_exploded = data_loader.explode_transactions(df_transactions)
        df_transactions_exploded.cache()

        df_product_categories = data_loader.load_product_categories()
        df_product_categories.cache()

        df_categories = data_loader.load_categories()
        df_categories.cache()

        results = metrics_calculator.generate_executive_summary(
            df_transactions,
            df_transactions_exploded,
            df_product_categories,
            df_categories,
        )

        write_results_to_s3(
            results,
            args.output_prefix,
            args.top_n,
            args.dag_id,
            args.task_id,
        )

        df_transactions.unpersist()
        df_transactions_exploded.unpersist()
        df_product_categories.unpersist()
        df_categories.unpersist()

    finally:
        print("\n🧹 Cerrando sesión de Spark...")
        spark.stop()


if __name__ == "__main__":
    main()


