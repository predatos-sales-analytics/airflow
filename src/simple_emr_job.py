"""
Job sencillo para EMR Serverless.

Lee un subconjunto pequeño del dataset de transacciones desde S3 y genera
un resumen básico (conteo total y transacciones por día). El resultado se
sube nuevamente a S3 en formato JSON, por lo que requiere únicamente boto3
además de PySpark.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Tuple

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructField, StructType, IntegerType, StringType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Job ligero para contar transacciones y actividad diaria."
    )
    parser.add_argument(
        "--transactions-path",
        required=True,
        help="Ruta en S3 al directorio con archivos CSV de transacciones.",
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="Ruta en S3 donde se escribirá el resumen (ej. s3://bucket/results/sample).",
    )
    parser.add_argument(
        "--separator",
        default="|",
        help="Separador usado en los CSV (default: |).",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=5000,
        help="Máximo de registros a procesar para mantener el job liviano (0 = todos).",
    )
    parser.add_argument(
        "--job-name",
        default="SampleSummaryJob",
        help="Nombre para la sesión de Spark.",
    )
    parser.add_argument(
        "--dag-id",
        default="02_sample_summary",
        help="Se incluye en la metadata exportada.",
    )
    parser.add_argument(
        "--task-id",
        default="run_sample_summary",
        help="Se incluye en la metadata exportada.",
    )
    return parser.parse_args()


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"URI S3 inválida: {uri}")
    without_scheme = uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket:
        raise ValueError(f"No se encontró bucket en {uri}")
    return bucket, key.rstrip("/")


def read_transactions(
    spark: SparkSession, path: str, separator: str, max_records: int
) -> DataFrame:
    schema = StructType(
        [
            StructField("transaction_date", StringType(), True),
            StructField("store_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("products", StringType(), True),
        ]
    )

    df = (
        spark.read.option("sep", separator)
        .schema(schema)
        .csv(f"{path.rstrip('/')}/")
        .withColumn("store_id", col("store_id").cast("int"))
        .withColumn("customer_id", col("customer_id").cast("int"))
    )

    if max_records and max_records > 0:
        df = df.limit(max_records)

    return df


def build_daily_summary(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("date", to_date(col("transaction_date")))
        .groupBy("date")
        .count()
        .orderBy("date")
    )


def upload_json(bucket: str, key: str, payload: dict) -> None:
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"),
    )
    print(f"   ↳ s3://{bucket}/{key}")


def main():
    args = parse_args()
    spark = SparkSession.builder.appName(args.job_name).getOrCreate()

    try:
        df_transactions = read_transactions(
            spark,
            args.transactions_path,
            args.separator,
            args.max_records,
        )
        df_transactions.cache()

        total_transactions = df_transactions.count()
        distinct_stores = df_transactions.select("store_id").distinct().count()

        daily_summary = build_daily_summary(df_transactions)
        daily_rows = [json.loads(row) for row in daily_summary.toJSON().collect()]

        bucket, key_prefix = parse_s3_uri(args.output_prefix)
        summary_key = f"{key_prefix.rstrip('/')}/sample_summary.json"
        metadata_key = f"{key_prefix.rstrip('/')}/metadata.json"

        upload_json(
            bucket,
            summary_key,
            {
                "total_transactions": total_transactions,
                "distinct_stores": distinct_stores,
                "daily_counts": daily_rows,
                "generated_at": datetime.utcnow().isoformat(),
            },
        )

        upload_json(
            bucket,
            metadata_key,
            {
                "dag_id": args.dag_id,
                "task_id": args.task_id,
                "execution_timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "max_records": args.max_records,
            },
        )

        df_transactions.unpersist()

        print("✅ Resumen ligero generado correctamente.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()







