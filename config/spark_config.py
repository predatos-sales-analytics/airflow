"""
Spark configuration project
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os


def create_spark_session(app_name="SparkConfig"):
    """
    Create and configure a Spark session

    Args:
        app_name (str): Spark application name

    Returns:
        SparkSession: Configured Spark session
    """
    conf = SparkConf()

    # Basic configurations optimized
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # Specific configurations to avoid problems
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    conf.set("spark.python.worker.reuse", "false")
    conf.set("spark.sql.execution.pyspark.udf.faulthandler.enabled", "false")
    conf.set("spark.python.worker.faulthandler.enabled", "false")

    # More permissive network configurations
    conf.set("spark.network.timeout", "800s")
    conf.set("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "false")

    # Memory configurations optimized for larger datasets
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.maxResultSize", "2g")

    # Configurations to reduce concurrency problems
    conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")

    # Configure temporary working directory
    try:
        temp_dir = os.path.join(os.getcwd(), "spark-temp")
        os.makedirs(temp_dir, exist_ok=True)
        conf.set("spark.local.dir", temp_dir)
    except:
        pass  # If it cannot be created, use default

    master_url = os.getenv("SPARK_MASTER_URL", "local[2]")
    jars_packages = os.getenv("SPARK_JARS_PACKAGES")
    for key, value in os.environ.items():
        if key.startswith("SPARK_CONF__"):
            spark_key = key.replace("SPARK_CONF__", "").lower().replace("_", ".")
            conf.set(spark_key, value)

    if jars_packages:
        conf.set("spark.jars.packages", jars_packages)

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config(conf=conf)
        .getOrCreate()
    )

    # Configure logging level to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def stop_spark_session(spark):
    """
    Stop Spark session safely

    Args:
        spark (SparkSession): Spark session to stop
    """
    try:
        if spark:
            spark.stop()
    except Exception as e:
        print(f"Warning when closing Spark: {e}")
        # Try to force termination
        try:
            spark.sparkContext.stop()
        except:
            pass

