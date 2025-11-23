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

    # Memory configurations
    # Driver: 1GB (enough to coordinate)
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.driver.maxResultSize", "512m")

    # Executor: 3GB (of 4GB available, leave ~1GB for system overhead and JVM)
    conf.set("spark.executor.memory", "3g")
    conf.set("spark.executor.memoryOverhead", "512m")

    # Modern memory configuration
    conf.set("spark.memory.fraction", "0.8")
    conf.set("spark.memory.storageFraction", "0.3")

    # 1 worker with 4 cores
    conf.set("spark.executor.instances", "1")
    conf.set("spark.executor.cores", "4")

    # Parallelism: optimized for 1 worker with 4 cores
    conf.set("spark.sql.shuffle.partitions", "8")
    conf.set("spark.default.parallelism", "8")

    # Network
    conf.set("spark.network.timeout", "600s")
    conf.set("spark.executor.heartbeatInterval", "60s")

    # Broadcast
    conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

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
