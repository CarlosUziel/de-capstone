import multiprocessing
from configparser import ConfigParser

import pyspark
from pyspark.sql import SparkSession


def create_spark_session(user_config: ConfigParser, dl_config: ConfigParser):
    """
    Create spark session

    Args:
        user_config: user configuration parameters.
        dl_config: data lake configuration parameters.
    """
    # 1. Create Spark session with S3 access
    spark = (
        SparkSession.builder.appName("Sparkify")
        .config(
            "spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{pyspark.__version__}"
        )
        .config("spark.worker.cores", multiprocessing.cpu_count() - 2)
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )

    # 2. Set S3 parameters
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set(
        "fs.s3a.endpoint", f"s3-{dl_config.get('GENERAL', 'REGION')}.amazonaws.com"
    )
    hadoop_conf.set("fs.s3a.access.key", user_config.get("AWS", "AWS_ACCESS_KEY_ID"))
    hadoop_conf.set(
        "fs.s3a.secret.key", user_config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    )

    return spark
