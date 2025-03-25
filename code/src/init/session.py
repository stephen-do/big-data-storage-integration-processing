"""
Module Name: session.py
Author: tuyendn3
Version: 1.0
Description: a spark session to run the job
"""

from typing import Optional, Tuple
from pyspark.sql import SparkSession
from init.config import create_spark_config
from init.logger import Logger


def create_spark_session(
    job_name: Optional[str], env: Optional[str] = "prod"
) -> Tuple[SparkSession, Logger]:
    """Create spark session to run the job

    :param job_name: job name
    :type job_name: str
    :param env: environment
    :type env: str
    :return: spark and logger
    :rtype: Tuple[SparkSession,Logger]
    """
    conf = create_spark_config(env)
    spark = SparkSession.builder.appName(job_name).config(conf=conf).getOrCreate()
    logger = Logger(spark=spark)
    return spark, logger
