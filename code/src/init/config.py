"""
Module Name: config.py
Author: tuyendn3
Version: 1.0
Description: a spark config for initialized spark session
"""

from typing import Optional
from pyspark.conf import SparkConf
# import boto3


def get_key():
    """Get Key from Secret Manager"""


def create_spark_config(env: Optional[str] = "prod") -> SparkConf:
    """Create spark config to initialized spark session
    :rtype: SparkConf
    """
    assert isinstance(env, str)
    # client = boto3.client(env)
    config = SparkConf()
    ## clickhouse
    config.set("spark.sql.catalog.clickhouse.database", "ods_rt")
    config.set("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY")
    config.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    config.set("spark.sql.parquet.writeLegacyFormat", "true")
    return config
