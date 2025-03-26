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
    config = SparkConf()
    return config
