"""
Module Name: template.py
Author: tuyendn3
Version: 1.0
Description: template for all jobs
"""

from configparser import ConfigParser
from pyspark.sql import SparkSession
from init.logger import Logger


def run(spark: SparkSession, logger: Logger, config: ConfigParser, business_date: str, **kwargs) -> None:
    """
    Example Template for All job
    :param: spark
    :ptype: Sparksession
    :param: logger
    :ptype: Logger
    :rtype: None
    """
    # use kwargs
    logger.info(kwargs)
    logger.info(config)
    assert isinstance(spark, SparkSession)
    assert isinstance(logger, Logger)
    df = spark.sql("select * from raw.fiinapi.stx_mrk_upcomstock")
    df.show()
    logger.info("completed")
