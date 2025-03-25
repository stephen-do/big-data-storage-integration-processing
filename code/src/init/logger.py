"""
Module Name: logger.py
Author: tuyendn3
Version: 1.0
Description: a spark logger
"""

from typing import NoReturn
from pyspark.sql import SparkSession


class Logger:
    """Wrapper class for Log4j JVM object.
    :param spark: SparkSession object.
    """

    def __init__(self, spark: SparkSession):
        spark.sparkContext.setLogLevel("INFO")
        conf = spark.sparkContext.getConf()
        app_id = conf.get("spark.app.id")
        app_name = conf.get("spark.app.name")

        log4j = spark._jvm.org.apache.log4j
        message_prefix = "<" + app_name + " " + app_id + ">"
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message: str) -> NoReturn:
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)

    def warn(self, message: str) -> NoReturn:
        """Log a warning.
        :param: Warning message to write to log
        :return: None
        """
        self.logger.warn(message)

    def info(self, message: str) -> NoReturn:
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
