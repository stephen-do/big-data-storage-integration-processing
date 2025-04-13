"""
Module Name: golden_stock_index_rsi.py

Version: 1.0
Description: template for all jobs
"""

from configparser import ConfigParser
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
    df = spark.sql("select * from golden.fiinapi.stock_historical_price")
    window_spec = Window.partitionBy("ticker").orderBy("tradingdate")
    df = df.withColumn("prev_close", F.lag("closeprice").over(window_spec))
    df = df.withColumn("delta", F.col("closeprice") - F.col("prev_close"))
    df = df.withColumn("gain", F.when(F.col("delta") > 0, F.col("delta")).otherwise(0))
    df = df.withColumn("loss", F.when(F.col("delta") < 0, -F.col("delta")).otherwise(0))
    rsi_window = Window.partitionBy("ticker").orderBy("tradingdate").rowsBetween(-13, 0)

    df = df.withColumn("avg_gain", F.avg("gain").over(rsi_window))
    df = df.withColumn("avg_loss", F.avg("loss").over(rsi_window))
    df = df.withColumn("rs", F.col("avg_gain") / F.col("avg_loss"))
    df = df.withColumn("rsi", 100 - (100 / (1 + F.col("rs"))))
    df = df.select("ticker", "tradingdate", "closeprice", "rsi")
    df.repartition(1).write.mode("overwrite").saveAsTable(f"golden.fiinapi.stock_index_rsi")
    df.show()
    logger.info("completed")
