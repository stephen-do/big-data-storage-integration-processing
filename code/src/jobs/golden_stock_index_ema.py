"""
Module Name: golden_stock_index_ema.py
Author: tuyendn3
Version: 1.0
Description: template for all jobs
"""

from configparser import ConfigParser
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from init.logger import Logger


def compute_ema(df, cycles):
    window_spec = Window.partitionBy("ticker").orderBy("tradingdate")

    for N in cycles:
        alpha = 2 / (N + 1)
        df = df.withColumn(f"sma_{N}", F.avg("closeprice").over(window_spec))
        df = df.withColumn(f"prev_ema_{N}", F.lag(f"sma_{N}", 1).over(window_spec))
        df = df.withColumn(f"ema_{N}", alpha * F.col("closeprice") + (1 - alpha) * F.col(f"prev_ema_{N}"))

    return df


def run(spark: SparkSession, logger: Logger, config: ConfigParser, business_date: str, **kwargs) -> None:
    """
    Example Template for All job
    :param: spark
    :ptype: Sparksession
    :param: logger
    :ptype: Logger
    :rtype: None
    """
    logger.info(kwargs)
    logger.info(config)
    assert isinstance(spark, SparkSession)
    assert isinstance(logger, Logger)
    df = spark.sql("select * from golden.fiinapi.stock_historical_price")
    cycles = [5, 12, 15, 20, 26, 50, 200]
    df_ema = compute_ema(df, cycles)
    df = df_ema.select(["ticker", "tradingdate", "closeprice"] + [f"ema_{N}" for N in cycles])
    df.repartition(1).write.mode("overwrite").saveAsTable(f"golden.fiinapi.stock_index_ema")
    logger.info("completed")
