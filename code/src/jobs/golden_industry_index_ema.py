"""
Module Name: golden_industry_index_ema.py

Version: 1.0
Description: template for all jobs
"""

from configparser import ConfigParser
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from init.logger import Logger


def compute_ema(df, periods):
    for N in periods:
        alpha = 2 / (N + 1)
        window_spec = Window.partitionBy("icbcode", "icbname").orderBy("tradingdate")
        df = df.withColumn(f"sma_{N}", F.avg("closeindex").over(window_spec.rowsBetween(-N+1, 0)))
        df = df.withColumn(f"prev_ema_{N}", F.lag(f"sma_{N}", 1).over(window_spec))
        df = df.withColumn(f"ema_{N}", alpha * F.col("closeindex") + (1 - alpha) * F.col(f"prev_ema_{N}"))

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
    df = spark.sql(f"""select msi.icbname, msi.icbcode, mi.closeindex, mi.tradingdate, msi.icblevel
                       from raw.fiinapi.stx_mrk_icbindustry mi 
                        inner join raw.fiinapi.stx_mst_icbindustry msi on mi.icbcode = msi.icbcode
                       where mi.comgroupcode = 'HOHAUP' order by tradingdate""")
    ema_cycles = [5, 12, 15, 20, 26, 50, 200]
    df_ema = compute_ema(df, ema_cycles)
    df_ema = df_ema.select(["icbcode", "icbname", "tradingdate", "closeindex"] + [f"ema_{N}" for N in ema_cycles])
    df_ema.repartition(1).write.mode("overwrite").saveAsTable(f"golden.fiinapi.industry_index_ema")
    logger.info("completed")
