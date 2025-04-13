"""
Module Name: interface_stock_growth.py

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
    df = spark.sql("""
    select t.ticker
        , net_sale/nvl(lag(net_sale, 1) over (partition by t.yearreport, t.lengthreport order by t.yearreport), net_sale) - 1 netsale
        , net_profit/nvl(lag(net_profit, 1) over (partition by t.yearreport, t.lengthreport order by t.yearreport), net_profit) - 1 net_profit
        , equity/nvl(lag(equity, 1) over (partition by t.yearreport, t.lengthreport order by t.yearreport), equity) - 1 equity
        , lengthreport
        , yearreport
        , current_timestamp() createdate
        , current_timestamp() updatedate
        , gross_profit/nvl(lag(gross_profit, 1) over (partition by t.yearreport, t.lengthreport order by t.yearreport), gross_profit) - 1 gross_profit
        , profit/nvl(lag(profit, 1) over (partition by t.yearreport, t.lengthreport order by t.yearreport), profit) - 1 profit_before_tax
        , icbcode
        , icbname
        , 0 as icb_netsale
        , 0 as icb_net_profit
        , 0 as icb_equity
    from (
        select t.ticker
            , case when icb_lv2 = '8300' then isb38
                when icb_lv2 = '8500' then isi64
                else isa3 end net_sale
            , isa20 net_profit
            , case when icb_lv2 = '8300' then isb27
                when icb_lv2 = '8500' then isi100
                else isa5 end gross_profit
            , isa16 profit
            , bsa78 equity
            , t.yearreport
            , t.lengthreport
            , icbname
            , icb_lv2 icbcode
        from raw.fiinapi.stx_fsc_incomestatement t
        inner join raw.fiinapi.stx_fsc_balancesheet b on b.ticker = t.ticker and t.yearreport = b.yearreport and t.lengthreport = b.lengthreport
        inner join (select ticker, icbcode from raw.fiinapi.stx_cpf_organization) o on o.ticker = t.ticker
        inner join (select icbcode, split_part(icbcodepath, '/', 2) as icb_lv2, split_part(icbnamepath, '/', 2) icbname from raw.fiinapi.stx_mst_icbindustry) i on i.icbcode = o.icbcode
        where t.lengthreport in (1, 2, 3, 4) and t.status = 1 and b.status = 1
    ) t
    """)
    df.repartition(1).write.mode("overwrite").saveAsTable(f"interface.serving.stock_growth")
    logger.info("completed")