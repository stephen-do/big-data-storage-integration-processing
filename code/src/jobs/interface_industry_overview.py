"""
Module Name: interface_industry_overview.py
Author: tuyendn3
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
    df = spark.sql(f"""
    select t.icbcode
        , t.icbname
        , cast(NULL as DECIMAL(38, 18)) as open_index
        , cast(NULL as DECIMAL(38, 18)) as close_index
        , average_index_10d
        , current_timestamp() createdate
        , current_timestamp() updatedate
        , t.tradingdate
        , t.uptrend
        , case when icblevel = 2 then (average_price_10d_rank_tmp/total) * 10
            else null end liquidity
        , t.marcap
        , case when icblevel = 2 then ((accumulate_point_rank_tmp/total) * 10) * 50 / 100 + ((uptrend_rank_tmp/total) * 10) * 50 / 100
            else null end buy_point
        , case when icblevel = 2 then (marcap_rank_tmp/total) * 10
            else null end marcap_rank
        , t.accumulate_point
        , case when icblevel = 2 then (uptrend_rank_tmp/total) * 10
            else null end uptrend_rank
        , t.cash_flow
        , case when icblevel = 2 then (cash_flow_rank_tmp/total) * 10
            else null end cash_flow_rank
        , t.pe
        , t.pb
        , t.dividend_yield
        , t.ev_ebitda
        , t.icblevel
    from (
        select t.*
            , rank () over (partition by icblevel order by t.uptrend asc) as uptrend_rank_tmp
            , rank () over (partition by icblevel order by t.marcap asc) as marcap_rank_tmp
            , rank () over (partition by icblevel order by t.average_index_10d asc) as average_price_10d_rank_tmp
            , rank () over (partition by icblevel order by t.accumulate_point asc) as accumulate_point_rank_tmp
            , rank () over (partition by icblevel order by t.cash_flow asc) as cash_flow_rank_tmp
            , t1.total
        from (
            select t.*
                , case when icb_ema_200 is not null and icb_ema_200 != 0 then (icb_ema_5 + icb_ema_15 - icb_ema_20 - icb_ema_50) / icb_ema_200
                    else null end uptrend
                , case when stddev_close_index is not null and stddev_close_index != 0 then icb_ema_20 / stddev_close_index
                    else null end accumulate_point
                , case when barindex > 200 then (icb_ema_15 - icb_ema_50) / icb_ema_200 + (icb_ema_5 - icb_ema_20) / icb_ema_200
                    else (0 - 999) / 999 + (0 - 999) / 999 end cash_flow
            from (
                select SourceTable.*
                    , t.average_index_10d
                    , t.stddev_close_index
                    , t1.marcap
                    , t1.pe
                    , t1.pb
                    , t1.dividend_yield
                    , t1.ev_ebitda
                    , t2.barindex
                    , t4.icblevel
                from (
                    select icbcode, icbname, tradingdate, closeindex,
                        ema_5 icb_ema_5, ema_15 icb_ema_15, ema_20 icb_ema_20, ema_50 icb_ema_50, ema_200 icb_ema_200
                    from golden.fiinapi.industry_index_ema
                    where tradingdate = '{business_date}'
                ) SourceTable
                left join (select icbcode
                            , avg(t.closeindex) average_index_10d
                            , stddev(t.closeindex) stddev_close_index
                        from (select icbcode, closeindex, tradingdate,
                                row_number() over (partition by icbcode order by tradingdate desc) rn
                              from raw.fiinapi.stx_mrk_icbindustry smi
                              where comgroupcode = 'HOHAUP' and tradingdate <= '{business_date}') t
                        where t.rn <= 10 group by icbcode) t on t.icbcode = SourceTable.icbcode
                left join (select t.* from (select r.icbcode, r.rsd11 marcap, r.rsd21 pe, r.rsd25 pb, r.rsd36 dividend_yield, r.rsd30 ev_ebitda, row_number() over (partition by icbcode order by tradingdate desc) rn
                        from raw.fiinapi.stx_rto_ratioicbindustry r
                        where r.comgroupcode = 'HOHAUP'
                        and r.tradingdate <= '{business_date}') t where t.rn = 1) t1 on t1.icbcode = SourceTable.icbcode
                left join (select t.icbcode, count(1) barindex
                        from (select icbcode, row_number() over (partition by icbcode order by tradingdate desc) rn
                              from raw.fiinapi.stx_mrk_icbindustry
                              where comgroupcode = 'HOHAUP' and tradingdate <= '{business_date}') t
                        where t.rn <= 400
                        group by icbcode) t2 on t2.icbcode = SourceTable.icbcode
                left join (select icblevel, icbcode
                        from raw.fiinapi.stx_mst_icbindustry) t4 on t4.icbcode = SourceTable.icbcode
            ) t
        ) t,  (select count(1) total from raw.fiinapi.stx_mst_icbindustry s where icblevel = 2) t1
    ) t
    """)
    spark.sql(f"""delete from interface.serving.industry_overview where tradingdate = '{business_date}'""")
    df.repartition(1).write.mode("append").saveAsTable(f"interface.serving.industry_overview")
    logger.info("completed")
