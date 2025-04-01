"""
Module Name: interface_stock_overview.py
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
    with closepricedaily as (
    select *, `Volume ` totalmatchvolume, row_number() over (partition by ticker order by tradingdate desc) rn 
    from golden.fiinapi.stock_historical_price
    where tradingdate <= '{business_date}'
    )
    select t.*
        , (average_price_10d_rank_tmp/v_total_stock) * 100 liquidity 
        , (marcap_rank_tmp/v_total_stock) * 100 marcap_rank 
        , ((accumulate_point_rank_tmp/v_total_stock) * 100) * 50 / 100 + ((uptrend_rank_tmp/v_total_stock) * 100) * 50 / 100 buy_point 
        , (uptrend_rank_tmp/v_total_stock) * 100 uptrend_rank 
        , (cash_flow_rank_tmp/v_total_stock) * 10 cash_flow_rank 
        , (cash_flow_momentum_rank_tmp/v_total_stock) * 10 cash_flow_momentum_rank
        , CAST('{business_date}' AS DATE) tradingdate
    from (
        select 
            t.*
            , rank () over (order by t.uptrend asc) as uptrend_rank_tmp
            , rank () over (order by t.marcap asc) as marcap_rank_tmp
            , rank () over (order by t.average_price_10d asc) as average_price_10d_rank_tmp
            , rank () over (order by t.accumulate_point asc) as accumulate_point_rank_tmp
            , rank () over (order by t.cash_flow_momentum asc) as cash_flow_momentum_rank_tmp
            , rank () over (order by t.cash_flow asc) as cash_flow_rank_tmp
            , t1.v_total_stock v_total_stock
        from (
            select t.*
                , t.stock_valuation - (t.stock_valuation*20/100) buy_zone
                , t.stock_valuation attractive_zone
                , t.stock_valuation + (t.stock_valuation*20/100) tracking_zone
                , case when t.closeprice/1000 + ((t.closeprice/1000)*20/100) > t.stock_valuation + (t.stock_valuation*50/100) then t.closeprice/1000 + ((t.closeprice/1000)*20/100)
                    else t.stock_valuation + (t.stock_valuation*50/100) end sell_zone
                , t1.total_assets_growth
                , t1.paid_in_capital_growth
                , t.pe/eps peg
            from (
                select t.*
                    , case when v_stddev_close_price != 0 then t5.ema_20/t.v_stddev_close_price
                        else 0 end accumulate_point
                    , t6.barindex
                    , case when ema_200 is not null and ema_200 != 0 then (ema_5 + ema_15 - ema_20 - ema_50)/ema_200
                        else 0 end uptrend
                    , case when barindex is not null and barindex > 200 then (ema_15 - ema_50) / ema_200 + (ema_5 - ema_20) / ema_200
                        else 0 end cash_flow
                    , case when v.v_ma > 10000 then (ema_15 - ema_50) / ema_200 + (ema_5 - ema_20) / ema_200
                        else -999 end cash_flow_momentum
                    , t8.rtd11 marcap
                    , t8.rtd3 outstanding_shares
                    , t8.rtd5 free_float
                    , t8.rtd14 eps
                    , t8.rtd21 pe
                    , t8.rtd25 pb
                    , t8.rtd30 ev_ebitda
                    , t8.rtd7 book_value
                    , average_pe_12m
                    , average_pb_12m
                    , case when t9.comtypecode in ('CK', 'NH', 'BH') then (average_pb_12m * book_value) /1000
                        else (average_pe_12m * book_value) /1000 end stock_valuation
                from (
                    select t.*
                        , t2.average_price_10d
                        , t2.v_stddev_close_price
                        , t3.average_price_20d
                        , t3.average_volume_20d
                        , t4.average_price_50d
                        , t4.average_volume_50d
                    from (
                        select o.openprice
                            , o.totalmatchvolume
                            , t.*
                            , case when t.closeprice is not null and t.perfomance_6 is not null then (t.closeprice /t.perfomance_6 - 1)*100
                                else 0 end stock_perfomance_5d
                        from (select * from closepricedaily where rn = 1) o
                        left join (select * from (select t.ticker, t.closeprice, t.rn
                                        from closepricedaily t
                                        where t.rn in (1, 6, 21, 120, 240)
                                        ) as SourceTable pivot (
                                        max(closeprice) FOR rn in (1 as closeprice, 6 as perfomance_6, 21 as perfomance_21, 120 as perfomance_120, 240 as perfomance_240)
                        )) t on t.ticker = o.ticker
                    ) t
                    left join (select t.ticker, avg(t.closeprice) average_price_10d, stddev(t.closeprice) v_stddev_close_price
                               from closepricedaily t
                               where t.rn <= 10
                               group by ticker) t2 on t2.ticker = t.ticker
                    left join (select t.ticker, avg(t.closeprice) average_price_20d, avg(t.totalmatchvolume) average_volume_20d
                               from closepricedaily t
                               where t.rn <= 20
                               group by ticker) t3 on t3.ticker = t.ticker
                    left join (select t.ticker, avg(t.closeprice) average_price_50d, avg(t.totalmatchvolume) average_volume_50d
                               from closepricedaily t
                               where t.rn <= 50
                               group by ticker) t4 on t4.ticker = t.ticker
                ) t
                left join golden.fiinapi.stock_index_ema t5 on t.ticker = t5.ticker and t5.tradingdate = '{business_date}'
                left join (select t.ticker, sum(t.totalmatchvolume) v_ma
                           from closepricedaily t
                           where t.rn <= 20
                           group by t.ticker) v on v.ticker = t.ticker
                left join (select t.ticker, count(1) barindex
                           from closepricedaily t
                           where t.rn <= 400
                           group by t.ticker
                ) t6 on t.ticker = t6.ticker
                left join (select * 
                           from (select rr.rtd11, rr.rtd3, rr.rtd5, rr.rtd14, rr.rtd21, rr.rtd25, rr.rtd30, rr.ticker , rr.rtd7, row_number() over (partition by ticker order by tradingdate desc) rn
                                 from raw.fiinapi.stx_rto_ratiottmdaily rr
                                 where rr.tradingdate <=  '{business_date}'
                                ) t where rn = 1
                ) t8 on t8.ticker = t.ticker
                left join (select t.ticker, avg(t.pe) average_pe_12m, avg(t.pb) average_pb_12m, t.comtypecode
                           from (select o.ticker, o.comtypecode
                                    , case when o.comtypecode in ('CK', 'BH', 'NH') then null
                                        else r.rtd21 end as pe
                                    , case when o.comtypecode in ('CK', 'BH', 'NH') then r.rtd25
                                        else null end as pb
                                    , row_number () over (partition by r.ticker order by r.tradingdate desc) ronum
                                from raw.fiinapi.stx_rto_ratiottmdaily r left join raw.fiinapi.stx_cpf_organization o on o.ticker = r.ticker
                                where r.tradingdate <= '{business_date}') t
                           where t.ronum <= 240
                           group by t.ticker, comtypecode
                ) t9 on t9.ticker = t.ticker
            ) t
            left join (
                select t.ticker
                    , case when t.v_total_assets_same_period is not null then ((t.v_last_total_assets / t.v_total_assets_same_period) - 1)*100 else null end total_assets_growth
                    , case when t.v_paid_in_capital_same_period is not null then ((t.v_last_paid_in_capital / t.v_paid_in_capital_same_period) - 1)*100 else null end paid_in_capital_growth
                from (
                    select *
                    from (
                        select t.ticker
                            , t.yearreport
                            , t.lengthreport
                            , t.bsa53 v_last_total_assets
                            , lag(t.bsa53, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) v_total_assets_same_period
                            , t.bsa80 v_last_paid_in_capital
                            , lag(t.bsa80, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) v_paid_in_capital_same_period
                            , t.rn
                        from (
                            select t.*
                                , row_number () over (partition by ticker order by yearreport desc, lengthreport desc) rn
                            from raw.fiinapi.stx_fsc_balancesheet t
                            where lengthreport in (1, 2, 3, 4)
                            ) t
                        ) t
                    where t.rn = 1
                ) t
            ) t1 on t1.ticker = t.ticker
        ) t, (select count(1) v_total_stock from closepricedaily where rn = 1) t1
    ) t""")
    spark.sql(f"""delete from interface.serving.stock_overview where tradingdate = '{business_date}'""")
    df.repartition(1).write.mode("append").saveAsTable(f"interface.serving.stock_overview")
    logger.info("completed")