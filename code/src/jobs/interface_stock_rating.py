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
    df = spark.sql(f"""
        with closepricedaily as (
        select t.* , row_number() over (partition by ticker order by tradingdate desc) rn
        from (select closepriceadjusted closeprice, ticker, tradingdate, totalmatchvolume, 'HOSE' comgroupcode, highestpriceadjusted highestprice, lowestpriceadjusted lowestprice, openpriceadjusted openprice
              from raw.fiinapi.stx_mrk_hosestock
              where tradingdate <= '{business_date}' and status = 1
              union all
              select closepriceadjusted closeprice, ticker, tradingdate, totalmatchvolume, 'HNX' comgroupcode, highestpriceadjusted highestprice, lowestpriceadjusted lowestprice, openpriceadjusted openprice
              from raw.fiinapi.stx_mrk_hnxstock
              where tradingdate <= '{business_date}' and status = 1
              union all
              select closeprice closeprice, ticker, tradingdate, totalmatchvolume, 'UPCOM' comgroupcode, highestprice highestprice, lowestprice lowestprice, openprice openprice
              from raw.fiinapi.stx_mrk_upcomstock
              where tradingdate <= '{business_date}' and status = 1
            ) t
        )
        select monotonically_increasing_id() AS id
            , t.ticker
            , current_date() date_create
            , t.growth
            , t.value
            , t.momentum
            , (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 dsc_rating
            , t.growth_ranking
            , t.momentum_ranking
            , t.value_ranking
            , case when (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 is null then 'N/A'
                when (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 >= 8 then 'A'
                when (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 < 8 and (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 >=6.5 then 'B'
                when (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 < 6.5 and (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 >= 5 then 'C'
                when (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 <5 and (((v_growth_rank_number/v_total_stock) * 10 + momentum) * 50) /100 >= 0 then 'D'
                end dsc_rating_ranking
            , CAST('{business_date}' AS TIMESTAMP) tradingdate
        from (
            select t.ticker
                , t.total growth
                , case when total is not null then
                        case when total > 3.5 then 'A'
                        when total > 2.5 and total <= 3.5 then 'B'
                        when total > 1.5 and total <= 2.5 then 'C'
                        when total <= 1.5 and total > -9999 then 'D' end
                    when MONTH('{business_date}') in (3, 6, 9, 12) then 'N/A'
                    end growth_ranking
                , t1.value
                , t1.value_ranking
                , t2.momentum
                , t2.momentum_ranking
                , v_total_stock
                , rank () over (order by t.total asc) as v_growth_rank_number
            from (select t.*
                    , case when comtypecode = 'NH' then (v_toi * 1 + tnlt * 1 + v_lntt * 1 + v_lnst * 1 + v_vcsh * 2) / 6 
                        when comtypecode = 'BH' then (v_dtt_hdkd_bh * 1 + v_ln_hdtc * 1 + v_lntt * 1 + v_lnst * 1 + v_vcsh * 2) / 6
                        else (v_dtt * 1 + v_lng * 1 + v_lntt * 1 + v_lnst * 1 + v_vcsh * 2) / 6 end total
                from (
                    select t.*
                        , case when toi >= 0.3 then 4
                            when toi < 0.3 and toi >= 0.15 then 3
                            when toi < 0.15 and toi >= 0.075 then 2
                            when toi < 0.075 and toi > 0 then 1
                            when toi <= 0 then 0 end v_toi
                        , case when tnlt >= 0.3 then 4
                            when tnlt < 0.3 and tnlt >= 0.15 then 3
                            when tnlt < 0.15 and tnlt >= 0.075 then 2
                            when tnlt < 0.075 and tnlt > 0 then 1
                            when tnlt <= 0 then 0 end v_tnlt
                        , case when lntt >= 0.3 then 4
                            when lntt < 0.3 and lntt >= 0.15 then 3
                            when lntt < 0.15 and lntt >= 0.075 then 2
                            when lntt < 0.075 and lntt > 0 then 1
                            when lntt <= 0 then 0 end v_lntt
                        , case when lnst >= 0.3 then 4
                            when lnst < 0.3 and lnst >= 0.15 then 3
                            when lnst < 0.15 and lnst >= 0.075 then 2
                            when lnst < 0.075 and lnst > 0 then 1
                            when lnst <= 0 then 0 end v_lnst
                        , case when vcsh >= 0.3 then 4
                            when vcsh < 0.3 and vcsh >= 0.15 then 3
                            when vcsh < 0.15 and vcsh >= 0.075 then 2
                            when vcsh < 0.075 and vcsh > 0 then 1
                            when vcsh <= 0 then 0 end v_vcsh
                        , case when dtt_hdkd_bh >= 0.3 then 4
                            when dtt_hdkd_bh < 0.3 and dtt_hdkd_bh >= 0.15 then 3
                            when dtt_hdkd_bh < 0.15 and dtt_hdkd_bh >= 0.075 then 2
                            when dtt_hdkd_bh < 0.075 and dtt_hdkd_bh > 0 then 1
                            when dtt_hdkd_bh <= 0 then 0 end v_dtt_hdkd_bh
                        , case when ln_hdtc >= 0.3 then 4
                            when ln_hdtc < 0.3 and ln_hdtc >= 0.15 then 3
                            when ln_hdtc < 0.15 and ln_hdtc >= 0.075 then 2
                            when ln_hdtc < 0.075 and ln_hdtc > 0 then 1
                            when ln_hdtc <= 0 then 0 end v_ln_hdtc
                        , case when dtt >= 0.3 then 4
                            when dtt < 0.3 and dtt >= 0.15 then 3
                            when dtt < 0.15 and dtt >= 0.075 then 2
                            when dtt < 0.075 and dtt > 0 then 1
                            when dtt <= 0 then 0 end v_dtt
                        , case when lng >= 0.3 then 4
                            when lng < 0.3 and lng >= 0.15 then 3
                            when lng < 0.15 and lng >= 0.075 then 2
                            when lng < 0.075 and lng > 0 then 1
                            when lng <= 0 then 0 end v_lng
                    from (
                        select t.*
                            , case when t.total_operating_income_last_quarter < t.total_operating_income_same_period then 0
                               else abs((t.total_operating_income_last_quarter / t.total_operating_income_same_period) - 1) end toi
                            , case when t.net_interest_income_last_quarter < t.net_interest_income_same_period then 0
                               else abs((t.net_interest_income_last_quarter / t.net_interest_income_same_period) - 1) end tnlt
                            , case when t.net_profit_before_tax_last_quarter < t.net_profit_before_tax_same_period then 0
                               else abs((t.net_profit_before_tax_last_quarter / t.net_profit_before_tax_same_period) - 1) end lntt
                            , case when t1.total_asset_last_quarter < t1.total_asset_same_period then 0
                               else abs((t1.total_asset_last_quarter / t1.total_asset_same_period) - 1) end lnst
                            , case when t1.chartercapital_last_quarter < t1.chartercapital_same_period then 0
                               else abs((t1.chartercapital_last_quarter / t1.chartercapital_same_period) - 1) end vcsh
                            , case when t.net_sale_from_insurance_business_last_quarter < t.net_sale_from_insurance_business_same_period then 0
                               else abs((t.net_sale_from_insurance_business_last_quarter / t.net_sale_from_insurance_business_same_period) - 1) end dtt_hdkd_bh
                            , case when t.profit_from_financial_activities_last_quarter < t.profit_from_financial_activities_same_period then 0
                               else abs((t.profit_from_financial_activities_last_quarter / t.profit_from_financial_activities_same_period) - 1) end ln_hdtc
                            , case when t.netsale_last_quarter < t.netsale_same_period then 0
                               else abs((t.netsale_last_quarter / t.netsale_same_period) - 1) end dtt
                            , case when t.gross_profit_last_quarter < t.gross_profit_same_period then 0
                               else abs((t.gross_profit_last_quarter / t.gross_profit_same_period) - 1) end lng
                        from (
                            select * 
                            from (
                                select t.ticker
                                    , t.yearreport
                                    , t.lengthreport
                                    , t.comtypecode
                                    , t.isb38 total_operating_income_last_quarter
                                    , lag(t.isb38, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) total_operating_income_same_period
                                    , t.isb27 net_interest_income_last_quarter
                                    , lag(t.isb27, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) net_interest_income_same_period
                                    , t.isa16 net_profit_before_tax_last_quarter
                                    , lag(t.isa16, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) net_profit_before_tax_same_period
                                    , t.isi64 net_sale_from_insurance_business_last_quarter
                                    , lag(t.isi64, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) net_sale_from_insurance_business_same_period
                                    , t.isi100 profit_from_financial_activities_last_quarter
                                    , lag(t.isi100, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) profit_from_financial_activities_same_period
                                    , t.isa3 netsale_last_quarter
                                    , lag(t.isa3, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) netsale_same_period
                                    , t.isa5 gross_profit_last_quarter
                                    , lag(t.isa5, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) gross_profit_same_period
                                    , t.rn 
                                from (
                                    select t.*
                                        , row_number () over (partition by ticker order by yearreport desc, lengthreport desc) rn
                                    from raw.fiinapi.stx_fsc_incomestatement t
                                    where lengthreport in (1, 2, 3, 4)
                                ) t
                            ) t
                            where t.rn = 1
                        ) t inner join (select * 
                            from (
                                select t.ticker
                                    , t.yearreport
                                    , t.lengthreport
                                    , t.bsa53 total_asset_last_quarter
                                    , lag(t.bsa53, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) total_asset_same_period
                                    , t.bsa80 chartercapital_last_quarter
                                    , lag(t.bsa80, 1) over (partition by t.ticker, t.lengthreport order by t.yearreport) chartercapital_same_period
                                    , t.rn
                                from (
                                    select t.*
                                        , row_number () over (partition by ticker order by yearreport desc, lengthreport desc) rn
                                    from raw.fiinapi.stx_fsc_balancesheet t
                                    where lengthreport in (1, 2, 3, 4)
                                ) t
                            ) t
                            where t.rn = 1) t1 on t1.ticker = t.ticker
                    ) t
                ) t
            ) t
            inner join (
                select t.ticker
                    , t.v_SVALUE value
                    , case when v_SVALUE > 3.5 then 'A'
                        when v_SVALUE > 2.5 and v_SVALUE <= 3.5 then 'B'
                        when v_SVALUE > 1.5 and v_SVALUE <= 2.5 then 'C'
                        when v_SVALUE <= 1.5 then 'D' end value_ranking
                from ( select t.*
                       , (v_spe + v_spb) / 2 v_SVALUE
                      from (
                        select t.*
                            , case when v_stdevpb_sub2 >= v_pb then 4
                                when v_stdevpb_sub2 < v_pb and v_pb <= v_stdevpb_sub1 then 3
                                when v_stdevpb_sub1 < v_pb and v_pb <= v_pb400 then 2
                                when v_pb400 < v_pb and v_pb <= v_stdevpb_add1 then 1
                                when v_stdevpb_add1 < v_pb then 0
                                else 0 end v_spb
                            , case when v_stdevpe_sub2 >= v_pe then 4
                                when v_stdevpe_sub2 < v_pe and v_pe <= v_stdevpe_sub1 then 3
                                when v_stdevpe_sub1 < v_pe and v_pe <= v_pe400 then 2
                                when v_pe400 < v_pe and v_pe <= v_stdevpe_add1 then 1
                                when v_stdevpe_add1 < v_pe then 0
                                else 0 end v_spe
                        from (
                            select t.v_pb400 + t.v_stdevpb v_stdevpb_add1
                                , t.v_pb400 - t.v_stdevpb v_stdevpb_sub1
                                , t.v_pb400 - t.v_stdevpb * 2 v_stdevpb_sub2
                                , t.v_pe400 + t.v_stdevpe v_stdevpe_add1
                                , t.v_pe400 - t.v_stdevpe v_stdevpe_sub1
                                , t.v_pe400 - t.v_stdevpe * 2 v_stdevpe_sub2
                                , t1.v_pb
                                , t1.v_pe
                                , t.*
                            from (
                                select t.ticker, avg(t.rtd25) v_pb400, stddev(rtd25) v_stdevpb, AVG(rtd21) v_pe400, stddev(rtd21) v_stdevpe
                                from (
                                    select ticker, rtd25, rtd21, row_number() over (partition by ticker order by tradingdate desc) rn
                                    from raw.fiinapi.stx_rto_ratiottmdaily
                                ) t
                                where t.rn <= 400
                                group by ticker
                            ) t inner join (select ticker, v_pb, v_pe
                                            from (
                                                 select ticker, d.rtd25 v_pb, rtd21 v_pe, row_number() over (partition by ticker order by tradingdate desc) rn
                                                 from raw.fiinapi.stx_rto_ratiottmdaily d
                                            ) t where t.rn = 1
                            ) t1 on t.ticker = t1.ticker
                        ) t
                      ) t
                ) t
                where v_SVALUE is not null
            ) t1 on t.ticker = t1.ticker
            inner join (
                select t.ticker
                    , t.comgroupcode
                    , t.momentum
                    , case when momentum >= 9 and momentum <= 10 then 'A'
                        when momentum >= 8 and momentum < 9 then 'B'
                        when momentum >= 7 and momentum < 8 then 'C'
                        when momentum < 7 then 'D'
                        else 'N/A' end momentum_ranking
                from (
                    select t.*
                        , cash_flow_momentum_rank * 0.8 + v_diemmua_tmp1 + v_diemmua_tmp2 momentum
                    from (
                        select t.*
                            , case when r.rsi > 85 then 0
                                else r.rsi * 0.01177 end v_diemmua_tmp1
                            , case when v_cmf > 0.1 then 1
                                else 0 end v_diemmua_tmp2
                        from (
                            select t.ticker, t.comgroupcode, t.barindex, t1.v_sum_volume
                                , case when t1.v_sum_volume > 0 then t2.v_cmf_tmp / t1.v_sum_volume
                                    else 0 end v_cmf
                            from (
                                select t.ticker, t.comgroupcode, count(1) barindex
                                from closepricedaily t
                                where t.rn <= 400
                                group by t.ticker, t.comgroupcode
                            ) t
                            inner join (
                                select t.ticker, t.comgroupcode, sum(totalmatchvolume) v_sum_volume
                                from closepricedaily t
                                where t.rn <= 50
                                group by t.ticker, t.comgroupcode
                            ) t1 on t.ticker = t1.ticker
                            inner join (
                                select t.ticker, t.comgroupcode, sum(v_cmf) v_cmf_tmp
                                from (
                                    select t.ticker, t.comgroupcode
                                        , case when highestprice > lowestprice and totalmatchvolume > 0 then (((closeprice - lowestprice) - (highestprice - closeprice)) / (highestprice - lowestprice)) * totalmatchvolume
                                            else 0 end v_cmf
                                    from closepricedaily t
                                    where t.rn <= 50) t
                                group by t.ticker, t.comgroupcode
                            ) t2 on t.ticker = t2.ticker
                        ) t
                        left join golden.fiinapi.stock_index_rsi r on r.ticker = t.ticker
                    ) t
                    inner join
                     (select *
                        from (
                        select *, row_number() over (partition by ticker order by tradingdate desc) rn
                        from interface.serving.stock_overview
                        where tradingdate <= '{business_date}') t
                        where rn = 1)
                        o on o.ticker = t.ticker
                ) t
            ) t2 on t2.ticker = t.ticker
            , (select count(1) v_total_stock from closepricedaily where rn = 1) t1
        where t.total is not null
        ) t
    """)
    spark.sql(f"""delete from interface.serving.stock_rating where tradingdate = '{business_date}'""")
    df.repartition(1).write.mode("append").saveAsTable(f"interface.serving.stock_rating")
    logger.info("completed")
