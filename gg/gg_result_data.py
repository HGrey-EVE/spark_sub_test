# -*- coding: utf-8 -*-

'''
项目：招商证券投研分析平台项目-智能舆情监控
'''

import os
import re
from typing import Text
from datetime import date

# 由于MD5模块在python3中被移除，在python3中使用hashlib模块进行md5操作
from hashlib import md5
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pyspark import SparkConf,Row
from pyspark.sql import SparkSession, DataFrame


DWH_NAME = "dw"
SPARK_APP_NAME = "zszqyuqing_result_data"
#生产环境
OUTPUT_HDFS_PATH = "hdfs:///user/zszqyuqing/zszq/gg"
#测试环境
# OUTPUT_HDFS_PATH = "hdfs:///user/zszqyuqing/zszq/v2/gg"
# 测试环境外管局名单存放路径
# ORIGINAL_LIS_PATH = "hdfs:///tmp/thk/data/zentao16226/"
DISPLAY_SQL = True

LIHAO = "企业合作|重大交易|并购重组|企业盈利|产品发布/升级|股权融资|债权融资|债务重组|企业荣誉|人才引进"
LIKONG = "停业破产|安全事故|工程质量|信息泄露|企业亏损|大量裁员|组织架构变动|制假售假|虚假宣传|环境污染|行业处罚|高管负面|财务欺诈|履行连带担保责任|歇业停业|重组失败|业绩下滑|资产负面|资金困难|账户风险|实际控制人变更|评级调整|涉嫌传销诈骗|涉嫌非法集资|信息披露违规|实际控制人涉及诉讼|证券交易违规|产品反馈|企业退市|股权变动|债务抵押|品牌声誉|实际控制人涉诉仲裁"
ZHONGXING = "产品销售|股权投资|债权投资|对外股票减持|对外股票增持"

#创建SparkSession
def get_spark_session():
    conf = SparkConf()
    conf.setMaster("yarn")
    conf.set("spark.broadcast.blockSize", 1024)
    conf.set("spark.default.parallelism", 100)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.executor.instances", 8)
    conf.set("spark.executor.memory", "5g")
    conf.set("spark.python.worker.memory", "5g")
    conf.set("spark.shuffle.file.buffer", "512k")
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    conf.set("spark.sql.shuffle.partitions", 200)
    conf.set("spark.shuffle.memoryFraction", 0.3)
    conf.set("spark.yarn.am.cores", 3)
    # 关闭资源动态分配，酌情添加
    conf.set("spark.dynamicAllocation.enabled", False)
    conf.set("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
    spark = (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark

#执行sql得到DataFrame
def ss_sql(ss: SparkSession, sql: Text) -> DataFrame:
    if DISPLAY_SQL:
        print(sql)
    return ss.sql(sql)

def main(ss: SparkSession):

    # 公告事件结果
    gg_event_df = ss_sql(ss,f"""
        select
            concat('GG',id) as id,
            stock_name as event_subject_short,
            event_subject,
            subject_id,
            CASE
                WHEN (news_type==5) THEN 'A股上市公司'
                WHEN (news_type==6) THEN '港股上市公司'
                ELSE ''
            END AS subject_type,
            '' as subject_ass_public_company,
            '' as with_event_subject_relation,
            '' as event_object_short,
            '' as event_object,
            '' as object_id,
            '' as object_type,
            '' as object_ass_public_company,
            '' as with_event_object_relation,
            event_type,
            '' as event_property,
            '' as event_repeat_count,
            '公告' as source_type,
            collect_set(bbd_url) over(
                    partition by event_type,subject_id
                ) as related_news_url,
            collect_list(bbd_unique_id) over(
                    partition by event_type,subject_id
                ) as news_source,
            '' as event_main
        from pj.gg_event
        WHERE dt={newest_dt('gg_event','pj')}
    """)

    all_data_result_df = gg_event_df.dropDuplicates(["event_type","subject_id","object_id"])
    all_data_result_df.createOrReplaceTempView("zszq_event_table")

    # 得到上市公司信息
    ica_df = ss_sql(ss, f"""
            select
                company_name,
                'a' as type
            from pj.ms_ipo_company_a ic
            where dt={newest_dt('ms_ipo_company_a','pj')}
            """)
    ich_df = ss_sql(ss, f"""
            select
                company_name,
                'h' as type
            from pj.ms_ipo_company_h ic
            where dt={newest_dt('ms_ipo_company_h','pj')}
            """)
    ica_df.union(ich_df).createOrReplaceTempView("ic_table_old")
    ss_sql(ss, f"""
            select
              company_name,
              collect_list(type) over(partition by company_name) type
            from ic_table_old
            """).dropDuplicates().createOrReplaceTempView("ic_table")

    subject_df = ss_sql(ss, f"""
            select
              zet.id,
              zet.event_subject_short,
              zet.event_subject,
              zet.subject_id,
              zet.subject_type,
              zet.subject_ass_public_company,
              zet.with_event_subject_relation,
              zet.event_object_short,
              zet.event_object,
              zet.object_id,
              zet.object_type,
              zet.object_ass_public_company,
              zet.with_event_object_relation,
              zet.event_type,
              zet.event_property,
              zet.event_repeat_count,
              zet.source_type,
              zet.related_news_url,
              zet.news_source,
              zet.event_main,
              it.company_name,
              it.type
            from zszq_event_table zet
            left join ic_table it
            on zet.event_subject=it.company_name
            """)
    subject_df = subject_df.withColumn("subject_type", get_is_public_company("company_name", "type", "subject_type")).select("id","event_subject_short","event_subject","subject_id","subject_type","subject_ass_public_company","with_event_subject_relation","event_object_short","event_object","object_id","object_type","object_ass_public_company","with_event_object_relation","event_type","event_property","event_repeat_count","source_type","related_news_url","news_source","event_main")
    subject_df.createOrReplaceTempView("all_data_table")

    # 将今天时间格式化成指定形式
    today = date.today().strftime("%Y%m%d")
    # 列出要存入最终结果表的所有字段
    all_data_fields = ["id","event_subject_short","event_subject","subject_id","subject_type","subject_ass_public_company","with_event_subject_relation","event_object_short","event_object","object_id","object_type","object_ass_public_company","with_event_object_relation","event_type","event_property","event_repeat_count","source_type","related_news_url","news_source","event_main"]

    # 将结果存入pj.ms_ipo_public_sentiment_events
    ss_sql(ss,f"""
        insert overwrite table
            pj.gg_event_result partition(dt={today})
        select
            {', '.join(all_data_fields)}
        from all_data_table
    """)

    # 将array<string>转换为string，否则存不进csv
    all_data_result_df = all_data_result_df\
        .withColumn("related_news_url",all_data_result_df.related_news_url.cast(StringType()))\
        .withColumn("news_source", all_data_result_df.news_source.cast(StringType()))

    all_data_result_df.createOrReplaceTempView("result_df_temp")
    # 列出要存入csv的所有字段
    result_all_data_fields = [
        "id as `舆情事件ID`",
        "event_subject_short as `公司1简称`",
        "event_subject as `公司1`",
        "subject_id as `公司1 ID`",
        "subject_type as `公司1 类型`",
        "subject_ass_public_company as `公司1 关联上市公司`",
        "with_event_subject_relation as `与公司1的关系`",
        "event_object_short as `公司2简称`",
        "event_object as `公司2`",
        "object_id as `公司2 ID`",
        "object_type as `公司2类型`",
        "object_ass_public_company as `公司2 关联上市公司`",
        "with_event_object_relation as `与公司2的关系`",
        "event_type as `事件类型`",
        "event_property as `事件属性`",
        "event_repeat_count as `事件热度`",
        "source_type as `来源类型`",
        "related_news_url as `事件网址`",
        "news_source as `新闻来源`",
        "event_main as `事件原文`"]

    result_df_end = ss_sql(ss, f"select {', '.join(result_all_data_fields)} from result_df_temp")
    # 将结果存入csv
    result_df_end.repartition(1).write.format("csv").option('header','true').save(f"{OUTPUT_HDFS_PATH}/{today}")



if __name__ == "__main__":
    ss = get_spark_session()
    main(ss)

    ss.stop()
