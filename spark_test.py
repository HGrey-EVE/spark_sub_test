from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as fun
from pyspark.sql.functions import collect_set
from pyspark import SparkContext
import re

def getSparkSession():
    conf = SparkConf()
    # conf.setMaster('yarn-client')
    # conf.setMaster('local[*]')
    conf.setAppName("My app")
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate
    # spark = SparkContext(conf=conf)
    return spark


if __name__ == '__main__':
    spark = getSparkSession()
    # rdd = spark.textFile('/data/wordcount.txt')
    # # rdd = spark.textFile('wordcount.txt')
    # wc = rdd.flatMap(lambda x: re.split('[，。：]', x)).repartition(5)
    # wc.saveAsTextFile('/data/result.txt')

    # 用SparkSQL计算
    spdf1 = spark.read.csv('/data/SQL_retro.csv', header=True)
    spdf1.createOrReplaceTempView('company_avg_score')
    spdf2 = spark.read.csv('/data/SQL_retro2.csv', header=True)
    spdf2.createOrReplaceTempView('company_grp')
    res = spark.sql('''
          WITH cr AS 
                (
             SELECT *, 
                CASE WHEN LOCATE( '存续', qiye_status )> 0 THEN 2 ELSE 1 END flag 
                FROM company_avg_score 
                )

             SELECT qiye_id,qiye_status,pub_date,score,
                row_number() over ( PARTITION BY qiye_id ORDER BY flag DESC, pub_date DESC ) rk 
                FROM cr 


    ''')
    res.repartition(1).write.format("csv").option('header', 'true').save(f"/data/SQL_retro")

