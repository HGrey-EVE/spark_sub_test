from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as fun
from pyspark.sql.functions import collect_set
from pyspark import SparkContext
import re

def getSparkSession():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    # conf.setMaster('local[*]')
    conf.setAppName("My app")
    # spark = SparkSession.builder.appName("My APP").config(conf=conf).enableHiveSupport().getOrCreate
    spark = SparkContext(conf=conf)
    return spark


if __name__ == '__main__':
    spark = getSparkSession()
    rdd = spark.textFile('hdfs:///data1/wordcount.txt')
    # rdd = spark.textFile('wordcount.txt')
    wc = rdd.flatMap(lambda x: re.split('[，。：]', x)).repartition(5)
    wc.saveAsTextFile('hdfs:///data1/result.txt')
    spark.stop()


