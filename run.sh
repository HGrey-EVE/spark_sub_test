spark-submit --conf spark.pyspark.driver.python=/User/python3/bin/python3 --conf spark.pyspark.python=/User/python3/bin/python3 --master yarn --deploy-mode client --name "APP_name" --num-executors 3 --executor-memory 512m /root/projects/spark_sub_test/spark_sub_test/spark_test.py "options"