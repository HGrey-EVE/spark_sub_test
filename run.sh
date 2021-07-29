export HADOOP_CONF_DIR=""
spark-submit --conf spark.pyspark.driver.python=/User/python3/bin/python3 \
  --conf spark.pyspark.python=/User/python3/bin/python3 \
  --name "My app" \
  --num-executors 1 \
  --master yarn\
  --executor-memory 512m \
  --driver-memory 512m \
  /root/projects/spark_sub_test/spark_sub_test/spark_test.py "options"
