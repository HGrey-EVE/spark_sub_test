CWD='/data2/zszqyuqing/gg'

export PATH="/opt/anaconda3/bin:$PATH" \
&& export PYSPARK_PYTHON="/opt/anaconda3/bin/python3" \
&& /opt/spark-2.4.2/bin/spark-submit \
    --queue vip.zszqyuqing \
    --master yarn-client \
    --conf spark.hadoop.dfs.replication=2 \
    $CWD/gg_result_data.py > $CWD/logs/running_gg_result_data.log 2>&1