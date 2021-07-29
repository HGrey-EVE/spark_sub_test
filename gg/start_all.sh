#!/bin/sh

cur_date=`date +"%Y-%m-%d %H:%M:%S"`
/bin/sh /data2/zszqyuqing/gg/sqoop_start.sh > /data2/zszqyuqing/gg//logs/running_sqoop_start.log 2>&1
wait
/bin/sh /data2/zszqyuqing/gg//submit_job_gg_result_data.sh
wait
sleep 5s
if [[ $? = 0 ]]; then
   sleep 5s
   echo "${cur_date}_submit_job_gg_result_data ok." > /data2/zszqyuqing/gg//logs/running_email.log 2>&1
   /opt/anaconda3/bin/python3 /data2/zszqyuqing/gg//email_to_all.py >> /data2/zszqyuqing/gg//logs/running_email.log 2>&1
else
   sleep 5s
   echo "${cur_date}_submit_job_result_data failed." >> /data2/zszqyuqing/gg//logs/running_email.log 2>&1
   /opt/anaconda3/bin/python3 /data2/zszqyuqing/gg//email_to_all.py >> /data2/zszqyuqing/gg//logs/running_email.log 2>&1
fi
