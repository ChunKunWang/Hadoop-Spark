#!/bin/bash
HD_FILE="ADU-only_2009-04-01_anon"
#HD_FILE="ADU-only_20080418_anon_100K"
HD_PATH="/user/amos/input/"
HD_OUTPUT="/user/amos/output"

echo -e "# of reduce tasks: 4"
for i in 0 1 2
do
    hadoop fs -rm -R ${HD_OUTPUT}
    hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 4 >> ./result/log_rt_4 2>&1 
done

echo -e "# of reduce tasks: 8"
for i in 0 1 2
do
    hadoop fs -rm -R ${HD_OUTPUT}
    hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 8 >> ./result/log_rt_8 2>&1 
done

echo -e "# of reduce tasks: 10"
for i in 0 1 2
do
    hadoop fs -rm -R ${HD_OUTPUT}
    hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 10 >> ./result/log_rt_10 2>&1 
done

echo -e "# of reduce tasks: 16"
for i in 0 1 2
do
    hadoop fs -rm -R ${HD_OUTPUT}
    hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 16 >> ./result/log_rt_16 2>&1 
done

echo -e "# of reduce tasks: 20"
for i in 0 1 2
do
    hadoop fs -rm -R ${HD_OUTPUT}
    hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 20 >> ./result/log_rt_20 2>&1 
done

echo -e "# of reduce tasks: 32"
for i in 0 1 2
do
    hadoop fs -rm -R ${HD_OUTPUT}
    hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 32 >> ./result/log_rt_32 2>&1 
done

echo -e "# of reduce tasks: 40"
for i in 0 1 2
do
    hadoop fs -rm -R ${HD_OUTPUT}
    hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 40 >> ./result/log_rt_40 2>&1 
done

echo -e "Mission Complete!"

