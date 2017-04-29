rm /shared/amos/JavaRecvCount.jar
cp JavaRecvCount.jar /shared/amos

/var/spark/bin/spark-submit --master spark://tsail:7077 --class JavaRecvCount --executor-memory 2G --total-executor-cores 94 file:///shared/amos/JavaRecvCount.jar hdfs://tsail:8020/user/amos/input/ADU-only_2009-04-01_anon

