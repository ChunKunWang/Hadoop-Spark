rm /shared/amos/ADUQuery.jar
cp ADUQuery.jar /shared/amos

/var/spark/bin/spark-submit --master spark://tsail:7077 --class ADUQuery --executor-memory 4G --total-executor-cores 94 file:///shared/amos/ADUQuery.jar hdfs://tsail:8020/user/amos/input/ADU-only_2009-04-01_anon

