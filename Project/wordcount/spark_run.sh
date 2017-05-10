rm /shared/amos/WordCount.jar
cp WordCount.jar /shared/amos

/var/spark/bin/spark-submit --master spark://tsail:7077 --class WordCount --executor-memory 2G --total-executor-cores 94 file:///shared/amos/WordCount.jar hdfs://tsail:8020/user/amos/test/wikipages CountResult

