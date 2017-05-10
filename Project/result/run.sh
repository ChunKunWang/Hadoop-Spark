/var/spark/bin/spark-submit --master spark://tsail:7077 --class RedditComment --executor-memory 2G --total-executor-cores 94 file:///shared/amos/RedditComment.jar hdfs://tsail:8020/user/amos/Corpus/$1 $2

