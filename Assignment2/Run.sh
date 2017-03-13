#!/bin/bash
HD_FILE="ADU-only_2009-04-01_anon"
#HD_FILE="ADU-only_20080418_anon_100K"
HD_PATH="/user/amos/input/"
HD_OUTPUT="/user/amos/output"
HD_RESULT="./result/results_hd"

hadoop fs -rm -R ${HD_OUTPUT}
rm ${HD_RESULT}

hadoop jar SenRecCount.jar SenRecCount ${HD_PATH}${HD_FILE} /user/amos/output 10 &&\

hadoop fs -getmerge ${HD_OUTPUT} ${HD_RESULT} 

