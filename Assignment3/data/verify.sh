#!/bin/bash

HD_FILE="ADU-only_2009-04-01_anon"
HD_PATH="/user/amos/input/"

if [ -f "log_temp" ] 
then
    rm log_temp
fi

if [ -f "log_verify" ] 
then
    rm log_verify 
fi

if [ -f "log_hd_sorted" ] 
then
    rm log_hd_sorted
fi

#echo -e "Sorting hadoop results..."
#sort results_hd > log_hd_sorted &&\

echo -e "Extracting source file..."
hadoop fs -cat ${HD_PATH}${HD_FILE} | grep ">" | awk -F" " '{print $5}' | awk -F"." '{print $1"."$2"."$3"."$4}' >> log_temp &&\

hadoop fs -cat ${HD_PATH}${HD_FILE} | grep "<" | awk -F" " '{print $3}' | awk -F"." '{print $1"."$2"."$3"."$4}' >> log_temp &&\

echo -e "Sorting my own verified file..."
sort log_temp | uniq -c | awk '{print $2"\t"$1}' > log_verify
sort -k2 -n -r log_verify > log_verify_sorted

