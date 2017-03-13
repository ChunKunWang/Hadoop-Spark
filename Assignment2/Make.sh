#!/bin/bash

echo -e "Compile SenRecCountMapper"
./mapred_javac.sh SenRecCountMapper.java &&\

echo -e "Compile SenRecCountReducer"
./mapred_javac.sh SenRecCountReducer.java &&\

echo -e "Compile SenRecCount"
./mapred_javac.sh SenRecCount.java &&\

echo -e "Compile SenRecCount"
./mapred_jar.sh SenRecCount.jar

