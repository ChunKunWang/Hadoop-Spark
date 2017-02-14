#!/bin/bash

echo -e "Compile SenderCountMapper"
./mapred_javac.sh SenderCountMapper.java &&\

echo -e "Compile SenderCountReducer"
./mapred_javac.sh SenderCountReducer.java &&\

echo -e "Compile SenderCount"
./mapred_javac.sh SenderCount.java &&\

echo -e "Compile SenderCount"
./mapred_jar.sh SenderCount.jar

