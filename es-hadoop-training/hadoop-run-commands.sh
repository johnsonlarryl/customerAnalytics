#!/usr/bin/sh;

cd ..; mvn clean install; cd target; hadoop fs -rm -r output; hadoop jar es-hadoop-training-1.0-SNAPSHOT.jar com.statefarm.es_hadoop.StubDriver -libjars /home/training/LOCAL_LIB/elasticsearch-1.6.0.jar,/home/training/LOCAL_LIB/elasticsearch-hadoop-2.1.0.rc1.jar data  output
