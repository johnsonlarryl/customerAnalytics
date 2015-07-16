# cd .. 
# mvn clean install 
# cd target 
hadoop fs -rm -r output
hadoop jar es-hadoop-hack-1.0-SNAPSHOT.jar com.statefarm.hackday.AOADriver -libjars ${LOCAL_LIB} data  output
