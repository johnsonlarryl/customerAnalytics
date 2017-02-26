mvn clean install -Dmaven.test.skip=true
cd target 
hadoop fs -rm -r output
hadoop jar es-hadoop-hack-1.0-SNAPSHOT.jar com.statefarm.hackday.AOADriver -libjars ${HACKLIBS} data  output
cd ..
